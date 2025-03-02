package consensus

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

type RaftNode struct {
	mu sync.RWMutex

	// Node identity
	id       string
	state    NodeState
	term     uint64
	votedFor string
	leaderId string

	// Timing
	electionTimeout time.Duration
	heartbeatTicker *time.Ticker
	electionTimer   *time.Timer
	lastHeartbeat   time.Time

	// Channels
	voteCh   chan VoteRequest
	appendCh chan AppendEntriesRequest
	stopCh   chan struct{}
	done     chan struct{}

	// Callbacks
	onLeaderElected func(leaderId string)
	onStateChange   func(oldState, newState NodeState)

	// Cluster info
	peers []string

	// Test hooks
	sendVoteRequest   func(peer string, req VoteRequest) VoteResponse
	sendAppendEntries func(peer string, req AppendEntriesRequest) AppendEntriesResponse
}

type VoteRequest struct {
	Term         uint64
	CandidateId  string
	ResponseChan chan VoteResponse
}

type VoteResponse struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderId     string
	ResponseChan chan AppendEntriesResponse
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
}

func NewRaftNode(id string, peers []string) *RaftNode {
	r := &RaftNode{
		id:              id,
		state:           Follower,
		peers:           peers,
		term:            0,
		votedFor:        "",
		leaderId:        "",
		electionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
		voteCh:          make(chan VoteRequest, 100),
		appendCh:        make(chan AppendEntriesRequest, 100),
		stopCh:          make(chan struct{}),
		done:            make(chan struct{}),
	}

	// Initialize timers with faster heartbeat
	r.heartbeatTicker = time.NewTicker(75 * time.Millisecond)
	r.electionTimer = time.NewTimer(r.electionTimeout)

	// Start Raft loop
	go r.run()

	return r
}

func (r *RaftNode) run() {
	defer close(r.done)
	defer r.heartbeatTicker.Stop()
	defer r.electionTimer.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-r.electionTimer.C:
			r.startElection()
		case voteReq := <-r.voteCh:
			r.handleVoteRequest(voteReq)
		case appendReq := <-r.appendCh:
			r.handleAppendEntries(appendReq)
		case <-r.heartbeatTicker.C:
			if r.state == Leader {
				r.sendHeartbeats()
			}
		}
	}
}

// truncateID safely truncates a node ID for logging
func truncateID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func (r *RaftNode) startElection() {
	r.mu.Lock()
	if r.state == Leader {
		r.mu.Unlock()
		return
	}

	// Increment term and become candidate
	r.term++
	r.state = Candidate
	r.votedFor = r.id
	currentTerm := r.term
	r.mu.Unlock()

	log.Printf("[Node %s] Starting election for term %d with %d peers", truncateID(r.id), currentTerm, len(r.peers))

	// Vote for self
	votes := 1
	voteCh := make(chan bool, len(r.peers))

	// Request votes from all peers
	for _, peer := range r.peers {
		go func(peer string) {
			req := VoteRequest{
				Term:         currentTerm,
				CandidateId:  r.id,
				ResponseChan: make(chan VoteResponse, 1),
			}

			if r.sendVoteRequest != nil {
				resp := r.sendVoteRequest(peer, req)
				voteCh <- resp.VoteGranted
			} else {
				// In production, implement actual RPC call here
				voteCh <- false
			}
		}(peer)
	}

	// Wait for votes with timeout
	timer := time.NewTimer(r.electionTimeout)
	defer timer.Stop()

	for i := 0; i < len(r.peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				// Check if we have a majority
				if votes > (len(r.peers)+1)/2 {
					r.mu.Lock()
					if r.state == Candidate && r.term == currentTerm {
						r.becomeLeader()
					}
					r.mu.Unlock()
					return
				}
			}
		case <-timer.C:
			// Election timeout, start new election
			r.mu.Lock()
			if r.state == Candidate {
				r.state = Follower
				r.votedFor = ""
			}
			r.mu.Unlock()
			return
		case <-r.stopCh:
			return
		}
	}

	// If we get here without winning the election, revert to follower
	r.mu.Lock()
	if r.state == Candidate {
		r.state = Follower
		r.votedFor = ""
	}
	r.mu.Unlock()
}

func (r *RaftNode) becomeLeader() {
	r.state = Leader
	r.leaderId = r.id
	log.Printf("[Node %s] Became leader for term %d", truncateID(r.id), r.term)

	// Reset election timer
	r.resetElectionTimer()

	// Notify callback if registered
	if r.onLeaderElected != nil {
		r.onLeaderElected(r.id)
	}

	// Start sending heartbeats immediately
	r.sendHeartbeats()
}

func (r *RaftNode) handleVoteRequest(req VoteRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := VoteResponse{
		Term:        r.term,
		VoteGranted: false,
	}

	if req.Term < r.term {
		req.ResponseChan <- resp
		return
	}

	if req.Term > r.term {
		r.term = req.Term
		r.state = Follower
		r.votedFor = ""
		r.leaderId = ""
	}

	if r.votedFor == "" || r.votedFor == req.CandidateId {
		r.votedFor = req.CandidateId
		resp.VoteGranted = true
		r.resetElectionTimer()
	}

	req.ResponseChan <- resp
}

func (r *RaftNode) handleAppendEntries(req AppendEntriesRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := AppendEntriesResponse{
		Term:    r.term,
		Success: false,
	}

	if req.Term < r.term {
		req.ResponseChan <- resp
		return
	}

	// Valid AppendEntries, reset election timer
	r.resetElectionTimer()

	if req.Term > r.term {
		r.term = req.Term
		r.state = Follower
		r.votedFor = ""
	}

	r.leaderId = req.LeaderId
	resp.Success = true
	req.ResponseChan <- resp
}

func (r *RaftNode) sendHeartbeats() {
	r.mu.RLock()
	if r.state != Leader {
		r.mu.RUnlock()
		return
	}
	currentTerm := r.term
	r.mu.RUnlock()

	for _, peer := range r.peers {
		go func(peer string) {
			req := AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     r.id,
				ResponseChan: make(chan AppendEntriesResponse, 1),
			}

			if r.sendAppendEntries != nil {
				resp := r.sendAppendEntries(peer, req)
				if resp.Term > currentTerm {
					r.mu.Lock()
					if resp.Term > r.term {
						r.term = resp.Term
						r.state = Follower
						r.votedFor = ""
						r.leaderId = ""
					}
					r.mu.Unlock()
				}
			}
		}(peer)
	}
}

func (r *RaftNode) resetElectionTimer() {
	r.electionTimeout = time.Duration(150+rand.Intn(150)) * time.Millisecond
	if !r.electionTimer.Stop() {
		select {
		case <-r.electionTimer.C:
		default:
		}
	}
	r.electionTimer.Reset(r.electionTimeout)
}

func (r *RaftNode) getState() NodeState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *RaftNode) GetLeader() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderId
}

func (r *RaftNode) IsLeader() bool {
	return r.getState() == Leader
}

func (r *RaftNode) SetStateChangeCallback(cb func(oldState, newState NodeState)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onStateChange = cb
}

func (r *RaftNode) SetLeaderElectedCallback(cb func(leaderId string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onLeaderElected = cb
}

func (r *RaftNode) Stop() {
	close(r.stopCh)
	<-r.done
}

// UpdatePeers updates the list of peers in the cluster
func (r *RaftNode) UpdatePeers(peers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = peers
	log.Printf("[Node %s] Updated peers: %v", truncateID(r.id), peers)
}
