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
		electionTimeout: time.Duration(300+rand.Intn(150)) * time.Millisecond,
		voteCh:          make(chan VoteRequest, 100),
		appendCh:        make(chan AppendEntriesRequest, 100),
		stopCh:          make(chan struct{}),
		done:            make(chan struct{}),
	}

	// Initialize timers
	r.heartbeatTicker = time.NewTicker(150 * time.Millisecond)
	r.electionTimer = time.NewTimer(r.electionTimeout)

	// Start Raft loop
	go r.run()

	return r
}

func (r *RaftNode) run() {
	defer func() {
		r.heartbeatTicker.Stop()
		r.electionTimer.Stop()
		select {
		case <-r.done:
			// Channel already closed
		default:
			close(r.done)
		}
	}()

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
			if r.getState() == Leader {
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
	oldState := r.state
	r.state = Candidate
	r.votedFor = r.id
	currentTerm := r.term
	r.mu.Unlock()

	if r.onStateChange != nil {
		r.onStateChange(oldState, Candidate)
	}

	log.Printf("[Raft] Node %s starting election for term %d", truncateID(r.id), currentTerm)

	// Reset election timer with random timeout
	r.electionTimeout = time.Duration(300+rand.Intn(150)) * time.Millisecond
	r.resetElectionTimer()

	// Request votes from all peers
	votes := 1 // Vote for self
	voteCh := make(chan bool, len(r.peers))

	for _, peer := range r.peers {
		go func(peerId string) {
			req := VoteRequest{
				Term:         currentTerm,
				CandidateId:  r.id,
				ResponseChan: make(chan VoteResponse, 1),
			}

			var resp VoteResponse
			if r.sendVoteRequest != nil {
				// Use direct method call for testing
				resp = r.sendVoteRequest(peerId, req)
			} else {
				// Use channel-based communication
				select {
				case r.voteCh <- req:
					resp = <-req.ResponseChan
				case <-time.After(time.Second):
					voteCh <- false
					return
				}
			}

			if resp.Term > currentTerm {
				r.mu.Lock()
				if resp.Term > r.term {
					r.term = resp.Term
					r.state = Follower
					r.votedFor = ""
					r.leaderId = ""
				}
				r.mu.Unlock()
				voteCh <- false
			} else {
				voteCh <- resp.VoteGranted
			}
		}(peer)
	}

	// Wait for votes
	timeout := time.After(r.electionTimeout)
	for i := 0; i < len(r.peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				if votes > len(r.peers)/2 {
					r.becomeLeader()
					return
				}
			}
		case <-timeout:
			return
		case <-r.stopCh:
			return
		}
	}

	// If we get here, we didn't win the election
	r.mu.Lock()
	if r.state == Candidate {
		r.state = Follower
		r.votedFor = ""
	}
	r.mu.Unlock()
}

func (r *RaftNode) becomeLeader() {
	r.mu.Lock()
	if r.state != Candidate {
		r.mu.Unlock()
		return
	}

	oldState := r.state
	r.state = Leader
	r.leaderId = r.id
	r.mu.Unlock()

	if r.onStateChange != nil {
		r.onStateChange(oldState, Leader)
	}

	if r.onLeaderElected != nil {
		r.onLeaderElected(r.id)
	}

	log.Printf("[Raft] Node %s became leader for term %d", truncateID(r.id), r.term)
	r.sendHeartbeats()
}

func (r *RaftNode) handleVoteRequest(req VoteRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := VoteResponse{Term: r.term, VoteGranted: false}

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

	// Grant vote if we haven't voted for anyone else in this term
	// or if we've already voted for this candidate
	if (r.votedFor == "" || r.votedFor == req.CandidateId) && r.state != Leader {
		r.votedFor = req.CandidateId
		resp.VoteGranted = true
		r.resetElectionTimer()
	}

	req.ResponseChan <- resp
}

func (r *RaftNode) handleAppendEntries(req AppendEntriesRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := AppendEntriesResponse{Term: r.term, Success: false}

	if req.Term < r.term {
		req.ResponseChan <- resp
		return
	}

	// Valid heartbeat from leader
	if req.Term >= r.term {
		r.leaderId = req.LeaderId
		r.term = req.Term
		if r.state != Follower {
			r.state = Follower
			r.votedFor = ""
		}
		r.resetElectionTimer()
		resp.Success = true
	}

	req.ResponseChan <- resp
}

func (r *RaftNode) sendHeartbeats() {
	r.mu.RLock()
	currentTerm := r.term
	r.mu.RUnlock()

	for _, peer := range r.peers {
		go func(peerId string) {
			req := AppendEntriesRequest{
				Term:         currentTerm,
				LeaderId:     r.id,
				ResponseChan: make(chan AppendEntriesResponse, 1),
			}

			var resp AppendEntriesResponse
			if r.sendAppendEntries != nil {
				// Use direct method call for testing
				resp = r.sendAppendEntries(peerId, req)
			} else {
				// Use channel-based communication
				select {
				case r.appendCh <- req:
					resp = <-req.ResponseChan
				case <-time.After(time.Second):
					return
				}
			}

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
		}(peer)
	}
}

func (r *RaftNode) resetElectionTimer() {
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
	r.mu.Lock()
	select {
	case <-r.stopCh:
		// Channel already closed
		r.mu.Unlock()
		return
	default:
		close(r.stopCh)
	}
	r.mu.Unlock()
	<-r.done
}

// UpdatePeers updates the list of peers in the Raft cluster
func (r *RaftNode) UpdatePeers(peers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = peers
}
