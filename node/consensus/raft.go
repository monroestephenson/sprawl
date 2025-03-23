package consensus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
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

	// Timing - Increased timeouts for stability
	electionTimeout time.Duration
	heartbeatTicker *time.Ticker
	electionTimer   *time.Timer
	lastHeartbeat   time.Time

	// Channels
	voteCh   chan VoteRequest
	appendCh chan AppendEntriesRequest
	stopCh   chan struct{}
	done     chan struct{}

	// Flag to track if node is stopped
	stopped bool

	// Callbacks
	onLeaderElected func(leaderId string)
	onStateChange   func(oldState, newState NodeState)

	// Cluster info and networking
	peers      []string
	nodeInfos  map[string]NodeInfo // Maps node ID to its HTTP address info
	httpClient *http.Client

	// Test hooks
	sendVoteRequest   func(peer string, req VoteRequest) VoteResponse
	sendAppendEntries func(peer string, req AppendEntriesRequest) AppendEntriesResponse
}

// NodeInfo stores info needed to communicate with other nodes
type NodeInfo struct {
	ID       string
	Address  string
	HTTPPort int
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

// RPC message structures for network communication
type RPCVoteRequest struct {
	Term        uint64 `json:"term"`
	CandidateId string `json:"candidate_id"`
}

type RPCVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type RPCAppendEntriesRequest struct {
	Term     uint64 `json:"term"`
	LeaderId string `json:"leader_id"`
}

type RPCAppendEntriesResponse struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

func NewRaftNode(id string, peers []string) *RaftNode {
	r := &RaftNode{
		id:       id,
		state:    Follower,
		peers:    peers,
		term:     0,
		votedFor: "",
		leaderId: "",
		// Increase timeouts for better stability in real networks
		electionTimeout: time.Duration(500+rand.Intn(500)) * time.Millisecond, // 500-1000ms
		voteCh:          make(chan VoteRequest, 100),
		appendCh:        make(chan AppendEntriesRequest, 100),
		stopCh:          make(chan struct{}),
		done:            make(chan struct{}),
		nodeInfos:       make(map[string]NodeInfo),
		httpClient:      &http.Client{Timeout: 2 * time.Second},
		stopped:         false,
	}

	// Initialize timers with appropriate heartbeat (shorter than election timeout)
	r.heartbeatTicker = time.NewTicker(250 * time.Millisecond)
	r.electionTimer = time.NewTimer(r.electionTimeout)

	// Start Raft loop
	go r.run()

	return r
}

func (r *RaftNode) run() {
	defer func() {
		r.mu.Lock()
		// Only close the done channel if we're marked as stopped
		// This prevents multiple goroutines from trying to close it
		if r.stopped {
			select {
			case <-r.done:
				// Already closed
			default:
				close(r.done)
			}
		}
		r.mu.Unlock()
	}()
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
	oldState := r.state
	r.state = Candidate
	r.votedFor = r.id
	currentTerm := r.term
	r.mu.Unlock()

	// Notify state change
	if oldState != Candidate && r.onStateChange != nil {
		r.onStateChange(oldState, Candidate)
	}

	log.Printf("[Node %s] Starting election for term %d with %d peers", truncateID(r.id), currentTerm, len(r.peers))

	// Vote for self
	votes := 1
	voteCh := make(chan bool, len(r.peers))

	// Request votes from all peers
	for _, peer := range r.peers {
		go func(peer string) {
			vote := false

			// Try using test hook if available
			if r.sendVoteRequest != nil {
				req := VoteRequest{
					Term:         currentTerm,
					CandidateId:  r.id,
					ResponseChan: make(chan VoteResponse, 1),
				}
				resp := r.sendVoteRequest(peer, req)
				vote = resp.VoteGranted
				log.Printf("[Node %s] Got vote response from %s: granted=%v for term %d",
					truncateID(r.id), truncateID(peer), vote, currentTerm)
			} else {
				// Use HTTP for real deployments
				vote = r.sendVoteRequestHTTP(peer, currentTerm)
				log.Printf("[Node %s] Got HTTP vote response from %s: granted=%v for term %d",
					truncateID(r.id), truncateID(peer), vote, currentTerm)
			}

			voteCh <- vote
		}(peer)
	}

	// Wait for votes with timeout
	timer := time.NewTimer(r.electionTimeout * 2) // Longer timeout for vote collection
	defer timer.Stop()

	for i := 0; i < len(r.peers); i++ {
		select {
		case granted := <-voteCh:
			if granted {
				votes++
				log.Printf("[Node %s] Vote count now %d out of %d needed for majority in term %d",
					truncateID(r.id), votes, (len(r.peers)+1)/2+1, currentTerm)

				// Check if we have a majority
				if votes > (len(r.peers)+1)/2 {
					r.mu.Lock()
					if r.state == Candidate && r.term == currentTerm {
						log.Printf("[Node %s] Won election with %d votes for term %d",
							truncateID(r.id), votes, currentTerm)
						r.becomeLeader()
					} else {
						log.Printf("[Node %s] Won votes but state changed (state=%d, term=%d vs current=%d)",
							truncateID(r.id), r.state, currentTerm, r.term)
					}
					r.mu.Unlock()
					return
				}
			}
		case <-timer.C:
			// Election timeout, log and revert to follower
			log.Printf("[Node %s] Election timeout for term %d after collecting %d votes",
				truncateID(r.id), currentTerm, votes)
			r.mu.Lock()
			if r.state == Candidate {
				oldState := r.state
				r.state = Follower
				r.votedFor = ""
				// Notify state change
				if r.onStateChange != nil {
					r.onStateChange(oldState, Follower)
				}

				// After a failed election, use a randomized timeout for the next attempt
				// to reduce the chance of repeated election conflicts
				r.electionTimeout = time.Duration(500+rand.Intn(500)) * time.Millisecond
				r.resetElectionTimer()
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
		oldState := r.state
		r.state = Follower
		r.votedFor = ""
		// Notify state change
		if r.onStateChange != nil {
			r.onStateChange(oldState, Follower)
		}

		// Use a randomized timeout again
		r.electionTimeout = time.Duration(500+rand.Intn(500)) * time.Millisecond
		r.resetElectionTimer()
		log.Printf("[Node %s] Reverting to follower after election attempt for term %d",
			truncateID(r.id), currentTerm)
	}
	r.mu.Unlock()
}

// sendVoteRequestHTTP sends vote requests over HTTP
func (r *RaftNode) sendVoteRequestHTTP(peer string, term uint64) bool {
	// Get node info
	nodeInfo, ok := r.getNodeInfo(peer)
	if !ok {
		log.Printf("[Node %s] Failed to find node info for peer %s", truncateID(r.id), truncateID(peer))
		return false
	}

	// Create request
	voteReq := RPCVoteRequest{
		Term:        term,
		CandidateId: r.id,
	}

	data, err := json.Marshal(voteReq)
	if err != nil {
		log.Printf("[Node %s] Failed to marshal vote request: %v", truncateID(r.id), err)
		return false
	}

	// Send request
	url := fmt.Sprintf("http://%s:%d/raft/vote", nodeInfo.Address, nodeInfo.HTTPPort)
	log.Printf("[Node %s] Sending vote request to %s (%s) for term %d",
		truncateID(r.id), truncateID(peer), url, term)

	resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[Node %s] Failed to send vote request to %s: %v", truncateID(r.id), truncateID(peer), err)
		return false
	}
	defer resp.Body.Close()

	// Parse response
	var voteResp RPCVoteResponse
	if err := json.NewDecoder(resp.Body).Decode(&voteResp); err != nil {
		log.Printf("[Node %s] Failed to decode vote response: %v", truncateID(r.id), err)
		return false
	}

	log.Printf("[Node %s] Received vote response from %s: granted=%v, term=%d",
		truncateID(r.id), truncateID(peer), voteResp.VoteGranted, voteResp.Term)

	// Check higher term
	if voteResp.Term > term {
		r.mu.Lock()
		if voteResp.Term > r.term {
			oldState := r.state
			r.term = voteResp.Term
			r.state = Follower
			r.votedFor = ""
			r.leaderId = ""

			log.Printf("[Node %s] Discovered higher term %d from %s, reverting to follower",
				truncateID(r.id), voteResp.Term, truncateID(peer))

			// Notify state change if needed
			if r.onStateChange != nil && oldState != Follower {
				r.onStateChange(oldState, Follower)
			}
		}
		r.mu.Unlock()
	}

	return voteResp.VoteGranted
}

// sendAppendEntriesHTTP sends heartbeats over HTTP
func (r *RaftNode) sendAppendEntriesHTTP(peer string, term uint64, leaderId string) bool {
	// Get node info
	nodeInfo, ok := r.getNodeInfo(peer)
	if !ok {
		log.Printf("[Node %s] Failed to find node info for peer %s", truncateID(r.id), truncateID(peer))
		return false
	}

	// Create request
	appendReq := RPCAppendEntriesRequest{
		Term:     term,
		LeaderId: leaderId,
	}

	data, err := json.Marshal(appendReq)
	if err != nil {
		log.Printf("[Node %s] Failed to marshal append entries request: %v", truncateID(r.id), err)
		return false
	}

	// Send request
	url := fmt.Sprintf("http://%s:%d/raft/append", nodeInfo.Address, nodeInfo.HTTPPort)
	resp, err := r.httpClient.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("[Node %s] Failed to send append entries to %s: %v", truncateID(r.id), truncateID(peer), err)
		return false
	}
	defer resp.Body.Close()

	// Parse response
	var appendResp RPCAppendEntriesResponse
	if err := json.NewDecoder(resp.Body).Decode(&appendResp); err != nil {
		log.Printf("[Node %s] Failed to decode append entries response: %v", truncateID(r.id), err)
		return false
	}

	// Check higher term
	if appendResp.Term > term {
		r.mu.Lock()
		if appendResp.Term > r.term {
			r.term = appendResp.Term
			r.state = Follower
			r.votedFor = ""
			r.leaderId = ""
		}
		r.mu.Unlock()
		return false
	}

	return appendResp.Success
}

func (r *RaftNode) becomeLeader() {
	oldState := r.state
	r.state = Leader
	r.leaderId = r.id
	log.Printf("[Node %s] Became leader for term %d", truncateID(r.id), r.term)

	// Reset election timer
	r.resetElectionTimer()

	// Broadcast leadership immediately to all peers
	go r.sendHeartbeats()

	// Notify leadership change
	if r.onStateChange != nil && oldState != Leader {
		r.onStateChange(oldState, Leader)
		log.Printf("[Node %s] Called state change callback: %s -> %s",
			truncateID(r.id), stateToString(oldState), stateToString(Leader))
	}

	// Notify callback if registered
	if r.onLeaderElected != nil {
		r.onLeaderElected(r.id)
		log.Printf("[Node %s] Called leader elected callback with ID: %s",
			truncateID(r.id), truncateID(r.id))
	}
}

// stateToString converts NodeState to a string for logging
func stateToString(state NodeState) string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (r *RaftNode) handleVoteRequest(req VoteRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := VoteResponse{
		Term:        r.term,
		VoteGranted: false,
	}

	log.Printf("[Node %s] Received vote request from %s for term %d (my term: %d, voted for: %s)",
		truncateID(r.id), truncateID(req.CandidateId), req.Term, r.term,
		truncateID(r.votedFor))

	if req.Term < r.term {
		log.Printf("[Node %s] Rejecting vote for %s: term %d < current term %d",
			truncateID(r.id), truncateID(req.CandidateId), req.Term, r.term)
		req.ResponseChan <- resp
		return
	}

	if req.Term > r.term {
		oldState := r.state
		r.term = req.Term
		r.state = Follower
		r.votedFor = ""
		r.leaderId = ""

		log.Printf("[Node %s] Converting to follower due to higher term %d from %s",
			truncateID(r.id), req.Term, truncateID(req.CandidateId))

		// Notify state change
		if r.onStateChange != nil && oldState != Follower {
			r.onStateChange(oldState, Follower)
		}
	}

	// Vote if we haven't voted yet in this term or already voted for this candidate
	if r.votedFor == "" || r.votedFor == req.CandidateId {
		r.votedFor = req.CandidateId
		resp.VoteGranted = true
		r.resetElectionTimer()
		log.Printf("[Node %s] Granting vote to %s for term %d",
			truncateID(r.id), truncateID(req.CandidateId), req.Term)
	} else {
		log.Printf("[Node %s] Rejecting vote for %s: already voted for %s in term %d",
			truncateID(r.id), truncateID(req.CandidateId), truncateID(r.votedFor), r.term)
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
		log.Printf("[Node %s] Rejecting append entries from %s: term %d < current term %d",
			truncateID(r.id), truncateID(req.LeaderId), req.Term, r.term)
		req.ResponseChan <- resp
		return
	}

	// Valid AppendEntries, reset election timer
	r.resetElectionTimer()

	if req.Term > r.term {
		oldState := r.state
		r.term = req.Term
		r.state = Follower
		r.votedFor = ""

		log.Printf("[Node %s] Converting to follower due to higher term %d in append entries from %s",
			truncateID(r.id), req.Term, truncateID(req.LeaderId))

		// Notify state change if needed
		if r.onStateChange != nil && oldState != Follower {
			r.onStateChange(oldState, Follower)
		}
	}

	// Update leader if it's a valid heartbeat
	if r.leaderId != req.LeaderId {
		log.Printf("[Node %s] Recognizing %s as leader for term %d",
			truncateID(r.id), truncateID(req.LeaderId), r.term)
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
	leaderId := r.id
	r.mu.RUnlock()

	for _, peer := range r.peers {
		go func(peer string) {
			success := false

			// Try using test hook if available
			if r.sendAppendEntries != nil {
				req := AppendEntriesRequest{
					Term:         currentTerm,
					LeaderId:     leaderId,
					ResponseChan: make(chan AppendEntriesResponse, 1),
				}
				resp := r.sendAppendEntries(peer, req)
				success = resp.Success

				if resp.Term > currentTerm {
					r.mu.Lock()
					if resp.Term > r.term {
						oldState := r.state
						r.term = resp.Term
						r.state = Follower
						r.votedFor = ""
						r.leaderId = ""

						// Notify state change
						if r.onStateChange != nil && oldState != Follower {
							r.onStateChange(oldState, Follower)
						}
					}
					r.mu.Unlock()
				}
			} else {
				// Use HTTP for real deployments
				success = r.sendAppendEntriesHTTP(peer, currentTerm, leaderId)
			}

			if !success {
				log.Printf("[Node %s] Failed to send heartbeat to %s", truncateID(r.id), truncateID(peer))
			}
		}(peer)
	}
}

func (r *RaftNode) resetElectionTimer() {
	// Randomize timeout between 500-1000ms
	r.electionTimeout = time.Duration(500+rand.Intn(500)) * time.Millisecond
	if !r.electionTimer.Stop() {
		select {
		case <-r.electionTimer.C:
		default:
		}
	}
	r.electionTimer.Reset(r.electionTimeout)
	log.Printf("[Node %s] Reset election timer to %v", truncateID(r.id), r.electionTimeout)
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

// Stop stops the Raft node and releases resources
func (r *RaftNode) Stop() {
	r.mu.Lock()
	if r.stopped {
		// Already stopped
		r.mu.Unlock()
		return
	}

	// Mark as stopped and close the stop channel
	r.stopped = true
	close(r.stopCh)
	r.mu.Unlock()

	// Stop the timers
	r.heartbeatTicker.Stop()
	r.electionTimer.Stop()

	// Wait for the run goroutine to finish
	select {
	case <-r.done:
		// Already done
	case <-time.After(1 * time.Second):
		// Timeout, just continue
		log.Printf("[Node %s] Timeout waiting for Raft node to stop", truncateID(r.id))
	}
}

// UpdatePeers updates the list of peers in the cluster
func (r *RaftNode) UpdatePeers(peers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers = peers
	log.Printf("[Node %s] Updated peers: %v", truncateID(r.id), peers)
}

// UpdateNodeInfo updates the node connection information for a peer
func (r *RaftNode) UpdateNodeInfo(id, address string, httpPort int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodeInfos[id] = NodeInfo{
		ID:       id,
		Address:  address,
		HTTPPort: httpPort,
	}
}

// getNodeInfo gets the connection info for a node
func (r *RaftNode) getNodeInfo(nodeID string) (NodeInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	info, ok := r.nodeInfos[nodeID]
	return info, ok
}

// HandleVote handles HTTP vote requests from other nodes
func (r *RaftNode) HandleVote(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var voteReq RPCVoteRequest
	if err := json.NewDecoder(req.Body).Decode(&voteReq); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Process the vote request
	respChan := make(chan VoteResponse, 1)
	localReq := VoteRequest{
		Term:         voteReq.Term,
		CandidateId:  voteReq.CandidateId,
		ResponseChan: respChan,
	}

	// Send the request to the Raft node
	r.voteCh <- localReq

	// Wait for the response
	voteResp := <-respChan

	// Send the response back
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(RPCVoteResponse{
		Term:        voteResp.Term,
		VoteGranted: voteResp.VoteGranted,
	}); err != nil {
		log.Printf("[Raft %s] Error encoding vote response: %v", truncateID(r.id), err)
	}
}

// HandleAppendEntries handles HTTP appendEntries requests from other nodes
func (r *RaftNode) HandleAppendEntries(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var appendReq RPCAppendEntriesRequest
	if err := json.NewDecoder(req.Body).Decode(&appendReq); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// Process the append request
	respChan := make(chan AppendEntriesResponse, 1)
	localReq := AppendEntriesRequest{
		Term:         appendReq.Term,
		LeaderId:     appendReq.LeaderId,
		ResponseChan: respChan,
	}

	// Send the request to the Raft node
	r.appendCh <- localReq

	// Wait for the response
	appendResp := <-respChan

	// Send the response back
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(RPCAppendEntriesResponse{
		Term:    appendResp.Term,
		Success: appendResp.Success,
	}); err != nil {
		log.Printf("[Raft] Error encoding append entries response: %v", err)
	}
}
