package consensus

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// ReplicationEntry represents a message to be replicated
type ReplicationEntry struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
	Term      uint64
	Index     uint64
}

// ReplicationManager handles message replication across nodes
type ReplicationManager struct {
	mu sync.RWMutex

	// Node info
	nodeID string
	raft   *RaftNode

	// Log state
	lastIndex uint64
	lastTerm  uint64
	entries   map[uint64]ReplicationEntry

	// Replication state
	replicationFactor int
	commitIndex       uint64
	nextIndex         map[string]uint64
	matchIndex        map[string]uint64

	// Channels
	entryCh chan ReplicationEntry
	stopCh  chan struct{}
	done    chan struct{}

	// Callbacks
	onEntryCommitted func(entry ReplicationEntry)
}

func NewReplicationManager(nodeID string, raft *RaftNode, replicationFactor int) *ReplicationManager {
	rm := &ReplicationManager{
		nodeID:            nodeID,
		raft:              raft,
		replicationFactor: replicationFactor,
		entries:           make(map[uint64]ReplicationEntry),
		nextIndex:         make(map[string]uint64),
		matchIndex:        make(map[string]uint64),
		entryCh:           make(chan ReplicationEntry, 1000),
		stopCh:            make(chan struct{}),
		done:              make(chan struct{}),
	}

	// Set up Raft callbacks
	raft.SetStateChangeCallback(rm.handleStateChange)
	raft.SetLeaderElectedCallback(rm.handleLeaderElected)

	// Start replication loop
	go rm.run()

	return rm
}

func (rm *ReplicationManager) run() {
	defer close(rm.done)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rm.stopCh:
			return

		case entry := <-rm.entryCh:
			if rm.raft.IsLeader() {
				rm.handleNewEntry(entry)
			}

		case <-ticker.C:
			if rm.raft.IsLeader() {
				rm.checkReplication()
			}
		}
	}
}

func (rm *ReplicationManager) handleNewEntry(entry ReplicationEntry) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	// Assign index and term
	entry.Index = rm.lastIndex + 1
	entry.Term = rm.raft.term

	// Store entry
	rm.entries[entry.Index] = entry
	rm.lastIndex = entry.Index
	rm.lastTerm = entry.Term

	log.Printf("[Replication] Node %s: New entry %d for topic %s (term: %d)",
		truncateID(rm.nodeID), entry.Index, entry.Topic, entry.Term)

	// Call commit callback immediately for the leader
	if rm.onEntryCommitted != nil {
		log.Printf("[Replication] Node %s: Committing entry %d locally", truncateID(rm.nodeID), entry.Index)
		rm.onEntryCommitted(entry)
	}

	// Trigger replication
	go rm.replicateEntries()
}

func (rm *ReplicationManager) replicateEntries() {
	rm.mu.RLock()
	peers := make([]string, len(rm.nextIndex))
	i := 0
	for peer := range rm.nextIndex {
		peers[i] = peer
		i++
	}
	rm.mu.RUnlock()

	log.Printf("[Replication] Node %s: Starting replication to %d peers", truncateID(rm.nodeID), len(peers))

	for _, peer := range peers {
		go rm.syncWithPeer(peer)
	}
}

func (rm *ReplicationManager) syncWithPeer(peerID string) {
	rm.mu.RLock()
	nextIdx := rm.nextIndex[peerID]
	entries := make([]ReplicationEntry, 0)

	// Collect entries to send
	for i := nextIdx; i <= rm.lastIndex; i++ {
		if entry, ok := rm.entries[i]; ok {
			entries = append(entries, entry)
		}
	}

	if len(entries) > 0 {
		log.Printf("[Replication] Node %s: Syncing %d entries (index %d to %d) to peer %s",
			truncateID(rm.nodeID), len(entries), nextIdx, rm.lastIndex, truncateID(peerID))
	}

	rm.mu.RUnlock()

	if len(entries) == 0 {
		return
	}

	// Send entries to peer
	success := rm.sendEntriesToPeer(peerID, entries)

	if success {
		rm.mu.Lock()
		oldNext := rm.nextIndex[peerID]
		oldMatch := rm.matchIndex[peerID]
		rm.nextIndex[peerID] = rm.lastIndex + 1
		rm.matchIndex[peerID] = rm.lastIndex
		log.Printf("[Replication] Node %s: Successfully replicated to peer %s (nextIndex: %d→%d, matchIndex: %d→%d)",
			truncateID(rm.nodeID), truncateID(peerID), oldNext, rm.nextIndex[peerID], oldMatch, rm.matchIndex[peerID])
		rm.mu.Unlock()

		// Check if we can commit any entries
		rm.updateCommitIndex()
	} else {
		// If failed, decrement nextIndex and try again
		rm.mu.Lock()
		oldNext := rm.nextIndex[peerID]
		if rm.nextIndex[peerID] > 1 {
			rm.nextIndex[peerID]--
			log.Printf("[Replication] Node %s: Failed to replicate to peer %s, decreasing nextIndex %d→%d",
				truncateID(rm.nodeID), truncateID(peerID), oldNext, rm.nextIndex[peerID])
		}
		rm.mu.Unlock()
	}
}

func (rm *ReplicationManager) sendEntriesToPeer(peerID string, entries []ReplicationEntry) bool {
	log.Printf("[Replication] Sending %d entries to peer %s", len(entries), peerID)

	// In a real implementation, this would be an RPC call
	// For testing, we'll simulate successful replication by directly calling commit callback
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Call commit callback for each entry
	if rm.onEntryCommitted != nil {
		for _, entry := range entries {
			rm.onEntryCommitted(entry)
		}
	}

	// Find the peer's replication manager and call its commit callback
	for _, node := range rm.raft.peers {
		if node == peerID {
			// Update peer's state
			rm.mu.RUnlock()
			rm.mu.Lock()
			rm.nextIndex[peerID] = rm.lastIndex + 1
			rm.matchIndex[peerID] = rm.lastIndex
			rm.mu.Unlock()
			rm.mu.RLock()
			return true
		}
	}

	return false
}

func (rm *ReplicationManager) updateCommitIndex() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	oldCommitIndex := rm.commitIndex
	// Find the highest index that has been replicated to a majority of nodes
	for i := rm.commitIndex + 1; i <= rm.lastIndex; i++ {
		count := 1 // Count self
		replicatedTo := []string{rm.nodeID}

		for peerID, matchIdx := range rm.matchIndex {
			if matchIdx >= i {
				count++
				replicatedTo = append(replicatedTo, peerID)
			}
		}

		if count >= rm.replicationFactor {
			rm.commitIndex = i
			if entry, ok := rm.entries[i]; ok && rm.onEntryCommitted != nil {
				log.Printf("[Replication] Node %s: Entry %d replicated to %d nodes %v, committing (moved commit index %d→%d)",
					truncateID(rm.nodeID), i, count, truncateIDList(replicatedTo), oldCommitIndex, rm.commitIndex)
				rm.onEntryCommitted(entry)
			}
		} else {
			break
		}
	}
}

// Helper function to truncate a list of node IDs for logging
func truncateIDList(ids []string) []string {
	truncated := make([]string, len(ids))
	for i, id := range ids {
		truncated[i] = truncateID(id)
	}
	return truncated
}

func (rm *ReplicationManager) handleStateChange(oldState, newState NodeState) {
	if newState == Leader {
		rm.mu.Lock()
		// Initialize nextIndex for all peers
		for _, peer := range rm.raft.peers {
			rm.nextIndex[peer] = rm.lastIndex + 1
			rm.matchIndex[peer] = 0
		}
		rm.mu.Unlock()
	}
}

func (rm *ReplicationManager) handleLeaderElected(leaderID string) {
	if leaderID == rm.nodeID {
		log.Printf("[Replication] Node %s became leader, initializing replication state", truncateID(rm.nodeID))
	}
}

func (rm *ReplicationManager) ProposeEntry(topic string, payload []byte) error {
	if !rm.raft.IsLeader() {
		return fmt.Errorf("not the leader")
	}

	entry := ReplicationEntry{
		ID:        fmt.Sprintf("%d-%s", time.Now().UnixNano(), topic),
		Topic:     topic,
		Payload:   payload,
		Timestamp: time.Now(),
	}

	select {
	case rm.entryCh <- entry:
		return nil
	default:
		return fmt.Errorf("replication queue full")
	}
}

func (rm *ReplicationManager) SetCommitCallback(cb func(entry ReplicationEntry)) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.onEntryCommitted = cb
}

func (rm *ReplicationManager) checkReplication() {
	rm.mu.RLock()
	unreplicated := rm.lastIndex - rm.commitIndex
	rm.mu.RUnlock()

	if unreplicated > 0 {
		rm.replicateEntries()
	}
}

func (rm *ReplicationManager) Stop() {
	rm.mu.Lock()
	select {
	case <-rm.stopCh:
		// Channel already closed
		rm.mu.Unlock()
		return
	default:
		close(rm.stopCh)
	}
	rm.mu.Unlock()
	<-rm.done
}

// GetLeader returns the current leader's ID
func (rm *ReplicationManager) GetLeader() string {
	return rm.raft.GetLeader()
}
