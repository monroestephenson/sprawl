package node

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"sprawl/node/utils"
)

// MembershipState represents the state of a cluster node
type MembershipState string

const (
	// MemberStateJoining represents a node in the process of joining
	MemberStateJoining MembershipState = "joining"
	// MemberStateActive represents an active participating node
	MemberStateActive MembershipState = "active"
	// MemberStateSuspect represents a node suspected of failure
	MemberStateSuspect MembershipState = "suspect"
	// MemberStateFailed represents a failed node
	MemberStateFailed MembershipState = "failed"
	// MemberStateLeaving represents a node gracefully leaving
	MemberStateLeaving MembershipState = "leaving"
	// MemberStateLeft represents a node that has left
	MemberStateLeft MembershipState = "left"
)

// MembershipEvent represents a membership change event
type MembershipEvent struct {
	EventType   string          `json:"event_type"`
	NodeID      string          `json:"node_id"`
	OldState    MembershipState `json:"old_state,omitempty"`
	NewState    MembershipState `json:"new_state"`
	Timestamp   time.Time       `json:"timestamp"`
	Description string          `json:"description,omitempty"`
}

// MembershipSnapshot represents the complete state of cluster membership
type MembershipSnapshot struct {
	Version   int64                       `json:"version"`
	Timestamp time.Time                   `json:"timestamp"`
	Members   map[string]*MembershipEntry `json:"members"`
	LeaderID  string                      `json:"leader_id,omitempty"`
}

// MembershipEntry represents a single node in the membership list
type MembershipEntry struct {
	NodeID          string                 `json:"node_id"`
	Address         string                 `json:"address"`
	Port            int                    `json:"port"`
	HTTPPort        int                    `json:"http_port"`
	State           MembershipState        `json:"state"`
	JoinTime        time.Time              `json:"join_time"`
	LastSeen        time.Time              `json:"last_seen"`
	LastStateChange time.Time              `json:"last_state_change"`
	Tags            map[string]string      `json:"tags,omitempty"`
	Version         string                 `json:"version,omitempty"`
	Metadata        map[string]interface{} `json:"metadata,omitempty"`
}

// MembershipChangeHandler is a function that handles membership changes
type MembershipChangeHandler func(event MembershipEvent)

// MembershipManager handles cluster membership management
type MembershipManager struct {
	mu            sync.RWMutex
	members       map[string]*MembershipEntry
	version       int64
	localNodeID   string
	leaderID      string
	eventHistory  []MembershipEvent
	eventHandlers []MembershipChangeHandler

	// Failure detector integration
	failureDetector *FailureDetector

	// Configuration
	suspectTimeout time.Duration
	failureTimeout time.Duration
	cleanupTimeout time.Duration
	maxHistorySize int
}

// NewMembershipManager creates a new membership manager
func NewMembershipManager(localNodeID string) *MembershipManager {
	return &MembershipManager{
		members:        make(map[string]*MembershipEntry),
		localNodeID:    localNodeID,
		eventHistory:   make([]MembershipEvent, 0, 100),
		eventHandlers:  make([]MembershipChangeHandler, 0),
		version:        time.Now().UnixNano(),
		suspectTimeout: 30 * time.Second,
		failureTimeout: 60 * time.Second,
		cleanupTimeout: 24 * time.Hour,
		maxHistorySize: 1000,
	}
}

// SetFailureDetector associates a failure detector
func (mm *MembershipManager) SetFailureDetector(fd *FailureDetector) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.failureDetector = fd
}

// RegisterMembershipChangeHandler adds a handler for membership changes
func (mm *MembershipManager) RegisterMembershipChangeHandler(handler MembershipChangeHandler) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	mm.eventHandlers = append(mm.eventHandlers, handler)
}

// AddMember adds a new member to the cluster
func (mm *MembershipManager) AddMember(nodeID, address string, port, httpPort int, metadata map[string]interface{}) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	now := time.Now()

	// Check if we already have this member
	oldState := MemberStateJoining
	if existing, exists := mm.members[nodeID]; exists {
		oldState = existing.State

		// Update the existing entry
		existing.Address = address
		existing.Port = port
		existing.HTTPPort = httpPort
		existing.LastSeen = now

		// Only update metadata if provided
		if metadata != nil {
			existing.Metadata = metadata
		}

		// Don't change state if member is already active or in a failed state
		if existing.State != MemberStateActive && existing.State != MemberStateFailed {
			existing.State = MemberStateActive
			existing.LastStateChange = now
		}
	} else {
		// Create a new entry
		mm.members[nodeID] = &MembershipEntry{
			NodeID:          nodeID,
			Address:         address,
			Port:            port,
			HTTPPort:        httpPort,
			State:           MemberStateActive,
			JoinTime:        now,
			LastSeen:        now,
			LastStateChange: now,
			Metadata:        metadata,
			Tags:            make(map[string]string),
		}
	}

	// Create event for new member or state change to active
	if oldState != MemberStateActive {
		event := MembershipEvent{
			EventType:   "member_added",
			NodeID:      nodeID,
			OldState:    oldState,
			NewState:    MemberStateActive,
			Timestamp:   now,
			Description: fmt.Sprintf("Node %s added to membership", utils.TruncateID(nodeID)),
		}

		mm.addEvent(event)
	}

	// Update version
	mm.version = now.UnixNano()
}

// UpdateMemberState updates a member's state
func (mm *MembershipManager) UpdateMemberState(nodeID string, state MembershipState, reason string) bool {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	member, exists := mm.members[nodeID]
	if !exists {
		log.Printf("[Membership] Cannot update state for unknown node %s", utils.TruncateID(nodeID))
		return false
	}

	// Don't update if state hasn't changed
	if member.State == state {
		return false
	}

	oldState := member.State
	member.State = state
	member.LastStateChange = time.Now()

	// Create event for state change
	event := MembershipEvent{
		EventType:   "state_change",
		NodeID:      nodeID,
		OldState:    oldState,
		NewState:    state,
		Timestamp:   time.Now(),
		Description: reason,
	}

	mm.addEvent(event)

	// Update version
	mm.version = time.Now().UnixNano()

	log.Printf("[Membership] Node %s state changed from %s to %s: %s",
		utils.TruncateID(nodeID), oldState, state, reason)

	return true
}

// UpdateLastSeen updates the last seen timestamp for a member
func (mm *MembershipManager) UpdateLastSeen(nodeID string) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	member, exists := mm.members[nodeID]
	if !exists {
		log.Printf("[Membership] Cannot update last seen for unknown node %s", utils.TruncateID(nodeID))
		return
	}

	// Update last seen timestamp
	member.LastSeen = time.Now()

	// If node was suspect but is now seen, mark as active
	if member.State == MemberStateSuspect {
		log.Printf("[Membership] Node %s recovered from suspect state", utils.TruncateID(nodeID))
		mm.UpdateMemberState(nodeID, MemberStateActive, "Node recovered")
	}
}

// MarkSuspect marks a member as suspect
func (mm *MembershipManager) MarkSuspect(nodeID string, reason string) bool {
	return mm.UpdateMemberState(nodeID, MemberStateSuspect, reason)
}

// MarkFailed marks a member as failed
func (mm *MembershipManager) MarkFailed(nodeID string, reason string) bool {
	return mm.UpdateMemberState(nodeID, MemberStateFailed, reason)
}

// RemoveMember removes a member from the cluster
func (mm *MembershipManager) RemoveMember(nodeID string, graceful bool) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	member, exists := mm.members[nodeID]
	if !exists {
		log.Printf("[Membership] Cannot remove unknown node %s", utils.TruncateID(nodeID))
		return
	}

	oldState := member.State
	eventType := "member_removed"

	if graceful {
		// For graceful departure, mark as left but keep in members list
		member.State = MemberStateLeft
		member.LastStateChange = time.Now()
		eventType = "member_left"
	} else {
		// For non-graceful, actually remove the member
		delete(mm.members, nodeID)
	}

	// Create event for member removal
	event := MembershipEvent{
		EventType:   eventType,
		NodeID:      nodeID,
		OldState:    oldState,
		NewState:    MemberStateLeft,
		Timestamp:   time.Now(),
		Description: fmt.Sprintf("Node %s removed from membership", utils.TruncateID(nodeID)),
	}

	mm.addEvent(event)

	// If this was the leader, clear leader ID
	if mm.leaderID == nodeID {
		mm.leaderID = ""

		// Create event for leader change
		leaderEvent := MembershipEvent{
			EventType:   "leader_change",
			NodeID:      nodeID,
			Timestamp:   time.Now(),
			Description: "Leader node left or failed",
		}

		mm.addEvent(leaderEvent)
	}

	// Update version
	mm.version = time.Now().UnixNano()
}

// GetMembers returns a map of all members
func (mm *MembershipManager) GetMembers() map[string]*MembershipEntry {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Create a copy to avoid race conditions
	result := make(map[string]*MembershipEntry, len(mm.members))
	for id, member := range mm.members {
		// Deep copy
		memberCopy := *member
		result[id] = &memberCopy
	}

	return result
}

// GetMember returns information about a specific member
func (mm *MembershipManager) GetMember(nodeID string) *MembershipEntry {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	member, exists := mm.members[nodeID]
	if !exists {
		return nil
	}

	// Return a copy to avoid race conditions
	memberCopy := *member
	return &memberCopy
}

// GetMemberCount returns the number of members in each state
func (mm *MembershipManager) GetMemberCount() map[MembershipState]int {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	counts := make(map[MembershipState]int)

	for _, member := range mm.members {
		counts[member.State]++
	}

	return counts
}

// GetActiveMemberIDs returns the IDs of all active members
func (mm *MembershipManager) GetActiveMemberIDs() []string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	var activeMembers []string

	for id, member := range mm.members {
		if member.State == MemberStateActive {
			activeMembers = append(activeMembers, id)
		}
	}

	return activeMembers
}

// ElectLeader performs leader election using a deterministic algorithm
func (mm *MembershipManager) ElectLeader() string {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Get active members
	var activeMembers []string
	for id, member := range mm.members {
		if member.State == MemberStateActive {
			activeMembers = append(activeMembers, id)
		}
	}

	// No active members, no leader
	if len(activeMembers) == 0 {
		mm.leaderID = ""
		return ""
	}

	// Select lowest node ID as leader (simple deterministic algorithm)
	var leaderID string
	for _, id := range activeMembers {
		if leaderID == "" || id < leaderID {
			leaderID = id
		}
	}

	// If leader changed, create an event
	if mm.leaderID != leaderID {
		oldLeader := mm.leaderID
		mm.leaderID = leaderID

		event := MembershipEvent{
			EventType: "leader_change",
			NodeID:    leaderID,
			Timestamp: time.Now(),
			Description: fmt.Sprintf("New leader elected: %s (old: %s)",
				utils.TruncateID(leaderID), utils.TruncateID(oldLeader)),
		}

		mm.addEvent(event)

		log.Printf("[Membership] New leader elected: %s", utils.TruncateID(leaderID))
	}

	return leaderID
}

// GetLeaderID returns the current leader's ID
func (mm *MembershipManager) GetLeaderID() string {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.leaderID
}

// IsLeader returns true if the local node is the leader
func (mm *MembershipManager) IsLeader() bool {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.leaderID == mm.localNodeID
}

// GetSnapshot returns a snapshot of the current membership state
func (mm *MembershipManager) GetSnapshot() MembershipSnapshot {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	members := make(map[string]*MembershipEntry, len(mm.members))
	for id, member := range mm.members {
		// Deep copy
		memberCopy := *member
		members[id] = &memberCopy
	}

	return MembershipSnapshot{
		Version:   mm.version,
		Timestamp: time.Now(),
		Members:   members,
		LeaderID:  mm.leaderID,
	}
}

// MergeSnapshot merges an external snapshot with the current state
func (mm *MembershipManager) MergeSnapshot(snapshot MembershipSnapshot) int {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	// Only merge if snapshot is newer than our current version
	if snapshot.Version <= mm.version {
		return 0
	}

	changes := 0

	// Merge members from the snapshot
	for id, member := range snapshot.Members {
		existing, exists := mm.members[id]

		if !exists {
			// New member
			mm.members[id] = member
			changes++

			// Create event for new member
			event := MembershipEvent{
				EventType:   "member_added",
				NodeID:      id,
				NewState:    member.State,
				Timestamp:   time.Now(),
				Description: fmt.Sprintf("Node %s added from snapshot", utils.TruncateID(id)),
			}

			mm.addEvent(event)
		} else if member.LastStateChange.After(existing.LastStateChange) {
			// Member exists but snapshot has newer state
			oldState := existing.State

			// Update the member with snapshot data
			*existing = *member
			changes++

			// Create event for state change if state changed
			if oldState != member.State {
				event := MembershipEvent{
					EventType:   "state_change",
					NodeID:      id,
					OldState:    oldState,
					NewState:    member.State,
					Timestamp:   time.Now(),
					Description: fmt.Sprintf("Node %s state updated from snapshot", utils.TruncateID(id)),
				}

				mm.addEvent(event)
			}
		}
	}

	// Update leader if snapshot has one and we don't
	if snapshot.LeaderID != "" && mm.leaderID == "" {
		mm.leaderID = snapshot.LeaderID
		changes++

		// Create event for leader change
		event := MembershipEvent{
			EventType:   "leader_change",
			NodeID:      snapshot.LeaderID,
			Timestamp:   time.Now(),
			Description: fmt.Sprintf("Leader %s from snapshot", utils.TruncateID(snapshot.LeaderID)),
		}

		mm.addEvent(event)
	}

	// Update version to match snapshot
	mm.version = snapshot.Version

	return changes
}

// GetEvents returns the recent membership events
func (mm *MembershipManager) GetEvents(limit int) []MembershipEvent {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	count := len(mm.eventHistory)
	if count == 0 {
		return []MembershipEvent{}
	}

	if limit <= 0 || limit > count {
		limit = count
	}

	// Return the most recent events
	result := make([]MembershipEvent, limit)
	copy(result, mm.eventHistory[count-limit:])

	return result
}

// GetVersion returns the current membership version
func (mm *MembershipManager) GetVersion() int64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.version
}

// cleanupLeftMembers removes members that have been in the left state for too long
func (mm *MembershipManager) cleanupLeftMembers() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	now := time.Now()
	for id, member := range mm.members {
		if member.State == MemberStateLeft && now.Sub(member.LastStateChange) > mm.cleanupTimeout {
			delete(mm.members, id)

			event := MembershipEvent{
				EventType:   "member_cleanup",
				NodeID:      id,
				OldState:    MemberStateLeft,
				Timestamp:   now,
				Description: fmt.Sprintf("Node %s removed after cleanup timeout", utils.TruncateID(id)),
			}

			mm.addEvent(event)
		}
	}
}

// checkMembersHealth checks all members for health and updates states
func (mm *MembershipManager) checkMembersHealth() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	now := time.Now()
	for id, member := range mm.members {
		// Skip local node
		if id == mm.localNodeID {
			continue
		}

		timeSinceLastSeen := now.Sub(member.LastSeen)

		switch member.State {
		case MemberStateActive:
			// Check if node should become suspect
			if timeSinceLastSeen > mm.suspectTimeout {
				member.State = MemberStateSuspect
				member.LastStateChange = now

				event := MembershipEvent{
					EventType:   "state_change",
					NodeID:      id,
					OldState:    MemberStateActive,
					NewState:    MemberStateSuspect,
					Timestamp:   now,
					Description: fmt.Sprintf("Node %s suspected due to timeout", utils.TruncateID(id)),
				}

				mm.addEvent(event)

				log.Printf("[Membership] Node %s marked SUSPECT (last seen %s ago)",
					utils.TruncateID(id), timeSinceLastSeen)
			}

		case MemberStateSuspect:
			// Check if suspect node should be marked failed
			if timeSinceLastSeen > mm.failureTimeout {
				member.State = MemberStateFailed
				member.LastStateChange = now

				event := MembershipEvent{
					EventType:   "state_change",
					NodeID:      id,
					OldState:    MemberStateSuspect,
					NewState:    MemberStateFailed,
					Timestamp:   now,
					Description: fmt.Sprintf("Node %s failed after timeout", utils.TruncateID(id)),
				}

				mm.addEvent(event)

				log.Printf("[Membership] Node %s marked FAILED (last seen %s ago)",
					utils.TruncateID(id), timeSinceLastSeen)

				// If this was the leader, elect a new one
				if mm.leaderID == id {
					mm.ElectLeader()
				}
			}
		}
	}
}

// addEvent adds an event to the history and notifies handlers
func (mm *MembershipManager) addEvent(event MembershipEvent) {
	// Add to history
	mm.eventHistory = append(mm.eventHistory, event)

	// Keep history size manageable
	if len(mm.eventHistory) > mm.maxHistorySize {
		mm.eventHistory = mm.eventHistory[len(mm.eventHistory)-mm.maxHistorySize:]
	}

	// Call handlers (make a copy of the handlers slice to avoid deadlocks)
	handlers := make([]MembershipChangeHandler, len(mm.eventHandlers))
	copy(handlers, mm.eventHandlers)

	// Call handlers outside the lock
	go func(e MembershipEvent, hs []MembershipChangeHandler) {
		for _, handler := range hs {
			handler(e)
		}
	}(event, handlers)
}

// ToJSON converts the membership state to JSON
func (mm *MembershipManager) ToJSON() ([]byte, error) {
	snapshot := mm.GetSnapshot()
	return json.Marshal(snapshot)
}

// Start begins running the membership manager
func (mm *MembershipManager) Start() {
	go mm.maintenanceLoop()
}

// maintenanceLoop runs periodic maintenance tasks
func (mm *MembershipManager) maintenanceLoop() {
	cleanupTicker := time.NewTicker(1 * time.Hour)
	healthTicker := time.NewTicker(5 * time.Second)

	defer cleanupTicker.Stop()
	defer healthTicker.Stop()

	for {
		select {
		case <-cleanupTicker.C:
			mm.cleanupLeftMembers()
		case <-healthTicker.C:
			mm.checkMembersHealth()
		}
	}
}
