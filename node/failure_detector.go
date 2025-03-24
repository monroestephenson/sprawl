package node

import (
	"log"
	"sync"
	"time"

	"sprawl/node/dht"
	"sprawl/node/utils"
)

// FailureState represents node health status
type FailureState int

const (
	// StateHealthy indicates node is operating normally
	StateHealthy FailureState = iota
	// StateSuspect indicates node may be failing
	StateSuspect
	// StateFailed indicates node is considered failed
	StateFailed
)

// NodeHealth tracks health information for a node
type NodeHealth struct {
	NodeID           string
	LastSeen         time.Time
	State            FailureState
	FailureTimestamp time.Time
	SuspectTimestamp time.Time
	MissedHeartbeats int
	ResponseTimes    []time.Duration // Track recent response times
	ErrorCount       int             // Track consecutive errors
}

// FailureDetector manages node failure detection
type FailureDetector struct {
	mu               sync.RWMutex
	nodes            map[string]*NodeHealth
	suspectThreshold time.Duration
	failureThreshold time.Duration
	heartbeatTimeout time.Duration
	responseWindow   int // Number of responses to track for health calculation
	dht              *dht.DHT
	gossipManager    *GossipManager
	stopping         bool
	stopCh           chan struct{}
	wg               sync.WaitGroup
}

// NewFailureDetector creates a new failure detector
func NewFailureDetector(dht *dht.DHT) *FailureDetector {
	fd := &FailureDetector{
		nodes:            make(map[string]*NodeHealth),
		suspectThreshold: 5 * time.Second,  // Time until node becomes suspect
		failureThreshold: 15 * time.Second, // Time until suspect node is marked failed
		heartbeatTimeout: 2 * time.Second,  // Timeout for heartbeat responses
		responseWindow:   10,               // Track last 10 responses
		dht:              dht,
		stopCh:           make(chan struct{}),
	}

	return fd
}

// SetGossipManager sets the gossip manager reference
func (fd *FailureDetector) SetGossipManager(gm *GossipManager) {
	fd.mu.Lock()
	defer fd.mu.Unlock()
	fd.gossipManager = gm
}

// Start begins failure detection process
func (fd *FailureDetector) Start() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	if fd.stopping {
		return
	}

	fd.wg.Add(1)
	go fd.monitorNodes()
}

// Stop halts failure detection
func (fd *FailureDetector) Stop() {
	fd.mu.Lock()
	fd.stopping = true
	fd.mu.Unlock()

	close(fd.stopCh)
	fd.wg.Wait()
}

// RecordHeartbeat marks a node as having sent a heartbeat
func (fd *FailureDetector) RecordHeartbeat(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	now := time.Now()

	health, exists := fd.nodes[nodeID]
	if !exists {
		// New node
		health = &NodeHealth{
			NodeID:        nodeID,
			LastSeen:      now,
			State:         StateHealthy,
			ResponseTimes: make([]time.Duration, 0, fd.responseWindow),
		}
		fd.nodes[nodeID] = health
	} else {
		// Update existing node
		health.LastSeen = now

		// Reset error counts if we were in suspect state
		if health.State == StateSuspect {
			health.State = StateHealthy
			health.MissedHeartbeats = 0
			health.ErrorCount = 0
			log.Printf("[FailureDetector] Node %s recovered from suspect state", utils.TruncateID(nodeID))
		}
	}
}

// RecordResponseTime records a response time for a node
func (fd *FailureDetector) RecordResponseTime(nodeID string, responseTime time.Duration) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	health, exists := fd.nodes[nodeID]
	if !exists {
		// New node
		health = &NodeHealth{
			NodeID:        nodeID,
			LastSeen:      time.Now(),
			State:         StateHealthy,
			ResponseTimes: make([]time.Duration, 0, fd.responseWindow),
		}
		fd.nodes[nodeID] = health
	}

	// Add response time to sliding window
	health.ResponseTimes = append(health.ResponseTimes, responseTime)
	if len(health.ResponseTimes) > fd.responseWindow {
		// Remove oldest entry to maintain window size
		health.ResponseTimes = health.ResponseTimes[1:]
	}
}

// RecordError records an error communicating with a node
func (fd *FailureDetector) RecordError(nodeID string) {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	health, exists := fd.nodes[nodeID]
	if !exists {
		// New node starting with an error
		health = &NodeHealth{
			NodeID:        nodeID,
			LastSeen:      time.Now().Add(-fd.heartbeatTimeout), // Consider it already timed out
			State:         StateHealthy,                         // Start healthy but with errors
			ErrorCount:    1,
			ResponseTimes: make([]time.Duration, 0, fd.responseWindow),
		}
		fd.nodes[nodeID] = health
		return
	}

	// Increment error count
	health.ErrorCount++
	health.MissedHeartbeats++

	// If too many consecutive errors, mark as suspect
	if health.State == StateHealthy && health.ErrorCount >= 3 {
		health.State = StateSuspect
		health.SuspectTimestamp = time.Now()
		log.Printf("[FailureDetector] Node %s marked SUSPECT after %d consecutive errors",
			utils.TruncateID(nodeID), health.ErrorCount)
	}
}

// GetNodeState returns the current failure state of a node
func (fd *FailureDetector) GetNodeState(nodeID string) FailureState {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	health, exists := fd.nodes[nodeID]
	if !exists {
		return StateHealthy // Default to healthy if unknown
	}
	return health.State
}

// GetFailedNodes returns a list of nodes considered failed
func (fd *FailureDetector) GetFailedNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	var failedNodes []string
	for nodeID, health := range fd.nodes {
		if health.State == StateFailed {
			failedNodes = append(failedNodes, nodeID)
		}
	}
	return failedNodes
}

// GetSuspectNodes returns a list of nodes in suspect state
func (fd *FailureDetector) GetSuspectNodes() []string {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	var suspectNodes []string
	for nodeID, health := range fd.nodes {
		if health.State == StateSuspect {
			suspectNodes = append(suspectNodes, nodeID)
		}
	}
	return suspectNodes
}

// monitorNodes periodically checks node health
func (fd *FailureDetector) monitorNodes() {
	defer fd.wg.Done()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-fd.stopCh:
			return
		case <-ticker.C:
			fd.checkNodeHealth()
		}
	}
}

// checkNodeHealth evaluates all nodes for health status
func (fd *FailureDetector) checkNodeHealth() {
	fd.mu.Lock()
	defer fd.mu.Unlock()

	now := time.Now()
	var failedNodes []string
	var nodesToProbe []string

	for nodeID, health := range fd.nodes {
		timeSinceLastSeen := now.Sub(health.LastSeen)

		switch health.State {
		case StateHealthy:
			// Check if node should become suspect
			if timeSinceLastSeen > fd.suspectThreshold {
				health.State = StateSuspect
				health.SuspectTimestamp = now
				log.Printf("[FailureDetector] Node %s marked SUSPECT (last seen %s ago)",
					utils.TruncateID(nodeID), timeSinceLastSeen)

				// Add to probe list
				nodesToProbe = append(nodesToProbe, nodeID)
			}

		case StateSuspect:
			// Check if suspect node should be marked failed
			timeSinceSuspect := now.Sub(health.SuspectTimestamp)
			if timeSinceSuspect > fd.failureThreshold {
				health.State = StateFailed
				health.FailureTimestamp = now
				failedNodes = append(failedNodes, nodeID)
				log.Printf("[FailureDetector] Node %s marked FAILED (suspect for %s)",
					utils.TruncateID(nodeID), timeSinceSuspect)
			} else {
				// Add to probe list
				nodesToProbe = append(nodesToProbe, nodeID)
			}

		case StateFailed:
			// Node already failed, do nothing but track how long it's been down
			timeSinceFailed := now.Sub(health.FailureTimestamp)
			if timeSinceFailed > 5*time.Minute {
				// After significant time, clean up the node from our tracking
				delete(fd.nodes, nodeID)
				log.Printf("[FailureDetector] Removed tracking for failed node %s after %s",
					utils.TruncateID(nodeID), timeSinceFailed)
			}
		}
	}

	// Handle newly failed nodes
	if len(failedNodes) > 0 && fd.dht != nil {
		for _, nodeID := range failedNodes {
			// Remove the node from DHT
			fd.dht.RemoveNode(nodeID)
			log.Printf("[FailureDetector] Removed failed node %s from DHT", utils.TruncateID(nodeID))
		}
	}

	// Release the lock before probing nodes to avoid deadlocks
	fd.mu.Unlock()

	// Probe suspect nodes outside the lock
	if fd.gossipManager != nil {
		for _, nodeID := range nodesToProbe {
			// We need to access GossipManager's probeNode method, but we need to be careful with circular imports
			// Instead of direct call, use method through the interface
			go fd.requestProbe(nodeID)
		}
	}

	// Re-acquire the lock
	fd.mu.Lock()
}

// requestProbe asks the gossip manager to probe a node
func (fd *FailureDetector) requestProbe(nodeID string) {
	// This safely calls GossipManager.probeNode without creating circular dependencies
	if fd.gossipManager != nil && fd.gossipManager.ProbeNodeFn != nil {
		fd.gossipManager.ProbeNodeFn(nodeID)
	} else {
		log.Printf("[FailureDetector] Cannot probe node %s: no probe function available",
			utils.TruncateID(nodeID))
	}
}

// GetNodeHealth returns detailed health information for a node
func (fd *FailureDetector) GetNodeHealth(nodeID string) *NodeHealth {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	health, exists := fd.nodes[nodeID]
	if !exists {
		return nil
	}

	// Return a copy to avoid race conditions
	return &NodeHealth{
		NodeID:           health.NodeID,
		LastSeen:         health.LastSeen,
		State:            health.State,
		FailureTimestamp: health.FailureTimestamp,
		SuspectTimestamp: health.SuspectTimestamp,
		MissedHeartbeats: health.MissedHeartbeats,
		ErrorCount:       health.ErrorCount,
		// Copy response times
		ResponseTimes: append([]time.Duration{}, health.ResponseTimes...),
	}
}

// GetAverageResponseTime calculates average response time for a node
func (fd *FailureDetector) GetAverageResponseTime(nodeID string) time.Duration {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	health, exists := fd.nodes[nodeID]
	if !exists || len(health.ResponseTimes) == 0 {
		return 0
	}

	var total time.Duration
	for _, rt := range health.ResponseTimes {
		total += rt
	}

	return total / time.Duration(len(health.ResponseTimes))
}

// CheckClusterHealth returns an overall health assessment
func (fd *FailureDetector) CheckClusterHealth() map[string]interface{} {
	fd.mu.RLock()
	defer fd.mu.RUnlock()

	totalNodes := len(fd.nodes)
	healthyCount := 0
	suspectCount := 0
	failedCount := 0

	for _, health := range fd.nodes {
		switch health.State {
		case StateHealthy:
			healthyCount++
		case StateSuspect:
			suspectCount++
		case StateFailed:
			failedCount++
		}
	}

	return map[string]interface{}{
		"total_nodes":   totalNodes,
		"healthy_nodes": healthyCount,
		"suspect_nodes": suspectCount,
		"failed_nodes":  failedCount,
		"healthy_ratio": float64(healthyCount) / float64(max(totalNodes, 1)),
		"timestamp":     time.Now().Unix(),
	}
}

// max returns the larger of a or b
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
