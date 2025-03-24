package ack

import (
	"container/heap"
	"log"
	"sync"
	"time"
)

// RetryItem represents a message that needs retry
type RetryItem struct {
	MessageID  string
	NodeID     string
	RetryTime  time.Time
	RetryCount int
	Index      int // For heap implementation
}

// RetryQueue is a priority queue of retry items
type RetryQueue []*RetryItem

// RetryManager handles message retry scheduling and execution
type RetryManager struct {
	tracker        *Tracker
	queue          RetryQueue
	mu             sync.Mutex
	retrySignal    chan struct{}
	nodeFailures   map[string]*NodeFailureStats
	nodeFailureMu  sync.RWMutex
	circuitTimeout time.Duration
}

// NodeFailureStats tracks failure statistics for a node
type NodeFailureStats struct {
	ConsecutiveFailures int
	LastFailure         time.Time
	CircuitOpen         bool
	CircuitOpenTime     time.Time
}

// NewRetryManager creates a new retry manager
func NewRetryManager(tracker *Tracker) *RetryManager {
	return &RetryManager{
		tracker:        tracker,
		queue:          make(RetryQueue, 0),
		retrySignal:    make(chan struct{}, 1),
		nodeFailures:   make(map[string]*NodeFailureStats),
		circuitTimeout: 30 * time.Second, // Default circuit breaker timeout
	}
}

// ScheduleRetry schedules a message for immediate retry
func (r *RetryManager) ScheduleRetry(msgID string, nodeID string) {
	r.ScheduleRetryWithDelay(msgID, nodeID, 0)
}

// ScheduleRetryWithDelay schedules a message for retry after a delay
func (r *RetryManager) ScheduleRetryWithDelay(msgID string, nodeID string, delay time.Duration) {
	// Check if the circuit is open for this node
	if r.isCircuitOpen(nodeID) {
		// Skip retry if circuit is open
		log.Printf("[RetryManager] Circuit open for node %s, skipping retry for message %s", nodeID, msgID)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	retryTime := time.Now().Add(delay)
	item := &RetryItem{
		MessageID: msgID,
		NodeID:    nodeID,
		RetryTime: retryTime,
	}

	heap.Push(&r.queue, item)

	// Signal that a new item is available
	select {
	case r.retrySignal <- struct{}{}:
	default:
		// Channel already has a signal, no need to add another
	}
}

// Run starts the retry manager's processing loop
func (r *RetryManager) Run(shutdownCh <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.retrySignal:
			// Process retry queue when signaled
			r.processRetryQueue()
		case <-ticker.C:
			// Regularly check retry queue for timed items
			r.processRetryQueue()
		case <-shutdownCh:
			// Shutdown requested
			log.Println("[RetryManager] Shutting down")
			return
		}
	}
}

// processRetryQueue processes due retry items
func (r *RetryManager) processRetryQueue() {
	now := time.Now()

	r.mu.Lock()
	// Process all items that are due
	for r.queue.Len() > 0 && r.queue[0].RetryTime.Before(now) {
		item := heap.Pop(&r.queue).(*RetryItem)
		r.mu.Unlock() // Unlock while processing to avoid blocking

		// Process the retry
		r.processRetry(item)

		r.mu.Lock() // Lock again before checking the queue
	}
	r.mu.Unlock()
}

// processRetry handles a single retry item
func (r *RetryManager) processRetry(item *RetryItem) {
	// Skip if circuit is open for this node
	if r.isCircuitOpen(item.NodeID) {
		log.Printf("[RetryManager] Circuit open for node %s, skipping retry", item.NodeID)
		return
	}

	// Attempt delivery here
	// In a real implementation, this would call into the message router
	// or directly to the node client to deliver the message
	log.Printf("[RetryManager] Attempting delivery of message %s to node %s", item.MessageID, item.NodeID)

	// This is a placeholder - actual delivery logic would be wired up
	// to the router or other delivery mechanism
	success := false

	if success {
		// Record successful acknowledgment
		if err := r.tracker.RecordAck(item.MessageID, item.NodeID); err != nil {
			log.Printf("[RetryManager] Error recording ACK: %v", err)
		}
		// Reset failure count for this node
		r.recordNodeSuccess(item.NodeID)
	} else {
		// Record failure
		if err := r.tracker.RecordFailure(item.MessageID, item.NodeID); err != nil {
			log.Printf("[RetryManager] Error recording failure: %v", err)
		}
		// Update node failure stats
		r.recordNodeFailure(item.NodeID)
	}
}

// recordNodeFailure updates the failure statistics for a node
func (r *RetryManager) recordNodeFailure(nodeID string) {
	r.nodeFailureMu.Lock()
	defer r.nodeFailureMu.Unlock()

	stats, exists := r.nodeFailures[nodeID]
	if !exists {
		stats = &NodeFailureStats{}
		r.nodeFailures[nodeID] = stats
	}

	stats.ConsecutiveFailures++
	stats.LastFailure = time.Now()

	// Check if we should open the circuit breaker
	if stats.ConsecutiveFailures >= 5 { // Threshold for circuit breaking
		stats.CircuitOpen = true
		stats.CircuitOpenTime = time.Now()
		log.Printf("[RetryManager] Circuit opened for node %s after %d consecutive failures",
			nodeID, stats.ConsecutiveFailures)
	}
}

// recordNodeSuccess resets the failure statistics for a node
func (r *RetryManager) recordNodeSuccess(nodeID string) {
	r.nodeFailureMu.Lock()
	defer r.nodeFailureMu.Unlock()

	stats, exists := r.nodeFailures[nodeID]
	if !exists {
		return
	}

	// Reset consecutive failures
	stats.ConsecutiveFailures = 0

	// If circuit was open, close it
	if stats.CircuitOpen {
		stats.CircuitOpen = false
		log.Printf("[RetryManager] Circuit closed for node %s after successful delivery", nodeID)
	}
}

// isCircuitOpen checks if the circuit breaker is open for a node
func (r *RetryManager) isCircuitOpen(nodeID string) bool {
	r.nodeFailureMu.RLock()
	defer r.nodeFailureMu.RUnlock()

	stats, exists := r.nodeFailures[nodeID]
	if !exists {
		return false
	}

	if !stats.CircuitOpen {
		return false
	}

	// Check if the circuit timeout has elapsed
	if time.Since(stats.CircuitOpenTime) > r.circuitTimeout {
		// Auto-reset circuit after timeout
		stats.CircuitOpen = false
		log.Printf("[RetryManager] Circuit auto-reset for node %s after timeout", nodeID)
		return false
	}

	return true
}

// GetNodeFailureStats returns the failure statistics for all nodes
func (r *RetryManager) GetNodeFailureStats() map[string]map[string]interface{} {
	r.nodeFailureMu.RLock()
	defer r.nodeFailureMu.RUnlock()

	result := make(map[string]map[string]interface{})
	for nodeID, stats := range r.nodeFailures {
		result[nodeID] = map[string]interface{}{
			"consecutive_failures": stats.ConsecutiveFailures,
			"last_failure":         stats.LastFailure,
			"circuit_open":         stats.CircuitOpen,
			"circuit_open_time":    stats.CircuitOpenTime,
		}
	}

	return result
}

// SetCircuitTimeout sets the circuit breaker timeout
func (r *RetryManager) SetCircuitTimeout(timeout time.Duration) {
	r.nodeFailureMu.Lock()
	defer r.nodeFailureMu.Unlock()
	r.circuitTimeout = timeout
}

// Implementation of heap.Interface for RetryQueue
func (rq RetryQueue) Len() int { return len(rq) }

func (rq RetryQueue) Less(i, j int) bool {
	return rq[i].RetryTime.Before(rq[j].RetryTime)
}

func (rq RetryQueue) Swap(i, j int) {
	rq[i], rq[j] = rq[j], rq[i]
	rq[i].Index = i
	rq[j].Index = j
}

func (rq *RetryQueue) Push(x interface{}) {
	n := len(*rq)
	item := x.(*RetryItem)
	item.Index = n
	*rq = append(*rq, item)
}

func (rq *RetryQueue) Pop() interface{} {
	old := *rq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // avoid memory leak
	item.Index = -1 // for safety
	*rq = old[0 : n-1]
	return item
}
