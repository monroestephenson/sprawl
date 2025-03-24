package ack

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

// DeliveryStatus represents the status of a message delivery to a node
type DeliveryStatus string

const (
	StatusPending   DeliveryStatus = "pending"
	StatusDelivered DeliveryStatus = "delivered"
	StatusFailed    DeliveryStatus = "failed"
)

// AckCallback is a function called when a message acknowledgment is received
type AckCallback func(msgID string, nodeID string, status DeliveryStatus)

// Config contains configuration options for the ACK tracker
type Config struct {
	RetryInterval   time.Duration
	MaxRetryCount   int
	CleanupInterval time.Duration
	MaxAckAge       time.Duration
	BatchSize       int
}

// DefaultConfig returns a default configuration
func DefaultConfig() Config {
	return Config{
		RetryInterval:   5 * time.Second,
		MaxRetryCount:   10,
		CleanupInterval: 5 * time.Minute,
		MaxAckAge:       24 * time.Hour,
		BatchSize:       1000,
	}
}

// AckState represents the state of an acknowledgment
type AckState struct {
	MessageID    string
	Destinations map[string]DeliveryStatus
	CreatedAt    time.Time
	LastAttempt  time.Time
	RetryCount   int
	Callbacks    []AckCallback
	CompleteChan chan struct{}
	mu           sync.RWMutex
}

// AckHandle provides methods to interact with an acknowledgment
type AckHandle struct {
	msgID    string
	tracker  *Tracker
	complete chan struct{}
}

// Tracker manages acknowledgment tracking for messages
type Tracker struct {
	config        Config
	acks          map[string]*AckState
	metrics       *Metrics
	persistence   *Persistence
	retryManager  *RetryManager
	cleanupWorker *CleanupWorker
	shutdown      chan struct{}
	mu            sync.RWMutex
}

// NewTracker creates a new acknowledgment tracker
func NewTracker(config Config, persistencePath string) (*Tracker, error) {
	// Initialize persistence
	persistence, err := NewPersistence(persistencePath)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize persistence: %w", err)
	}

	// Initialize metrics
	metrics := NewMetrics()

	// Create the tracker
	tracker := &Tracker{
		config:      config,
		acks:        make(map[string]*AckState),
		metrics:     metrics,
		persistence: persistence,
		shutdown:    make(chan struct{}),
	}

	// Initialize retry manager
	tracker.retryManager = NewRetryManager(tracker)

	// Initialize cleanup worker
	tracker.cleanupWorker = NewCleanupWorker(tracker, config.CleanupInterval)

	// Start background workers
	go tracker.retryManager.Run(tracker.shutdown)
	go tracker.cleanupWorker.Run(tracker.shutdown)

	// Recover persisted states
	if err := tracker.recoverPersistedStates(); err != nil {
		return nil, fmt.Errorf("failed to recover persisted states: %w", err)
	}

	return tracker, nil
}

// TrackMessage starts tracking acknowledgments for a message
func (t *Tracker) TrackMessage(ctx context.Context, msgID string, destinations []string) (*AckHandle, error) {
	if len(destinations) == 0 {
		return nil, errors.New("no destinations provided")
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if already tracking this message
	if _, exists := t.acks[msgID]; exists {
		return nil, fmt.Errorf("already tracking message %s", msgID)
	}

	// Create new ACK state
	now := time.Now()
	destinationMap := make(map[string]DeliveryStatus, len(destinations))
	for _, dest := range destinations {
		destinationMap[dest] = StatusPending
	}

	ackState := &AckState{
		MessageID:    msgID,
		Destinations: destinationMap,
		CreatedAt:    now,
		LastAttempt:  now,
		RetryCount:   0,
		CompleteChan: make(chan struct{}),
		Callbacks:    []AckCallback{},
	}

	// Store in memory
	t.acks[msgID] = ackState

	// Persist
	start := time.Now()
	if err := t.persistence.Store(ackState); err != nil {
		log.Printf("[Tracker] Error persisting ACK state: %v", err)
	}
	t.metrics.recordPersistenceLatency(time.Since(start))
	t.metrics.incrementStorageOps()
	t.metrics.incrementPendingAcks()

	// Return handle
	return &AckHandle{
		msgID:    msgID,
		tracker:  t,
		complete: ackState.CompleteChan,
	}, nil
}

// RecordAck records an acknowledgment from a node
func (t *Tracker) RecordAck(msgID string, nodeID string) error {
	start := time.Now()
	defer func() {
		t.metrics.recordAckLatency(time.Since(start))
	}()

	t.mu.RLock()
	ackState, exists := t.acks[msgID]
	t.mu.RUnlock()

	if !exists {
		return fmt.Errorf("message %s not being tracked", msgID)
	}

	ackState.mu.Lock()
	defer ackState.mu.Unlock()

	// Check if this node is a valid destination
	if _, ok := ackState.Destinations[nodeID]; !ok {
		return fmt.Errorf("node %s is not a destination for message %s", nodeID, msgID)
	}

	// Update status
	ackState.Destinations[nodeID] = StatusDelivered
	ackState.LastAttempt = time.Now()

	// Update metrics
	t.metrics.recordNodeSuccess(nodeID)

	// Persist updated state
	start = time.Now()
	if err := t.persistence.Store(ackState); err != nil {
		log.Printf("[Tracker] Error persisting ACK state: %v", err)
	}
	t.metrics.recordPersistenceLatency(time.Since(start))
	t.metrics.incrementStorageOps()

	// Check if complete
	if t.isCompleteInternal(ackState) {
		// Mark as complete
		close(ackState.CompleteChan)
		t.metrics.decrementPendingAcks()
		t.metrics.incrementCompletedAcks()

		// Call callbacks
		for _, callback := range ackState.Callbacks {
			go callback(msgID, nodeID, StatusDelivered)
		}

		// Remove from tracker (async to avoid blocking this call)
		go func() {
			t.mu.Lock()
			delete(t.acks, msgID)
			t.mu.Unlock()

			// Delete from persistence
			if err := t.persistence.Delete(msgID); err != nil {
				log.Printf("[Tracker] Error deleting completed ACK: %v", err)
			}
		}()
	} else {
		// Call callbacks for this node
		for _, callback := range ackState.Callbacks {
			go callback(msgID, nodeID, StatusDelivered)
		}
	}

	return nil
}

// RecordFailure records a delivery failure to a node
func (t *Tracker) RecordFailure(msgID string, nodeID string) error {
	t.mu.RLock()
	ackState, exists := t.acks[msgID]
	t.mu.RUnlock()

	if !exists {
		return fmt.Errorf("message %s not being tracked", msgID)
	}

	ackState.mu.Lock()
	defer ackState.mu.Unlock()

	// Check if this node is a valid destination
	if _, ok := ackState.Destinations[nodeID]; !ok {
		return fmt.Errorf("node %s is not a destination for message %s", nodeID, msgID)
	}

	// Update status
	ackState.Destinations[nodeID] = StatusFailed
	ackState.LastAttempt = time.Now()
	ackState.RetryCount++

	// Update metrics
	t.metrics.recordNodeFailure(nodeID)

	// Persist updated state
	start := time.Now()
	if err := t.persistence.Store(ackState); err != nil {
		log.Printf("[Tracker] Error persisting ACK state: %v", err)
	}
	t.metrics.recordPersistenceLatency(time.Since(start))
	t.metrics.incrementStorageOps()

	// Schedule retry if we haven't exceeded max retries
	if ackState.RetryCount < t.config.MaxRetryCount {
		backoff := t.calculateBackoff(ackState.RetryCount)
		t.retryManager.ScheduleRetryWithDelay(msgID, nodeID, backoff)
		t.metrics.incrementRetriesIssued()
	} else {
		// We've exceeded max retries
		t.metrics.incrementFailedAcks()

		// Call callbacks for this node
		for _, callback := range ackState.Callbacks {
			go callback(msgID, nodeID, StatusFailed)
		}
	}

	return nil
}

// RegisterCallback registers a callback function for acknowledgment notifications
func (t *Tracker) RegisterCallback(msgID string, callback AckCallback) error {
	t.mu.RLock()
	ackState, exists := t.acks[msgID]
	t.mu.RUnlock()

	if !exists {
		return fmt.Errorf("message %s not being tracked", msgID)
	}

	ackState.mu.Lock()
	defer ackState.mu.Unlock()

	ackState.Callbacks = append(ackState.Callbacks, callback)
	return nil
}

// IsComplete checks if a message has been fully acknowledged
func (t *Tracker) IsComplete(msgID string) (bool, error) {
	t.mu.RLock()
	ackState, exists := t.acks[msgID]
	t.mu.RUnlock()

	if !exists {
		// Check persistence for completed ACKs that might have been removed from memory
		persisted, err := t.persistence.Load(msgID)
		if err != nil {
			return false, fmt.Errorf("error checking persistence for message: %w", err)
		}
		if persisted == nil {
			return false, fmt.Errorf("message %s not being tracked", msgID)
		}
		return t.isCompleteInternal(persisted), nil
	}

	ackState.mu.RLock()
	defer ackState.mu.RUnlock()
	return t.isCompleteInternal(ackState), nil
}

// isCompleteInternal checks if all destinations have acknowledged
func (t *Tracker) isCompleteInternal(state *AckState) bool {
	for _, status := range state.Destinations {
		if status != StatusDelivered {
			return false
		}
	}
	return true
}

// calculateBackoff computes the backoff time based on retry count
func (t *Tracker) calculateBackoff(retryCount int) time.Duration {
	// Exponential backoff with jitter
	backoff := t.config.RetryInterval
	for i := 0; i < retryCount; i++ {
		backoff *= 2
	}

	// Add jitter (±20%)
	jitterFactor := 0.8 + 0.4*rand.Float64()
	return time.Duration(float64(backoff) * jitterFactor)
}

// Shutdown gracefully stops the tracker and its components
func (t *Tracker) Shutdown(ctx context.Context) error {
	// Signal workers to stop
	close(t.shutdown)

	// Persist all pending ACKs one last time
	t.persistAllStates()

	// Close persistence
	return t.persistence.Close()
}

// recoverPersistedStates loads and recovers persisted ACK states
func (t *Tracker) recoverPersistedStates() error {
	states, err := t.persistence.LoadAll()
	if err != nil {
		return fmt.Errorf("failed to load persisted states: %w", err)
	}

	log.Printf("[Tracker] Recovering %d persisted ACK states", len(states))

	t.mu.Lock()
	defer t.mu.Unlock()

	for _, state := range states {
		// Recreate channels and mutexes that weren't persisted
		state.CompleteChan = make(chan struct{})

		// Check if already complete
		if t.isCompleteInternal(state) {
			close(state.CompleteChan)
			// Delete complete states
			if err := t.persistence.Delete(state.MessageID); err != nil {
				log.Printf("[Tracker] Error deleting completed ACK during recovery: %v", err)
			}
			continue
		}

		// Store in memory
		t.acks[state.MessageID] = state
		t.metrics.incrementPendingAcks()

		// Schedule retries for failed destinations
		for nodeID, status := range state.Destinations {
			if status == StatusFailed {
				backoff := t.calculateBackoff(state.RetryCount)
				t.retryManager.ScheduleRetryWithDelay(state.MessageID, nodeID, backoff)
				t.metrics.incrementRetriesIssued()
			}
		}
	}

	return nil
}

// persistAllStates persists all in-memory ACK states
func (t *Tracker) persistAllStates() {
	t.mu.RLock()
	states := make([]*AckState, 0, len(t.acks))
	for _, state := range t.acks {
		states = append(states, state)
	}
	t.mu.RUnlock()

	if len(states) > 0 {
		start := time.Now()
		if err := t.persistence.StoreAll(states); err != nil {
			log.Printf("[Tracker] Error persisting all ACK states: %v", err)
		}
		t.metrics.recordPersistenceLatency(time.Since(start))
		t.metrics.incrementStorageOps()
	}
}

// getAckStatesSnapshot returns a snapshot of all ACK states
// Used by cleanup worker
func (t *Tracker) getAckStatesSnapshot() map[string]*AckState {
	t.mu.RLock()
	defer t.mu.RUnlock()

	snapshot := make(map[string]*AckState, len(t.acks))
	for msgID, state := range t.acks {
		snapshot[msgID] = state
	}
	return snapshot
}

// removeAck removes an ACK from the tracker
// Used by cleanup worker
func (t *Tracker) removeAck(msgID string) {
	t.mu.Lock()
	delete(t.acks, msgID)
	t.mu.Unlock()
}

// GetMetrics returns all metrics
func (t *Tracker) GetMetrics() map[string]interface{} {
	return t.metrics.getAll()
}

// WaitForCompletion waits for the acknowledgment to complete
func (h *AckHandle) WaitForCompletion(ctx context.Context) error {
	select {
	case <-h.complete:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel cancels tracking for this message
func (h *AckHandle) Cancel() error {
	h.tracker.removeAck(h.msgID)
	return nil
}
