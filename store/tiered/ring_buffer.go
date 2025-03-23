package tiered

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Message represents a message in the ring buffer
type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
	TTL       int
}

// RingBuffer implements a thread-safe circular buffer for messages
type RingBuffer struct {
	buffer   []Message
	head     uint64 // Read position
	tail     uint64 // Write position
	size     uint64 // Size of the buffer
	count    uint64 // Number of items in the buffer
	mu       sync.RWMutex
	notEmpty *sync.Cond
	notFull  *sync.Cond

	// Memory management
	memoryLimit uint64 // Maximum memory usage in bytes
	currentMem  uint64 // Current memory usage in bytes

	// Metrics
	messagesDropped uint64
	totalEnqueued   uint64
	totalDequeued   uint64

	// Pressure monitoring
	pressureLevel    atomic.Int32 // 0: Normal, 1: High, 2: Critical
	pressureCallback func(int32)  // Callback for pressure level changes
	memCheckInterval time.Duration
	stopMemCheck     chan struct{}
	closed           atomic.Bool // Indicates if the buffer is closed
}

// NewRingBuffer creates a new ring buffer with the specified size and memory limit
func NewRingBuffer(size uint64, memoryLimit uint64) *RingBuffer {
	rb := &RingBuffer{
		buffer:           make([]Message, size),
		size:             size,
		memoryLimit:      memoryLimit,
		memCheckInterval: time.Second,
		stopMemCheck:     make(chan struct{}),
	}
	rb.notEmpty = sync.NewCond(&rb.mu)
	rb.notFull = sync.NewCond(&rb.mu)

	// Start memory pressure monitoring
	go rb.monitorMemoryPressure()

	return rb
}

// Enqueue adds a message to the ring buffer
func (rb *RingBuffer) Enqueue(msg Message) error {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Calculate message size
	msgSize := uint64(len(msg.Payload) + len(msg.Topic) + 40) // Base struct size + payload + topic

	// Check memory limit
	if atomic.LoadUint64(&rb.currentMem)+msgSize > rb.memoryLimit {
		atomic.AddUint64(&rb.messagesDropped, 1)
		// Update pressure immediately when hitting memory limit
		rb.updatePressureLevel()
		return errors.New("memory limit exceeded")
	}

	// Check if buffer is full
	if rb.count >= rb.size {
		// Wait for space if pressure is not critical
		if rb.pressureLevel.Load() < 2 {
			rb.notFull.Wait()
		} else {
			atomic.AddUint64(&rb.messagesDropped, 1)
			return errors.New("buffer full and under critical pressure")
		}
	}

	// Add message to buffer
	rb.buffer[rb.tail%rb.size] = msg
	rb.tail++
	rb.count++
	atomic.AddUint64(&rb.currentMem, msgSize)
	atomic.AddUint64(&rb.totalEnqueued, 1)

	// Update pressure level after adding message
	rb.updatePressureLevel()

	// Signal that buffer is not empty
	rb.notEmpty.Signal()

	return nil
}

// Dequeue removes and returns a message from the ring buffer
func (rb *RingBuffer) Dequeue() (Message, error) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	// Wait for messages if buffer is empty
	for rb.count == 0 {
		if rb.closed.Load() {
			return Message{}, errors.New("buffer is closed")
		}
		rb.notEmpty.Wait()
		if rb.closed.Load() {
			return Message{}, errors.New("buffer is closed")
		}
	}

	msg := rb.buffer[rb.head%rb.size]
	rb.head++
	rb.count--

	// Update memory usage
	msgSize := uint64(len(msg.Payload) + len(msg.Topic) + 40)
	atomic.AddUint64(&rb.currentMem, ^(msgSize - 1)) // Subtract msgSize
	atomic.AddUint64(&rb.totalDequeued, 1)

	// Signal that buffer is not full
	rb.notFull.Signal()

	return msg, nil
}

// monitorMemoryPressure continuously monitors memory usage
func (rb *RingBuffer) monitorMemoryPressure() {
	ticker := time.NewTicker(rb.memCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rb.stopMemCheck:
			return
		case <-ticker.C:
			rb.updatePressureLevel()
		}
	}
}

// updatePressureLevel updates the current memory pressure level
func (rb *RingBuffer) updatePressureLevel() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate pressure based on both ring buffer and system memory
	bufferUsage := float64(atomic.LoadUint64(&rb.currentMem)) / float64(rb.memoryLimit)

	var newLevel int32
	if bufferUsage > 0.9 {
		newLevel = 2 // Critical
	} else if bufferUsage > 0.7 {
		newLevel = 1 // High
	}

	// Update pressure level if changed
	oldLevel := rb.pressureLevel.Load()
	if oldLevel != newLevel {
		rb.pressureLevel.Store(newLevel)
		if rb.pressureCallback != nil {
			rb.pressureCallback(newLevel)
		}
	}
}

// GetMetrics returns current metrics
func (rb *RingBuffer) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"size":             rb.size,
		"count":            atomic.LoadUint64(&rb.count),
		"memory_usage":     atomic.LoadUint64(&rb.currentMem),
		"memory_limit":     rb.memoryLimit,
		"dropped_messages": atomic.LoadUint64(&rb.messagesDropped),
		"total_enqueued":   atomic.LoadUint64(&rb.totalEnqueued),
		"total_dequeued":   atomic.LoadUint64(&rb.totalDequeued),
		"pressure_level":   rb.pressureLevel.Load(),
	}
}

// SetPressureCallback sets the callback for pressure level changes
func (rb *RingBuffer) SetPressureCallback(cb func(int32)) {
	rb.pressureCallback = cb
}

// Close stops the memory pressure monitoring and signals waiting consumers
func (rb *RingBuffer) Close() {
	if rb.closed.CompareAndSwap(false, true) {
		close(rb.stopMemCheck)
	}

	// Wake up any waiting consumers
	rb.mu.Lock()
	rb.notEmpty.Broadcast()
	rb.mu.Unlock()
}

// GetOldestMessages retrieves the oldest messages from the buffer
// up to the specified count and older than the retention period
func (rb *RingBuffer) GetOldestMessages(count int, retention time.Duration) ([]Message, error) {
	if count <= 0 {
		return nil, errors.New("count must be positive")
	}

	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return []Message{}, nil
	}

	// Calculate the cutoff time
	cutoff := time.Now().Add(-retention)

	// Collect oldest messages
	var oldMessages []Message
	currentPos := rb.head
	remaining := min(uint64(count), rb.count)

	for remaining > 0 {
		// Get message at current position
		msg := rb.buffer[currentPos%rb.size]

		// Check if it's older than the retention period
		if msg.ID != "" && msg.Timestamp.Before(cutoff) {
			// Make a copy of the message to avoid race conditions
			oldMessages = append(oldMessages, Message{
				ID:        msg.ID,
				Topic:     msg.Topic,
				Payload:   append([]byte(nil), msg.Payload...),
				Timestamp: msg.Timestamp,
				TTL:       msg.TTL,
			})
		}

		// Move to next position
		currentPos++
		remaining--
	}

	return oldMessages, nil
}

// Delete removes a message from the buffer by ID
// Returns true if message was found and deleted, false otherwise
func (rb *RingBuffer) Delete(id string) bool {
	if id == "" {
		return false
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	if rb.count == 0 {
		return false
	}

	// Search for message with given ID
	currentPos := rb.head
	found := false
	var foundPos uint64

	// Scan buffer to find the message
	for i := uint64(0); i < rb.count; i++ {
		pos := currentPos % rb.size
		if rb.buffer[pos].ID == id {
			found = true
			foundPos = pos
			break
		}
		currentPos++
	}

	if !found {
		return false
	}

	// Mark the message as deleted by clearing its ID
	// This is more efficient than physically removing it
	msgSize := uint64(len(rb.buffer[foundPos].Payload))

	// Update memory usage
	if rb.currentMem >= msgSize {
		rb.currentMem -= msgSize
	}

	// Clear the message
	rb.buffer[foundPos] = Message{}

	// Note: We don't decrement count since the slot is still occupied
	// It will be reused when tail wraps around

	return true
}
