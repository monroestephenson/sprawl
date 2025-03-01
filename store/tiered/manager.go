package tiered

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// StorageLevel represents different storage tiers
type StorageLevel int

const (
	MemoryLevel StorageLevel = iota
	DiskLevel
	CloudLevel
)

// StoragePolicy defines when to move messages between tiers
type StoragePolicy struct {
	MemoryRetention time.Duration // How long to keep in memory
	DiskRetention   time.Duration // How long to keep on disk
	CloudRetention  time.Duration // How long to keep in cloud
	MemoryThreshold float64       // Memory usage threshold to trigger disk offload
	DiskThreshold   float64       // Disk usage threshold to trigger cloud offload
	BatchSize       int           // Number of messages to move in one batch
	ArchiveInterval time.Duration // How often to check for messages to archive
}

// Manager coordinates different storage tiers
type Manager struct {
	memoryStore *RingBuffer
	diskStore   *RocksStore
	policy      StoragePolicy
	metrics     *ManagerMetrics
	mu          sync.RWMutex
	stopCh      chan struct{}
	done        chan struct{}
}

type ManagerMetrics struct {
	messagesInMemory uint64
	messagesOnDisk   uint64
	memoryOffloads   uint64
	diskOffloads     uint64
	retrievals       uint64
}

// NewManager creates a new tiered storage manager
func NewManager(memSize uint64, memLimit uint64, dbPath string, policy StoragePolicy) (*Manager, error) {
	// Initialize memory store
	memStore := NewRingBuffer(memSize, memLimit)

	// Initialize disk store
	diskStore, err := NewRocksStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk store: %w", err)
	}

	manager := &Manager{
		memoryStore: memStore,
		diskStore:   diskStore,
		policy:      policy,
		metrics:     &ManagerMetrics{},
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}

	// Set memory pressure callback
	memStore.SetPressureCallback(manager.handleMemoryPressure)

	// Start archival process
	go manager.archiveLoop()

	return manager, nil
}

// Store stores a message in the appropriate tier
func (m *Manager) Store(msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Always store to disk first for persistence
	if err := m.diskStore.Store(msg); err != nil {
		return fmt.Errorf("failed to store message on disk: %w", err)
	}
	atomic.AddUint64(&m.metrics.messagesOnDisk, 1)

	// Then try to store in memory if there's space
	err := m.memoryStore.Enqueue(msg)
	if err == nil {
		atomic.AddUint64(&m.metrics.messagesInMemory, 1)
	}

	return nil
}

// Retrieve gets a message from the appropriate tier
func (m *Manager) Retrieve(id string) (*Message, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Try disk store first since it's our source of truth
	msg, err := m.diskStore.Retrieve(id)
	if err == nil {
		atomic.AddUint64(&m.metrics.retrievals, 1)
		return msg, nil
	}

	// If not found in disk store, return error
	return nil, fmt.Errorf("message not found: %s", id)
}

// GetTopicMessages gets all message IDs for a topic
func (m *Manager) GetTopicMessages(topic string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// For now, we only check disk store as it's our source of truth
	return m.diskStore.GetTopicMessages(topic)
}

// handleMemoryPressure handles memory pressure by moving messages to disk
func (m *Manager) handleMemoryPressure(level int32) {
	if level < 1 {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Move messages to disk in batches
	moved := 0
	for moved < m.policy.BatchSize {
		_, err := m.memoryStore.Dequeue()
		if err != nil {
			break
		}

		moved++
		atomic.AddUint64(&m.metrics.messagesInMemory, ^uint64(0)) // Decrement
		atomic.AddUint64(&m.metrics.memoryOffloads, 1)
	}
}

// archiveLoop periodically checks for messages to archive
func (m *Manager) archiveLoop() {
	ticker := time.NewTicker(m.policy.ArchiveInterval)
	defer ticker.Stop()
	defer close(m.done)

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.archiveOldMessages()
		}
	}
}

// archiveOldMessages moves old messages between tiers based on policy
func (m *Manager) archiveOldMessages() {
	// Don't hold the main lock during the entire archival process
	if m.policy.DiskRetention > 0 {
		// Use a read lock to check if we should proceed
		m.mu.RLock()
		if m.diskStore == nil {
			m.mu.RUnlock()
			return
		}
		m.mu.RUnlock()

		// Delete old messages without holding the main lock
		if err := m.diskStore.DeleteOldMessages(m.policy.DiskRetention); err != nil {
			// Log error but continue
			fmt.Printf("Failed to delete old messages: %v\n", err)
		}
	}
}

// GetMetrics returns current metrics
func (m *Manager) GetMetrics() map[string]interface{} {
	// Get store metrics
	memMetrics := m.memoryStore.GetMetrics()
	diskMetrics := m.diskStore.GetMetrics()

	// Get current metric values atomically
	inMemory := atomic.LoadUint64(&m.metrics.messagesInMemory)
	onDisk := atomic.LoadUint64(&m.metrics.messagesOnDisk)
	memOffloads := atomic.LoadUint64(&m.metrics.memoryOffloads)
	diskOffloads := atomic.LoadUint64(&m.metrics.diskOffloads)
	retrievals := atomic.LoadUint64(&m.metrics.retrievals)

	return map[string]interface{}{
		"memory_store": memMetrics,
		"disk_store":   diskMetrics,
		"manager": map[string]interface{}{
			"messages_in_memory": inMemory,
			"messages_on_disk":   onDisk,
			"memory_offloads":    memOffloads,
			"disk_offloads":      diskOffloads,
			"retrievals":         retrievals,
		},
	}
}

// Close closes all storage tiers
func (m *Manager) Close() error {
	// Signal archival loop to stop
	close(m.stopCh)

	// Wait for archival loop to finish
	<-m.done

	// Acquire lock for final cleanup
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close stores
	if m.memoryStore != nil {
		m.memoryStore.Close()
		m.memoryStore = nil
	}

	if m.diskStore != nil {
		m.diskStore.Close()
		m.diskStore = nil
	}

	return nil
}
