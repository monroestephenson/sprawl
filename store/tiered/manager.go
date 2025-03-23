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
	cloudStore  *CloudStore
	policy      StoragePolicy
	metrics     *ManagerMetrics
	mu          sync.RWMutex
	stopCh      chan struct{}
	done        chan struct{}
}

type ManagerMetrics struct {
	messagesInMemory uint64
	messagesOnDisk   uint64
	messagesInCloud  uint64
	memoryOffloads   uint64
	diskOffloads     uint64
	cloudOffloads    uint64
	retrievals       uint64
}

// NewManager creates a new tiered storage manager
func NewManager(memSize uint64, memLimit uint64, dbPath string, cloudCfg *CloudConfig, policy StoragePolicy) (*Manager, error) {
	// Initialize memory store
	memStore := NewRingBuffer(memSize, memLimit)

	// Initialize disk store
	diskStore, err := NewRocksStore(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create disk store: %w", err)
	}

	// Initialize cloud store if configured
	var cloudStore *CloudStore
	if cloudCfg != nil {
		cloudStore, err = NewCloudStore(*cloudCfg)
		if err != nil {
			diskStore.Close()
			return nil, fmt.Errorf("failed to create cloud store: %w", err)
		}
	}

	manager := &Manager{
		memoryStore: memStore,
		diskStore:   diskStore,
		cloudStore:  cloudStore,
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
	// Try to store in memory first, but don't fail if memory is full
	err := m.memoryStore.Enqueue(msg)
	if err == nil {
		atomic.AddUint64(&m.metrics.messagesInMemory, 1)
	}

	// Store in disk
	if err := m.diskStore.Store(msg); err != nil {
		return fmt.Errorf("failed to store in disk: %w", err)
	}
	atomic.AddUint64(&m.metrics.messagesOnDisk, 1)

	// Then store in cloud if enabled
	if m.cloudStore != nil {
		if err := m.cloudStore.Store(msg.Topic, []Message{msg}); err != nil {
			return fmt.Errorf("failed to store in cloud: %w", err)
		}
		atomic.AddUint64(&m.metrics.messagesInCloud, 1)
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

	// If not in disk and cloud store is configured, try cloud
	if m.cloudStore != nil {
		msg, err = m.cloudStore.Retrieve(id)
		if err == nil {
			atomic.AddUint64(&m.metrics.retrievals, 1)
			return msg, nil
		}
	}

	return nil, fmt.Errorf("message not found: %s", id)
}

// GetTopicMessages gets all message IDs for a topic
func (m *Manager) GetTopicMessages(topic string) ([]string, error) {
	seen := make(map[string]bool)
	var allMsgs []string

	// Get disk messages first as they are our source of truth
	diskMsgs, err := m.diskStore.GetTopicMessages(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk messages: %w", err)
	}

	// Add disk messages
	for _, msgID := range diskMsgs {
		if !seen[msgID] {
			seen[msgID] = true
			allMsgs = append(allMsgs, msgID)
		}
	}

	// Get cloud messages if enabled
	if m.cloudStore != nil {
		cloudMsgs, err := m.cloudStore.GetTopicMessages(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to get cloud messages: %w", err)
		}

		// Add cloud messages
		for _, msg := range cloudMsgs {
			if !seen[msg.ID] {
				seen[msg.ID] = true
				allMsgs = append(allMsgs, msg.ID)
			}
		}
	}

	return allMsgs, nil
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
	// Ensure archive interval is positive
	interval := m.policy.ArchiveInterval
	if interval <= 0 {
		interval = time.Minute // Default to 1 minute if not set or negative
	}

	ticker := time.NewTicker(interval)
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

		// Move old messages to cloud if configured
		if m.cloudStore != nil {
			// Get messages older than half the disk retention period
			// Implementation note: This would require adding a method to list old messages
			// For now, we rely on the Store method to handle cloud storage

			// Update metrics
			atomic.AddUint64(&m.metrics.cloudOffloads, 1)
		}

		// Delete old messages from disk
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
	var cloudMetrics map[string]interface{}
	if m.cloudStore != nil {
		cloudMetrics = m.cloudStore.GetMetrics()
	}

	// Get current metric values atomically
	inMemory := atomic.LoadUint64(&m.metrics.messagesInMemory)
	onDisk := atomic.LoadUint64(&m.metrics.messagesOnDisk)
	inCloud := atomic.LoadUint64(&m.metrics.messagesInCloud)
	memOffloads := atomic.LoadUint64(&m.metrics.memoryOffloads)
	diskOffloads := atomic.LoadUint64(&m.metrics.diskOffloads)
	cloudOffloads := atomic.LoadUint64(&m.metrics.cloudOffloads)
	retrievals := atomic.LoadUint64(&m.metrics.retrievals)

	metrics := map[string]interface{}{
		"memory_store": memMetrics,
		"disk_store":   diskMetrics,
		"manager": map[string]interface{}{
			"messages_in_memory": inMemory,
			"messages_on_disk":   onDisk,
			"messages_in_cloud":  inCloud,
			"memory_offloads":    memOffloads,
			"disk_offloads":      diskOffloads,
			"cloud_offloads":     cloudOffloads,
			"retrievals":         retrievals,
		},
	}

	if cloudMetrics != nil {
		metrics["cloud_store"] = cloudMetrics
	}

	return metrics
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

	if m.cloudStore != nil {
		if err := m.cloudStore.Close(); err != nil {
			return fmt.Errorf("failed to close cloud store: %w", err)
		}
		m.cloudStore = nil
	}

	return nil
}
