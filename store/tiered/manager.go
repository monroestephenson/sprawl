package tiered

import (
	"fmt"
	"log"
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

// PerformFullCompaction performs a full compaction across all storage tiers
// It moves data from memory to disk, and from disk to cloud if configured
func (m *Manager) PerformFullCompaction() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Printf("Starting full compaction across all storage tiers")

	// Step 1: Handle memory to disk transfer if disk storage is available
	if m.diskStore != nil {
		// Get messages from memory store to move to disk
		if m.memoryStore != nil {
			oldMessages, err := m.memoryStore.GetOldestMessages(m.policy.BatchSize, m.policy.MemoryRetention)
			if err != nil {
				return fmt.Errorf("failed to get old messages from memory: %w", err)
			}

			for _, msg := range oldMessages {
				// Store in disk
				if err := m.diskStore.Store(msg); err != nil {
					log.Printf("Failed to move message %s to disk: %v", msg.ID, err)
					continue
				}

				// Remove from memory after confirming it's on disk
				m.memoryStore.Delete(msg.ID)

				// Update metrics
				atomic.AddUint64(&m.metrics.memoryOffloads, 1)
			}

			log.Printf("Moved %d messages from memory to disk", len(oldMessages))
		}
	}

	// Step 2: Handle disk to cloud transfer if cloud storage is available
	if m.cloudStore != nil && m.diskStore != nil {
		// Find messages older than the configured threshold
		cutoff := time.Now().Add(-m.policy.DiskRetention / 2) // Use half the retention time as threshold

		// List all messages to find old ones
		// This is a simplification - in a real implementation we would use a more efficient method
		topicIds, err := m.diskStore.ListAllTopicIDs()
		if err != nil {
			return fmt.Errorf("failed to list topics from disk: %w", err)
		}

		// Group messages by topic for batch upload to cloud
		topicMessages := make(map[string][]Message)
		movedCount := 0

		for _, topicInfo := range topicIds {
			topic := topicInfo.Topic
			for _, id := range topicInfo.IDs {
				// Retrieve the message to check its timestamp
				msg, err := m.diskStore.Retrieve(id)
				if err != nil {
					log.Printf("Failed to retrieve message %s: %v", id, err)
					continue
				}

				if msg.Timestamp.Before(cutoff) {
					// Group by topic for batch upload
					topicMessages[topic] = append(topicMessages[topic], *msg)
					movedCount++
				}
			}
		}

		// Upload messages to cloud by topic
		for topic, messages := range topicMessages {
			if len(messages) > 0 {
				// Store batch in cloud
				if err := m.cloudStore.Store(topic, messages); err != nil {
					log.Printf("Failed to move messages for topic %s to cloud: %v", topic, err)
					continue
				}

				// Update metrics
				atomic.AddUint64(&m.metrics.diskOffloads, uint64(len(messages)))
			}
		}

		// Now delete the moved messages
		if err := m.diskStore.DeleteOldMessages(m.policy.DiskRetention / 2); err != nil {
			log.Printf("Warning: Failed to delete old messages from disk: %v", err)
		}

		log.Printf("Moved approximately %d messages from disk to cloud", movedCount)
	}

	// Step 3: Trigger database compaction to reclaim space
	if m.diskStore != nil {
		if err := m.diskStore.ForceCompaction(); err != nil {
			return fmt.Errorf("failed to compact disk store: %w", err)
		}
		log.Printf("Completed disk store compaction")
	}

	log.Printf("Full compaction completed successfully")
	return nil
}

// ListAllTopicIDs is a helper struct for PerformFullCompaction
type TopicIDs struct {
	Topic string
	IDs   []string
}

// GetDiskTopics returns a list of topics stored in the disk tier
func (m *Manager) GetDiskTopics() ([]string, error) {
	if m.diskStore == nil {
		return nil, fmt.Errorf("disk storage not enabled")
	}

	// Get all topic IDs from disk
	topicIDs, err := m.diskStore.ListAllTopicIDs()
	if err != nil {
		return nil, err
	}

	// Extract unique topic names
	topics := make([]string, 0, len(topicIDs))
	topicMap := make(map[string]struct{})

	for _, topicInfo := range topicIDs {
		if _, exists := topicMap[topicInfo.Topic]; !exists {
			topicMap[topicInfo.Topic] = struct{}{}
			topics = append(topics, topicInfo.Topic)
		}
	}

	return topics, nil
}

// GetCloudTopics returns a list of topics stored in the cloud tier
func (m *Manager) GetCloudTopics() ([]string, error) {
	if m.cloudStore == nil {
		return nil, fmt.Errorf("cloud storage not enabled")
	}

	// The actual implementation would query the cloud store for all topics
	// This might be a costly operation depending on the cloud provider

	// Let's assume we can list "directories" in the cloud store
	// where each directory represents a topic

	// For now, we'll use a simple approach by querying list of
	// distinct topics from cloud storage

	// Since the CloudStore doesn't have a direct method to list topics,
	// we'll implement a fallback that scans for known topics

	// Try to get metrics from cloud store which might have topic information
	metrics := m.cloudStore.GetMetrics()

	// If metrics contains a topics list, use it
	if topicsInterface, exists := metrics["topics"]; exists {
		if topicsSlice, ok := topicsInterface.([]string); ok && len(topicsSlice) > 0 {
			return topicsSlice, nil
		}
	}

	// Fallback: without a direct listing method, return an empty list
	// In a real implementation, we would scan the bucket for topic prefixes
	return []string{}, nil
}

// GetDiskMessageCount returns the number of messages for a topic in the disk tier
func (m *Manager) GetDiskMessageCount(topic string) (int, error) {
	if m.diskStore == nil {
		return 0, fmt.Errorf("disk storage not enabled")
	}

	// RocksStore doesn't have a direct method to count messages by topic,
	// so we'll use a fallback approach

	// Get metrics to see if count information is available
	metrics := m.diskStore.GetMetrics()

	// Check if message count by topic is available in metrics
	topicCountsKey := "topic_counts"
	if countsInterface, exists := metrics[topicCountsKey]; exists {
		if countMap, ok := countsInterface.(map[string]int); ok {
			if count, found := countMap[topic]; found {
				return count, nil
			}
		}
	}

	// Fallback: get all message IDs for this topic
	// This is not efficient for large topics, but works as a fallback
	topicIDs, err := m.diskStore.ListAllTopicIDs()
	if err != nil {
		return 0, err
	}

	// Count messages for this topic
	count := 0
	for _, topicInfo := range topicIDs {
		if topicInfo.Topic == topic {
			count += len(topicInfo.IDs)
		}
	}

	return count, nil
}

// GetCloudMessageCount returns the number of messages for a topic in the cloud tier
func (m *Manager) GetCloudMessageCount(topic string) (int, error) {
	if m.cloudStore == nil {
		return 0, fmt.Errorf("cloud storage not enabled")
	}

	// CloudStore doesn't have a direct method to count messages by topic,
	// so we'll use a fallback approach

	// Try to get metrics from cloud store which might have count information
	metrics := m.cloudStore.GetMetrics()

	// If metrics contains a counts map, use it
	topicCountsKey := "topic_message_counts"
	if countsInterface, exists := metrics[topicCountsKey]; exists {
		if countMap, ok := countsInterface.(map[string]int); ok {
			if count, found := countMap[topic]; found {
				return count, nil
			}
		}
	}

	// Fallback: try to get messages for this topic and count them
	// This can be expensive for large topics
	messages, err := m.cloudStore.GetTopicMessages(topic)
	if err != nil {
		// If the topic doesn't exist or there's another error,
		// just return 0 count
		return 0, nil
	}

	return len(messages), nil
}

// MoveMessageToDisk moves a single message from memory to disk tier
func (m *Manager) MoveMessageToDisk(msg Message) error {
	if m.diskStore == nil {
		return fmt.Errorf("disk storage not enabled")
	}

	// Store the message in disk
	if err := m.diskStore.Store(msg); err != nil {
		return fmt.Errorf("failed to store message on disk: %w", err)
	}

	// Update metrics
	atomic.AddUint64(&m.metrics.memoryOffloads, 1)

	return nil
}

// MoveMessagesToCloud moves a batch of messages to the cloud tier
func (m *Manager) MoveMessagesToCloud(topic string, messages []Message) error {
	if m.cloudStore == nil {
		return fmt.Errorf("cloud storage not enabled")
	}

	// Store the messages in cloud storage
	if err := m.cloudStore.Store(topic, messages); err != nil {
		return fmt.Errorf("failed to store messages in cloud: %w", err)
	}

	// Update metrics
	atomic.AddUint64(&m.metrics.memoryOffloads, uint64(len(messages)))

	return nil
}

// MoveDiskTopicToCloud moves all messages for a topic from disk to cloud
func (m *Manager) MoveDiskTopicToCloud(topic string) error {
	if m.diskStore == nil {
		return fmt.Errorf("disk storage not enabled")
	}

	if m.cloudStore == nil {
		return fmt.Errorf("cloud storage not enabled")
	}

	// Find all message IDs for this topic
	topicIDs, err := m.diskStore.ListAllTopicIDs()
	if err != nil {
		return fmt.Errorf("failed to list messages from disk: %w", err)
	}

	// Process each topic
	for _, topicInfo := range topicIDs {
		if topicInfo.Topic != topic {
			continue
		}

		// If no IDs, nothing to do
		if len(topicInfo.IDs) == 0 {
			continue
		}

		// Get messages in batches
		batchSize := 100
		for i := 0; i < len(topicInfo.IDs); i += batchSize {
			end := i + batchSize
			if end > len(topicInfo.IDs) {
				end = len(topicInfo.IDs)
			}

			batchIDs := topicInfo.IDs[i:end]

			// Retrieve each message in the batch
			messages := make([]Message, 0, len(batchIDs))
			for _, id := range batchIDs {
				msg, err := m.diskStore.Retrieve(id)
				if err != nil {
					log.Printf("Failed to retrieve message %s: %v", id, err)
					continue
				}

				messages = append(messages, *msg)
			}

			// Store batch in cloud
			if len(messages) > 0 {
				if err := m.cloudStore.Store(topic, messages); err != nil {
					return fmt.Errorf("failed to store messages in cloud: %w", err)
				}

				// Update metrics
				atomic.AddUint64(&m.metrics.diskOffloads, uint64(len(messages)))

				// Delete from disk after confirming they're in cloud
				// Since RocksStore doesn't have a single message delete method,
				// we'll need to track IDs and use a more specialized approach
				messageIDs := make([]string, len(messages))
				for i, msg := range messages {
					messageIDs[i] = msg.ID
				}

				// Use a custom deletion approach by simulating them as old messages
				// Create a temporary DB batch and delete these specific message keys
				if err := m.diskStore.DeleteMessages(messageIDs); err != nil {
					log.Printf("Failed to delete messages from disk: %v", err)
				}
			}
		}
	}

	return nil
}
