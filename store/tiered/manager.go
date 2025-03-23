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

// cleanupExpiredMessages checks for and removes messages with expired TTLs
func (m *Manager) cleanupExpiredMessages() {
	now := time.Now()
	deleted := 0

	// Check disk store for expired messages
	if m.diskStore != nil {
		// Get all topic IDs
		topicIDs, err := m.diskStore.ListAllTopicIDs()
		if err != nil {
			log.Printf("Failed to list topics for TTL cleanup: %v", err)
			return
		}

		// Collect messages to delete
		var messagesToDelete []string

		// Check messages in each topic
		for _, topicInfo := range topicIDs {
			for _, msgID := range topicInfo.IDs {
				// Get the message
				msg, err := m.diskStore.Retrieve(msgID)
				if err != nil {
					continue
				}

				// Check if TTL is set and expired
				if msg.TTL > 0 && msg.Timestamp.Add(time.Duration(msg.TTL)*time.Second).Before(now) {
					messagesToDelete = append(messagesToDelete, msgID)
				}
			}
		}

		// Delete all expired messages
		if len(messagesToDelete) > 0 {
			if err := m.diskStore.DeleteMessages(messagesToDelete); err != nil {
				log.Printf("Failed to delete expired messages: %v", err)
			} else {
				atomic.AddUint64(&m.metrics.messagesOnDisk, ^uint64(0)&uint64(len(messagesToDelete)))
				deleted += len(messagesToDelete)
				log.Printf("TTL cleanup: Deleted %d expired messages from disk", len(messagesToDelete))
			}
		}
	}

	// Also check memory store
	if m.memoryStore != nil {
		// We can't enumerate all messages in the ring buffer,
		// but we'll check old messages that would be moved to disk anyway
		oldMessages, err := m.memoryStore.GetOldestMessages(1000, 0) // Get oldest without time filter
		if err == nil && len(oldMessages) > 0 {
			for _, msg := range oldMessages {
				// Check if TTL is set and expired
				if msg.TTL > 0 && msg.Timestamp.Add(time.Duration(msg.TTL)*time.Second).Before(now) {
					// Delete the message
					if m.memoryStore.Delete(msg.ID) {
						atomic.AddUint64(&m.metrics.messagesInMemory, ^uint64(0)) // Decrement
						deleted++
					}
				}
			}
		}
	}

	if deleted > 0 {
		log.Printf("TTL cleanup: Deleted %d expired messages total", deleted)
	}
}

// archiveLoop periodically checks for messages to archive
func (m *Manager) archiveLoop() {
	defer close(m.done)

	// Use default interval if not set or negative
	interval := m.policy.ArchiveInterval
	if interval <= 0 {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			// Final cleanup before exit
			m.cleanupExpiredMessages()
			m.archiveOldMessages()
			return
		case <-ticker.C:
			// First clean up expired messages based on TTL
			m.cleanupExpiredMessages()
			// Then perform regular archival based on retention
			m.archiveOldMessages()
		}
	}
}

// archiveOldMessages archives old messages based on policy
func (m *Manager) archiveOldMessages() {
	// Process TTL cleanup immediately
	now := time.Now()

	// Process memory to disk archival
	if m.memoryStore != nil && m.diskStore != nil {
		oldMemoryMessages, err := m.memoryStore.GetOldestMessages(1000, m.policy.MemoryRetention)
		if err != nil {
			log.Printf("Failed to get old memory messages: %v", err)
		} else if len(oldMemoryMessages) > 0 {
			log.Printf("Moving %d old messages from memory to disk", len(oldMemoryMessages))

			for _, msg := range oldMemoryMessages {
				// Skip any messages that have expired based on TTL
				if msg.TTL > 0 && msg.Timestamp.Add(time.Duration(msg.TTL)*time.Second).Before(now) {
					// Delete expired message
					m.memoryStore.Delete(msg.ID)
					atomic.AddUint64(&m.metrics.messagesInMemory, ^uint64(0)) // Decrement
					continue
				}

				// Store on disk
				if err := m.diskStore.Store(msg); err != nil {
					log.Printf("Failed to move message %s to disk: %v", msg.ID, err)
					continue
				}

				// Delete from memory
				m.memoryStore.Delete(msg.ID)

				// Update metrics
				atomic.AddUint64(&m.metrics.memoryOffloads, 1)
			}
		}
	}

	// Process disk to cloud archival
	if m.diskStore != nil && m.cloudStore != nil {
		// List all topic IDs from disk
		topicIDs, err := m.diskStore.ListAllTopicIDs()
		if err != nil {
			log.Printf("Failed to list topic IDs for archival: %v", err)
			return
		}

		diskRetentionCutoff := now.Add(-m.policy.DiskRetention)
		messagesToMove := make(map[string][]Message)
		messagesToDelete := make([]string, 0)

		// Find messages to archive by topic
		for _, topicInfo := range topicIDs {
			for _, msgID := range topicInfo.IDs {
				msg, err := m.diskStore.Retrieve(msgID)
				if err != nil {
					continue
				}

				// First check TTL
				if msg.TTL > 0 && msg.Timestamp.Add(time.Duration(msg.TTL)*time.Second).Before(now) {
					// Message is expired, add to delete list
					messagesToDelete = append(messagesToDelete, msgID)
					continue
				}

				// Check if message exceeds disk retention
				if msg.Timestamp.Before(diskRetentionCutoff) {
					// Message is older than retention period, add to move list
					messagesToMove[topicInfo.Topic] = append(messagesToMove[topicInfo.Topic], *msg)
					messagesToDelete = append(messagesToDelete, msgID)
				}
			}
		}

		// First process TTL-expired messages
		if len(messagesToDelete) > 0 {
			if err := m.diskStore.DeleteMessages(messagesToDelete); err != nil {
				log.Printf("Failed to delete old/expired messages: %v", err)
			} else {
				atomic.AddUint64(&m.metrics.messagesOnDisk, ^uint64(0)&uint64(len(messagesToDelete)))
				log.Printf("Deleted %d old/expired messages from disk", len(messagesToDelete))
			}
		}

		// Process topic by topic for cloud storage
		moveCount := 0
		for topic, messages := range messagesToMove {
			if len(messages) == 0 {
				continue
			}

			// First store in cloud
			if err := m.cloudStore.Store(topic, messages); err != nil {
				log.Printf("Failed to archive messages for topic %s: %v", topic, err)
				continue
			}

			// Update metrics
			atomic.AddUint64(&m.metrics.messagesInCloud, uint64(len(messages)))
			atomic.AddUint64(&m.metrics.diskOffloads, uint64(len(messages)))
			moveCount += len(messages)
		}

		if moveCount > 0 {
			log.Printf("Moved %d messages from disk to cloud", moveCount)
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

	// First clean up expired messages
	m.cleanupExpiredMessages()

	// Then perform regular archival
	m.archiveOldMessages()

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

// GetCloudTopics returns the list of topics stored in the cloud tier
func (m *Manager) GetCloudTopics() ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.cloudStore == nil {
		return nil, fmt.Errorf("cloud storage not enabled")
	}

	// Use the improved topic listing method from cloud store
	topics, err := m.cloudStore.ListTopics()
	if err != nil {
		return nil, fmt.Errorf("failed to list topics from cloud store: %w", err)
	}

	return topics, nil
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
