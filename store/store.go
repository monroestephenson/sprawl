package store

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"sprawl/store/tiered"
)

// Store handles message storage and delivery
type Store struct {
	mu            sync.RWMutex
	messages      map[string][]Message
	subscribers   map[string][]SubscriberFunc
	metrics       Metrics
	tieredManager *tiered.Manager // Added tiered storage manager
	diskEnabled   bool
	cloudEnabled  bool
}

// Message represents a pub/sub message
type Message struct {
	ID        string
	Topic     string
	Payload   []byte
	Timestamp time.Time
	TTL       int
}

// SubscriberFunc is a callback function for message delivery
type SubscriberFunc func(msg Message)

// Global store instance for system-wide access
var globalStore *Store
var globalStoreMu sync.RWMutex

// topicTierMapping maps topics to their assigned storage tiers
type topicTierMapping struct {
	mu    sync.RWMutex
	tiers map[string]string // topic -> tier
}

// Initialize global tier mapping
var globalTopicTiers = &topicTierMapping{
	tiers: make(map[string]string),
}

// GetGlobalStore returns the global store instance, creating it if needed
func GetGlobalStore() *Store {
	globalStoreMu.RLock()
	if globalStore != nil {
		defer globalStoreMu.RUnlock()
		return globalStore
	}
	globalStoreMu.RUnlock()

	// Need to acquire write lock to initialize
	globalStoreMu.Lock()
	defer globalStoreMu.Unlock()

	// Double-check under write lock
	if globalStore != nil {
		return globalStore
	}

	// Initialize global store
	globalStore = NewStore()
	return globalStore
}

// NewStore creates a new Store
func NewStore() *Store {
	store := &Store{
		messages:    make(map[string][]Message),
		subscribers: make(map[string][]SubscriberFunc),
		metrics:     NewMetrics(),
	}

	// Initialize tiered storage if enabled by environment variables
	store.initTieredStorage()

	return store
}

// initTieredStorage initializes tiered storage based on environment variables
func (s *Store) initTieredStorage() {
	// Check storage type from environment variable
	storageType := os.Getenv("SPRAWL_STORAGE_TYPE")

	// Set disk enabled based on storage type
	diskEnabled := storageType == "disk" || storageType == "s3" || os.Getenv("SPRAWL_STORAGE_DISK_ENABLED") == "true" || os.Getenv("SPRAWL_DISK_STORAGE") == "true"
	s.diskEnabled = diskEnabled

	// Set cloud enabled based on storage type
	cloudEnabled := storageType == "s3" || os.Getenv("SPRAWL_STORAGE_CLOUD_ENABLED") == "true"
	s.cloudEnabled = cloudEnabled

	// If neither is enabled, return early
	if !diskEnabled && !cloudEnabled {
		log.Println("[Store] Tiered storage not enabled")
		return
	}

	// Parse memory storage settings
	memSize := uint64(104857600) // Default: 100MB
	if envSize := os.Getenv("SPRAWL_STORAGE_MEMORY_MAX_SIZE"); envSize != "" {
		if size, err := strconv.ParseUint(envSize, 10, 64); err == nil && size > 0 {
			memSize = size
		}
	}

	memToAge := int64(3600) // Default: 1 hour
	if envAge := os.Getenv("SPRAWL_STORAGE_MEMORY_TO_DISK_AGE"); envAge != "" {
		if age, err := strconv.ParseInt(envAge, 10, 64); err == nil && age > 0 {
			memToAge = age
		}
	}

	diskToAge := int64(86400) // Default: 24 hours
	if envAge := os.Getenv("SPRAWL_STORAGE_DISK_TO_CLOUD_AGE"); envAge != "" {
		if age, err := strconv.ParseInt(envAge, 10, 64); err == nil && age > 0 {
			diskToAge = age
		}
	}

	// Set up disk store path
	diskPath := "/data/rocksdb" // Default path
	if envPath := os.Getenv("SPRAWL_STORAGE_DISK_PATH"); envPath != "" {
		diskPath = envPath
	} else if envPath := os.Getenv("SPRAWL_STORAGE_PATH"); envPath != "" {
		diskPath = envPath
	}

	// Set up cloud store configuration
	var cloudCfg *tiered.CloudConfig
	if cloudEnabled {
		endpoint := os.Getenv("MINIO_ENDPOINT")
		accessKey := os.Getenv("MINIO_ACCESS_KEY")
		secretKey := os.Getenv("MINIO_SECRET_KEY")
		bucket := os.Getenv("SPRAWL_STORAGE_CLOUD_BUCKET")
		if bucket == "" {
			bucket = "sprawl-messages"
		}
		region := os.Getenv("MINIO_REGION")
		if region == "" {
			region = "us-east-1"
		}

		if endpoint != "" && accessKey != "" && secretKey != "" {
			cloudCfg = &tiered.CloudConfig{
				Endpoint:        endpoint,
				AccessKeyID:     accessKey,
				SecretAccessKey: secretKey,
				Bucket:          bucket,
				Region:          region,
				BatchSize:       100,
				BatchTimeout:    time.Second * 5,
				RetentionPeriod: time.Hour * 24 * 30, // 30 days
				UploadWorkers:   3,
			}
		} else {
			log.Println("[Store] Cloud storage enabled but missing required environment variables")
			s.cloudEnabled = false
		}
	}

	// Create storage policy
	policy := tiered.StoragePolicy{
		MemoryRetention: time.Duration(memToAge) * time.Second,
		DiskRetention:   time.Duration(diskToAge) * time.Second,
		CloudRetention:  time.Hour * 24 * 30, // 30 days
		MemoryThreshold: 0.8,                 // 80% memory usage
		DiskThreshold:   0.9,                 // 90% disk usage
		BatchSize:       100,
		ArchiveInterval: time.Minute * 5,
	}

	// Create tiered storage manager
	manager, err := tiered.NewManager(memSize, memSize, diskPath, cloudCfg, policy)
	if err != nil {
		log.Printf("[Store] Failed to initialize tiered storage: %v", err)
		// Fallback to basic memory store
		s.diskEnabled = false
		s.cloudEnabled = false
		return
	}

	s.tieredManager = manager
	log.Println("[Store] Tiered storage initialized successfully")

	// Log which tiers are enabled
	if s.diskEnabled {
		log.Println("[Store] Disk tier enabled")
	}
	if s.cloudEnabled {
		log.Println("[Store] Cloud tier enabled")
	}
}

// Publish a message to a topic
func (s *Store) Publish(msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add the message to the topic
	s.messages[msg.Topic] = append(s.messages[msg.Topic], msg)

	// Record the message in metrics
	s.metrics.RecordMessageReceived(msg.Topic)

	// If tiered storage is enabled, store in tiered storage
	if s.tieredManager != nil {
		tieredMsg := tiered.Message{
			ID:        msg.ID,
			Topic:     msg.Topic,
			Payload:   msg.Payload,
			Timestamp: msg.Timestamp,
			TTL:       msg.TTL,
		}

		if err := s.tieredManager.Store(tieredMsg); err != nil {
			log.Printf("[Store] Failed to store message in tiered storage: %v", err)
			// Continue with memory-only storage
		}
	}

	// Notify subscribers
	subs, exists := s.subscribers[msg.Topic]
	if !exists || len(subs) == 0 {
		log.Printf("[Store] No subscribers for topic %s", msg.Topic)
		return nil
	}

	log.Printf("[Store] Publishing message to topic %s with %d subscribers", msg.Topic, len(subs))

	// Deliver to all subscribers
	for _, subscriber := range subs {
		// In a production system, this would likely be done in a goroutine
		// with proper error handling and retry logic
		subscriber(msg)
		s.metrics.RecordMessageDelivered(msg.Topic)
	}

	log.Printf("[Store] All subscribers notified for topic %s", msg.Topic)
	return nil
}

// Subscribe adds a subscriber for a topic
func (s *Store) Subscribe(topic string, callback SubscriberFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("[Store] Adding subscriber for topic %s", topic)

	// Initialize subscriber list if needed
	if _, exists := s.subscribers[topic]; !exists {
		s.subscribers[topic] = []SubscriberFunc{}
	}

	// Add the subscriber
	s.subscribers[topic] = append(s.subscribers[topic], callback)

	// Record subscription in metrics
	s.metrics.RecordSubscriptionAdded(topic)

	log.Printf("[Store] Topic %s now has %d subscribers", topic, len(s.subscribers[topic]))
}

// Unsubscribe removes a subscriber for a topic
func (s *Store) Unsubscribe(topic string, callback SubscriberFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if topic exists
	subs, exists := s.subscribers[topic]
	if !exists {
		return errors.New("topic not found")
	}

	// Store the callback in a variable with concrete memory address
	// so we can reference it consistently
	callbackToRemove := callback

	// Find and remove the subscriber by function identity
	found := false
	newSubs := make([]SubscriberFunc, 0, len(subs))

	for i, sub := range subs {
		// In Go, we can't compare function values for equality directly.
		// Since subscribers are registered by function reference, we need
		// to iterate over each one and build a new slice without the target.
		// We use the function pointer itself as an indicator.

		// Skip the current subscriber (to remove it)
		if fmt.Sprintf("%p", sub) == fmt.Sprintf("%p", callbackToRemove) {
			// Found the subscriber to remove
			found = true
			log.Printf("[Store] Removing subscriber at index %d from topic %s", i, topic)
			continue
		}

		// Keep this subscriber
		newSubs = append(newSubs, sub)
	}

	if !found {
		return errors.New("subscriber not found")
	}

	// Update the subscribers list
	s.subscribers[topic] = newSubs

	// Record unsubscription in metrics
	s.metrics.RecordSubscriptionRemoved(topic)

	log.Printf("[Store] Removed subscriber from topic %s, %d subscribers remain",
		topic, len(newSubs))
	return nil
}

// TierStats represents statistics for a single storage tier
type TierStats struct {
	Enabled       bool      `json:"enabled"`
	UsedBytes     int64     `json:"used_bytes"`
	MessageCount  int       `json:"message_count"`
	Topics        []string  `json:"topics"`
	OldestMessage time.Time `json:"oldest_message,omitempty"`
	NewestMessage time.Time `json:"newest_message,omitempty"`
}

// TimestampInfo contains the oldest and newest message timestamps for a topic
type TimestampInfo struct {
	Oldest time.Time `json:"oldest"`
	Newest time.Time `json:"newest"`
}

// TierConfig represents the configuration for storage tiers
type TierConfig struct {
	DiskEnabled                bool  `json:"disk_enabled"`
	CloudEnabled               bool  `json:"cloud_enabled"`
	MemoryToDiskThresholdBytes int64 `json:"memory_to_disk_threshold_bytes"`
	MemoryToDiskAgeSeconds     int64 `json:"memory_to_disk_age_seconds"`
	DiskToCloudAgeSeconds      int64 `json:"disk_to_cloud_age_seconds"`
	DiskToCloudThresholdBytes  int64 `json:"disk_to_cloud_threshold_bytes"`
}

// GetMemoryUsage returns the estimated memory usage in bytes
func (s *Store) GetMemoryUsage() int64 {
	if s.tieredManager != nil {
		metrics := s.tieredManager.GetMetrics()
		if memMetrics, ok := metrics["memory_store"].(map[string]interface{}); ok {
			if memUsage, ok := memMetrics["memory_usage"].(uint64); ok {
				return int64(memUsage)
			}
		}
	}

	// Calculate actual memory usage by estimating message size
	var totalBytes int64

	// Get message count per topic
	topicMap := make(map[string]int)
	s.mu.RLock()
	for topic, messages := range s.messages {
		topicMap[topic] = len(messages)
	}
	s.mu.RUnlock()

	// Calculate approximate memory usage
	for topic, count := range topicMap {
		// Get average message size (or use estimate)
		avgSize := s.getAverageMessageSize(topic)
		if avgSize == 0 {
			avgSize = 256 // Default average size estimate if no data
		}

		// Add memory for message contents plus overhead
		totalBytes += int64(count) * (int64(avgSize) + 64) // 64 bytes for message metadata
	}

	// Add overhead for topic structures
	totalBytes += int64(len(topicMap)) * 256 // Approximate overhead per topic

	return totalBytes
}

// getAverageMessageSize calculates average message size for a topic
// by sampling messages if available
func (s *Store) getAverageMessageSize(topic string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	messages, exists := s.messages[topic]
	if !exists || len(messages) == 0 {
		return 0
	}

	// Sample up to 10 messages to estimate average size
	sampleSize := min(10, len(messages))
	totalSize := 0

	// Calculate average size from sampled messages
	for i := 0; i < sampleSize; i++ {
		totalSize += len(messages[i].Payload)
	}

	return totalSize / sampleSize
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetMessageCount returns the total number of messages across all topics
func (s *Store) GetMessageCount() int {
	count := 0
	// For now, just return the metrics count
	if s.metrics != nil {
		count = int(s.metrics.GetMessageCount())
	}
	return count
}

// GetTopics returns a list of all topics in the store
func (s *Store) GetTopics() []string {
	// Query all storage tiers and aggregate topics
	topics := make(map[string]struct{}) // Use map to deduplicate topics

	// First, get topics from memory (always available)
	s.mu.RLock()
	for topic := range s.messages {
		topics[topic] = struct{}{}
	}

	// Get topics from subscribers too
	for topic := range s.subscribers {
		topics[topic] = struct{}{}
	}
	s.mu.RUnlock()

	// If tiered storage is enabled, query other tiers
	if s.tieredManager != nil {
		// Check if we can access disk store
		if s.diskEnabled {
			diskTopics, err := s.tieredManager.GetDiskTopics()
			if err == nil && len(diskTopics) > 0 {
				for _, topic := range diskTopics {
					topics[topic] = struct{}{}
				}
			}
		}

		// Check if we can access cloud store
		if s.cloudEnabled {
			cloudTopics, err := s.tieredManager.GetCloudTopics()
			if err == nil && len(cloudTopics) > 0 {
				for _, topic := range cloudTopics {
					topics[topic] = struct{}{}
				}
			}
		}
	}

	// Convert map keys to slice
	result := make([]string, 0, len(topics))
	for topic := range topics {
		result = append(result, topic)
	}

	// Update the metrics if available
	if s.metrics != nil {
		// Note: This approach has a limitation as it only updates metrics
		// when this method is called. A more robust approach would continuously
		// update metrics as messages are added/removed.
		s.metrics.(*MetricsImpl).UpdateTopics(result)
	}

	return result
}

// GetDiskStats returns statistics for the disk storage tier
func (s *Store) GetDiskStats() *TierStats {
	if !s.diskEnabled || s.tieredManager == nil {
		return nil
	}

	metrics := s.tieredManager.GetMetrics()
	diskMetrics, ok := metrics["disk_store"].(map[string]interface{})
	if !ok {
		return nil
	}

	// Extract manager metrics for message count
	managerMetrics, ok := metrics["manager"].(map[string]interface{})
	if !ok {
		return nil
	}

	// Get message count from manager metrics
	var messageCount int
	if diskMsgCount, ok := managerMetrics["messages_on_disk"].(uint64); ok {
		messageCount = int(diskMsgCount)
	}

	// Get disk used bytes
	var usedBytes int64
	if diskUsed, ok := diskMetrics["total_bytes"].(uint64); ok {
		usedBytes = int64(diskUsed)
	}

	// Create and return tier stats
	return &TierStats{
		Enabled:      true,
		UsedBytes:    usedBytes,
		MessageCount: messageCount,
		Topics:       s.GetTopics(), // For now, just use the same topics
	}
}

// GetCloudStats returns statistics for the cloud storage tier
func (s *Store) GetCloudStats() *TierStats {
	if !s.cloudEnabled || s.tieredManager == nil {
		return nil
	}

	metrics := s.tieredManager.GetMetrics()
	cloudMetrics, ok := metrics["cloud_store"].(map[string]interface{})
	if !ok {
		return nil
	}

	// Extract manager metrics for message count
	managerMetrics, ok := metrics["manager"].(map[string]interface{})
	if !ok {
		return nil
	}

	// Get message count from manager metrics
	var messageCount int
	if cloudMsgCount, ok := managerMetrics["messages_in_cloud"].(uint64); ok {
		messageCount = int(cloudMsgCount)
	}

	// Get cloud used bytes
	var usedBytes int64
	if cloudUsed, ok := cloudMetrics["total_bytes_uploaded"].(uint64); ok {
		usedBytes = int64(cloudUsed)
	}

	// Create and return tier stats
	return &TierStats{
		Enabled:      true,
		UsedBytes:    usedBytes,
		MessageCount: messageCount,
		Topics:       s.GetTopics(), // For now, just use the same topics
	}
}

// GetTierConfig returns the configuration for storage tiers
func (s *Store) GetTierConfig() TierConfig {
	// Parse memory storage settings
	memSize := int64(104857600) // Default: 100MB
	if envSize := os.Getenv("SPRAWL_STORAGE_MEMORY_MAX_SIZE"); envSize != "" {
		if size, err := strconv.ParseInt(envSize, 10, 64); err == nil && size > 0 {
			memSize = size
		}
	}

	memToAge := int64(3600) // Default: 1 hour
	if envAge := os.Getenv("SPRAWL_STORAGE_MEMORY_TO_DISK_AGE"); envAge != "" {
		if age, err := strconv.ParseInt(envAge, 10, 64); err == nil && age > 0 {
			memToAge = age
		}
	}

	diskToAge := int64(86400) // Default: 24 hours
	if envAge := os.Getenv("SPRAWL_STORAGE_DISK_TO_CLOUD_AGE"); envAge != "" {
		if age, err := strconv.ParseInt(envAge, 10, 64); err == nil && age > 0 {
			diskToAge = age
		}
	}

	diskSize := int64(1073741824) // Default: 1GB
	if envSize := os.Getenv("SPRAWL_STORAGE_DISK_MAX_SIZE"); envSize != "" {
		if size, err := strconv.ParseInt(envSize, 10, 64); err == nil && size > 0 {
			diskSize = size
		}
	}

	return TierConfig{
		DiskEnabled:                s.diskEnabled,
		CloudEnabled:               s.cloudEnabled,
		MemoryToDiskThresholdBytes: memSize,
		MemoryToDiskAgeSeconds:     memToAge,
		DiskToCloudAgeSeconds:      diskToAge,
		DiskToCloudThresholdBytes:  diskSize,
	}
}

// Compact triggers a compaction of the storage tiers
func (s *Store) Compact() error {
	if s.tieredManager == nil {
		// No tiered storage, just log the action
		log.Println("[Store] Compaction requested (no-op implementation)")
		return nil
	}

	log.Println("[Store] Starting compaction for tiered storage")

	// First, check if we can access the tiered manager
	s.mu.RLock()
	tm := s.tieredManager
	s.mu.RUnlock()

	if tm == nil {
		return fmt.Errorf("tiered storage manager is not available")
	}

	// Trigger archive operation on the tiered manager
	// This will move data from memory to disk and from disk to cloud if configured
	err := tm.PerformFullCompaction()
	if err != nil {
		log.Printf("[Store] Compaction failed: %v", err)
		return fmt.Errorf("compaction failed: %w", err)
	}

	log.Println("[Store] Compaction completed successfully")
	return nil
}

// GetMessageCountForTopic returns the number of messages for a specific topic
func (s *Store) GetMessageCountForTopic(topic string) int {
	count := 0

	// First, count messages in memory
	s.mu.RLock()
	if messages, exists := s.messages[topic]; exists {
		count += len(messages)
	}
	s.mu.RUnlock()

	// If tiered storage is enabled, query other tiers
	if s.tieredManager != nil {
		// Check disk tier
		if s.diskEnabled {
			diskCount, err := s.tieredManager.GetDiskMessageCount(topic)
			if err == nil {
				count += diskCount
			}
		}

		// Check cloud tier
		if s.cloudEnabled {
			cloudCount, err := s.tieredManager.GetCloudMessageCount(topic)
			if err == nil {
				count += cloudCount
			}
		}
	}

	// Update metrics
	if s.metrics != nil {
		s.metrics.(*MetricsImpl).UpdateTopicMessageCount(topic, int64(count))
	}

	return count
}

// GetSubscriberCountForTopic returns the number of subscribers for a specific topic
func (s *Store) GetSubscriberCountForTopic(topic string) int {
	// In a real implementation, this would query the subscription registry
	// For now, return 1 if we have any subscribers, 0 otherwise
	if s.HasSubscribers(topic) {
		return 1
	}
	return 0
}

// GetTopicTimestamps returns timestamp information for a topic
func (s *Store) GetTopicTimestamps(topic string) *TimestampInfo {
	// In a real implementation, this would query all tiers
	// For now, return nil if no messages for this topic
	msgs, exists := s.messages[topic]
	if !exists || len(msgs) == 0 {
		return nil
	}

	// Find oldest and newest messages
	oldest := msgs[0].Timestamp
	newest := msgs[0].Timestamp
	for _, msg := range msgs {
		if msg.Timestamp.Before(oldest) {
			oldest = msg.Timestamp
		}
		if msg.Timestamp.After(newest) {
			newest = msg.Timestamp
		}
	}

	return &TimestampInfo{
		Oldest: oldest,
		Newest: newest,
	}
}

// HasSubscribers checks if a topic has any subscribers
func (s *Store) HasSubscribers(topic string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs, exists := s.subscribers[topic]
	return exists && len(subs) > 0
}

// Shutdown cleanly closes the store
func (s *Store) Shutdown() {
	if s.tieredManager != nil {
		if err := s.tieredManager.Close(); err != nil {
			log.Printf("[Store] Error closing tiered manager: %v", err)
		}
	}
}

// GetStorageType returns the current storage type
func (s *Store) GetStorageType() string {
	if s.cloudEnabled {
		return "s3"
	}
	if s.diskEnabled {
		return "disk"
	}
	return "memory"
}

// SetStorageTierForTopic assigns a topic to a specific storage tier
// tier must be one of: "memory", "disk", or "cloud"
func (s *Store) SetStorageTierForTopic(topic string, tier string) error {
	// Validate tier
	if tier != "memory" && tier != "disk" && tier != "cloud" {
		return fmt.Errorf("invalid tier: %s, must be 'memory', 'disk', or 'cloud'", tier)
	}

	// Check if tiered storage is enabled
	if s.tieredManager == nil {
		// If tiered storage is not enabled and trying to use disk or cloud
		if tier != "memory" {
			return fmt.Errorf("tiered storage is not enabled, only 'memory' tier is available")
		}
		// Memory tier is always available
		return nil
	}

	// Check if disk tier is enabled if requesting disk
	if tier == "disk" && !s.diskEnabled {
		return fmt.Errorf("disk tier is not enabled")
	}

	// Check if cloud tier is enabled if requesting cloud
	if tier == "cloud" && !s.cloudEnabled {
		return fmt.Errorf("cloud tier is not enabled")
	}

	// Store the tier preference for this topic in our global mapping
	globalTopicTiers.mu.Lock()
	globalTopicTiers.tiers[topic] = tier
	globalTopicTiers.mu.Unlock()

	// Log the tier assignment
	log.Printf("[Store] Topic %s assigned to %s tier", topic, tier)

	// Handle immediate migration if necessary
	if err := s.migrateTopicToTier(topic, tier); err != nil {
		log.Printf("[Store] Warning: could not fully migrate topic %s to tier %s: %v",
			topic, tier, err)
		// We don't return the error here because the tier assignment is still recorded
		// and data will gradually move to the correct tier through normal compaction
	}

	return nil
}

// migrateTopicToTier moves existing topic messages to the appropriate tier
func (s *Store) migrateTopicToTier(topic string, tier string) error {
	// Only attempty migration if tiered manager is available
	if s.tieredManager == nil {
		return nil
	}

	switch tier {
	case "memory":
		// Nothing to do - existing messages will stay in their current tiers
		// New messages will be written to memory
		return nil

	case "disk":
		// If messages should be in disk tier, move any in-memory messages to disk
		s.mu.RLock()
		messages, exists := s.messages[topic]
		s.mu.RUnlock()

		if !exists || len(messages) == 0 {
			return nil // No messages to migrate
		}

		// Copy the messages slice to avoid concurrent modification issues
		messagesToMigrate := make([]Message, len(messages))
		copy(messagesToMigrate, messages)

		// Move messages to disk tier
		moved := 0
		for _, msg := range messagesToMigrate {
			// Convert store.Message to tiered.Message
			tieredMsg := tiered.Message{
				ID:        msg.ID,
				Topic:     msg.Topic,
				Payload:   msg.Payload,
				Timestamp: msg.Timestamp,
				TTL:       msg.TTL,
			}
			if err := s.tieredManager.MoveMessageToDisk(tieredMsg); err != nil {
				log.Printf("[Store] Error moving message %s to disk: %v", msg.ID, err)
				continue
			}
			moved++

			// Remove from memory after confirming it's on disk
			s.mu.Lock()
			// Re-get messages list in case it changed
			if currentMessages, exists := s.messages[topic]; exists {
				for i, currentMsg := range currentMessages {
					if currentMsg.ID == msg.ID {
						// Remove this message
						s.messages[topic] = append(currentMessages[:i], currentMessages[i+1:]...)
						break
					}
				}
			}
			s.mu.Unlock()
		}

		log.Printf("[Store] Migrated %d/%d messages for topic %s to disk tier",
			moved, len(messagesToMigrate), topic)

	case "cloud":
		// If messages should be in cloud tier, move any in-memory or disk messages to cloud
		// Start with memory tier
		s.mu.RLock()
		memoryMessages, exists := s.messages[topic]
		s.mu.RUnlock()

		if exists && len(memoryMessages) > 0 {
			// Copy to avoid concurrent modification
			messagesToMigrate := make([]Message, len(memoryMessages))
			copy(messagesToMigrate, memoryMessages)

			// Handle in batches
			batchSize := 100
			for i := 0; i < len(messagesToMigrate); i += batchSize {
				end := i + batchSize
				if end > len(messagesToMigrate) {
					end = len(messagesToMigrate)
				}

				batch := messagesToMigrate[i:end]
				// Convert slice of store.Message to slice of tiered.Message
				tieredBatch := make([]tiered.Message, len(batch))
				for i, msg := range batch {
					tieredBatch[i] = tiered.Message{
						ID:        msg.ID,
						Topic:     msg.Topic,
						Payload:   msg.Payload,
						Timestamp: msg.Timestamp,
						TTL:       msg.TTL,
					}
				}
				if err := s.tieredManager.MoveMessagesToCloud(topic, tieredBatch); err != nil {
					log.Printf("[Store] Error moving batch to cloud: %v", err)
					continue
				}

				// Remove migrated messages from memory
				for _, msg := range batch {
					s.mu.Lock()
					if currentMessages, exists := s.messages[topic]; exists {
						for i, currentMsg := range currentMessages {
							if currentMsg.ID == msg.ID {
								s.messages[topic] = append(currentMessages[:i], currentMessages[i+1:]...)
								break
							}
						}
					}
					s.mu.Unlock()
				}
			}
		}

		// Also move any messages from disk to cloud
		if s.diskEnabled {
			if err := s.tieredManager.MoveDiskTopicToCloud(topic); err != nil {
				log.Printf("[Store] Error migrating disk messages to cloud: %v", err)
				return err
			}
		}
	}

	return nil
}

// GetStorageTierForTopic returns the configured storage tier for a topic
func (s *Store) GetStorageTierForTopic(topic string) string {
	// Check if there's a specific tier assignment for this topic
	globalTopicTiers.mu.RLock()
	tier, exists := globalTopicTiers.tiers[topic]
	globalTopicTiers.mu.RUnlock()

	if exists {
		return tier
	}

	// If no specific assignment, return the default tier
	if s.tieredManager == nil {
		return "memory" // Only memory tier is available without tiered storage
	}

	// Default to memory tier
	return "memory"
}

// GetMessages returns all messages for a specific topic
func (s *Store) GetMessages(topic string) []Message {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Return a copy of the messages to avoid concurrent modification issues
	messages, exists := s.messages[topic]
	if !exists {
		return []Message{}
	}

	// Make a copy to avoid any potential race conditions
	result := make([]Message, len(messages))
	copy(result, messages)
	return result
}
