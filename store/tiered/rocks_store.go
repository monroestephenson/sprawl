package tiered

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/linxGnu/grocksdb"
)

// RocksStore implements persistent storage using RocksDB
type RocksStore struct {
	db         *grocksdb.DB
	writeOpts  *grocksdb.WriteOptions
	readOpts   *grocksdb.ReadOptions
	mu         sync.RWMutex
	metrics    *RocksMetrics
	indexCache sync.Map // Topic -> []MessageID cache
	compacting bool
}

type RocksMetrics struct {
	messagesStored uint64
	bytesStored    uint64
	readOps        uint64
	writeOps       uint64
	compactions    uint64
}

// NewRocksStore creates a new RocksDB-backed store
func NewRocksStore(path string) (*RocksStore, error) {
	// Create RocksDB options
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(grocksdb.CompressionType(0)) // NoCompression = 0
	opts.SetWriteBufferSize(64 * 1024 * 1024)        // 64MB
	opts.SetMaxWriteBufferNumber(3)
	opts.SetTargetFileSizeBase(64 * 1024 * 1024)
	opts.SetMaxBackgroundJobs(4) // Modern replacement for SetMaxBackgroundCompactions

	// Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Open database
	db, err := grocksdb.OpenDb(opts, path)
	if err != nil {
		return nil, fmt.Errorf("failed to open RocksDB: %w", err)
	}

	store := &RocksStore{
		db:        db,
		writeOpts: grocksdb.NewDefaultWriteOptions(),
		readOpts:  grocksdb.NewDefaultReadOptions(),
		metrics:   &RocksMetrics{},
	}

	// Start background compaction monitor
	go store.monitorCompaction()

	return store, nil
}

// Store persists a message to RocksDB
func (rs *RocksStore) Store(msg Message) error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	// Create batch for atomic writes
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Serialize message
	msgData, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Store message by ID
	msgKey := []byte("msg:" + msg.ID)
	batch.Put(msgKey, msgData)

	// Update topic index
	topicKey := []byte("topic:" + msg.Topic)
	var topicMsgs []string

	// Check cache first
	if cached, ok := rs.indexCache.Load(msg.Topic); ok {
		topicMsgs = cached.([]string)
	} else {
		// Read from DB if not in cache
		existing, err := rs.db.Get(rs.readOpts, topicKey)
		if err == nil && existing.Size() > 0 {
			if err := json.Unmarshal(existing.Data(), &topicMsgs); err != nil {
				existing.Free()
				return fmt.Errorf("failed to unmarshal topic index: %w", err)
			}
			existing.Free()
		}
	}

	// Add new message ID to topic index
	topicMsgs = append(topicMsgs, msg.ID)
	topicData, err := json.Marshal(topicMsgs)
	if err != nil {
		return fmt.Errorf("failed to marshal topic index: %w", err)
	}

	batch.Put(topicKey, topicData)

	// Update cache
	rs.indexCache.Store(msg.Topic, topicMsgs)

	// Write batch
	if err := rs.db.Write(rs.writeOpts, batch); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	// Update metrics
	rs.metrics.messagesStored++
	rs.metrics.bytesStored += uint64(len(msgData))
	rs.metrics.writeOps++

	return nil
}

// Retrieve gets a message by ID
func (rs *RocksStore) Retrieve(id string) (*Message, error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	msgKey := []byte("msg:" + id)
	data, err := rs.db.Get(rs.readOpts, msgKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return nil, fmt.Errorf("message not found: %s", id)
	}

	var msg Message
	if err := json.Unmarshal(data.Data(), &msg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	rs.metrics.readOps++
	return &msg, nil
}

// GetTopicMessages retrieves all message IDs for a topic
func (rs *RocksStore) GetTopicMessages(topic string) ([]string, error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	// Check cache first
	if cached, ok := rs.indexCache.Load(topic); ok {
		rs.metrics.readOps++ // Increment readOps for cache hits too
		return cached.([]string), nil
	}

	topicKey := []byte("topic:" + topic)
	data, err := rs.db.Get(rs.readOpts, topicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to read topic index: %w", err)
	}
	defer data.Free()

	if data.Size() == 0 {
		return []string{}, nil
	}

	var msgIDs []string
	if err := json.Unmarshal(data.Data(), &msgIDs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal topic index: %w", err)
	}

	// Update cache
	rs.indexCache.Store(topic, msgIDs)
	rs.metrics.readOps++

	return msgIDs, nil
}

// DeleteOldMessages removes messages older than the retention period
func (rs *RocksStore) DeleteOldMessages(retention time.Duration) error {
	// Use read lock for scanning
	rs.mu.RLock()
	if rs.db == nil {
		rs.mu.RUnlock()
		return fmt.Errorf("database is closed")
	}

	// Create a snapshot for consistent iteration
	snapshot := rs.db.NewSnapshot()
	readOpts := grocksdb.NewDefaultReadOptions()
	readOpts.SetSnapshot(snapshot)
	it := rs.db.NewIterator(readOpts)
	rs.mu.RUnlock()

	defer func() {
		it.Close()
		readOpts.Destroy()
		rs.db.ReleaseSnapshot(snapshot)
	}()

	cutoff := time.Now().Add(-retention)
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Maps to track changes
	toDelete := make(map[string]struct{})
	topicMessages := make(map[string][]string)

	// First pass: identify messages to delete
	for it.SeekToFirst(); it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()
		keyStr := string(key.Data())

		if !bytes.HasPrefix(key.Data(), []byte("msg:")) {
			key.Free()
			value.Free()
			continue
		}

		var msg Message
		if err := json.Unmarshal(value.Data(), &msg); err != nil {
			key.Free()
			value.Free()
			continue
		}

		if msg.Timestamp.Before(cutoff) {
			toDelete[keyStr] = struct{}{}
			// Track messages by topic
			topicMessages[msg.Topic] = append(topicMessages[msg.Topic], msg.ID)
		}

		key.Free()
		value.Free()
	}

	// No messages to delete
	if len(toDelete) == 0 {
		return nil
	}

	// Second pass: update topic indices
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.db == nil {
		return fmt.Errorf("database was closed during deletion")
	}

	for topic, deletedIDs := range topicMessages {
		topicKey := []byte("topic:" + topic)
		data, err := rs.db.Get(rs.readOpts, topicKey)
		if err != nil {
			continue
		}

		var currentIDs []string
		if err := json.Unmarshal(data.Data(), &currentIDs); err != nil {
			data.Free()
			continue
		}
		data.Free()

		// Remove deleted IDs
		deletedSet := make(map[string]bool, len(deletedIDs))
		for _, id := range deletedIDs {
			deletedSet[id] = true
		}

		newIDs := make([]string, 0, len(currentIDs))
		for _, id := range currentIDs {
			if !deletedSet[id] {
				newIDs = append(newIDs, id)
			}
		}

		// Update topic index
		if topicData, err := json.Marshal(newIDs); err == nil {
			batch.Put(topicKey, topicData)
			rs.indexCache.Store(topic, newIDs)
		}
	}

	// Delete messages
	for key := range toDelete {
		batch.Delete([]byte(key))
	}

	// Write batch
	if err := rs.db.Write(rs.writeOpts, batch); err != nil {
		return fmt.Errorf("failed to delete old messages: %w", err)
	}

	return nil
}

// monitorCompaction periodically checks if compaction is needed
func (rs *RocksStore) monitorCompaction() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if rs.db != nil {
			if rs.shouldCompact() {
				rs.compact()
			}
		}
	}
}

func (rs *RocksStore) shouldCompact() bool {
	if rs.db == nil {
		return false
	}

	// Get approximate size of database
	ranges := []grocksdb.Range{{Start: []byte{0}, Limit: []byte{255}}}
	sizes, err := rs.db.GetApproximateSizes(ranges)
	if err != nil || len(sizes) == 0 {
		return false
	}

	// Compact if database is larger than 1GB
	return sizes[0] > 1024*1024*1024
}

func (rs *RocksStore) compact() {
	rs.mu.Lock()
	if rs.compacting || rs.db == nil {
		rs.mu.Unlock()
		return
	}
	rs.compacting = true
	rs.mu.Unlock()

	defer func() {
		rs.mu.Lock()
		rs.compacting = false
		rs.mu.Unlock()
	}()

	// Compact entire database
	rng := grocksdb.Range{
		Start: []byte{0},
		Limit: []byte{255},
	}
	rs.db.CompactRange(rng)
	rs.metrics.compactions++
}

// GetMetrics returns current metrics
func (rs *RocksStore) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"messages_stored": rs.metrics.messagesStored,
		"bytes_stored":    rs.metrics.bytesStored,
		"read_ops":        rs.metrics.readOps,
		"write_ops":       rs.metrics.writeOps,
		"compactions":     rs.metrics.compactions,
	}
}

// Close closes the database
func (rs *RocksStore) Close() {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.writeOpts != nil {
		rs.writeOpts.Destroy()
		rs.writeOpts = nil
	}
	if rs.readOpts != nil {
		rs.readOpts.Destroy()
		rs.readOpts = nil
	}
	if rs.db != nil {
		rs.db.Close()
		rs.db = nil
	}
}

// ListAllTopicIDs returns a list of all topics and their message IDs
func (rs *RocksStore) ListAllTopicIDs() ([]TopicIDs, error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	result := make([]TopicIDs, 0)

	// Get iterator with prefix "topic:" to find all topics
	prefix := []byte("topic:")
	iter := rs.db.NewIterator(rs.readOpts)
	defer iter.Close()

	// Seek to the beginning of topics
	iter.Seek(prefix)

	// Process all topic entries
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		keyData := key.Data()

		// Check if still in the "topic:" prefix
		if !bytes.HasPrefix(keyData, prefix) {
			key.Free()
			break
		}

		// Extract topic name (after prefix)
		topic := string(keyData[len(prefix):])

		// Get all IDs for this topic
		ids, err := rs.GetTopicMessages(topic)
		if err != nil {
			key.Free()
			return nil, fmt.Errorf("failed to get messages for topic %s: %w", topic, err)
		}

		if len(ids) > 0 {
			result = append(result, TopicIDs{
				Topic: topic,
				IDs:   ids,
			})
		}

		key.Free()
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("iterator error: %w", err)
	}

	return result, nil
}

// ForceCompaction triggers a manual compaction of the database
func (rs *RocksStore) ForceCompaction() error {
	rs.mu.Lock()
	defer rs.mu.Unlock()

	if rs.compacting {
		return errors.New("compaction already in progress")
	}

	rs.compacting = true
	defer func() {
		rs.compacting = false
	}()

	// Increment the compaction count
	rs.metrics.compactions++

	// Trigger compaction from the beginning to the end of the database
	// This is a synchronous operation and may take some time for large databases
	startKey := []byte{0}        // Start of keyspace
	endKey := []byte{0xFF, 0xFF} // End of keyspace

	// Compact the entire database
	// The CompactRange method might have different names in different versions of grocksdb
	// Using the currently documented method
	rs.db.CompactRange(grocksdb.Range{
		Start: startKey,
		Limit: endKey,
	})

	return nil
}

// DeleteMessages removes specific messages by ID
func (rs *RocksStore) DeleteMessages(messageIDs []string) error {
	if rs.db == nil {
		return fmt.Errorf("database not open")
	}

	if len(messageIDs) == 0 {
		return nil
	}

	// Create a set of the message IDs for faster lookup
	toDelete := make(map[string]struct{}, len(messageIDs))
	for _, id := range messageIDs {
		toDelete[id] = struct{}{}
	}

	// Create a new write batch
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()

	// Find all keys for these messages
	for _, id := range messageIDs {
		// Get the message metadata to find the key
		messageKey := fmt.Sprintf("msg:%s", id)
		data, err := rs.db.Get(rs.readOpts, []byte(messageKey))

		if err != nil || data == nil || !data.Exists() {
			// Skip if message doesn't exist
			if data != nil {
				data.Free()
			}
			continue
		}

		// Delete the message metadata
		batch.Delete([]byte(messageKey))
		data.Free()

		// Also delete the actual message data
		dataKey := fmt.Sprintf("data:%s", id)
		batch.Delete([]byte(dataKey))
	}

	// Commit the batch
	err := rs.db.Write(rs.writeOpts, batch)
	if err != nil {
		return fmt.Errorf("failed to delete messages: %w", err)
	}

	return nil
}

// QueryFilter defines filtering parameters for message queries
type QueryFilter struct {
	Topic           string     // Filter by topic
	TimestampBefore *time.Time // Messages before this time
	TimestampAfter  *time.Time // Messages after this time
	Limit           int        // Max number of messages to return
	Offset          int        // Offset for pagination
}

// ListTopics lists all topics in the store
func (rs *RocksStore) ListTopics() ([]string, error) {
	rs.mu.RLock()
	defer rs.mu.RUnlock()

	topics := make(map[string]struct{})

	// Create iterator options
	readOpts := grocksdb.NewDefaultReadOptions()
	it := rs.db.NewIterator(readOpts)
	defer it.Close()
	defer readOpts.Destroy()

	// Prefix for topic keys
	prefix := []byte("topic:")

	// Iterate through all keys starting with the topic prefix
	for it.Seek(prefix); it.Valid(); it.Next() {
		key := it.Key()
		keyData := key.Data()

		// Check if key has the topic prefix
		if bytes.HasPrefix(keyData, prefix) {
			// Extract topic name from key
			topicName := string(keyData[len(prefix):])
			topics[topicName] = struct{}{}
		}

		key.Free()
	}

	// Convert map to slice
	result := make([]string, 0, len(topics))
	for topic := range topics {
		result = append(result, topic)
	}

	return result, nil
}

// QueryMessages returns messages matching the given filter
func (rs *RocksStore) QueryMessages(filter *QueryFilter) ([]Message, error) {
	if filter == nil {
		return nil, errors.New("filter cannot be nil")
	}

	// If no topic is specified, return an error
	if filter.Topic == "" {
		return nil, errors.New("topic must be specified in filter")
	}

	// Get all message IDs for the topic
	messageIDs, err := rs.GetTopicMessages(filter.Topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get message IDs for topic: %w", err)
	}

	// Apply limit and offset
	limit := len(messageIDs)
	if filter.Limit > 0 && filter.Limit < limit {
		limit = filter.Limit
	}

	offset := filter.Offset
	if offset >= len(messageIDs) {
		return []Message{}, nil // Nothing to return
	}

	// Apply offset and limit
	end := offset + limit
	if end > len(messageIDs) {
		end = len(messageIDs)
	}

	// Slice to hold filtered messages
	filteredMessages := make([]Message, 0, end-offset)

	// Retrieve each message and apply time filters
	for _, msgID := range messageIDs[offset:end] {
		msg, err := rs.Retrieve(msgID)
		if err != nil {
			// Log the error but continue with other messages
			fmt.Printf("Error retrieving message %s: %v\n", msgID, err)
			continue
		}

		// Apply timestamp filters
		if filter.TimestampBefore != nil && !msg.Timestamp.Before(*filter.TimestampBefore) {
			continue
		}
		if filter.TimestampAfter != nil && !msg.Timestamp.After(*filter.TimestampAfter) {
			continue
		}

		// Message passed all filters
		filteredMessages = append(filteredMessages, *msg)
	}

	return filteredMessages, nil
}
