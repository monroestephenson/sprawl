package tiered

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRocksStore_BasicOperations(t *testing.T) {
	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "rocksdb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := NewRocksStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create RocksStore: %v", err)
	}
	defer store.Close()

	// Test message
	msg := Message{
		ID:        "test1",
		Topic:     "test-topic",
		Payload:   []byte("test message"),
		Timestamp: time.Now(),
		TTL:       3,
	}

	// Test Store
	if err := store.Store(msg); err != nil {
		t.Errorf("Failed to store message: %v", err)
	}

	// Test Retrieve
	retrieved, err := store.Retrieve(msg.ID)
	if err != nil {
		t.Errorf("Failed to retrieve message: %v", err)
	}

	if retrieved.ID != msg.ID {
		t.Errorf("Expected message ID %s, got %s", msg.ID, retrieved.ID)
	}

	if string(retrieved.Payload) != string(msg.Payload) {
		t.Errorf("Expected payload %s, got %s", string(msg.Payload), string(retrieved.Payload))
	}

	// Test GetTopicMessages
	msgIDs, err := store.GetTopicMessages(msg.Topic)
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}

	if len(msgIDs) != 1 || msgIDs[0] != msg.ID {
		t.Errorf("Expected topic to have one message with ID %s", msg.ID)
	}

	// Verify metrics
	metrics := store.GetMetrics()
	if metrics["messages_stored"].(uint64) != 1 {
		t.Error("Expected messages_stored to be 1")
	}
	if metrics["write_ops"].(uint64) != 1 {
		t.Error("Expected write_ops to be 1")
	}
	if metrics["read_ops"].(uint64) != 2 { // One for retrieve, one for topic messages
		t.Error("Expected read_ops to be 2")
	}
}

func TestRocksStore_MessageRetention(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "rocksdb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := NewRocksStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create RocksStore: %v", err)
	}
	defer store.Close()

	// Store old message
	oldMsg := Message{
		ID:        "old1",
		Topic:     "test-topic",
		Payload:   []byte("old message"),
		Timestamp: time.Now().Add(-2 * time.Hour),
		TTL:       3,
	}
	if err := store.Store(oldMsg); err != nil {
		t.Errorf("Failed to store old message: %v", err)
	}

	// Store new message
	newMsg := Message{
		ID:        "new1",
		Topic:     "test-topic",
		Payload:   []byte("new message"),
		Timestamp: time.Now(),
		TTL:       3,
	}
	if err := store.Store(newMsg); err != nil {
		t.Errorf("Failed to store new message: %v", err)
	}

	// Delete messages older than 1 hour
	if err := store.DeleteOldMessages(time.Hour); err != nil {
		t.Errorf("Failed to delete old messages: %v", err)
	}

	// Verify old message is gone
	_, err = store.Retrieve(oldMsg.ID)
	if err == nil {
		t.Error("Expected old message to be deleted")
	}

	// Verify new message still exists
	msg, err := store.Retrieve(newMsg.ID)
	if err != nil {
		t.Errorf("Failed to retrieve new message: %v", err)
	}
	if msg.ID != newMsg.ID {
		t.Error("Expected new message to still exist")
	}

	// Verify topic index is updated
	msgIDs, err := store.GetTopicMessages(newMsg.Topic)
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}
	if len(msgIDs) != 1 || msgIDs[0] != newMsg.ID {
		t.Error("Expected topic index to only contain new message")
	}
}

func TestRocksStore_Compaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "rocksdb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := NewRocksStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create RocksStore: %v", err)
	}
	defer store.Close()

	// Store many messages to trigger compaction
	for i := 0; i < 1000; i++ {
		msg := Message{
			ID:        string(rune(i)),
			Topic:     "test-topic",
			Payload:   make([]byte, 1024), // 1KB payload
			Timestamp: time.Now(),
			TTL:       3,
		}
		if err := store.Store(msg); err != nil {
			t.Errorf("Failed to store message %d: %v", i, err)
		}
	}

	// Force compaction
	store.compact()

	// Verify compaction metrics
	metrics := store.GetMetrics()
	if metrics["compactions"].(uint64) != 1 {
		t.Error("Expected compactions to be 1")
	}
}

func TestRocksStore_ConcurrentAccess(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "rocksdb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "test.db")
	store, err := NewRocksStore(dbPath)
	if err != nil {
		t.Fatalf("Failed to create RocksStore: %v", err)
	}
	defer store.Close()

	// Concurrent writes and reads
	done := make(chan bool)
	go func() {
		for i := 0; i < 100; i++ {
			msg := Message{
				ID:        string(rune(i)),
				Topic:     "test-topic",
				Payload:   []byte("test message"),
				Timestamp: time.Now(),
				TTL:       3,
			}
			if err := store.Store(msg); err != nil {
				t.Errorf("Failed to store message %d: %v", i, err)
			}
		}
		done <- true
	}()

	go func() {
		for i := 0; i < 100; i++ {
			if _, err := store.GetTopicMessages("test-topic"); err != nil {
				t.Errorf("Failed to get topic messages: %v", err)
			}
		}
		done <- true
	}()

	// Wait for both goroutines to finish
	<-done
	<-done

	// Verify final state
	msgIDs, err := store.GetTopicMessages("test-topic")
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}
	if len(msgIDs) != 100 {
		t.Errorf("Expected 100 messages, got %d", len(msgIDs))
	}
}

func TestListTopics(t *testing.T) {
	// Create a temporary directory for RocksDB
	tempDir := t.TempDir()

	// Create a new RocksStore
	store, err := NewRocksStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to create RocksStore: %v", err)
	}
	defer store.Close()

	// Initially, there should be no topics
	topics, err := store.ListTopics()
	if err != nil {
		t.Fatalf("Error listing topics: %v", err)
	}
	if len(topics) != 0 {
		t.Errorf("Expected 0 topics initially, got %d", len(topics))
	}

	// Add messages for different topics
	topicNames := []string{"topic1", "topic2", "topic3"}
	for i, topicName := range topicNames {
		msg := Message{
			ID:        fmt.Sprintf("msg%d", i),
			Topic:     topicName,
			Payload:   []byte(fmt.Sprintf("data for %s", topicName)),
			Timestamp: time.Now(),
		}
		if err := store.Store(msg); err != nil {
			t.Fatalf("Failed to store message: %v", err)
		}
	}

	// Now there should be 3 topics
	topics, err = store.ListTopics()
	if err != nil {
		t.Fatalf("Error listing topics after adding messages: %v", err)
	}

	if len(topics) != 3 {
		t.Errorf("Expected 3 topics, got %d", len(topics))
	}

	// Verify all topics are present
	topicMap := make(map[string]bool)
	for _, topic := range topics {
		topicMap[topic] = true
	}

	for _, expectedTopic := range topicNames {
		if !topicMap[expectedTopic] {
			t.Errorf("Expected topic %s not found in results", expectedTopic)
		}
	}
}

func TestQueryMessages(t *testing.T) {
	// Create a temporary directory for RocksDB
	tempDir := t.TempDir()

	// Create a new RocksStore
	store, err := NewRocksStore(tempDir)
	if err != nil {
		t.Fatalf("Failed to create RocksStore: %v", err)
	}
	defer store.Close()

	// Add messages with different timestamps
	now := time.Now()
	topic := "query-topic"

	// Create messages for the past, present, and future
	messages := []struct {
		id        string
		timestamp time.Time
		payload   string
	}{
		{"past1", now.Add(-2 * time.Hour), "past message 1"},
		{"past2", now.Add(-1 * time.Hour), "past message 2"},
		{"present1", now, "present message 1"},
		{"present2", now.Add(time.Second), "present message 2"},
		{"future1", now.Add(1 * time.Hour), "future message 1"},
		{"future2", now.Add(2 * time.Hour), "future message 2"},
	}

	// Store all messages
	for _, m := range messages {
		msg := Message{
			ID:        m.id,
			Topic:     topic,
			Payload:   []byte(m.payload),
			Timestamp: m.timestamp,
		}
		if err := store.Store(msg); err != nil {
			t.Fatalf("Failed to store message %s: %v", m.id, err)
		}
	}

	// Query messages before a certain time
	pastTime := now.Add(-30 * time.Minute)
	pastFilter := &QueryFilter{
		Topic:           topic,
		TimestampBefore: &pastTime,
	}

	pastResults, err := store.QueryMessages(pastFilter)
	if err != nil {
		t.Fatalf("Error querying past messages: %v", err)
	}

	if len(pastResults) != 2 {
		t.Errorf("Expected 2 past messages, got %d", len(pastResults))
	}

	// Query messages after a certain time
	futureTime := now.Add(30 * time.Minute)
	futureFilter := &QueryFilter{
		Topic:          topic,
		TimestampAfter: &futureTime,
	}

	futureResults, err := store.QueryMessages(futureFilter)
	if err != nil {
		t.Fatalf("Error querying future messages: %v", err)
	}

	if len(futureResults) != 2 {
		t.Errorf("Expected 2 future messages, got %d", len(futureResults))
	}

	// Query with both before and after
	rangeFilter := &QueryFilter{
		Topic:           topic,
		TimestampAfter:  &pastTime,
		TimestampBefore: &futureTime,
	}

	rangeResults, err := store.QueryMessages(rangeFilter)
	if err != nil {
		t.Fatalf("Error querying range messages: %v", err)
	}

	if len(rangeResults) != 2 {
		t.Errorf("Expected 2 present messages in range, got %d", len(rangeResults))
	}

	// Query with limit
	limitFilter := &QueryFilter{
		Topic: topic,
		Limit: 3,
	}

	limitResults, err := store.QueryMessages(limitFilter)
	if err != nil {
		t.Fatalf("Error querying with limit: %v", err)
	}

	if len(limitResults) != 3 {
		t.Errorf("Expected 3 messages with limit, got %d", len(limitResults))
	}
}
