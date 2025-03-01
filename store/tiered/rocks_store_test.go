package tiered

import (
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
