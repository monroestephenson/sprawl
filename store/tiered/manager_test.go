package tiered

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestManager_BasicOperations(t *testing.T) {
	// Create temporary directory for test database
	tmpDir, err := os.MkdirTemp("", "tiered-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	policy := StoragePolicy{
		MemoryRetention: time.Hour,
		DiskRetention:   24 * time.Hour,
		MemoryThreshold: 0.8,
		DiskThreshold:   0.8,
		BatchSize:       100,
		ArchiveInterval: time.Minute,
	}

	manager, err := NewManager(1000, 1024*1024, filepath.Join(tmpDir, "test.db"), nil, policy)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Test storing and retrieving a message
	msg := Message{
		ID:        "test1",
		Topic:     "test-topic",
		Payload:   []byte("test message"),
		Timestamp: time.Now(),
		TTL:       3,
	}

	if err := manager.Store(msg); err != nil {
		t.Errorf("Failed to store message: %v", err)
	}

	retrieved, err := manager.Retrieve(msg.ID)
	if err != nil {
		t.Errorf("Failed to retrieve message: %v", err)
	}

	if retrieved.ID != msg.ID {
		t.Errorf("Expected message ID %s, got %s", msg.ID, retrieved.ID)
	}

	// Test getting topic messages
	msgIDs, err := manager.GetTopicMessages(msg.Topic)
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}

	if len(msgIDs) != 1 || msgIDs[0] != msg.ID {
		t.Errorf("Expected topic to have one message with ID %s", msg.ID)
	}
}

func TestManager_TieredStorage(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tiered-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager with small memory limit to force disk storage
	policy := StoragePolicy{
		MemoryRetention: time.Hour,
		DiskRetention:   24 * time.Hour,
		MemoryThreshold: 0.8,
		DiskThreshold:   0.8,
		BatchSize:       10,
		ArchiveInterval: time.Minute,
	}

	manager, err := NewManager(5, 100, filepath.Join(tmpDir, "test.db"), nil, policy)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Store messages
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		msg := Message{
			ID:        string(rune(i)),
			Topic:     "test-topic",
			Payload:   make([]byte, 20), // Large enough to trigger memory pressure
			Timestamp: time.Now(),
			TTL:       3,
		}
		if err := manager.Store(msg); err != nil {
			t.Errorf("Failed to store message %d: %v", i, err)
		}
	}

	// Wait for memory pressure to trigger disk offload
	time.Sleep(100 * time.Millisecond)

	// Check metrics
	metrics := manager.GetMetrics()
	managerMetrics := metrics["manager"].(map[string]interface{})

	// All messages should be on disk
	if managerMetrics["messages_on_disk"].(uint64) != uint64(messageCount) {
		t.Errorf("Expected %d messages on disk, got %d", messageCount, managerMetrics["messages_on_disk"])
	}

	// Some messages might be in memory depending on pressure
	memoryMessages := managerMetrics["messages_in_memory"].(uint64)
	if memoryMessages > uint64(messageCount) {
		t.Errorf("Expected at most %d messages in memory, got %d", messageCount, memoryMessages)
	}

	// Verify we can retrieve all messages
	for i := 0; i < messageCount; i++ {
		msg, err := manager.Retrieve(string(rune(i)))
		if err != nil {
			t.Errorf("Failed to retrieve message %d: %v", i, err)
		}
		if msg.ID != string(rune(i)) {
			t.Errorf("Expected message ID %s, got %s", string(rune(i)), msg.ID)
		}
	}
}

func TestManager_MessageRetention(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tiered-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create manager with minimal retention periods
	policy := StoragePolicy{
		MemoryRetention: 10 * time.Millisecond,
		DiskRetention:   20 * time.Millisecond,
		MemoryThreshold: 0.8,
		DiskThreshold:   0.8,
		BatchSize:       10,
		ArchiveInterval: 5 * time.Millisecond, // Faster archival interval
	}

	manager, err := NewManager(100, 1024*1024, filepath.Join(tmpDir, "test.db"), nil, policy)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Store old message with short TTL (0.1 second)
	oldMsg := Message{
		ID:        "old1",
		Topic:     "test-topic",
		Payload:   []byte("old message"),
		Timestamp: time.Now().Add(-100 * time.Millisecond),
		TTL:       1, // 1 second TTL - should expire almost immediately
	}
	if err := manager.Store(oldMsg); err != nil {
		t.Errorf("Failed to store old message: %v", err)
	}

	// Store new message with longer TTL
	newMsg := Message{
		ID:        "new1",
		Topic:     "test-topic",
		Payload:   []byte("new message"),
		Timestamp: time.Now(),
		TTL:       300, // 5 minutes TTL - shouldn't expire during test
	}
	if err := manager.Store(newMsg); err != nil {
		t.Errorf("Failed to store new message: %v", err)
	}

	// Explicitly wait for TTL to expire
	time.Sleep(2 * time.Second)

	// Force compaction to clean up expired messages
	if err := manager.PerformFullCompaction(); err != nil {
		t.Errorf("Failed to perform compaction: %v", err)
	}

	// Create error channel for test errors
	errCh := make(chan error, 1)
	doneCh := make(chan struct{})

	go func() {
		defer close(doneCh)

		// Poll until old message is gone or timeout
		maxAttempts := 20
		for i := 0; i < maxAttempts; i++ {
			// Check if old message is gone
			if _, err := manager.Retrieve(oldMsg.ID); err != nil {
				// Verify new message still exists
				if msg, err := manager.Retrieve(newMsg.ID); err != nil {
					errCh <- fmt.Errorf("failed to retrieve new message: %v", err)
					return
				} else if msg.ID != newMsg.ID {
					errCh <- fmt.Errorf("expected new message to still exist")
					return
				}
				return // Success
			}

			// Try explicit compaction every few attempts
			if i > 0 && i%5 == 0 {
				if err := manager.PerformFullCompaction(); err != nil {
					errCh <- fmt.Errorf("compaction failed: %v", err)
					return
				}
			}

			time.Sleep(50 * time.Millisecond)
		}
		errCh <- fmt.Errorf("old message not deleted after %d attempts", maxAttempts)
	}()

	// Wait for test to complete or timeout
	select {
	case err := <-errCh:
		if err != nil {
			t.Error(err)
		}
	case <-doneCh:
		// Test completed successfully
	case <-time.After(3 * time.Second):
		t.Fatal("Test timed out")
	}

	// Clean up
	if err := manager.Close(); err != nil {
		t.Errorf("Failed to close manager: %v", err)
	}
}

func TestManager_Concurrent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tiered-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	policy := StoragePolicy{
		MemoryRetention: time.Hour,
		DiskRetention:   24 * time.Hour,
		MemoryThreshold: 0.8,
		DiskThreshold:   0.8,
		BatchSize:       100,
		ArchiveInterval: time.Minute,
	}

	manager, err := NewManager(1000, 1024*1024, filepath.Join(tmpDir, "test.db"), nil, policy)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	defer manager.Close()

	// Concurrent stores and retrieves
	done := make(chan bool)
	messageCount := 100
	errCh := make(chan error, 2)

	// Store messages
	go func() {
		defer func() { done <- true }()
		for i := 0; i < messageCount; i++ {
			msg := Message{
				ID:        fmt.Sprintf("msg-%d", i),
				Topic:     "test-topic",
				Payload:   []byte("test message"),
				Timestamp: time.Now(),
				TTL:       3,
			}
			if err := manager.Store(msg); err != nil {
				errCh <- fmt.Errorf("failed to store message %d: %v", i, err)
				return
			}
		}
	}()

	// Retrieve messages
	go func() {
		defer func() { done <- true }()
		for i := 0; i < messageCount; i++ {
			if _, err := manager.GetTopicMessages("test-topic"); err != nil {
				errCh <- fmt.Errorf("failed to get topic messages: %v", err)
				return
			}
		}
	}()

	// Wait for both goroutines
	<-done
	<-done

	// Check for any errors
	select {
	case err := <-errCh:
		t.Error(err)
	default:
	}

	// Verify final state
	msgIDs, err := manager.GetTopicMessages("test-topic")
	if err != nil {
		t.Errorf("Failed to get topic messages: %v", err)
	}

	if len(msgIDs) != messageCount {
		t.Errorf("Expected %d messages, got %d", messageCount, len(msgIDs))
	}

	// Check metrics
	metrics := manager.GetMetrics()
	if metrics == nil {
		t.Error("Failed to get metrics")
		return
	}

	managerMetrics := metrics["manager"].(map[string]interface{})
	onDisk := managerMetrics["messages_on_disk"].(uint64)
	if onDisk != uint64(messageCount) {
		t.Errorf("Expected %d messages on disk, got %d", messageCount, onDisk)
	}
}
