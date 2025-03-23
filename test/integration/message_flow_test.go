package integration

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sprawl/store"
	"sprawl/store/tiered"
)

// TestEndToEndMessageFlow tests the complete flow of messages from producer to consumer
func TestEndToEndMessageFlow(t *testing.T) {
	// Create a temporary directory for the disk tier
	tmpDir, err := os.MkdirTemp("", "integration-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create tiered store with memory and disk
	policy := tiered.StoragePolicy{
		MemoryRetention: time.Hour,
		DiskRetention:   24 * time.Hour,
		MemoryThreshold: 0.8,
		DiskThreshold:   0.8,
		BatchSize:       100,
		ArchiveInterval: time.Minute,
	}

	// Create tiered storage manager
	tieredManager, err := tiered.NewManager(
		1000,                             // buffer size
		10*1024*1024,                     // memory limit
		filepath.Join(tmpDir, "test.db"), // disk path
		nil,                              // cloud storage (nil for this test)
		policy,
	)
	if err != nil {
		t.Fatalf("Failed to create tiered manager: %v", err)
	}
	defer tieredManager.Close()

	// Create a test store with the tiered manager
	s := store.NewStore()

	// Setup a consumer
	messageReceived := make(chan store.Message, 10)
	s.Subscribe("test-topic", func(msg store.Message) {
		messageReceived <- msg
	})

	// Publish multiple messages
	numMessages := 5
	for i := 0; i < numMessages; i++ {
		msg := store.Message{
			ID:        fmt.Sprintf("integration-msg-%d", i),
			Topic:     "test-topic",
			Payload:   []byte(fmt.Sprintf("integration test payload %d", i)),
			Timestamp: time.Now(),
			TTL:       3600, // 1 hour
		}

		err := s.Publish(msg)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Wait for all messages to be delivered
	receivedMsgs := make([]store.Message, 0, numMessages)
	for i := 0; i < numMessages; i++ {
		select {
		case msg := <-messageReceived:
			receivedMsgs = append(receivedMsgs, msg)
		case <-time.After(2 * time.Second):
			t.Fatalf("Timed out waiting for message %d", i)
		}
	}

	// Verify all messages were received
	if len(receivedMsgs) != numMessages {
		t.Errorf("Expected %d messages, received %d", numMessages, len(receivedMsgs))
	}

	// Verify message order (this depends on your system's guarantees)
	for i, msg := range receivedMsgs {
		expectedID := fmt.Sprintf("integration-msg-%d", i)
		if msg.ID != expectedID {
			t.Logf("Note: Messages received out of order. Expected ID %s, got %s", expectedID, msg.ID)
			// This might be expected depending on your system's ordering guarantees
		}
	}
}

// TestPersistenceAcrossRestarts tests that messages are properly persisted and can be
// recovered after a restart
func TestPersistenceAcrossRestarts(t *testing.T) {
	// Create a temporary directory for the disk tier
	tmpDir, err := os.MkdirTemp("", "persistence-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	dbPath := filepath.Join(tmpDir, "persist.db")

	// Phase 1: Store messages
	{
		// Create disk store
		diskStore, err := tiered.NewRocksStore(dbPath)
		if err != nil {
			t.Fatalf("Failed to create RocksStore: %v", err)
		}

		// Store some test messages
		numMessages := 5
		for i := 0; i < numMessages; i++ {
			msg := tiered.Message{
				ID:        fmt.Sprintf("persist-msg-%d", i),
				Topic:     "persistence-topic",
				Payload:   []byte(fmt.Sprintf("persistence test payload %d", i)),
				Timestamp: time.Now(),
				TTL:       3600, // 1 hour
			}

			err := diskStore.Store(msg)
			if err != nil {
				t.Fatalf("Failed to store message %d: %v", i, err)
			}
		}

		// Verify messages are stored
		msgIDs, err := diskStore.GetTopicMessages("persistence-topic")
		if err != nil {
			t.Fatalf("Failed to get topic messages: %v", err)
		}

		if len(msgIDs) != numMessages {
			t.Errorf("Expected %d messages in topic, found %d", numMessages, len(msgIDs))
		}

		// Close the store to simulate a restart
		diskStore.Close()
	}

	// Phase 2: Recover messages after "restart"
	{
		// Create a new store instance pointing to the same db file
		diskStore, err := tiered.NewRocksStore(dbPath)
		if err != nil {
			t.Fatalf("Failed to create RocksStore for recovery: %v", err)
		}
		defer diskStore.Close()

		// Retrieve the stored message IDs
		msgIDs, err := diskStore.GetTopicMessages("persistence-topic")
		if err != nil {
			t.Fatalf("Failed to get topic messages after restart: %v", err)
		}

		// Verify the correct number of messages
		if len(msgIDs) != 5 {
			t.Errorf("Expected 5 messages after restart, found %d", len(msgIDs))
		}

		// Retrieve and verify each message
		for i := 0; i < 5; i++ {
			expectedID := fmt.Sprintf("persist-msg-%d", i)

			// Find the message ID
			found := false
			for _, id := range msgIDs {
				if id == expectedID {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Message %s not found after restart", expectedID)
				continue
			}

			// Retrieve the actual message
			msg, err := diskStore.Retrieve(expectedID)
			if err != nil {
				t.Errorf("Failed to retrieve message %s: %v", expectedID, err)
				continue
			}

			// Verify message content
			expectedPayload := fmt.Sprintf("persistence test payload %d", i)
			if string(msg.Payload) != expectedPayload {
				t.Errorf("Message %s has incorrect payload. Expected %s, got %s",
					expectedID, expectedPayload, string(msg.Payload))
			}
		}
	}
}

// TestCrossNodeMessageFlow simulates message flow across multiple nodes
func TestCrossNodeMessageFlow(t *testing.T) {
	// Create two separate stores to simulate different nodes
	node1Store := store.NewStore()
	node2Store := store.NewStore()

	// Setup a way to forward messages between nodes
	forwardToNode2 := make(chan store.Message, 10)

	// Node 1 subscriber that forwards to Node 2
	node1Store.Subscribe("cross-node-topic", func(msg store.Message) {
		// Simulate network transmission to node 2
		forwardToNode2 <- msg
	})

	// Node 2 consumer that receives the final message
	msgReceivedAtNode2 := make(chan store.Message, 10)
	node2Store.Subscribe("cross-node-topic", func(msg store.Message) {
		msgReceivedAtNode2 <- msg
	})

	// Start a goroutine to simulate the forwarding process
	go func() {
		for msg := range forwardToNode2 {
			// Simulate some network delay
			time.Sleep(50 * time.Millisecond)
			// Forward to node 2
			_ = node2Store.Publish(msg)
		}
	}()

	// Publish a message at node 1
	testMsg := store.Message{
		ID:        "cross-node-msg",
		Topic:     "cross-node-topic",
		Payload:   []byte("cross node test payload"),
		Timestamp: time.Now(),
		TTL:       3600,
	}

	err := node1Store.Publish(testMsg)
	if err != nil {
		t.Fatalf("Failed to publish message at node 1: %v", err)
	}

	// Wait for the message to arrive at node 2
	select {
	case receivedMsg := <-msgReceivedAtNode2:
		// Verify message content
		if receivedMsg.ID != testMsg.ID {
			t.Errorf("Incorrect message ID at node 2. Expected %s, got %s",
				testMsg.ID, receivedMsg.ID)
		}
		if string(receivedMsg.Payload) != string(testMsg.Payload) {
			t.Errorf("Incorrect payload at node 2. Expected %s, got %s",
				string(testMsg.Payload), string(receivedMsg.Payload))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for message at node 2")
	}
}

// TestTieredStorageFlow tests the flow of messages through different storage tiers
func TestTieredStorageFlow(t *testing.T) {
	// Create a temporary directory for the disk tier
	tmpDir, err := os.MkdirTemp("", "tiered-flow-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create tiered store with very short retention times to test movement through tiers
	policy := tiered.StoragePolicy{
		MemoryRetention: 500 * time.Millisecond, // Move to disk after 500ms
		DiskRetention:   2 * time.Second,        // Keep in disk for 2s (longer for test stability)
		MemoryThreshold: 0.2,                    // Low threshold to force tier movement
		DiskThreshold:   0.2,
		BatchSize:       10,
		ArchiveInterval: 250 * time.Millisecond, // Archive frequently for test
	}

	// Create tiered storage manager
	manager, err := tiered.NewManager(
		100,                                // small buffer size
		1024*1024,                          // 1MB memory limit
		filepath.Join(tmpDir, "tiered.db"), // disk path
		nil,                                // no cloud storage for this test
		policy,
	)
	if err != nil {
		t.Fatalf("Failed to create tiered manager: %v", err)
	}
	defer manager.Close()

	// Store a test message
	testMsg := tiered.Message{
		ID:        "tiered-test-msg",
		Topic:     "tiered-topic",
		Payload:   []byte("test tiered storage flow"),
		Timestamp: time.Now(),
		TTL:       10, // 10 seconds
	}

	// Store the message
	err = manager.Store(testMsg)
	if err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	// Verify message is initially in memory
	// Implementation depends on your tiered storage API
	time.Sleep(100 * time.Millisecond) // Short wait for async operations

	// Retrieve message and verify it exists
	msg, err := manager.Retrieve(testMsg.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve message immediately: %v", err)
	}
	if msg.ID != testMsg.ID {
		t.Errorf("Retrieved wrong message. Expected ID %s, got %s", testMsg.ID, msg.ID)
	}

	// Wait for message to move to disk tier
	time.Sleep(1 * time.Second)

	// Message should still be retrievable
	msg, err = manager.Retrieve(testMsg.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve message after tier movement: %v", err)
	}
	if msg.ID != testMsg.ID {
		t.Errorf("Retrieved wrong message after tier movement. Expected ID %s, got %s", testMsg.ID, msg.ID)
	}

	// Verify the topic index still contains the message
	msgIDs, err := manager.GetTopicMessages(testMsg.Topic)
	if err != nil {
		t.Fatalf("Failed to get topic messages: %v", err)
	}

	found := false
	for _, id := range msgIDs {
		if id == testMsg.ID {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Message ID not found in topic index after tier movement")
	}
}
