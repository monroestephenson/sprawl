package unit

import (
	"os"
	"testing"
	"time"

	"sprawl/store"
	"sprawl/store/tiered"
)

// TestBrokerBasicOperations tests basic message storage and retrieval
func TestBrokerBasicOperations(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Test publishing a message
	msg := store.Message{
		ID:        "test-message-1",
		Topic:     "test-topic",
		Payload:   []byte("test payload"),
		Timestamp: time.Now(),
		TTL:       60,
	}

	// Publish the message
	err := s.Publish(msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Set up a subscription to verify message delivery
	messageReceived := make(chan store.Message, 1)
	s.Subscribe(msg.Topic, func(received store.Message) {
		messageReceived <- received
	})

	// Publish another message that should trigger the subscription
	msg2 := store.Message{
		ID:        "test-message-2",
		Topic:     "test-topic",
		Payload:   []byte("test payload 2"),
		Timestamp: time.Now(),
		TTL:       60,
	}

	err = s.Publish(msg2)
	if err != nil {
		t.Fatalf("Failed to publish second message: %v", err)
	}

	// Wait for the message to be delivered
	select {
	case received := <-messageReceived:
		if received.ID != msg2.ID {
			t.Errorf("Expected message ID %s, got %s", msg2.ID, received.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message delivery")
	}
}

// TestBrokerMultipleTopics tests publishing to multiple topics
func TestBrokerMultipleTopics(t *testing.T) {
	s := store.NewStore()

	// Create channels to track message delivery
	topic1Received := make(chan store.Message, 1)
	topic2Received := make(chan store.Message, 1)

	// Subscribe to two different topics
	s.Subscribe("topic1", func(msg store.Message) {
		topic1Received <- msg
	})

	s.Subscribe("topic2", func(msg store.Message) {
		topic2Received <- msg
	})

	// Publish to topic1
	msg1 := store.Message{
		ID:        "test-message-topic1",
		Topic:     "topic1",
		Payload:   []byte("test payload for topic1"),
		Timestamp: time.Now(),
		TTL:       60,
	}
	err := s.Publish(msg1)
	if err != nil {
		t.Fatalf("Failed to publish to topic1: %v", err)
	}

	// Publish to topic2
	msg2 := store.Message{
		ID:        "test-message-topic2",
		Topic:     "topic2",
		Payload:   []byte("test payload for topic2"),
		Timestamp: time.Now(),
		TTL:       60,
	}
	err = s.Publish(msg2)
	if err != nil {
		t.Fatalf("Failed to publish to topic2: %v", err)
	}

	// Check topic1 message
	select {
	case received := <-topic1Received:
		if received.ID != msg1.ID {
			t.Errorf("Wrong message received for topic1: expected %s, got %s", msg1.ID, received.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message delivery to topic1")
	}

	// Check topic2 message
	select {
	case received := <-topic2Received:
		if received.ID != msg2.ID {
			t.Errorf("Wrong message received for topic2: expected %s, got %s", msg2.ID, received.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message delivery to topic2")
	}
}

// TestBrokerUnsubscribe tests unsubscribing from a topic
func TestBrokerUnsubscribe(t *testing.T) {
	s := store.NewStore()

	messageReceived := make(chan store.Message, 1)

	// Define the callback function
	callback := func(msg store.Message) {
		messageReceived <- msg
	}

	// Subscribe to a topic
	s.Subscribe("test-topic", callback)

	// Publish a message to confirm subscription works
	msg1 := store.Message{
		ID:        "test-message-1",
		Topic:     "test-topic",
		Payload:   []byte("test payload 1"),
		Timestamp: time.Now(),
		TTL:       60,
	}
	err := s.Publish(msg1)
	if err != nil {
		t.Fatalf("Failed to publish first message: %v", err)
	}

	// Wait for the message to be delivered
	select {
	case received := <-messageReceived:
		if received.ID != msg1.ID {
			t.Errorf("Expected message ID %s, got %s", msg1.ID, received.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for first message delivery")
	}

	// Now unsubscribe
	err = s.Unsubscribe("test-topic", callback)
	if err != nil {
		t.Fatalf("Failed to unsubscribe: %v", err)
	}

	// Publish another message after unsubscribing
	msg2 := store.Message{
		ID:        "test-message-2",
		Topic:     "test-topic",
		Payload:   []byte("test payload 2"),
		Timestamp: time.Now(),
		TTL:       60,
	}
	err = s.Publish(msg2)
	if err != nil {
		t.Fatalf("Failed to publish second message: %v", err)
	}

	// No message should be received after unsubscribing
	select {
	case <-messageReceived:
		t.Error("Received a message after unsubscribing")
	case <-time.After(500 * time.Millisecond):
		// This is the expected path - no message should be received
	}
}

// TestBrokerMessageTTL tests the Time-To-Live functionality
func TestBrokerMessageTTL(t *testing.T) {
	// Create a unique temporary directory for this test
	tempDir, err := os.MkdirTemp("", "ttl-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up after test

	// Create a store with tiered storage for TTL testing with very short archive interval
	manager, err := tiered.NewManager(100, 1024*1024, tempDir, nil, tiered.StoragePolicy{
		MemoryRetention: 1 * time.Second, // Very short retention
		DiskRetention:   2 * time.Second, // Very short retention
		ArchiveInterval: 1 * time.Second, // Process archives frequently
	})
	if err != nil {
		t.Fatalf("Failed to create tiered manager: %v", err)
	}
	defer manager.Close()

	// Create message with very short TTL
	msg := tiered.Message{
		ID:        "ttl-test-message",
		Topic:     "ttl-test-topic",
		Payload:   []byte("test payload"),
		Timestamp: time.Now().Add(-3 * time.Second), // Set timestamp in the past
		TTL:       1,                                // 1 second TTL, already expired based on timestamp
	}

	// Store the message
	err = manager.Store(msg)
	if err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	// Verify message exists immediately after storing
	// This should still work because archival hasn't run yet
	retrieved, err := manager.Retrieve(msg.ID)
	if err != nil {
		t.Fatalf("Failed to retrieve message immediately: %v", err)
	}
	if retrieved.ID != msg.ID {
		t.Errorf("Wrong message retrieved: expected %s, got %s", msg.ID, retrieved.ID)
	}

	// Wait for the archive loop to run (it should run every 1 second)
	// and for the message to be deleted based on TTL
	time.Sleep(4 * time.Second)

	// Message should no longer be accessible
	_, err = manager.Retrieve(msg.ID)
	if err == nil {
		// Try one more time with a longer wait
		t.Log("Message still accessible, waiting longer...")
		time.Sleep(2 * time.Second)

		_, err = manager.Retrieve(msg.ID)
		if err == nil {
			t.Error("Message still accessible after TTL expiration and extended wait")
		} else {
			t.Logf("Successfully verified message expiration after extended wait: %v", err)
		}
	} else {
		// Success - message is gone
		t.Logf("Successfully verified message expiration: %v", err)
	}
}
