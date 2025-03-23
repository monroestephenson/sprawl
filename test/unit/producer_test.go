package unit

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"sprawl/store"
)

// TestProducerSingleMessage tests publishing a single message
func TestProducerSingleMessage(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Create a test message
	msg := store.Message{
		ID:        "producer-test-1",
		Topic:     "producer-topic",
		Payload:   []byte("producer test payload"),
		Timestamp: time.Now(),
		TTL:       60,
	}

	// Set up a capture channel to verify message delivery
	messageReceived := make(chan store.Message, 1)
	s.Subscribe(msg.Topic, func(received store.Message) {
		messageReceived <- received
	})

	// Publish the message
	err := s.Publish(msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Wait for message delivery
	select {
	case received := <-messageReceived:
		// Verify message properties were preserved
		if received.ID != msg.ID {
			t.Errorf("Expected message ID %s, got %s", msg.ID, received.ID)
		}
		if received.Topic != msg.Topic {
			t.Errorf("Expected topic %s, got %s", msg.Topic, received.Topic)
		}
		if string(received.Payload) != string(msg.Payload) {
			t.Errorf("Expected payload %s, got %s", string(msg.Payload), string(received.Payload))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message delivery")
	}
}

// TestProducerMultipleMessages tests publishing multiple messages in sequence
func TestProducerMultipleMessages(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Set up a capture channel
	messageCount := 0
	var mu sync.Mutex
	messageReceived := make(chan struct{}, 10)

	// Subscribe to the topic
	s.Subscribe("multi-message-topic", func(received store.Message) {
		mu.Lock()
		messageCount++
		mu.Unlock()
		messageReceived <- struct{}{}
	})

	// Number of messages to publish
	numMessages := 5

	// Publish multiple messages
	for i := 0; i < numMessages; i++ {
		msg := store.Message{
			ID:        fmt.Sprintf("multi-msg-%d", i),
			Topic:     "multi-message-topic",
			Payload:   []byte(fmt.Sprintf("payload-%d", i)),
			Timestamp: time.Now(),
			TTL:       60,
		}

		err := s.Publish(msg)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Wait for all messages to be delivered
	for i := 0; i < numMessages; i++ {
		select {
		case <-messageReceived:
			// Got a message
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for message %d", i)
		}
	}

	// Verify the total count
	mu.Lock()
	if messageCount != numMessages {
		t.Errorf("Expected %d messages, received %d", numMessages, messageCount)
	}
	mu.Unlock()
}

// TestProducerConcurrent tests publishing messages concurrently
func TestProducerConcurrent(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Track received messages
	var receivedMessages sync.Map
	var wg sync.WaitGroup
	messageChan := make(chan struct{}, 100)

	// Subscribe to the topic
	s.Subscribe("concurrent-topic", func(received store.Message) {
		receivedMessages.Store(received.ID, received)
		messageChan <- struct{}{}
	})

	// Number of concurrent producers
	numProducers := 5
	messagesPerProducer := 20
	totalMessages := numProducers * messagesPerProducer

	// Launch multiple producers
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for m := 0; m < messagesPerProducer; m++ {
				msgID := fmt.Sprintf("producer-%d-msg-%d", producerID, m)
				msg := store.Message{
					ID:        msgID,
					Topic:     "concurrent-topic",
					Payload:   []byte(fmt.Sprintf("payload-%s", msgID)),
					Timestamp: time.Now(),
					TTL:       60,
				}

				err := s.Publish(msg)
				if err != nil {
					t.Errorf("Failed to publish message %s: %v", msgID, err)
				}

				// Small pause to avoid overwhelming the system
				time.Sleep(time.Millisecond)
			}
		}(p)
	}

	// Wait for all producers to finish
	wg.Wait()

	// Wait for all messages to be received
	timeout := time.After(5 * time.Second)
	received := 0

	for received < totalMessages {
		select {
		case <-messageChan:
			received++
		case <-timeout:
			t.Fatalf("Timed out waiting for messages, got %d/%d", received, totalMessages)
			return
		}
	}

	// Verify all messages were received correctly
	count := 0
	receivedMessages.Range(func(key, value interface{}) bool {
		count++
		return true
	})

	if count != totalMessages {
		t.Errorf("Expected %d total messages, received %d", totalMessages, count)
	}
}

// TestProducerLargePayload tests publishing a message with a large payload
func TestProducerLargePayload(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Create a large payload (1MB)
	payloadSize := 1024 * 1024
	payload := make([]byte, payloadSize)
	for i := 0; i < payloadSize; i++ {
		payload[i] = byte(i % 256)
	}

	// Create a message with the large payload
	msg := store.Message{
		ID:        "large-payload-msg",
		Topic:     "large-payload-topic",
		Payload:   payload,
		Timestamp: time.Now(),
		TTL:       60,
	}

	// Set up a capture channel
	messageReceived := make(chan store.Message, 1)
	s.Subscribe(msg.Topic, func(received store.Message) {
		messageReceived <- received
	})

	// Publish the message
	err := s.Publish(msg)
	if err != nil {
		t.Fatalf("Failed to publish large message: %v", err)
	}

	// Wait for message delivery
	select {
	case received := <-messageReceived:
		// Verify payload size and contents
		if len(received.Payload) != payloadSize {
			t.Errorf("Expected payload size %d, got %d", payloadSize, len(received.Payload))
		}

		// Check first, middle and last bytes
		if received.Payload[0] != payload[0] {
			t.Error("First byte of payload doesn't match")
		}

		middleIndex := payloadSize / 2
		if received.Payload[middleIndex] != payload[middleIndex] {
			t.Error("Middle byte of payload doesn't match")
		}

		if received.Payload[payloadSize-1] != payload[payloadSize-1] {
			t.Error("Last byte of payload doesn't match")
		}

	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for large message delivery")
	}
}
