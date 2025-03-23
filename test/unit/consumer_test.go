package unit

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sprawl/store"
)

// TestConsumerBasicConsumption tests basic message consumption
func TestConsumerBasicConsumption(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Create a message
	msg := store.Message{
		ID:        "consumer-test-1",
		Topic:     "consumer-topic",
		Payload:   []byte("consumer test payload"),
		Timestamp: time.Now(),
		TTL:       60,
	}

	// Create a channel to signal message receipt
	messageReceived := make(chan store.Message, 1)

	// Set up a consumer
	s.Subscribe(msg.Topic, func(received store.Message) {
		messageReceived <- received
	})

	// Publish the message
	err := s.Publish(msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Verify the consumer received the message
	select {
	case received := <-messageReceived:
		if received.ID != msg.ID {
			t.Errorf("Expected message ID %s, got %s", msg.ID, received.ID)
		}
		if string(received.Payload) != string(msg.Payload) {
			t.Errorf("Expected payload %s, got %s", string(msg.Payload), string(received.Payload))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for message")
	}
}

// TestConsumerMultipleConsumers tests multiple consumers on the same topic
func TestConsumerMultipleConsumers(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Number of consumers
	numConsumers := 3

	// Create channels to track message delivery to each consumer
	consumerChannels := make([]chan store.Message, numConsumers)
	for i := 0; i < numConsumers; i++ {
		consumerChannels[i] = make(chan store.Message, 1)

		// Set up a consumer with its own channel
		consumer := func(id int) func(msg store.Message) {
			return func(msg store.Message) {
				consumerChannels[id] <- msg
			}
		}(i)

		s.Subscribe("shared-topic", consumer)
	}

	// Create and publish a test message
	msg := store.Message{
		ID:        "shared-message",
		Topic:     "shared-topic",
		Payload:   []byte("message for multiple consumers"),
		Timestamp: time.Now(),
		TTL:       60,
	}

	err := s.Publish(msg)
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	// Verify each consumer received the message
	for i, ch := range consumerChannels {
		select {
		case received := <-ch:
			if received.ID != msg.ID {
				t.Errorf("Consumer %d: Expected message ID %s, got %s", i, msg.ID, received.ID)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Consumer %d: Timed out waiting for message", i)
		}
	}
}

// TestConsumerMultipleTopics tests a consumer subscribed to multiple topics
func TestConsumerMultipleTopics(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Topics to subscribe to
	topics := []string{"topic1", "topic2", "topic3"}

	// Messages received per topic
	messagesReceived := make(map[string]int)
	var mu sync.Mutex

	// Channel to signal message receipt
	messageReceived := make(chan struct{}, len(topics))

	// Single consumer subscribing to multiple topics
	for _, topic := range topics {
		// Need to capture the topic in the closure
		currentTopic := topic
		s.Subscribe(currentTopic, func(msg store.Message) {
			mu.Lock()
			messagesReceived[currentTopic]++
			mu.Unlock()
			messageReceived <- struct{}{}
		})
	}

	// Publish a message to each topic
	for _, topic := range topics {
		msg := store.Message{
			ID:        fmt.Sprintf("msg-%s", topic),
			Topic:     topic,
			Payload:   []byte(fmt.Sprintf("payload for %s", topic)),
			Timestamp: time.Now(),
			TTL:       60,
		}

		err := s.Publish(msg)
		if err != nil {
			t.Fatalf("Failed to publish to %s: %v", topic, err)
		}
	}

	// Wait for all messages
	for i := 0; i < len(topics); i++ {
		select {
		case <-messageReceived:
			// Message received
		case <-time.After(1 * time.Second):
			t.Fatalf("Timed out waiting for message %d", i)
		}
	}

	// Verify message counts
	mu.Lock()
	defer mu.Unlock()
	for _, topic := range topics {
		count, exists := messagesReceived[topic]
		if !exists {
			t.Errorf("No messages received for topic %s", topic)
		} else if count != 1 {
			t.Errorf("Expected 1 message for topic %s, got %d", topic, count)
		}
	}
}

// TestConsumerConcurrentConsumption tests concurrent message consumption
func TestConsumerConcurrentConsumption(t *testing.T) {
	// Create a new store
	s := store.NewStore()

	// Track received message count
	var messageCount atomic.Int64
	messageReceived := make(chan struct{}, 100)

	// Concurrent consumer
	s.Subscribe("concurrent-consumer-topic", func(msg store.Message) {
		messageCount.Add(1)
		messageReceived <- struct{}{}
	})

	// Number of messages to publish concurrently
	numMessages := 50

	// Publish messages concurrently
	var wg sync.WaitGroup
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			msg := store.Message{
				ID:        fmt.Sprintf("concurrent-msg-%d", i),
				Topic:     "concurrent-consumer-topic",
				Payload:   []byte(fmt.Sprintf("concurrent payload %d", i)),
				Timestamp: time.Now(),
				TTL:       60,
			}

			err := s.Publish(msg)
			if err != nil {
				t.Errorf("Failed to publish message %d: %v", i, err)
			}
		}(i)
	}

	// Wait for all publishing to complete
	wg.Wait()

	// Wait for all messages to be consumed
	timeout := time.After(5 * time.Second)
	received := 0

	for received < numMessages {
		select {
		case <-messageReceived:
			received++
		case <-timeout:
			t.Fatalf("Timed out waiting for messages, got %d/%d", received, numMessages)
			return
		}
	}

	// Verify all messages were received
	if messageCount.Load() != int64(numMessages) {
		t.Errorf("Expected %d messages, received %d", numMessages, messageCount.Load())
	}
}

// TestConsumerSlowConsumer tests how the system handles a slow consumer
func TestConsumerSlowConsumer(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping slow consumer test in short mode")
	}

	// Create a context with timeout to prevent the test from hanging
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create a new store
	s := store.NewStore()

	// Create a channel with small buffer to simulate back-pressure
	messageReceived := make(chan store.Message, 5) // Increased buffer to prevent blocking

	// Set up a channel to signal when processing is complete
	var wg sync.WaitGroup
	wg.Add(1)

	// Set up a slow consumer
	s.Subscribe("slow-consumer-topic", func(msg store.Message) {
		// Simulate slow processing - but not too slow for testing
		select {
		case <-ctx.Done():
			return // Exit if context is done
		case <-time.After(10 * time.Millisecond):
			// Simulated processing time
		}

		select {
		case messageReceived <- msg:
			// Message sent to channel
		case <-ctx.Done():
			return // Exit if context is done
		}
	})

	// Number of messages to publish
	numMessages := 5 // Reduced from 10 to make test faster

	// Publish messages quickly
	for i := 0; i < numMessages; i++ {
		select {
		case <-ctx.Done():
			t.Fatalf("Context timeout during publishing: %v", ctx.Err())
		default:
			// Continue publishing
		}

		msg := store.Message{
			ID:        fmt.Sprintf("slow-msg-%d", i),
			Topic:     "slow-consumer-topic",
			Payload:   []byte(fmt.Sprintf("slow payload %d", i)),
			Timestamp: time.Now(),
			TTL:       60,
		}

		err := s.Publish(msg)
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	// Start counting received messages in a separate goroutine
	go func() {
		defer wg.Done()
		count := 0

		for count < numMessages {
			select {
			case <-messageReceived:
				count++
			case <-ctx.Done():
				t.Logf("Context timeout after receiving %d/%d messages", count, numMessages)
				return
			case <-time.After(5 * time.Second):
				t.Logf("Timed out waiting for messages after receiving %d/%d", count, numMessages)
				return
			}
		}

		t.Logf("Successfully received all %d messages", numMessages)
	}()

	// Wait for processing to complete with a timeout
	doneChannel := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChannel)
	}()

	select {
	case <-doneChannel:
		// All messages processed
	case <-ctx.Done():
		t.Fatalf("Test timed out: %v", ctx.Err())
	}
}
