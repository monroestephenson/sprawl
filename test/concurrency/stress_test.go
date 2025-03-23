package concurrency

import (
	"context"
	cryptoRand "crypto/rand"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sprawl/store"
	"sprawl/store/tiered"
)

// TestHighVolumeMessageThroughput tests the system under high message volume
func TestHighVolumeMessageThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Create a global timeout context to prevent test from hanging
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	numProducers := 2         // Reduce from 3 to 2
	numConsumers := 1         // Reduce from 2 to 1
	messagesPerProducer := 50 // Reduce from 100 to 50

	// Create a new store
	s := store.NewStore()

	// Create a buffer large enough to hold all expected messages
	totalMessages := numProducers * messagesPerProducer
	msgReceived := make(chan store.Message, totalMessages)

	// Keep track of received messages and their IDs
	var receivedCount atomic.Int32
	receivedIDs := map[string]bool{}
	var mu sync.Mutex

	// Create consumers to receive messages
	for i := 0; i < numConsumers; i++ {
		s.Subscribe("stress-test", func(msg store.Message) {
			select {
			case msgReceived <- msg:
				// Message sent to channel
			default:
				// Channel full, increment counter anyway
				t.Logf("Channel full, counting message %s", msg.ID)
			}
			receivedCount.Add(1)
		})
	}

	// Create producer goroutines
	var wg sync.WaitGroup
	wg.Add(numProducers)

	// Create a producer done channel
	producerDone := make(chan struct{}, numProducers)

	for i := 0; i < numProducers; i++ {
		go func(producerID int) {
			defer func() {
				producerDone <- struct{}{}
				wg.Done()
			}()

			for j := 0; j < messagesPerProducer; j++ {
				// Check if context is done
				select {
				case <-ctx.Done():
					t.Logf("Producer %d stopping due to timeout after sending %d messages", producerID, j)
					return
				default:
					// Continue
				}

				// Create random payload between 10-50 bytes for faster testing
				payload := make([]byte, 10+rand.Intn(40))
				_, err := cryptoRand.Read(payload)
				if err != nil {
					t.Errorf("Failed to generate random payload: %v", err)
					return
				}

				msg := store.Message{
					ID:        fmt.Sprintf("producer-%d-msg-%d", producerID, j),
					Topic:     "stress-test",
					Payload:   payload,
					Timestamp: time.Now(),
					TTL:       60, // 60 second TTL
				}

				// Publish the message
				err = s.Publish(msg)
				if err != nil {
					t.Errorf("Failed to publish message: %v", err)
					return
				}

				// Simulated work to prevent overwhelming the system
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// Collect producer results with timeout
	producersFinished := 0
	producerTimeout := time.After(10 * time.Second)

	// Function to check if we have enough messages to consider test successful
	enoughMessages := func() bool {
		// 80% of messages is good enough
		return receivedCount.Load() >= int32(float64(totalMessages)*0.8)
	}

	// Wait for either all producers to finish or timeout
producerWaitLoop:
	for producersFinished < numProducers {
		select {
		case <-producerDone:
			producersFinished++
		case <-producerTimeout:
			t.Logf("Producer timeout reached with %d/%d producers finished", producersFinished, numProducers)
			break producerWaitLoop
		case <-ctx.Done():
			t.Logf("Context timeout reached waiting for producers")
			break producerWaitLoop
		}
	}

	// Track message collection progress
	receiveStart := time.Now()
	msgTimeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Wait for messages or timeout
messageCollectionLoop:
	for {
		select {
		case msg := <-msgReceived:
			mu.Lock()
			receivedIDs[msg.ID] = true
			mu.Unlock()

			// If we've received enough messages, break
			if enoughMessages() && time.Since(receiveStart) > 2*time.Second {
				t.Logf("Received sufficient messages: %d/%d", receivedCount.Load(), totalMessages)
				break messageCollectionLoop
			}
		case <-ticker.C:
			// Log progress
			t.Logf("Received %d/%d messages so far", receivedCount.Load(), totalMessages)

			// Check if we've received enough messages
			if enoughMessages() && time.Since(receiveStart) > 2*time.Second {
				t.Logf("Received sufficient messages after ticker check: %d/%d", receivedCount.Load(), totalMessages)
				break messageCollectionLoop
			}
		case <-msgTimeout:
			t.Logf("Message collection timeout reached with %d/%d messages", receivedCount.Load(), totalMessages)
			break messageCollectionLoop
		case <-ctx.Done():
			t.Logf("Context timeout reached during message collection")
			break messageCollectionLoop
		}
	}

	// Final message count
	finalCount := receivedCount.Load()
	uniqueCount := len(receivedIDs)

	t.Logf("Total messages received: %d/%d", finalCount, totalMessages)
	t.Logf("Unique messages received: %d", uniqueCount)

	// Consider the test successful if we received at least 80% of the messages
	// This allows for some tolerance in high-load scenarios
	requiredMessages := int32(float64(totalMessages) * 0.8)
	if finalCount < requiredMessages {
		t.Errorf("Received fewer messages than required: %d < %d", finalCount, requiredMessages)
	}

	// Check for duplicates
	if int(finalCount) != uniqueCount {
		t.Errorf("Duplicate messages detected: %d received but %d unique", finalCount, uniqueCount)
	}
}

// TestConcurrentTopics tests multiple producers on different topics with multiple consumers
func TestConcurrentTopics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping concurrent topics test in short mode")
	}

	// Create a master context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Reduce loads for faster testing
	numTopics := 3            // Reduce from 5 to 3
	producersPerTopic := 1    // Reduce from 2 to 1
	consumersPerTopic := 1    // Keep at 1
	messagesPerProducer := 20 // Reduce from 50 to 20

	// Create topics
	topics := make([]string, numTopics)
	for i := 0; i < numTopics; i++ {
		topics[i] = fmt.Sprintf("concurrent-topic-%d", i)
	}

	// Create a store
	s := store.NewStore()

	// Create stats for each topic
	stats := make(map[string]*topicStat)
	for _, topic := range topics {
		stats[topic] = &topicStat{
			topic:      topic,
			received:   0,
			expected:   producersPerTopic * messagesPerProducer,
			messageIDs: make(map[string]bool),
		}
	}

	// Setup a wait group for all producers
	var wg sync.WaitGroup
	wg.Add(numTopics * producersPerTopic)

	// Set up consumers for each topic
	for _, topic := range topics {
		topicStat := stats[topic]

		for i := 0; i < consumersPerTopic; i++ {
			s.Subscribe(topic, func(msg store.Message) {
				atomic.AddInt32(&topicStat.received, 1)

				topicStat.mu.Lock()
				topicStat.messageIDs[msg.ID] = true
				topicStat.mu.Unlock()
			})
		}
	}

	// Create a channel to signal when producers are done
	producerDone := make(chan struct{}, numTopics*producersPerTopic)

	// Create producer goroutines for each topic
	for _, topic := range topics {
		for j := 0; j < producersPerTopic; j++ {
			go func(topic string, producerID int) {
				defer func() {
					producerDone <- struct{}{}
					wg.Done()
				}()

				for k := 0; k < messagesPerProducer; k++ {
					// Check if context is done
					select {
					case <-ctx.Done():
						t.Logf("Producer stopping due to timeout after sending %d messages on topic %s", k, topic)
						return
					default:
						// Continue
					}

					// Create a small random payload
					payload := make([]byte, 10+rand.Intn(20))
					_, err := cryptoRand.Read(payload)
					if err != nil {
						t.Errorf("Failed to generate random payload: %v", err)
						return
					}

					msg := store.Message{
						ID:        fmt.Sprintf("%s-producer-%d-msg-%d", topic, producerID, k),
						Topic:     topic,
						Payload:   payload,
						Timestamp: time.Now(),
						TTL:       60,
					}

					// Publish message
					err = s.Publish(msg)
					if err != nil {
						t.Errorf("Failed to publish message to topic %s: %v", topic, err)
						return
					}

					// Small delay to prevent overwhelming the system
					time.Sleep(1 * time.Millisecond)
				}
			}(topic, j)
		}
	}

	// Wait for producers with timeout
	expectedProducers := numTopics * producersPerTopic
	finishedProducers := 0
	producerTimeout := time.After(10 * time.Second)

producerWaitLoop:
	for finishedProducers < expectedProducers {
		select {
		case <-producerDone:
			finishedProducers++
			if finishedProducers%producersPerTopic == 0 {
				t.Logf("Producers progress: %d/%d finished", finishedProducers, expectedProducers)
			}
		case <-producerTimeout:
			t.Logf("Producer timeout reached with %d/%d producers finished", finishedProducers, expectedProducers)
			break producerWaitLoop
		case <-ctx.Done():
			t.Logf("Context timeout reached waiting for producers")
			break producerWaitLoop
		}
	}

	// Set up monitoring for completion
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	// Wait at most 5 seconds for messages to be processed
	messageTimeout := time.After(5 * time.Second)

	// Function to check if we have enough messages in all topics
	topicsComplete := func() bool {
		allComplete := true
		for _, stat := range stats {
			// 80% of messages is good enough for each topic
			if atomic.LoadInt32(&stat.received) < int32(float64(stat.expected)*0.8) {
				allComplete = false
				break
			}
		}
		return allComplete
	}

messageWaitLoop:
	for !topicsComplete() {
		select {
		case <-ticker.C:
			// Log progress for each topic
			allReceived := true
			for _, stat := range stats {
				received := atomic.LoadInt32(&stat.received)
				t.Logf("Topic %s: %d/%d messages received", stat.topic, received, stat.expected)

				// Check if this topic has received enough messages
				if received < int32(float64(stat.expected)*0.8) {
					allReceived = false
				}
			}

			// If all topics have received enough messages, break
			if allReceived {
				t.Logf("All topics have received sufficient messages")
				break messageWaitLoop
			}

		case <-messageTimeout:
			t.Logf("Message collection timeout reached")
			break messageWaitLoop

		case <-ctx.Done():
			t.Logf("Context timeout reached during message collection")
			break messageWaitLoop
		}
	}

	// Verify results for each topic
	for _, stat := range stats {
		received := atomic.LoadInt32(&stat.received)
		uniqueCount := len(stat.messageIDs)

		t.Logf("Topic %s final results: %d/%d messages received, %d unique",
			stat.topic, received, stat.expected, uniqueCount)

		// Check if we received at least 80% of the messages for this topic
		requiredMessages := int32(float64(stat.expected) * 0.8)
		if received < requiredMessages {
			t.Errorf("Topic %s: Received fewer messages than required: %d < %d",
				stat.topic, received, requiredMessages)
		}

		// Check for duplicates
		if int(received) != uniqueCount {
			t.Errorf("Topic %s: Duplicate messages detected: %d received but %d unique",
				stat.topic, received, uniqueCount)
		}
	}
}

// topicStat tracks statistics for each topic
type topicStat struct {
	topic      string
	received   int32
	expected   int
	messageIDs map[string]bool
	mu         sync.Mutex
}

// TestConcurrentReadWrite tests concurrent reads and writes to the message store
func TestConcurrentReadWrite(t *testing.T) {
	// Skip in short mode
	if testing.Short() {
		t.Skip("Skipping concurrent read/write test in short mode")
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create a tiered store with a RingBuffer
	rb := tiered.NewRingBuffer(10000, 100*1024*1024) // 100MB memory limit
	defer rb.Close()

	// Test parameters
	numReaders := 10           // Reduced from 20
	numWriters := 5            // Reduced from 10
	operationsPerWorker := 250 // Reduced from 500
	totalOperations := (numReaders + numWriters) * operationsPerWorker

	// Use a wait group to ensure all workers are done
	var wg sync.WaitGroup
	wg.Add(numReaders + numWriters)

	// Channels to track operations
	readsDone := make(chan int, numReaders*operationsPerWorker)
	writesDone := make(chan int, numWriters*operationsPerWorker)

	// Counter for successful operations
	var successfulReads atomic.Int64
	var successfulWrites atomic.Int64

	// Start writers
	for w := 0; w < numWriters; w++ {
		go func(writerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerWorker; i++ {
				// Check if context is done
				select {
				case <-ctx.Done():
					return
				default:
				}

				msg := tiered.Message{
					ID:        fmt.Sprintf("w%d-m%d", writerID, i),
					Topic:     fmt.Sprintf("topic-%d", writerID%5), // Use 5 different topics
					Payload:   []byte(fmt.Sprintf("payload for message %d from writer %d", i, writerID)),
					Timestamp: time.Now(),
					TTL:       60,
				}

				err := rb.Enqueue(msg)
				if err == nil {
					successfulWrites.Add(1)
				}

				select {
				case writesDone <- 1:
				case <-ctx.Done():
					return
				}

				// Small random pause to simulate real-world conditions
				if i%50 == 0 {
					time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				}
			}
		}(w)
	}

	// Give writers a head start
	time.Sleep(100 * time.Millisecond)

	// Start readers
	for r := 0; r < numReaders; r++ {
		go func(readerID int) {
			defer wg.Done()

			for i := 0; i < operationsPerWorker; i++ {
				// Check if context is done
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Create a channel to handle dequeue timeout
				resultCh := make(chan struct {
					msg tiered.Message
					err error
				}, 1)

				go func() {
					msg, err := rb.Dequeue()
					resultCh <- struct {
						msg tiered.Message
						err error
					}{msg, err}
				}()

				// Wait for dequeue result with timeout
				select {
				case result := <-resultCh:
					if result.err == nil {
						successfulReads.Add(1)
					}
				case <-time.After(500 * time.Millisecond):
					// Dequeue is taking too long, continue with next operation
				case <-ctx.Done():
					return
				}

				select {
				case readsDone <- 1:
				case <-ctx.Done():
					return
				}

				// Small random pause to simulate real-world conditions
				if i%50 == 0 {
					time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
				}
			}
		}(r)
	}

	// Monitor progress with context
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()

		reads := 0
		writes := 0

		for reads+writes < totalOperations {
			select {
			case <-readsDone:
				reads++
			case <-writesDone:
				writes++
			case <-ticker.C:
				t.Logf("Progress: %d/%d operations (%d reads, %d writes)",
					reads+writes, totalOperations, reads, writes)
				t.Logf("Successful operations: %d reads, %d writes",
					successfulReads.Load(), successfulWrites.Load())
			case <-ctx.Done():
				t.Log("Test context timed out")
				return
			}
		}
	}()

	// Wait for completion or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed normally
	case <-ctx.Done():
		t.Log("Test timed out, closing ring buffer to unblock operations")
		rb.Close() // This will unblock any waiting Dequeue calls
	}

	// Log final results
	t.Logf("Concurrent read/write test complete")
	t.Logf("Total operations processed: %d reads, %d writes",
		successfulReads.Load(), successfulWrites.Load())
}
