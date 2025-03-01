package tiered

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRingBuffer_BasicOperations(t *testing.T) {
	rb := NewRingBuffer(10, 1024*1024) // 1MB limit
	defer rb.Close()

	// Test Enqueue
	msg := Message{
		ID:        "test1",
		Topic:     "test-topic",
		Payload:   []byte("test message"),
		Timestamp: time.Now(),
		TTL:       3,
	}

	err := rb.Enqueue(msg)
	if err != nil {
		t.Errorf("Failed to enqueue message: %v", err)
	}

	// Test Dequeue
	dequeued, err := rb.Dequeue()
	if err != nil {
		t.Errorf("Failed to dequeue message: %v", err)
	}

	if dequeued.ID != msg.ID {
		t.Errorf("Expected message ID %s, got %s", msg.ID, dequeued.ID)
	}
}

func TestRingBuffer_MemoryPressure(t *testing.T) {
	// Create a buffer with a small memory limit
	memoryLimit := uint64(1024) // 1KB limit
	rb := NewRingBuffer(10, memoryLimit)
	defer rb.Close()

	pressureChanged := make(chan int32, 1)
	rb.SetPressureCallback(func(level int32) {
		select {
		case pressureChanged <- level:
		default:
		}
	})

	// Create messages that will exceed the memory limit
	msg := Message{
		ID:        "test1",
		Topic:     "test-topic",
		Payload:   make([]byte, 800), // Large enough to trigger pressure
		Timestamp: time.Now(),
		TTL:       3,
	}

	// Enqueue messages until we hit memory pressure
	var enqueued int
	for i := 0; i < 5; i++ {
		err := rb.Enqueue(msg)
		if err != nil {
			break
		}
		enqueued++
	}

	// Wait for pressure callback
	select {
	case level := <-pressureChanged:
		if level < 1 {
			t.Errorf("Expected pressure level >= 1, got %d", level)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Timeout waiting for pressure callback")
	}

	// Verify metrics
	metrics := rb.GetMetrics()
	memoryUsage := metrics["memory_usage"].(uint64)
	if memoryUsage <= 0 {
		t.Error("Expected non-zero memory usage")
	}
	if memoryUsage < memoryLimit*7/10 {
		t.Errorf("Expected memory usage > 70%% of limit, got %d/%d", memoryUsage, memoryLimit)
	}

	pressureLevel := metrics["pressure_level"].(int32)
	if pressureLevel < 1 {
		t.Errorf("Expected pressure level >= 1, got %d", pressureLevel)
	}
}

func TestRingBuffer_Concurrency(t *testing.T) {
	producers := 5
	consumers := 3
	messagesPerProducer := 100
	totalMessages := producers * messagesPerProducer

	// Create buffer large enough to hold all messages
	rb := NewRingBuffer(uint64(totalMessages), 1024*1024*1024) // 1GB memory limit
	defer rb.Close()

	var wg sync.WaitGroup
	var consumedCount atomic.Int64
	errCh := make(chan error, producers+consumers)
	messageSet := sync.Map{}
	allDone := make(chan struct{})
	var producerCount atomic.Int32

	// Start producers
	for i := 0; i < producers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer func() {
				wg.Done()
				if producerCount.Add(1) == int32(producers) {
					// All producers are done
					t.Log("All producers finished")
				}
			}()

			for j := 0; j < messagesPerProducer; j++ {
				msg := Message{
					ID:        fmt.Sprintf("p%d-msg%d", producerID, j),
					Topic:     "test-topic",
					Payload:   []byte("test message"),
					Timestamp: time.Now(),
					TTL:       3,
				}
				if err := rb.Enqueue(msg); err != nil {
					errCh <- fmt.Errorf("producer %d failed to enqueue message %d: %v", producerID, j, err)
					return
				}
				if j > 0 && j%10 == 0 {
					// Give consumers a chance to catch up
					time.Sleep(time.Microsecond)
				}
			}
		}(i)
	}

	// Start consumers
	for i := 0; i < consumers; i++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()
			for {
				msg, err := rb.Dequeue()
				if err != nil {
					if err.Error() == "buffer is closed" {
						return
					}
					errCh <- fmt.Errorf("consumer %d failed to dequeue: %v", consumerID, err)
					return
				}

				// Track consumed message
				if _, loaded := messageSet.LoadOrStore(msg.ID, true); loaded {
					errCh <- fmt.Errorf("message %s was consumed multiple times", msg.ID)
					return
				}

				newCount := consumedCount.Add(1)
				if newCount >= int64(totalMessages) {
					close(allDone)
					return
				}
			}
		}(i)
	}

	// Wait for completion or timeout
	select {
	case <-allDone:
		// Success case - all messages consumed
		rb.Close() // Signal remaining consumers to exit
	case <-time.After(5 * time.Second):
		rb.Close() // Signal all goroutines to exit
		t.Fatalf("Test timed out. Producers done: %d, Messages consumed: %d/%d",
			producerCount.Load(), consumedCount.Load(), totalMessages)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Check for any errors
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// Verify metrics
	metrics := rb.GetMetrics()
	totalEnqueued := metrics["total_enqueued"].(uint64)
	totalDequeued := metrics["total_dequeued"].(uint64)

	if totalEnqueued != uint64(totalMessages) {
		t.Errorf("Expected %d enqueued messages, got %d", totalMessages, totalEnqueued)
	}

	if totalDequeued != uint64(totalMessages) {
		t.Errorf("Expected %d dequeued messages, got %d", totalMessages, totalDequeued)
	}

	// Verify message count
	var uniqueMessages int
	messageSet.Range(func(key, value interface{}) bool {
		uniqueMessages++
		return true
	})

	if uniqueMessages != totalMessages {
		t.Errorf("Expected %d unique messages, got %d", totalMessages, uniqueMessages)
	}
}

func TestRingBuffer_FullBuffer(t *testing.T) {
	rb := NewRingBuffer(2, 1024*1024)
	defer rb.Close()

	msg := Message{
		ID:        "test",
		Topic:     "test-topic",
		Payload:   []byte("test message"),
		Timestamp: time.Now(),
		TTL:       3,
	}

	// Fill the buffer
	for i := 0; i < 2; i++ {
		err := rb.Enqueue(msg)
		if err != nil {
			t.Errorf("Failed to enqueue message %d: %v", i, err)
		}
	}

	// Try to enqueue when buffer is full (should block)
	enqueued := make(chan bool)
	go func() {
		err := rb.Enqueue(msg)
		enqueued <- (err == nil)
	}()

	// Dequeue one message to make space
	time.Sleep(100 * time.Millisecond) // Ensure the goroutine is blocked
	_, err := rb.Dequeue()
	if err != nil {
		t.Errorf("Failed to dequeue message: %v", err)
	}

	// Wait for enqueue to succeed
	select {
	case success := <-enqueued:
		if !success {
			t.Error("Expected successful enqueue after dequeue")
		}
	case <-time.After(time.Second):
		t.Error("Timeout waiting for enqueue")
	}
}
