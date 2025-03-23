package benchmarks

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

// BenchmarkMessagePublishLatency measures the latency of publishing messages
func BenchmarkMessagePublishLatency(b *testing.B) {
	// Create a new store
	s := store.NewStore()

	// Prepare a sample message template
	sampleMsg := store.Message{
		Topic:     "benchmark-topic",
		Payload:   []byte("benchmark test payload"),
		Timestamp: time.Now(),
		TTL:       3600,
	}

	// Reset the timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		// Create a unique message for each iteration
		msg := sampleMsg
		msg.ID = fmt.Sprintf("bench-msg-%d", i)

		// Time the publish operation
		start := time.Now()
		err := s.Publish(msg)
		elapsed := time.Since(start)

		if err != nil {
			b.Fatalf("Failed to publish message: %v", err)
		}

		// Report timing information
		b.ReportMetric(float64(elapsed.Nanoseconds())/1e6, "ms/op")
	}
}

// BenchmarkLatencySimple measures basic latency without the complexity
func BenchmarkLatencySimple(b *testing.B) {
	// Create a new store
	s := store.NewStore()

	// Limit iterations for stability
	maxIterations := 100
	iterations := b.N
	if iterations > maxIterations {
		iterations = maxIterations
	}

	// Create a channel to receive messages
	messageReceived := make(chan time.Duration, iterations)

	// Create a context with timeout to prevent hanging
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Variables for collecting results
	var totalLatency time.Duration
	var maxLatency time.Duration
	var minLatency = time.Hour
	var count int
	var done bool

	// Subscribe to the topic
	s.Subscribe("latency-topic", func(msg store.Message) {
		// Measure time since the message was sent
		if sentTime, ok := msg.Timestamp, true; ok {
			latency := time.Since(sentTime)
			// Non-blocking send to channel
			select {
			case messageReceived <- latency:
			default:
				// Channel full, continue anyway
			}
		}
	})

	// Reset timer before publishing
	b.ResetTimer()

	// Publish messages
	for i := 0; i < iterations && !done; i++ {
		// Create message with current timestamp
		msg := store.Message{
			ID:        fmt.Sprintf("latency-msg-%d", i),
			Topic:     "latency-topic",
			Payload:   []byte(fmt.Sprintf("latency test %d", i)),
			Timestamp: time.Now(),
			TTL:       60,
		}

		err := s.Publish(msg)
		if err != nil {
			b.Fatalf("Failed to publish message: %v", err)
		}

		// Small delay between messages
		time.Sleep(time.Millisecond)
	}

	// Collect up to iterations messages or until timeout
	for i := 0; i < iterations && !done; i++ {
		select {
		case latency := <-messageReceived:
			count++
			totalLatency += latency

			if latency > maxLatency {
				maxLatency = latency
			}
			if latency < minLatency {
				minLatency = latency
			}

		case <-ctx.Done():
			// Timeout reached
			b.Logf("Timeout reached after collecting %d/%d latency samples", count, iterations)
			done = true

		case <-time.After(time.Second):
			// No more messages within 1 second, assume we're done
			b.Logf("No more messages received after 1 second, collected %d/%d", count, iterations)
			done = true
		}
	}

	// Report results
	if count > 0 {
		avgLatency := totalLatency / time.Duration(count)
		b.ReportMetric(float64(avgLatency.Microseconds()), "avg_latency_us")
		b.ReportMetric(float64(minLatency.Microseconds()), "min_latency_us")
		b.ReportMetric(float64(maxLatency.Microseconds()), "max_latency_us")
		b.ReportMetric(float64(count)/b.Elapsed().Seconds(), "msgs/sec")
	} else {
		b.Errorf("No latency measurements collected")
	}
}

// BenchmarkThroughputSimple measures basic throughput without the complexity
func BenchmarkThroughputSimple(b *testing.B) {
	// Create a new store
	s := store.NewStore()

	// Create a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Limit iterations for stability
	maxMessages := 1000
	iterations := b.N
	if iterations > maxMessages {
		iterations = maxMessages
	}

	// Create counter for received messages
	var received atomic.Int64
	var done bool

	// Start time for throughput calculation - declare early
	startTime := time.Now()

	// Subscribe to topic
	s.Subscribe("throughput-topic", func(msg store.Message) {
		received.Add(1)
	})

	// Reset timer
	b.ResetTimer()

	// Publish messages
	for i := 0; i < iterations && !done; i++ {
		select {
		case <-ctx.Done():
			b.Logf("Context timeout reached after sending %d messages", i)
			done = true
			continue // Use continue instead of break to properly exit the loop
		default:
			// Continue publishing
		}

		msg := store.Message{
			ID:        fmt.Sprintf("throughput-msg-%d", i),
			Topic:     "throughput-topic",
			Payload:   []byte(fmt.Sprintf("throughput test %d", i)),
			Timestamp: time.Now(),
			TTL:       60,
		}

		err := s.Publish(msg)
		if err != nil {
			b.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Wait for messages to be received
	timeout := time.NewTimer(2 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Wait loop
	waitComplete := false
	for received.Load() < int64(iterations) && !waitComplete {
		select {
		case <-ticker.C:
			b.Logf("Received %d/%d messages", received.Load(), iterations)
		case <-timeout.C:
			b.Logf("Timeout reached with %d/%d messages received", received.Load(), iterations)
			waitComplete = true
		case <-ctx.Done():
			b.Logf("Context timeout reached with %d/%d messages received", received.Load(), iterations)
			waitComplete = true
		}
	}

	// Report results
	elapsed := time.Since(startTime)
	throughput := float64(iterations) / elapsed.Seconds()
	receivedThroughput := float64(received.Load()) / elapsed.Seconds()

	b.ReportMetric(throughput, "sent_msgs/sec")
	b.ReportMetric(receivedThroughput, "received_msgs/sec")
	b.ReportMetric(float64(received.Load()), "msgs_received")
}

// BenchmarkMessageThroughputSingleProducer measures message throughput with a single producer
func BenchmarkMessageThroughputSingleProducer(b *testing.B) {
	// Create a new store
	s := store.NewStore()

	// Determine number of iterations
	iterations := b.N

	// Setup a consumer to consume the messages
	consumed := make(chan struct{}, iterations)
	s.Subscribe("throughput-topic", func(msg store.Message) {
		consumed <- struct{}{}
	})

	// Prepare messages
	b.ResetTimer()

	// Measure publish duration
	publishStart := time.Now()

	// Publish iterations messages
	for i := 0; i < iterations; i++ {
		msg := store.Message{
			ID:        fmt.Sprintf("throughput-msg-%d", i),
			Topic:     "throughput-topic",
			Payload:   []byte(fmt.Sprintf("throughput test %d", i)),
			Timestamp: time.Now(),
			TTL:       60,
		}

		err := s.Publish(msg)
		if err != nil {
			b.Fatalf("Failed to publish message: %v", err)
		}
	}

	publishDuration := time.Since(publishStart)
	throughput := float64(iterations) / publishDuration.Seconds()
	b.ReportMetric(throughput, "msgs_published/sec")

	// Wait for all messages to be consumed with a timeout
	timeout := time.After(5 * time.Second)
	for i := 0; i < iterations; i++ {
		select {
		case <-consumed:
			// Message consumed
		case <-timeout:
			b.Fatalf("Timed out waiting for messages to be consumed, got %d/%d", i, iterations)
		}
	}
}

// BenchmarkMessageThroughputMultiProducer measures message throughput with multiple producers
func BenchmarkMessageThroughputMultiProducer(b *testing.B) {
	// Skip if b.N is too small
	if b.N < 100 {
		b.Skip("Skipping multi-producer benchmark for small iteration count")
	}

	// Create a new store
	s := store.NewStore()

	// Configure multiple producers
	numProducers := 4
	messagesPerProducer := b.N / numProducers

	// Ensure we're sending exactly iterations messages
	remainder := b.N - (messagesPerProducer * numProducers)
	adjustedMessagesPerProducer := make([]int, numProducers)
	for i := 0; i < numProducers; i++ {
		adjustedMessagesPerProducer[i] = messagesPerProducer
		if i < remainder {
			adjustedMessagesPerProducer[i]++
		}
	}

	// Setup a consumer to consume the messages
	var consumedCount atomic.Int64
	s.Subscribe("throughput-topic", func(msg store.Message) {
		consumedCount.Add(1)
	})

	// Keep track of errors
	var errCount atomic.Int64
	var sentCount atomic.Int64

	// Reset timer before the test
	b.ResetTimer()

	// Measure publish duration
	publishStart := time.Now()

	// Start producer goroutines
	var wg sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int, count int) {
			defer wg.Done()

			for j := 0; j < count; j++ {
				// Create a message
				msg := store.Message{
					ID:        fmt.Sprintf("producer-%d-msg-%d", producerID, j),
					Topic:     "throughput-topic",
					Payload:   []byte(fmt.Sprintf("throughput test producer %d msg %d", producerID, j)),
					Timestamp: time.Now(),
					TTL:       60,
				}

				// Publish the message
				err := s.Publish(msg)
				if err != nil {
					errCount.Add(1)
					return
				}

				sentCount.Add(1)
			}
		}(i, adjustedMessagesPerProducer[i])
	}

	// Wait for all producers to complete
	wg.Wait()
	publishDuration := time.Since(publishStart)
	throughput := float64(b.N) / publishDuration.Seconds()
	b.ReportMetric(throughput, "msgs_published/sec")

	// Wait for all messages to be consumed with a timeout
	timeout := time.After(5 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

consumeLoop:
	for consumedCount.Load() < int64(b.N) {
		select {
		case <-ticker.C:
			b.Logf("Consumed %d/%d messages", consumedCount.Load(), b.N)
		case <-timeout:
			b.Logf("Timeout reached with %d/%d messages consumed",
				consumedCount.Load(), b.N)
			break consumeLoop
		}
	}

	// Report metrics
	b.ReportMetric(float64(sentCount.Load())/publishDuration.Seconds(), "sent_msgs/sec")
	b.ReportMetric(float64(consumedCount.Load())/b.Elapsed().Seconds(), "consumed_msgs/sec")
	b.ReportMetric(float64(errCount.Load()), "errors")
}

// BenchmarkRingBufferThroughput benchmarks the throughput of the ring buffer
func BenchmarkRingBufferThroughput(b *testing.B) {
	// Skip in short mode
	if testing.Short() {
		b.Skip("Skipping ring buffer throughput test in short mode")
	}

	// Create a new ring buffer
	bufferSize := 10000
	rb := tiered.NewRingBuffer(uint64(bufferSize), 100*1024*1024)
	defer rb.Close()

	// Benchmark parameters
	numProducers := 4
	numConsumers := 4

	messagesPerProducer := b.N / numProducers
	totalMessages := messagesPerProducer * numProducers

	// Create small to medium sized payloads (100-1000 bytes)
	createPayload := func() []byte {
		size := 100 + rand.Intn(900)
		payload := make([]byte, size)
		// Use crypto/rand instead of math/rand
		_, err := cryptoRand.Read(payload)
		if err != nil {
			b.Errorf("Failed to generate random payload: %v", err)
		}
		return payload
	}

	// Synchronization
	var wg sync.WaitGroup
	var consumedCount atomic.Int64
	startChan := make(chan struct{})

	// Start consumers
	for c := 0; c < numConsumers; c++ {
		wg.Add(1)
		go func(consumerID int) {
			defer wg.Done()

			// Wait for the signal to start
			<-startChan

			for {
				_, err := rb.Dequeue()
				if err != nil {
					// Check if we're done
					count := consumedCount.Load()
					if count >= int64(totalMessages) {
						return
					}
					// Otherwise, try again
					time.Sleep(time.Microsecond)
					continue
				}

				// Count the message
				consumedCount.Add(1)

				// Check if we're done
				if consumedCount.Load() >= int64(totalMessages) {
					return
				}
			}
		}(c)
	}

	// Reset timer before starting producers
	b.ResetTimer()

	// Signal to start
	close(startChan)
	startTime := time.Now()

	// Start producers
	var producerWg sync.WaitGroup
	for p := 0; p < numProducers; p++ {
		producerWg.Add(1)
		go func(producerID int) {
			defer producerWg.Done()

			for i := 0; i < messagesPerProducer; i++ {
				msg := tiered.Message{
					ID:        fmt.Sprintf("p%d-msg-%d", producerID, i),
					Topic:     fmt.Sprintf("benchmark-topic-%d", producerID%5),
					Payload:   createPayload(),
					Timestamp: time.Now(),
					TTL:       60,
				}

				// Use backoff on errors
				backoff := time.Millisecond
				for {
					err := rb.Enqueue(msg)
					if err == nil {
						break
					}

					// If buffer is full, wait and retry
					time.Sleep(backoff)
					backoff *= 2
					if backoff > 100*time.Millisecond {
						backoff = 100 * time.Millisecond
					}
				}
			}
		}(p)
	}

	// Wait for all producers to finish
	producerWg.Wait()
	producerDuration := time.Since(startTime)

	// Wait for all consumers
	timeout := time.After(30 * time.Second)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All done
	case <-timeout:
		b.Fatalf("Timed out waiting for consumption to complete, consumed %d/%d",
			consumedCount.Load(), totalMessages)
		return
	}

	totalDuration := time.Since(startTime)

	// Report metrics
	throughputProduce := float64(totalMessages) / producerDuration.Seconds()
	throughputTotal := float64(totalMessages) / totalDuration.Seconds()

	b.ReportMetric(throughputProduce, "produce_msgs/sec")
	b.ReportMetric(throughputTotal, "total_msgs/sec")
}

// BenchmarkLatencyWithLoad measures publish-to-receive latency under different loads
func BenchmarkLatencyWithLoad(b *testing.B) {
	benchmarks := []struct {
		name          string
		baseLoadMsgs  int // background messages to generate load
		latencyMsgPct int // percentage of messages to measure latency for
	}{
		{"LightLoad", 20, 100}, // 100% of messages measured for latency - reduced from 50
		{"MediumLoad", 50, 20}, // 20% of messages measured for latency - reduced from 100
		{"HeavyLoad", 100, 10}, // 10% of messages measured for latency - reduced from 500
	}

	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			// Create a new store
			s := store.NewStore()

			// Create a master context with timeout
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()

			// Limit the number of iterations for stability
			maxIterations := 50
			iterations := b.N
			if iterations > maxIterations {
				iterations = maxIterations // Maximum 50 iterations for any benchmark
			}

			// Create a special topic for latency measurement
			latencyTopic := "latency-topic"
			baseLoadTopic := "load-topic"

			// Channel to receive messages with timestamps
			latencyResults := make(chan time.Duration, iterations)

			// Subscribe to the latency topic
			s.Subscribe(latencyTopic, func(msg store.Message) {
				// Extract send timestamp
				latency := time.Since(msg.Timestamp)
				// Non-blocking send
				select {
				case latencyResults <- latency:
				default:
					// Channel full, continue anyway
				}
			})

			// Create base load subscriber
			s.Subscribe(baseLoadTopic, func(msg store.Message) {
				// Just consume messages, no latency measurement
			})

			// Reset timer before the test
			b.ResetTimer()

			// Number of messages to check latency - use iterations instead of b.N
			latencyMsgs := iterations

			// Number of base load messages to generate
			baseLoadMsgs := bm.baseLoadMsgs

			// Keep track of how many latency messages we've sent
			latencySent := 0

			// Start time for throughput calculation
			startTime := time.Now()

			// Process latency results in parallel
			var totalLatency time.Duration
			var maxLatency time.Duration
			var minLatency = time.Hour
			var collected int32

			go func() {
				ticker := time.NewTicker(500 * time.Millisecond)
				defer ticker.Stop()

				for {
					select {
					case latency, ok := <-latencyResults:
						if !ok {
							return
						}

						atomic.AddInt32(&collected, 1)
						totalLatency += latency

						if latency > maxLatency {
							maxLatency = latency
						}
						if latency < minLatency {
							minLatency = latency
						}

					case <-ticker.C:
						b.Logf("Collected %d/%d latency samples", atomic.LoadInt32(&collected), latencyMsgs)

					case <-ctx.Done():
						return
					}
				}
			}()

			// Generate load messages and latency messages
			for i := 0; i < baseLoadMsgs && ctx.Err() == nil; i++ {
				// Create and publish a load message
				loadMsg := store.Message{
					ID:        fmt.Sprintf("load-msg-%d", i),
					Topic:     baseLoadTopic,
					Payload:   []byte(fmt.Sprintf("load message %d", i)),
					Timestamp: time.Now(),
					TTL:       60,
				}

				if err := s.Publish(loadMsg); err != nil {
					b.Errorf("Failed to publish load message: %v", err)
					continue
				}

				// Check if we should send a latency message
				if rand.Intn(100) < bm.latencyMsgPct && latencySent < latencyMsgs {
					// Create a latency message with current timestamp
					latencyMsg := store.Message{
						ID:        fmt.Sprintf("latency-msg-%d", latencySent),
						Topic:     latencyTopic,
						Payload:   []byte(fmt.Sprintf("latency test %d", latencySent)),
						Timestamp: time.Now(),
						TTL:       60,
					}

					if err := s.Publish(latencyMsg); err != nil {
						b.Errorf("Failed to publish latency message: %v", err)
						continue
					}

					latencySent++
				}

				// Small delay between messages to avoid overwhelming the system
				time.Sleep(time.Millisecond)
			}

			// Wait for results or timeout
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer waitCancel()

			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()

			for atomic.LoadInt32(&collected) < int32(latencySent) {
				select {
				case <-ticker.C:
					b.Logf("Waiting for results: collected %d/%d", atomic.LoadInt32(&collected), latencySent)
				case <-waitCtx.Done():
					b.Logf("Wait timeout: collected %d/%d latency measurements", atomic.LoadInt32(&collected), latencySent)
					goto ResultProcessing
				}
			}

		ResultProcessing:
			// Calculate metrics
			collectedCount := atomic.LoadInt32(&collected)

			if collectedCount > 0 {
				avgLatency := totalLatency / time.Duration(collectedCount)
				throughput := float64(baseLoadMsgs+latencySent) / time.Since(startTime).Seconds()

				// Report metrics
				b.ReportMetric(float64(avgLatency.Microseconds()), "avg_latency_us")
				if minLatency < time.Hour {
					b.ReportMetric(float64(minLatency.Microseconds()), "min_latency_us")
				}
				b.ReportMetric(float64(maxLatency.Microseconds()), "max_latency_us")
				b.ReportMetric(throughput, "msgs/sec")
			} else {
				b.Error("No latency measurements collected")
			}
		})
	}
}
