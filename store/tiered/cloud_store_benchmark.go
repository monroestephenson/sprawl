package tiered

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	mathrand "math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// BenchmarkConfig defines parameters for the cloud storage benchmark
type BenchmarkConfig struct {
	// Number of concurrent operations
	Concurrency int

	// Duration to run the benchmark for
	Duration time.Duration

	// Size of messages to use for testing (in bytes)
	MessageSize int

	// Number of topics to distribute messages across
	TopicCount int

	// Whether to perform reads as part of the benchmark
	IncludeReads bool

	// Whether to periodically log progress
	Verbose bool

	// Pause between operations (to control rate)
	OperationDelay time.Duration
}

// BenchmarkResult contains results of the benchmark
type BenchmarkResult struct {
	// Duration the benchmark actually ran for
	Duration time.Duration

	// Total messages written
	MessagesWritten uint64

	// Total messages read
	MessagesRead uint64

	// Total data transferred (bytes)
	BytesTransferred uint64

	// Errors encountered
	Errors uint64

	// Latencies for different operations
	WriteLatencyAvg time.Duration
	WriteLatencyP95 time.Duration
	WriteLatencyP99 time.Duration
	ReadLatencyAvg  time.Duration
	ReadLatencyP95  time.Duration
	ReadLatencyP99  time.Duration

	// Operations per second
	WritesPerSecond float64
	ReadsPerSecond  float64

	// Success rate
	SuccessRate float64
}

// CloudStoreBenchmark runs performance testing on cloud storage
func (cs *CloudStore) RunBenchmark(cfg BenchmarkConfig) (*BenchmarkResult, error) {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}

	if cfg.Duration <= 0 {
		cfg.Duration = 30 * time.Second
	}

	if cfg.MessageSize <= 0 {
		cfg.MessageSize = 1024 // 1KB default
	}

	if cfg.TopicCount <= 0 {
		cfg.TopicCount = 10
	}

	// Generate test topics
	topics := make([]string, cfg.TopicCount)
	for i := 0; i < cfg.TopicCount; i++ {
		topics[i] = fmt.Sprintf("bench-topic-%d", i)
	}

	// Create sample data
	sampleData := make([]byte, cfg.MessageSize)
	_, err := rand.Read(sampleData)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random data: %w", err)
	}

	// Variables to track results
	var (
		messagesWritten  uint64
		messagesRead     uint64
		bytesTransferred uint64
		errors           uint64
		writeLatencies   []time.Duration
		readLatencies    []time.Duration
		latenciesLock    sync.Mutex
	)

	// Create a context that will be canceled when the benchmark is done
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	// Start producer goroutines
	var wg sync.WaitGroup
	startTime := time.Now()

	log.Printf("[Benchmark] Starting cloud storage benchmark with %d concurrent operations for %v",
		cfg.Concurrency, cfg.Duration)

	// Start worker goroutines
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Create random data and IDs
			rnd := mathrand.New(mathrand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				// Check if benchmark is done
				if ctx.Err() != nil {
					return
				}

				// Decide whether to read or write (if reads are enabled)
				shouldRead := cfg.IncludeReads && rnd.Float32() < 0.3 // 30% reads if enabled

				if shouldRead {
					// Perform a read operation
					topicIndex := rnd.Intn(cfg.TopicCount)
					topic := topics[topicIndex]

					start := time.Now()
					messages, err := cs.GetTopicMessages(topic)
					duration := time.Since(start)

					latenciesLock.Lock()
					readLatencies = append(readLatencies, duration)
					latenciesLock.Unlock()

					if err != nil {
						atomic.AddUint64(&errors, 1)
						if cfg.Verbose {
							log.Printf("[Benchmark] Read error: %v", err)
						}
					} else {
						msgCount := uint64(len(messages))
						atomic.AddUint64(&messagesRead, msgCount)

						var bytes uint64
						for _, msg := range messages {
							bytes += uint64(len(msg.Payload))
						}
						atomic.AddUint64(&bytesTransferred, bytes)
					}
				} else {
					// Perform a write operation
					topicIndex := rnd.Intn(cfg.TopicCount)
					topic := topics[topicIndex]

					// Create a batch of messages
					batchSize := 1 + rnd.Intn(20) // 1-20 messages
					messages := make([]Message, batchSize)

					for j := 0; j < batchSize; j++ {
						id := fmt.Sprintf("bench-%d-%d-%d", workerID, time.Now().UnixNano(), j)
						messages[j] = Message{
							ID:        id,
							Payload:   sampleData,
							Timestamp: time.Now(),
						}
					}

					start := time.Now()
					err := cs.Store(topic, messages)
					duration := time.Since(start)

					latenciesLock.Lock()
					writeLatencies = append(writeLatencies, duration)
					latenciesLock.Unlock()

					if err != nil {
						atomic.AddUint64(&errors, 1)
						if cfg.Verbose {
							log.Printf("[Benchmark] Write error: %v", err)
						}
					} else {
						atomic.AddUint64(&messagesWritten, uint64(batchSize))
						atomic.AddUint64(&bytesTransferred, uint64(batchSize*cfg.MessageSize))
					}
				}

				// Sleep if requested to control rate
				if cfg.OperationDelay > 0 {
					time.Sleep(cfg.OperationDelay)
				}
			}
		}(i)
	}

	// Wait for benchmark to complete or be canceled
	<-ctx.Done()
	wg.Wait()

	// Calculate benchmark duration
	duration := time.Since(startTime)

	// Process latency statistics
	result := &BenchmarkResult{
		Duration:         duration,
		MessagesWritten:  messagesWritten,
		MessagesRead:     messagesRead,
		BytesTransferred: bytesTransferred,
		Errors:           errors,
	}

	// Calculate latency percentiles
	if len(writeLatencies) > 0 {
		result.WriteLatencyAvg = calculateAvgLatency(writeLatencies)
		result.WriteLatencyP95 = calculatePercentileLatency(writeLatencies, 95)
		result.WriteLatencyP99 = calculatePercentileLatency(writeLatencies, 99)
	}

	if len(readLatencies) > 0 {
		result.ReadLatencyAvg = calculateAvgLatency(readLatencies)
		result.ReadLatencyP95 = calculatePercentileLatency(readLatencies, 95)
		result.ReadLatencyP99 = calculatePercentileLatency(readLatencies, 99)
	}

	// Calculate rates
	durationSecs := duration.Seconds()
	if durationSecs > 0 {
		result.WritesPerSecond = float64(messagesWritten) / durationSecs
		result.ReadsPerSecond = float64(messagesRead) / durationSecs
	}

	// Calculate success rate
	totalOps := messagesWritten + messagesRead
	if totalOps > 0 {
		result.SuccessRate = 100.0 * (1.0 - float64(errors)/float64(totalOps))
	}

	log.Printf("[Benchmark] Completed: wrote %d messages, read %d messages in %v",
		messagesWritten, messagesRead, duration)
	log.Printf("[Benchmark] Throughput: %.2f writes/s, %.2f reads/s, %.2f MiB/s",
		result.WritesPerSecond, result.ReadsPerSecond,
		float64(bytesTransferred)/(1024*1024)/durationSecs)

	return result, nil
}

// Helper functions for latency calculations
func calculateAvgLatency(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}
	return sum / time.Duration(len(latencies))
}

func calculatePercentileLatency(latencies []time.Duration, percentile int) time.Duration {
	if len(latencies) == 0 {
		return 0
	}

	// Sort latencies
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)

	// Simple bubble sort for small sets
	for i := 0; i < len(sorted)-1; i++ {
		for j := 0; j < len(sorted)-i-1; j++ {
			if sorted[j] > sorted[j+1] {
				sorted[j], sorted[j+1] = sorted[j+1], sorted[j]
			}
		}
	}

	// Calculate percentile index
	index := (percentile * len(sorted)) / 100
	if index >= len(sorted) {
		index = len(sorted) - 1
	}

	return sorted[index]
}
