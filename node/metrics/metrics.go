package metrics

import (
	"math"
	"runtime"
	"sync"
	"time"
)

type Metrics struct {
	mu sync.RWMutex

	// System metrics
	CPUUsage            float64
	MemoryUsage         float64
	GoroutineCount      int
	HeapObjects         uint64
	GCCPUFraction       float64
	TotalAllocatedBytes uint64

	// Resource trends (for AI prediction)
	CPUSamples    []float64
	MemorySamples []float64
	maxSampleSize int

	// Message metrics
	MessagesSent     int64
	MessagesReceived int64
	MessageErrors    int64
	MessageRate      float64 // Messages per second
	TopicCount       int

	// DHT metrics
	DHTLookups     int64
	DHTUpdates     int64
	RouteCacheHits int64

	// Timing metrics
	AverageLatency   time.Duration
	lastUpdate       time.Time
	lastCPUTime      time.Time
	lastMessageCount int64

	// Control
	stopCh chan struct{}
	done   chan struct{}
}

func NewMetrics() *Metrics {
	m := &Metrics{
		lastUpdate:    time.Now(),
		lastCPUTime:   time.Now(),
		stopCh:        make(chan struct{}),
		done:          make(chan struct{}),
		CPUSamples:    make([]float64, 0, 60),
		MemorySamples: make([]float64, 0, 60),
		maxSampleSize: 60, // Keep last 60 samples (1 minute at 1s interval)
	}
	go m.collectSystemMetrics()
	return m
}

func (m *Metrics) collectSystemMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	defer close(m.done)

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.mu.Lock()

			// Memory metrics
			var memStats runtime.MemStats
			runtime.ReadMemStats(&memStats)

			m.MemoryUsage = float64(memStats.Alloc) / float64(memStats.Sys)
			m.HeapObjects = memStats.HeapObjects
			m.GCCPUFraction = memStats.GCCPUFraction
			m.TotalAllocatedBytes = memStats.TotalAlloc
			m.GoroutineCount = runtime.NumGoroutine()

			// Store memory samples for trends
			m.MemorySamples = append(m.MemorySamples, m.MemoryUsage)
			if len(m.MemorySamples) > m.maxSampleSize {
				m.MemorySamples = m.MemorySamples[1:]
			}

			// Simulated CPU usage (real implementation would use OS-specific methods)
			// This simulates CPU usage based on goroutines, memory allocation, and time since last collection
			timeSpent := time.Since(m.lastCPUTime).Seconds()
			// Very simple simulation - in a real implementation, you'd use OS-specific methods
			// to get actual CPU usage
			cpuEstimate := float64(m.GoroutineCount) * 0.01 // 1% per goroutine as a rough estimate
			// Factor in memory allocation rate
			allocRate := float64(memStats.TotalAlloc-m.TotalAllocatedBytes) / (1024 * 1024 * timeSpent) // MB/s
			cpuFromAlloc := math.Min(allocRate*0.2, 50)                                                 // 0.2% per MB/s, max 50%

			// Combine factors and add some randomness for realism
			rand := float64(time.Now().UnixNano()%1000) / 10000.0
			m.CPUUsage = math.Min(cpuEstimate+cpuFromAlloc+rand, 100.0)

			// Store CPU samples for trends
			m.CPUSamples = append(m.CPUSamples, m.CPUUsage)
			if len(m.CPUSamples) > m.maxSampleSize {
				m.CPUSamples = m.CPUSamples[1:]
			}

			// Calculate message rate (messages per second)
			currentMessageCount := m.MessagesSent + m.MessagesReceived
			m.MessageRate = float64(currentMessageCount-m.lastMessageCount) / timeSpent
			m.lastMessageCount = currentMessageCount

			m.lastCPUTime = time.Now()
			m.mu.Unlock()
		}
	}
}

// Stop gracefully stops the metrics collection
func (m *Metrics) Stop() {
	close(m.stopCh)
	<-m.done // Wait for collection to stop
}

func (m *Metrics) RecordMessage(sent bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sent {
		m.MessagesSent++
	} else {
		m.MessagesReceived++
	}
}

func (m *Metrics) RecordDHTLookup() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DHTLookups++
}

func (m *Metrics) SetTopicCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.TopicCount = count
}

func (m *Metrics) RecordLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Exponential moving average
	alpha := 0.1
	m.AverageLatency = time.Duration(float64(m.AverageLatency)*(1-alpha) + float64(d)*alpha)
}

// GetTrend returns the trend of a metric over the stored samples
// Returns:
// 1: Increasing trend
// 0: Stable
// -1: Decreasing trend
func (m *Metrics) GetTrend(metricType string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if metricType == "cpu" && len(m.CPUSamples) >= 10 {
		// Calculate trend over last 10 samples
		first5Avg := average(m.CPUSamples[:5])
		last5Avg := average(m.CPUSamples[len(m.CPUSamples)-5:])

		if last5Avg > first5Avg*1.1 {
			return 1 // Increasing
		} else if last5Avg < first5Avg*0.9 {
			return -1 // Decreasing
		}
	} else if metricType == "memory" && len(m.MemorySamples) >= 10 {
		first5Avg := average(m.MemorySamples[:5])
		last5Avg := average(m.MemorySamples[len(m.MemorySamples)-5:])

		if last5Avg > first5Avg*1.1 {
			return 1 // Increasing
		} else if last5Avg < first5Avg*0.9 {
			return -1 // Decreasing
		}
	}

	return 0 // Stable
}

func average(values []float64) float64 {
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

func (m *Metrics) GetSnapshot() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"cpu_usage":             m.CPUUsage,
		"memory_usage":          m.MemoryUsage,
		"goroutines":            m.GoroutineCount,
		"heap_objects":          m.HeapObjects,
		"gc_cpu_fraction":       m.GCCPUFraction,
		"total_allocated_bytes": m.TotalAllocatedBytes,
		"messages_sent":         m.MessagesSent,
		"messages_received":     m.MessagesReceived,
		"message_rate":          m.MessageRate,
		"topic_count":           m.TopicCount,
		"dht_lookups":           m.DHTLookups,
		"avg_latency_ms":        m.AverageLatency.Milliseconds(),
		"cpu_trend":             m.GetTrend("cpu"),
		"memory_trend":          m.GetTrend("memory"),
	}
}
