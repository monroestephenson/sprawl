package metrics

import (
	"log"
	"math"
	"runtime"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
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

			// Get real CPU usage (using gopsutil library)
			percent, err := cpu.Percent(0, false) // Get total CPU usage (not per CPU)
			if err != nil {
				log.Printf("Error getting CPU usage: %v", err)
				// Fallback to our estimation if there's an error
				timeSpent := time.Since(m.lastCPUTime).Seconds()
				cpuEstimate := float64(m.GoroutineCount) * 0.01
				allocRate := float64(memStats.TotalAlloc-m.TotalAllocatedBytes) / (1024 * 1024 * timeSpent)
				cpuFromAlloc := math.Min(allocRate*0.2, 50)
				m.CPUUsage = math.Min(cpuEstimate+cpuFromAlloc, 100.0)
			} else if len(percent) > 0 {
				m.CPUUsage = percent[0] // Use the first value which represents overall CPU usage
			}

			// Store CPU samples for trends
			m.CPUSamples = append(m.CPUSamples, m.CPUUsage)
			if len(m.CPUSamples) > m.maxSampleSize {
				m.CPUSamples = m.CPUSamples[1:]
			}

			// Calculate message rate (messages per second)
			currentMessageCount := m.MessagesSent + m.MessagesReceived
			m.MessageRate = float64(currentMessageCount-m.lastMessageCount) / time.Since(m.lastUpdate).Seconds()
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

// GetSystemMetrics returns current system metrics (CPU usage, memory usage, goroutine count)
func GetSystemMetrics() (float64, float64, int) {
	// Get memory stats from Go runtime
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate memory usage as percentage of total allocated memory
	// to system memory
	memoryUsagePercent := float64(memStats.Alloc) / float64(memStats.Sys) * 100.0

	// Get goroutine count
	goroutines := runtime.NumGoroutine()

	// Get real CPU usage using gopsutil
	cpuUsage := 0.0
	percent, err := cpu.Percent(500*time.Millisecond, false) // Get total CPU usage with a 500ms sample
	if err != nil {
		log.Printf("Error getting CPU usage: %v", err)
		// Fallback to estimation if real metrics are unavailable
		cpuEstimate := float64(goroutines) * 0.01
		allocRate := float64(memStats.TotalAlloc) / float64(memStats.Sys) * 40.0
		cpuUsage = math.Min(cpuEstimate+allocRate, 100.0)
	} else if len(percent) > 0 {
		cpuUsage = percent[0] // Use the first value which represents overall CPU usage
	}

	return cpuUsage, memoryUsagePercent, goroutines
}
