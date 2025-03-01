package metrics

import (
	"runtime"
	"sync"
	"time"
)

type Metrics struct {
	mu sync.RWMutex

	// System metrics
	CPUUsage       float64
	MemoryUsage    float64
	GoroutineCount int

	// Message metrics
	MessagesSent     int64
	MessagesReceived int64
	MessageErrors    int64

	// DHT metrics
	DHTLookups     int64
	DHTUpdates     int64
	RouteCacheHits int64

	// Timing metrics
	AverageLatency time.Duration
	lastUpdate     time.Time
}

func NewMetrics() *Metrics {
	m := &Metrics{
		lastUpdate: time.Now(),
	}
	go m.collectSystemMetrics()
	return m
}

func (m *Metrics) collectSystemMetrics() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		m.mu.Lock()

		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)

		m.MemoryUsage = float64(memStats.Alloc) / float64(memStats.Sys)
		m.GoroutineCount = runtime.NumGoroutine()

		m.mu.Unlock()
	}
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

func (m *Metrics) RecordLatency(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Exponential moving average
	alpha := 0.1
	m.AverageLatency = time.Duration(float64(m.AverageLatency)*(1-alpha) + float64(d)*alpha)
}

func (m *Metrics) GetSnapshot() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return map[string]interface{}{
		"cpu_usage":         m.CPUUsage,
		"memory_usage":      m.MemoryUsage,
		"goroutines":        m.GoroutineCount,
		"messages_sent":     m.MessagesSent,
		"messages_received": m.MessagesReceived,
		"dht_lookups":       m.DHTLookups,
		"avg_latency_ms":    m.AverageLatency.Milliseconds(),
	}
}
