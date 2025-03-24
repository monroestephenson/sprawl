package ack

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks statistics for the ACK tracking system
type Metrics struct {
	// Counters
	pendingAcks   atomic.Int64
	completedAcks atomic.Int64
	failedAcks    atomic.Int64
	totalAcks     atomic.Int64
	retriesIssued atomic.Int64
	storageOps    atomic.Int64

	// Latency tracking
	ackLatencySum           atomic.Int64 // in nanoseconds
	ackLatencyCount         atomic.Int64
	persistenceLatencySum   atomic.Int64 // in nanoseconds
	persistenceLatencyCount atomic.Int64
	p99AckLatencyNs         atomic.Int64
	maxAckLatencyNs         atomic.Int64

	// Node statistics
	nodeMu        sync.RWMutex
	nodeSuccesses map[string]int64
	nodeFailures  map[string]int64
	nodeLatencies map[string][]time.Duration

	// Timestamp tracking
	firstAckTime   time.Time
	lastAckTime    time.Time
	timeTrackingMu sync.RWMutex
}

// NewMetrics creates a new metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		nodeSuccesses: make(map[string]int64),
		nodeFailures:  make(map[string]int64),
		nodeLatencies: make(map[string][]time.Duration),
		firstAckTime:  time.Now(),
		lastAckTime:   time.Now(),
	}
}

// incrementPendingAcks increments the count of pending ACKs
func (m *Metrics) incrementPendingAcks() {
	m.pendingAcks.Add(1)
	m.totalAcks.Add(1)

	m.timeTrackingMu.Lock()
	m.lastAckTime = time.Now()
	m.timeTrackingMu.Unlock()
}

// decrementPendingAcks decrements the count of pending ACKs
func (m *Metrics) decrementPendingAcks() {
	m.pendingAcks.Add(-1)
}

// incrementCompletedAcks increments the count of completed ACKs
func (m *Metrics) incrementCompletedAcks() {
	m.completedAcks.Add(1)
}

// incrementFailedAcks increments the count of failed ACKs
func (m *Metrics) incrementFailedAcks() {
	m.failedAcks.Add(1)
}

// incrementRetriesIssued increments the count of retries issued
func (m *Metrics) incrementRetriesIssued() {
	m.retriesIssued.Add(1)
}

// incrementStorageOps increments the count of storage operations
func (m *Metrics) incrementStorageOps() {
	m.storageOps.Add(1)
}

// recordAckLatency records the latency of an ACK operation
func (m *Metrics) recordAckLatency(d time.Duration) {
	ns := d.Nanoseconds()
	m.ackLatencySum.Add(ns)
	m.ackLatencyCount.Add(1)

	// Update P99 latency (simple approximation)
	// In production, we'd use a more sophisticated algorithm like HDR Histogram
	currentP99 := m.p99AckLatencyNs.Load()
	if ns > currentP99 {
		m.p99AckLatencyNs.Store(ns)
	}

	// Update max latency
	currentMax := m.maxAckLatencyNs.Load()
	if ns > currentMax {
		m.maxAckLatencyNs.Store(ns)
	}
}

// recordPersistenceLatency records the latency of a persistence operation
func (m *Metrics) recordPersistenceLatency(d time.Duration) {
	ns := d.Nanoseconds()
	m.persistenceLatencySum.Add(ns)
	m.persistenceLatencyCount.Add(1)
}

// recordNodeSuccess records a successful delivery to a node
func (m *Metrics) recordNodeSuccess(nodeID string) {
	m.nodeMu.Lock()
	defer m.nodeMu.Unlock()

	m.nodeSuccesses[nodeID]++
}

// recordNodeFailure records a failed delivery to a node
func (m *Metrics) recordNodeFailure(nodeID string) {
	m.nodeMu.Lock()
	defer m.nodeMu.Unlock()

	m.nodeFailures[nodeID]++
}

// recordNodeLatency records the latency of a node operation
// nolint:unused // Kept for future metrics expansion
func (m *Metrics) recordNodeLatency(nodeID string, d time.Duration) {
	m.nodeMu.Lock()
	defer m.nodeMu.Unlock()

	// Keep only the last 100 latencies for memory efficiency
	latencies := m.nodeLatencies[nodeID]
	if len(latencies) >= 100 {
		// Remove oldest entry
		latencies = latencies[1:]
	}

	// Add new latency
	latencies = append(latencies, d)
	m.nodeLatencies[nodeID] = latencies
}

// getAverageAckLatency returns the average ACK latency
func (m *Metrics) getAverageAckLatency() time.Duration {
	count := m.ackLatencyCount.Load()
	if count == 0 {
		return 0
	}

	avg := m.ackLatencySum.Load() / count
	return time.Duration(avg) * time.Nanosecond
}

// getAveragePersistenceLatency returns the average persistence latency
func (m *Metrics) getAveragePersistenceLatency() time.Duration {
	count := m.persistenceLatencyCount.Load()
	if count == 0 {
		return 0
	}

	avg := m.persistenceLatencySum.Load() / count
	return time.Duration(avg) * time.Nanosecond
}

// getNodeStats returns statistics for all nodes
func (m *Metrics) getNodeStats() map[string]map[string]interface{} {
	m.nodeMu.RLock()
	defer m.nodeMu.RUnlock()

	result := make(map[string]map[string]interface{})

	for nodeID := range m.nodeSuccesses {
		successes := m.nodeSuccesses[nodeID]
		failures := m.nodeFailures[nodeID]
		latencies := m.nodeLatencies[nodeID]

		// Calculate average latency
		var avgLatency time.Duration
		if len(latencies) > 0 {
			var sum time.Duration
			for _, d := range latencies {
				sum += d
			}
			avgLatency = sum / time.Duration(len(latencies))
		}

		// Calculate success rate
		var successRate float64
		total := successes + failures
		if total > 0 {
			successRate = float64(successes) / float64(total) * 100.0
		}

		result[nodeID] = map[string]interface{}{
			"successes":    successes,
			"failures":     failures,
			"success_rate": successRate,
			"avg_latency":  avgLatency,
		}
	}

	return result
}

// getAll returns all metrics
func (m *Metrics) getAll() map[string]interface{} {
	m.timeTrackingMu.RLock()
	firstAckTime := m.firstAckTime
	lastAckTime := m.lastAckTime
	m.timeTrackingMu.RUnlock()

	return map[string]interface{}{
		"pending_acks":        m.pendingAcks.Load(),
		"completed_acks":      m.completedAcks.Load(),
		"failed_acks":         m.failedAcks.Load(),
		"total_acks":          m.totalAcks.Load(),
		"retries_issued":      m.retriesIssued.Load(),
		"storage_ops":         m.storageOps.Load(),
		"avg_ack_latency":     m.getAverageAckLatency(),
		"p99_ack_latency":     time.Duration(m.p99AckLatencyNs.Load()) * time.Nanosecond,
		"max_ack_latency":     time.Duration(m.maxAckLatencyNs.Load()) * time.Nanosecond,
		"persistence_latency": m.getAveragePersistenceLatency(),
		"first_ack_time":      firstAckTime,
		"last_ack_time":       lastAckTime,
		"uptime":              time.Since(firstAckTime),
		"node_stats":          m.getNodeStats(),
	}
}
