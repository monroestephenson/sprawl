package metrics

import (
	"testing"
	"time"
)

func TestMetricsCollection(t *testing.T) {
	metrics := NewMetrics()

	// Record some test data
	metrics.RecordMessage(true)
	metrics.RecordMessage(false)
	metrics.RecordDHTLookup()
	metrics.RecordLatency(100 * time.Millisecond)

	// Wait for metrics collection
	time.Sleep(2 * time.Second)

	snapshot := metrics.GetSnapshot()

	if snapshot["messages_sent"].(int64) != 1 {
		t.Error("Expected 1 message sent")
	}

	if snapshot["messages_received"].(int64) != 1 {
		t.Error("Expected 1 message received")
	}

	if snapshot["dht_lookups"].(int64) != 1 {
		t.Error("Expected 1 DHT lookup")
	}

	if snapshot["memory_usage"].(float64) <= 0 {
		t.Error("Expected non-zero memory usage")
	}
}
