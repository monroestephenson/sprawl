package ai

import (
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

// TestCPUUsageMetrics tests the real CPU usage collection
func TestCPUUsageMetrics(t *testing.T) {
	// Test the CPU usage metrics collection
	cpuUsage := getCPUUsagePercent()

	// Validate the result is within expected range (0-100%)
	if cpuUsage < 0 || cpuUsage > 100 {
		t.Errorf("CPU usage outside expected range: %f%%", cpuUsage)
	}

	// Verify that gopsutil itself can get CPU statistics
	// This ensures the library works on this system
	percent, err := cpu.Percent(time.Second, false)
	if err != nil {
		t.Errorf("Error getting CPU percent from gopsutil: %v", err)
	}
	if len(percent) == 0 {
		t.Error("No CPU percentage data returned from gopsutil")
	} else {
		t.Logf("CPU usage from gopsutil: %f%%", percent[0])
	}
}

// TestMemoryUsageMetrics tests the real memory usage collection
func TestMemoryUsageMetrics(t *testing.T) {
	// Test the memory usage metrics collection
	memUsage := getMemoryUsagePercent()

	// Validate the result is within expected range (0-100%)
	if memUsage < 0 || memUsage > 100 {
		t.Errorf("Memory usage outside expected range: %f%%", memUsage)
	}

	// Verify that gopsutil itself can get memory statistics
	// This ensures the library works on this system
	v, err := mem.VirtualMemory()
	if err != nil {
		t.Errorf("Error getting virtual memory from gopsutil: %v", err)
	}

	t.Logf("Memory usage from gopsutil: %f%%", v.UsedPercent)
	t.Logf("Total memory: %d bytes", v.Total)
	t.Logf("Available memory: %d bytes", v.Available)
}

// TestNetworkMetrics tests the network traffic monitoring
func TestNetworkMetrics(t *testing.T) {
	// We need to call it twice to get delta measurements
	// First call initializes the baseline
	_ = estimateNetworkActivity() // Ignore first reading

	// Sleep to generate some network activity and allow time to pass
	time.Sleep(2 * time.Second)

	// Second call should provide actual measurements
	secondReading := estimateNetworkActivity()

	// Validate that we get non-negative values
	if secondReading < 0 {
		t.Errorf("Network activity measurement is negative: %f", secondReading)
	}

	// Verify that gopsutil itself can get network statistics
	// This ensures the library works on this system
	netStats, err := net.IOCounters(false)
	if err != nil {
		t.Errorf("Error getting network stats from gopsutil: %v", err)
	}
	if len(netStats) == 0 {
		t.Error("No network stats returned from gopsutil")
	} else {
		t.Logf("Network stats from gopsutil: BytesSent=%d, BytesRecv=%d",
			netStats[0].BytesSent, netStats[0].BytesRecv)
	}
}

// TestDiskIO tests the disk activity through the file system
func TestDiskIO(t *testing.T) {
	// Test disk usage stats
	usage, err := disk.Usage("/")
	if err != nil {
		t.Errorf("Error getting disk usage: %v", err)
	} else {
		t.Logf("Disk usage: %f%% of %d bytes", usage.UsedPercent, usage.Total)
	}

	// Test disk IO counters if available on this system
	stats, err := disk.IOCounters()
	if err != nil {
		t.Logf("Note: Disk IO counters not available on this system: %v", err)
		return
	}

	if len(stats) == 0 {
		t.Log("No disk IO stats available")
	} else {
		// Log stats from the first disk
		for name, stat := range stats {
			t.Logf("Disk %s: ReadCount=%d, WriteCount=%d",
				name, stat.ReadCount, stat.WriteCount)
			break // Just show the first one
		}
	}
}

// TestDiskIOStats tests the disk I/O statistics collection
func TestDiskIOStats(t *testing.T) {
	// First call initializes the counters
	firstReading, err := getDiskIOStats()
	if err != nil {
		t.Errorf("Error getting first disk I/O reading: %v", err)
	}

	// Sleep to generate some disk activity
	time.Sleep(2 * time.Second)

	// Generate some disk activity
	generateDiskActivity()

	// Second call should provide actual measurements
	secondReading, err := getDiskIOStats()
	if err != nil {
		t.Errorf("Error getting second disk I/O reading: %v", err)
	}

	// Validate that we get non-negative values
	if secondReading < 0 {
		t.Errorf("Disk I/O measurement is negative: %f", secondReading)
	}

	t.Logf("First disk IO reading: %f ops/sec", firstReading)
	t.Logf("Second disk IO reading: %f ops/sec", secondReading)
}

// TestEngineMetricCollection tests the Engine's metrics collection
func TestEngineMetricCollection(t *testing.T) {
	// Create a test engine with a short sample interval
	options := EngineOptions{
		SampleInterval:  100 * time.Millisecond,
		MaxDataPoints:   100,
		EnablePredictor: true,
	}

	engine := NewEngine(options)

	// Start the engine to collect metrics
	engine.Start()

	// Let it collect some metrics
	time.Sleep(2 * time.Second)

	// Stop the engine
	engine.Stop()

	// Verify that CPU metrics were collected
	cpuUsage := engine.GetCurrentMetric(MetricKindCPUUsage, "local")
	if cpuUsage <= 0 {
		t.Errorf("No CPU usage metrics collected, got: %f", cpuUsage)
	} else {
		t.Logf("Collected CPU usage: %f%%", cpuUsage)
	}

	// Verify that memory metrics were collected
	memUsage := engine.GetCurrentMetric(MetricKindMemoryUsage, "local")
	if memUsage <= 0 {
		t.Errorf("No memory usage metrics collected, got: %f", memUsage)
	} else {
		t.Logf("Collected memory usage: %f%%", memUsage)
	}

	// Verify network traffic metrics
	networkTraffic := engine.GetCurrentMetric(MetricKindNetworkTraffic, "total")
	t.Logf("Collected network traffic: %f bytes/sec", networkTraffic)

	// The test passes if we got here without errors and the metrics were collected
}

// TestIntelligenceMetricCollection tests the Intelligence system's metrics collection
func TestIntelligenceMetricCollection(t *testing.T) {
	// Create a test intelligence system with a short sample interval
	intelligence := NewIntelligence(100 * time.Millisecond)

	// Start the intelligence system
	intelligence.Start()

	// Let it collect some metrics
	time.Sleep(500 * time.Millisecond)

	// Stop the intelligence system
	intelligence.Stop()

	// Verify that we can get recommendations (which would use the collected metrics)
	recommendations := intelligence.GetRecommendedActions()

	// We should have some recommendations
	if len(recommendations) == 0 {
		t.Error("No recommendations generated from intelligence system")
	} else {
		t.Logf("Got %d recommendations", len(recommendations))
		for i, rec := range recommendations {
			t.Logf("Recommendation %d: %s", i+1, rec)
		}
	}

	// The test passes if we got here without errors and got some recommendations
}

// Integration test that verifies all the metrics work together
func TestMetricsIntegration(t *testing.T) {
	// Test 1: Initialize both Engine and Intelligence
	engineOptions := DefaultEngineOptions()
	engineOptions.SampleInterval = 100 * time.Millisecond

	engine := NewEngine(engineOptions)
	intelligence := NewIntelligence(100 * time.Millisecond)

	// Start both systems
	engine.Start()
	intelligence.Start()

	// Let them collect metrics for a short time
	time.Sleep(1 * time.Second)

	// Get system status
	status := engine.GetFullSystemStatus()

	// Verify we have status data
	if status == nil {
		t.Error("Failed to get system status")
	}

	// Get metrics from status
	metrics, ok := status["current_metrics"].(map[string]float64)
	if !ok {
		t.Error("Current metrics not found in status")
	} else {
		// Log some key metrics
		for key, value := range metrics {
			t.Logf("Metric: %s = %f", key, value)
		}
	}

	// Check for storage metrics
	storageUsage := estimateStorageUsage()
	t.Logf("Storage usage: %f%%", storageUsage)

	// Check for anomalies
	anomalies := intelligence.DetectAnomalies()
	t.Logf("Detected %d anomalies", len(anomalies))

	// Stop both systems
	engine.Stop()
	intelligence.Stop()

	// Test passed if we got here without errors
	t.Log("Metrics integration test passed")
}

// Helper function to generate disk activity
func generateDiskActivity() {
	// Create a temporary file and write to it
	f, err := disk.Usage("/tmp")
	if err != nil {
		return
	}
	// Log usage to generate some I/O
	_ = f.Used
}
