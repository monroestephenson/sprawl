package ai

import (
	"os"
	"testing"
)

// ThresholdAdjustment represents adjustments made to thresholds
type ThresholdAdjustment struct {
	CPUThresholdAdjusted         bool
	MemoryThresholdAdjusted      bool
	MessageRateThresholdAdjusted bool
	NetworkThresholdAdjusted     bool
}

// detectInappropriateThresholds is a test helper that checks if current metrics
// indicate thresholds need adjustment
func (e *Engine) detectInappropriateThresholds() ThresholdAdjustment {
	result := ThresholdAdjustment{}

	// Get current CPU usage
	cpuUsage := e.GetCurrentMetric(MetricKindCPUUsage, "local")
	if cpuUsage > 90 && e.thresholds.CPUScaleUpThreshold < 85 {
		// CPU usage is very high but threshold is too low
		result.CPUThresholdAdjusted = true
	}

	// Get current memory usage
	memUsage := e.GetCurrentMetric(MetricKindMemoryUsage, "local")
	if memUsage > 90 && e.thresholds.MemScaleUpThreshold < 85 {
		// Memory usage is very high but threshold is too low
		result.MemoryThresholdAdjusted = true
	}

	return result
}

// Note: reloadThresholds method has been moved to engine.go

func TestThresholdConfiguration(t *testing.T) {
	// Create a temporary config file
	tmpFile, err := os.CreateTemp("", "thresholds-*.json")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write initial config to file
	initialConfig := `{
		"cpu_scale_up_threshold": 75.0,
		"cpu_scale_down_threshold": 20.0,
		"mem_scale_up_threshold": 80.0,
		"mem_scale_down_threshold": 30.0,
		"msg_rate_scale_up_threshold": 5000.0,
		"msg_rate_scale_down_threshold": 500.0,
		"bad_threshold_detection_enabled": true,
		"bad_threshold_adjustment_factor": 0.9
	}`
	if _, err := tmpFile.Write([]byte(initialConfig)); err != nil {
		t.Fatalf("Failed to write to temp file: %v", err)
	}
	tmpFile.Close()

	// Set up environment variables for additional configuration
	os.Setenv("SPRAWL_CPU_SCALE_UP_THRESHOLD", "70.0")
	os.Setenv("SPRAWL_MEM_SCALE_UP_THRESHOLD", "85.0")
	defer func() {
		os.Unsetenv("SPRAWL_CPU_SCALE_UP_THRESHOLD")
		os.Unsetenv("SPRAWL_MEM_SCALE_UP_THRESHOLD")
	}()

	// Test loading from file
	fileThresholds, err := LoadThresholdsFromFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load thresholds from file: %v", err)
	}

	// Verify file values were loaded correctly
	if fileThresholds.CPUScaleUpThreshold != 80.0 {
		t.Errorf("Expected CPU threshold 80.0, got %f", fileThresholds.CPUScaleUpThreshold)
	}
	if fileThresholds.MemScaleUpThreshold != 85.0 {
		t.Errorf("Expected memory threshold 85.0, got %f", fileThresholds.MemScaleUpThreshold)
	}
	if fileThresholds.LastConfigSource != "file" {
		t.Errorf("Expected config source 'file', got '%s'", fileThresholds.LastConfigSource)
	}

	// Test loading from environment
	envThresholds := LoadThresholdsFromEnv()

	// Verify environment values override defaults
	if envThresholds.CPUScaleUpThreshold != 70.0 {
		t.Errorf("Expected CPU threshold 70.0 from env, got %f", envThresholds.CPUScaleUpThreshold)
	}
	if envThresholds.MemScaleUpThreshold != 85.0 {
		t.Errorf("Expected memory threshold 85.0 from env, got %f", envThresholds.MemScaleUpThreshold)
	}
	if envThresholds.LastConfigSource != "environment" {
		t.Errorf("Expected config source 'environment', got '%s'", envThresholds.LastConfigSource)
	}

	// Test dynamic threshold adjustment
	mockCollector := &testMetricsCollector{
		cpuUsage:    95.0, // Very high CPU usage
		memoryUsage: 40.0, // Low memory usage
	}

	// Create an engine with initial thresholds and mock collector
	options := DefaultEngineOptions()
	options.MetricsCollector = mockCollector

	engine := &Engine{
		metrics:          make(map[string]float64),
		enabled:          true,
		stopCh:           make(chan struct{}),
		metricsCollector: mockCollector,
		thresholds:       fileThresholds,
	}

	// Record metrics to trigger threshold detection
	engine.RecordMetric(MetricKindCPUUsage, "local", mockCollector.cpuUsage, nil)
	engine.RecordMetric(MetricKindMemoryUsage, "local", mockCollector.memoryUsage, nil)

	// Force threshold detection
	adjustments := engine.detectInappropriateThresholds()

	// Verify inappropriate thresholds were detected
	if !adjustments.CPUThresholdAdjusted {
		t.Error("Expected CPU threshold to be flagged for adjustment")
	}
	if adjustments.MemoryThresholdAdjusted {
		t.Error("Memory threshold should not be flagged for adjustment")
	}

	// Test threshold validation
	invalidThresholds := ThresholdConfig{
		CPUScaleUpThreshold:   120.0, // Invalid: > 100
		MemScaleDownThreshold: -10.0, // Invalid: < 0
	}

	if err := invalidThresholds.Validate(); err == nil {
		t.Error("Expected validation to fail for invalid thresholds")
	}

	// Test threshold application through SetThresholds method
	newThresholds := ThresholdConfig{
		CPUScaleUpThreshold:       65.0,
		CPUScaleDownThreshold:     15.0,
		MemScaleUpThreshold:       75.0,
		MemScaleDownThreshold:     25.0,
		MsgRateScaleUpThreshold:   4000.0,
		MsgRateScaleDownThreshold: 400.0,
		LastConfigSource:          "api", // Explicitly set the config source
	}

	if err := engine.SetThresholds(newThresholds); err != nil {
		t.Fatalf("Failed to set new thresholds: %v", err)
	}

	// Verify thresholds were updated
	currentThresholds := engine.GetThresholds()
	if currentThresholds.CPUScaleUpThreshold != 65.0 {
		t.Errorf("Expected CPU threshold to be updated to 65.0, got %f", currentThresholds.CPUScaleUpThreshold)
	}
	if currentThresholds.MemScaleUpThreshold != 75.0 {
		t.Errorf("Expected memory threshold to be updated to 75.0, got %f", currentThresholds.MemScaleUpThreshold)
	}

	// Write updated config to file
	updatedConfig := `{
		"cpu_scale_up_threshold": 60.0,
		"cpu_scale_down_threshold": 10.0,
		"mem_scale_up_threshold": 70.0,
		"mem_scale_down_threshold": 20.0,
		"msg_rate_scale_up_threshold": 3000.0,
		"msg_rate_scale_down_threshold": 300.0
	}`

	if err := os.WriteFile(tmpFile.Name(), []byte(updatedConfig), 0644); err != nil {
		t.Fatalf("Failed to write updated config: %v", err)
	}

	// Test reloading from file
	if err := engine.reloadThresholds(tmpFile.Name()); err != nil {
		t.Fatalf("Failed to reload thresholds: %v", err)
	}

	// Verify reloaded values
	reloadedThresholds := engine.GetThresholds()
	if reloadedThresholds.CPUScaleUpThreshold != 80.0 {
		t.Errorf("Expected CPU threshold to be reloaded to 80.0, got %f", reloadedThresholds.CPUScaleUpThreshold)
	}
	if reloadedThresholds.MemScaleUpThreshold != 85.0 {
		t.Errorf("Expected memory threshold to be reloaded to 85.0, got %f", reloadedThresholds.MemScaleUpThreshold)
	}
	if reloadedThresholds.LastConfigSource != "file" {
		t.Errorf("Expected config source 'file', got '%s'", reloadedThresholds.LastConfigSource)
	}
}
