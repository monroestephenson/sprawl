package analytics

import (
	"testing"
	"time"
)

func TestDetectAnomaly(t *testing.T) {
	// Create test time series data
	now := time.Now()
	timestamps := make([]time.Time, 10)
	for i := range timestamps {
		timestamps[i] = now.Add(-time.Duration(i) * time.Hour)
	}

	t.Run("Normal data should not detect anomaly", func(t *testing.T) {
		normalData := []float64{10, 11, 10, 9, 11, 10, 9, 10, 11, 10}
		anomalies := DetectAnomalies("test", normalData, timestamps, 2.0)

		if len(anomalies) > 0 {
			t.Errorf("Expected no anomalies but got %d", len(anomalies))
		}
	})

	t.Run("Spike anomaly should be detected", func(t *testing.T) {
		// Add a spike at position 3
		spikeData := []float64{10, 11, 10, 20, 11, 10, 9, 10, 11, 10}
		anomalies := DetectAnomalies("test", spikeData, timestamps, 2.0)

		if len(anomalies) != 1 {
			t.Errorf("Expected 1 anomaly but got %d", len(anomalies))
		} else if anomalies[0].AnomalyType != "spike" {
			t.Errorf("Expected 'spike' anomaly type but got '%s'", anomalies[0].AnomalyType)
		}
	})

	t.Run("Drop anomaly should be detected", func(t *testing.T) {
		// Add a drop at position 5
		dropData := []float64{10, 11, 10, 9, 11, 1, 9, 10, 11, 10}
		anomalies := DetectAnomalies("test", dropData, timestamps, 2.0)

		if len(anomalies) != 1 {
			t.Errorf("Expected 1 anomaly but got %d", len(anomalies))
		} else if anomalies[0].AnomalyType != "drop" {
			t.Errorf("Expected 'drop' anomaly type but got '%s'", anomalies[0].AnomalyType)
		}
	})

	t.Run("Higher threshold should not detect anomaly", func(t *testing.T) {
		// Same spike data but higher threshold
		spikeData := []float64{10, 11, 10, 20, 11, 10, 9, 10, 11, 10}
		anomalies := DetectAnomalies("test", spikeData, timestamps, 5.0)

		if len(anomalies) > 0 {
			t.Errorf("Expected no anomalies with high threshold but got %d", len(anomalies))
		}
	})
}

func TestAnomalyConfidence(t *testing.T) {
	// Test confidence calculation at different deviation scores
	lowConf := calculateConfidence(1.1)
	if lowConf >= 0.5 {
		t.Errorf("Expected low confidence at low deviation score but got %.2f", lowConf)
	}

	medConf := calculateConfidence(3.0)
	if medConf < 0.5 || medConf > 0.8 {
		t.Errorf("Expected medium confidence around 0.67 but got %.2f", medConf)
	}

	highConf := calculateConfidence(10.0)
	if highConf < 0.8 {
		t.Errorf("Expected high confidence but got %.2f", highConf)
	}
}

func TestSeasonal(t *testing.T) {
	// Create seasonal data with a period of 4
	seasonalData := []float64{
		10, 20, 15, 5, // cycle 1
		10, 21, 15, 6, // cycle 2
		11, 22, 14, 5, // cycle 3
		12, 21, 16, 4, // cycle 4
	}

	timestamps := make([]time.Time, len(seasonalData))
	now := time.Now()

	for i := range timestamps {
		timestamps[i] = now.Add(time.Duration(-i) * time.Hour)
	}

	isSeasonal, period := DetectSeasonality(seasonalData, timestamps)

	if !isSeasonal {
		t.Error("Expected pattern to be detected as seasonal")
	}

	if period != 4 {
		t.Errorf("Expected seasonal period of 4 but got %d", period)
	}

	// Non-seasonal data
	randomData := []float64{10, 8, 15, 12, 9, 11, 13, 10, 8, 14, 11, 13, 9, 12}
	isSeasonal, _ = DetectSeasonality(randomData, timestamps[:len(randomData)])

	if isSeasonal {
		t.Error("Expected random data not to be detected as seasonal")
	}
}

func TestTrendAnalysis(t *testing.T) {
	// Test upward trend
	upwardData := []float64{10, 11, 12, 13, 14, 15, 16, 17, 18, 19}

	// Test downward trend
	downwardData := []float64{20, 19, 18, 17, 16, 15, 14, 13, 12, 11}

	// Test stable data
	stableData := []float64{10, 10, 11, 10, 9, 10, 11, 10, 9, 10}

	timestamps := make([]time.Time, 10)
	now := time.Now()

	for i := range timestamps {
		timestamps[i] = now.Add(time.Duration(-i) * time.Hour)
	}

	// Test upward trend detection - with time going backwards, this creates a positive slope
	trend, slope := AnalyzeTrend(upwardData, timestamps)

	if trend != "decreasing" {
		t.Errorf("Expected 'decreasing' trend for upward data but got '%s'", trend)
	}

	if slope <= 0 {
		t.Errorf("Expected positive slope for upward data but got %.2f", slope)
	}

	// Test downward trend detection - with time going backwards, this creates a negative slope
	trend, slope = AnalyzeTrend(downwardData, timestamps)

	if trend != "increasing" {
		t.Errorf("Expected 'increasing' trend for downward data but got '%s'", trend)
	}

	if slope >= 0 {
		t.Errorf("Expected negative slope for downward data but got %.2f", slope)
	}

	// Test stable data
	trend, slope = AnalyzeTrend(stableData, timestamps)

	if trend != "stable" {
		t.Errorf("Expected 'stable' trend but got '%s'", trend)
	}

	if slope < -0.5 || slope > 0.5 {
		t.Errorf("Expected slope close to zero for stable trend but got %.2f", slope)
	}
}
