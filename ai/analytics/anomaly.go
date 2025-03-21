// Package analytics provides data analysis capabilities for Sprawl
package analytics

import (
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

// AnomalyType defines the type of anomaly detected
type AnomalyType string

const (
	// SpikeAnomaly represents a sudden spike in a metric
	SpikeAnomaly AnomalyType = "spike"
	// DropAnomaly represents a sudden drop in a metric
	DropAnomaly AnomalyType = "drop"
	// LevelShiftAnomaly represents a sustained shift in the baseline
	LevelShiftAnomaly AnomalyType = "level_shift"
	// TrendChangeAnomaly represents a change in the metric's trend
	TrendChangeAnomaly AnomalyType = "trend_change"
	// VarianceChangeAnomaly represents a change in the metric's variance
	VarianceChangeAnomaly AnomalyType = "variance_change"
	// SeasonalityChangeAnomaly represents a change in the metric's seasonality
	SeasonalityChangeAnomaly AnomalyType = "seasonality_change"
)

// DetectionMethod defines the algorithm used for anomaly detection
type DetectionMethod string

const (
	// ZScoreMethod uses z-score for anomaly detection
	ZScoreMethod DetectionMethod = "z_score"
	// MADMethod uses Median Absolute Deviation for anomaly detection
	MADMethod DetectionMethod = "mad"
	// IQRMethod uses Interquartile Range for anomaly detection
	IQRMethod DetectionMethod = "iqr"
	// ExponentialSmoothingMethod uses exponential smoothing for anomaly detection
	ExponentialSmoothingMethod DetectionMethod = "exp_smoothing"
)

// AnomalyInfo represents details about a detected anomaly
type AnomalyInfo struct {
	Timestamp      time.Time
	AnomalyType    AnomalyType
	Metric         string
	Value          float64
	ExpectedValue  float64
	DeviationScore float64
	Confidence     float64
}

// AnomalyDetector detects anomalies in time series data
type AnomalyDetector struct {
	mu                sync.RWMutex
	metricHistory     map[string][]TimeSeriesPoint
	detectionMethods  map[string]DetectionMethod
	sensitivityLevels map[string]float64
	baselineWindows   map[string]time.Duration
	detectedAnomalies map[string][]AnomalyInfo
	anomalyRetention  time.Duration
	analysisInterval  time.Duration
	maxHistoryPoints  int
	stopCh            chan struct{}
}

// NewAnomalyDetector creates a new anomaly detector
func NewAnomalyDetector(analysisInterval time.Duration, retention time.Duration, maxPoints int) *AnomalyDetector {
	if maxPoints <= 0 {
		maxPoints = 10000 // Default: 10K points per metric
	}

	if retention <= 0 {
		retention = 7 * 24 * time.Hour // Default: keep anomalies for 7 days
	}

	return &AnomalyDetector{
		metricHistory:     make(map[string][]TimeSeriesPoint),
		detectionMethods:  make(map[string]DetectionMethod),
		sensitivityLevels: make(map[string]float64),
		baselineWindows:   make(map[string]time.Duration),
		detectedAnomalies: make(map[string][]AnomalyInfo),
		anomalyRetention:  retention,
		analysisInterval:  analysisInterval,
		maxHistoryPoints:  maxPoints,
		stopCh:            make(chan struct{}),
	}
}

// Start begins periodic anomaly detection
func (ad *AnomalyDetector) Start() {
	log.Println("Starting anomaly detector...")
	go ad.analysisLoop()
}

// Stop halts the anomaly detection process
func (ad *AnomalyDetector) Stop() {
	log.Println("Stopping anomaly detector...")
	close(ad.stopCh)
}

// analysisLoop periodically analyzes metrics for anomalies
func (ad *AnomalyDetector) analysisLoop() {
	ticker := time.NewTicker(ad.analysisInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ad.detectAnomalies()
		case <-ad.stopCh:
			return
		}
	}
}

// AddMetricPoint adds a data point for anomaly detection
func (ad *AnomalyDetector) AddMetricPoint(metricName string, value float64, timestamp time.Time, labels map[string]string) {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	// Create data point
	point := TimeSeriesPoint{
		Timestamp: timestamp,
		Value:     value,
		Labels:    labels,
	}

	// Add to metric history, initializing if needed
	if _, exists := ad.metricHistory[metricName]; !exists {
		ad.metricHistory[metricName] = []TimeSeriesPoint{}
		ad.detectionMethods[metricName] = ZScoreMethod  // Default method
		ad.sensitivityLevels[metricName] = 3.0          // Default: 3 std deviations
		ad.baselineWindows[metricName] = 24 * time.Hour // Default: 24 hour baseline
	}

	ad.metricHistory[metricName] = append(ad.metricHistory[metricName], point)

	// Enforce max points limit
	if len(ad.metricHistory[metricName]) > ad.maxHistoryPoints {
		// Remove oldest data points
		excess := len(ad.metricHistory[metricName]) - ad.maxHistoryPoints
		ad.metricHistory[metricName] = ad.metricHistory[metricName][excess:]
	}

	// Check for anomaly in real-time
	isAnomaly, anomalyInfo := ad.checkForAnomaly(metricName, point)
	if isAnomaly {
		if _, exists := ad.detectedAnomalies[metricName]; !exists {
			ad.detectedAnomalies[metricName] = []AnomalyInfo{}
		}
		ad.detectedAnomalies[metricName] = append(ad.detectedAnomalies[metricName], anomalyInfo)
		log.Printf("Anomaly detected in metric %s: %s at %s (score: %.2f, confidence: %.2f)\n",
			metricName, anomalyInfo.AnomalyType, anomalyInfo.Timestamp.Format(time.RFC3339),
			anomalyInfo.DeviationScore, anomalyInfo.Confidence)
	}
}

// SetDetectionMethod sets the anomaly detection method for a metric
func (ad *AnomalyDetector) SetDetectionMethod(metricName string, method DetectionMethod) {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	ad.detectionMethods[metricName] = method
}

// SetSensitivity sets the sensitivity level for anomaly detection
func (ad *AnomalyDetector) SetSensitivity(metricName string, level float64) {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	ad.sensitivityLevels[metricName] = level
}

// SetBaselineWindow sets the baseline window for anomaly detection
func (ad *AnomalyDetector) SetBaselineWindow(metricName string, window time.Duration) {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	ad.baselineWindows[metricName] = window
}

// detectAnomalies runs anomaly detection on all metrics
func (ad *AnomalyDetector) detectAnomalies() {
	ad.mu.Lock()
	defer ad.mu.Unlock()

	log.Println("Running scheduled anomaly detection...")

	// Process each metric
	for metricName, points := range ad.metricHistory {
		if len(points) < 10 {
			// Need at least 10 points for meaningful analysis
			continue
		}

		// Get the most recent point
		latestPoint := points[len(points)-1]

		// Check for anomaly
		isAnomaly, anomalyInfo := ad.checkForAnomaly(metricName, latestPoint)
		if isAnomaly {
			if _, exists := ad.detectedAnomalies[metricName]; !exists {
				ad.detectedAnomalies[metricName] = []AnomalyInfo{}
			}

			// Check if this is a duplicate of a recently detected anomaly
			isDuplicate := false
			for _, existing := range ad.detectedAnomalies[metricName] {
				// If we already detected an anomaly within 15 minutes, skip
				if latestPoint.Timestamp.Sub(existing.Timestamp) < 15*time.Minute &&
					existing.AnomalyType == anomalyInfo.AnomalyType {
					isDuplicate = true
					break
				}
			}

			if !isDuplicate {
				ad.detectedAnomalies[metricName] = append(ad.detectedAnomalies[metricName], anomalyInfo)
				log.Printf("Anomaly detected in metric %s: %s at %s (score: %.2f, confidence: %.2f)\n",
					metricName, anomalyInfo.AnomalyType, anomalyInfo.Timestamp.Format(time.RFC3339),
					anomalyInfo.DeviationScore, anomalyInfo.Confidence)
			}
		}
	}

	// Prune old anomalies
	now := time.Now()
	for metricName, anomalies := range ad.detectedAnomalies {
		newList := []AnomalyInfo{}
		for _, anomaly := range anomalies {
			if now.Sub(anomaly.Timestamp) < ad.anomalyRetention {
				newList = append(newList, anomaly)
			}
		}
		if len(newList) > 0 {
			ad.detectedAnomalies[metricName] = newList
		} else {
			delete(ad.detectedAnomalies, metricName)
		}
	}
}

// checkForAnomaly checks if a data point is anomalous
func (ad *AnomalyDetector) checkForAnomaly(metricName string, point TimeSeriesPoint) (bool, AnomalyInfo) {
	points := ad.metricHistory[metricName]
	method := ad.detectionMethods[metricName]
	sensitivity := ad.sensitivityLevels[metricName]
	baselineWindow := ad.baselineWindows[metricName]

	// No detection if insufficient history
	if len(points) < 10 {
		return false, AnomalyInfo{}
	}

	// Filter baseline window
	baselinePoints := []TimeSeriesPoint{}
	for _, p := range points {
		if point.Timestamp.Sub(p.Timestamp) <= baselineWindow {
			baselinePoints = append(baselinePoints, p)
		}
	}

	// If insufficient baseline points, use all available
	if len(baselinePoints) < 10 {
		baselinePoints = points
	}

	// Initialize anomaly info
	anomalyInfo := AnomalyInfo{
		Timestamp:  point.Timestamp,
		Metric:     metricName,
		Value:      point.Value,
		Confidence: 0.0,
	}

	// Detect using appropriate method
	switch method {
	case ZScoreMethod:
		return ad.detectWithZScore(point, baselinePoints, sensitivity, &anomalyInfo)
	case MADMethod:
		return ad.detectWithMAD(point, baselinePoints, sensitivity, &anomalyInfo)
	case IQRMethod:
		return ad.detectWithIQR(point, baselinePoints, sensitivity, &anomalyInfo)
	case ExponentialSmoothingMethod:
		return ad.detectWithExpSmoothing(point, baselinePoints, sensitivity, &anomalyInfo)
	default:
		// Default to Z-Score
		return ad.detectWithZScore(point, baselinePoints, sensitivity, &anomalyInfo)
	}
}

// detectWithZScore detects anomalies using Z-Score method
func (ad *AnomalyDetector) detectWithZScore(point TimeSeriesPoint, baselinePoints []TimeSeriesPoint, sensitivity float64, anomalyInfo *AnomalyInfo) (bool, AnomalyInfo) {
	// Calculate mean and standard deviation
	sum := 0.0
	for _, p := range baselinePoints {
		sum += p.Value
	}
	mean := sum / float64(len(baselinePoints))

	varianceSum := 0.0
	for _, p := range baselinePoints {
		diff := p.Value - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(len(baselinePoints)))

	// If standard deviation is close to zero, we can't calculate meaningful z-score
	if stdDev < 0.0001 {
		return false, *anomalyInfo
	}

	// Calculate z-score
	zScore := (point.Value - mean) / stdDev

	// Update anomaly info
	anomalyInfo.ExpectedValue = mean
	anomalyInfo.DeviationScore = math.Abs(zScore)

	// Determine anomaly type
	if zScore > sensitivity {
		anomalyInfo.AnomalyType = SpikeAnomaly
	} else if zScore < -sensitivity {
		anomalyInfo.AnomalyType = DropAnomaly
	} else {
		return false, *anomalyInfo
	}

	// Calculate confidence based on how much it exceeds threshold
	exceeds := math.Abs(zScore) - sensitivity
	anomalyInfo.Confidence = math.Min(0.99, 0.7+exceeds*0.1)

	return true, *anomalyInfo
}

// detectWithMAD detects anomalies using Median Absolute Deviation
func (ad *AnomalyDetector) detectWithMAD(point TimeSeriesPoint, baselinePoints []TimeSeriesPoint, sensitivity float64, anomalyInfo *AnomalyInfo) (bool, AnomalyInfo) {
	// Extract values
	values := make([]float64, len(baselinePoints))
	for i, p := range baselinePoints {
		values[i] = p.Value
	}

	// Calculate median
	sort.Float64s(values)
	median := values[len(values)/2]
	if len(values)%2 == 0 {
		median = (values[len(values)/2-1] + values[len(values)/2]) / 2.0
	}

	// Calculate absolute deviations
	deviations := make([]float64, len(values))
	for i, v := range values {
		deviations[i] = math.Abs(v - median)
	}

	// Calculate median of absolute deviations
	sort.Float64s(deviations)
	mad := deviations[len(deviations)/2]
	if len(deviations)%2 == 0 {
		mad = (deviations[len(deviations)/2-1] + deviations[len(deviations)/2]) / 2.0
	}

	// MAD standardization factor (for normal distribution)
	const madFactor = 1.4826
	standardizedMAD := mad * madFactor

	// If MAD is close to zero, we can't calculate meaningful score
	if standardizedMAD < 0.0001 {
		return false, *anomalyInfo
	}

	// Calculate deviation score (similar to z-score)
	madScore := math.Abs(point.Value-median) / standardizedMAD

	// Update anomaly info
	anomalyInfo.ExpectedValue = median
	anomalyInfo.DeviationScore = madScore

	// Determine anomaly type
	if point.Value > median+sensitivity*standardizedMAD {
		anomalyInfo.AnomalyType = SpikeAnomaly
	} else if point.Value < median-sensitivity*standardizedMAD {
		anomalyInfo.AnomalyType = DropAnomaly
	} else {
		return false, *anomalyInfo
	}

	// Calculate confidence based on how much it exceeds threshold
	exceeds := madScore - sensitivity
	anomalyInfo.Confidence = math.Min(0.99, 0.7+exceeds*0.1)

	return true, *anomalyInfo
}

// detectWithIQR detects anomalies using Interquartile Range
func (ad *AnomalyDetector) detectWithIQR(point TimeSeriesPoint, baselinePoints []TimeSeriesPoint, sensitivity float64, anomalyInfo *AnomalyInfo) (bool, AnomalyInfo) {
	// Extract values
	values := make([]float64, len(baselinePoints))
	for i, p := range baselinePoints {
		values[i] = p.Value
	}

	// Sort values
	sort.Float64s(values)

	// Calculate quartiles
	q1Index := len(values) / 4
	q3Index := 3 * len(values) / 4

	q1 := values[q1Index]
	q3 := values[q3Index]

	// Calculate IQR
	iqr := q3 - q1

	// If IQR is close to zero, we can't calculate meaningful outlier boundaries
	if iqr < 0.0001 {
		return false, *anomalyInfo
	}

	// Calculate outlier boundaries
	// Typically 1.5*IQR, but we use sensitivity parameter
	lowerBound := q1 - sensitivity*iqr
	upperBound := q3 + sensitivity*iqr

	// Determine if point is an outlier
	isOutlier := point.Value < lowerBound || point.Value > upperBound

	if !isOutlier {
		return false, *anomalyInfo
	}

	// Update anomaly info
	median := values[len(values)/2]
	if len(values)%2 == 0 {
		median = (values[len(values)/2-1] + values[len(values)/2]) / 2.0
	}

	anomalyInfo.ExpectedValue = median

	// Calculate deviation score
	if point.Value > upperBound {
		anomalyInfo.DeviationScore = (point.Value - upperBound) / iqr
		anomalyInfo.AnomalyType = SpikeAnomaly
	} else {
		anomalyInfo.DeviationScore = (lowerBound - point.Value) / iqr
		anomalyInfo.AnomalyType = DropAnomaly
	}

	// Calculate confidence
	exceeds := anomalyInfo.DeviationScore
	anomalyInfo.Confidence = math.Min(0.99, 0.7+exceeds*0.1)

	return true, *anomalyInfo
}

// detectWithExpSmoothing detects anomalies using Exponential Smoothing
func (ad *AnomalyDetector) detectWithExpSmoothing(point TimeSeriesPoint, baselinePoints []TimeSeriesPoint, sensitivity float64, anomalyInfo *AnomalyInfo) (bool, AnomalyInfo) {
	if len(baselinePoints) < 3 {
		return false, *anomalyInfo
	}

	// Parameters for exponential smoothing
	alpha := 0.3 // Smoothing factor

	// Initialize with first value
	smoothed := baselinePoints[0].Value
	errors := make([]float64, 0, len(baselinePoints))

	// Calculate smoothed values and prediction errors
	for i := 1; i < len(baselinePoints); i++ {
		prediction := smoothed
		actual := baselinePoints[i].Value
		error := math.Abs(actual - prediction)
		errors = append(errors, error)

		// Update smoothed value
		smoothed = alpha*actual + (1-alpha)*smoothed
	}

	// Calculate mean absolute error and standard deviation
	var sumErrors float64
	for _, e := range errors {
		sumErrors += e
	}

	meanError := sumErrors / float64(len(errors))

	var sumSqrDev float64
	for _, e := range errors {
		dev := e - meanError
		sumSqrDev += dev * dev
	}

	stdDevError := math.Sqrt(sumSqrDev / float64(len(errors)))

	// Make prediction based on last smoothed value
	prediction := smoothed

	// Calculate error for this point
	actualError := math.Abs(point.Value - prediction)

	// Calculate how many standard deviations away the error is
	var errorScore float64
	if stdDevError > 0 {
		errorScore = (actualError - meanError) / stdDevError
	} else {
		errorScore = 0
	}

	// Update anomaly info
	anomalyInfo.ExpectedValue = prediction
	anomalyInfo.DeviationScore = errorScore

	// Determine if it's an anomaly
	if errorScore < sensitivity {
		return false, *anomalyInfo
	}

	// Determine anomaly type
	if point.Value > prediction {
		anomalyInfo.AnomalyType = SpikeAnomaly
	} else {
		anomalyInfo.AnomalyType = DropAnomaly
	}

	// Calculate confidence
	exceeds := errorScore - sensitivity
	anomalyInfo.Confidence = math.Min(0.99, 0.7+exceeds*0.05)

	return true, *anomalyInfo
}

// GetAnomalies returns detected anomalies for a metric
func (ad *AnomalyDetector) GetAnomalies(metricName string, since time.Time) []AnomalyInfo {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	// Get all anomalies for the metric
	anomalies, exists := ad.detectedAnomalies[metricName]
	if !exists || len(anomalies) == 0 {
		return []AnomalyInfo{}
	}

	// Filter by time
	result := []AnomalyInfo{}
	for _, anomaly := range anomalies {
		if anomaly.Timestamp.After(since) {
			result = append(result, anomaly)
		}
	}

	return result
}

// GetAllAnomalies returns all detected anomalies
func (ad *AnomalyDetector) GetAllAnomalies(since time.Time) map[string][]AnomalyInfo {
	ad.mu.RLock()
	defer ad.mu.RUnlock()

	result := make(map[string][]AnomalyInfo)

	for metricName, anomalies := range ad.detectedAnomalies {
		filtered := []AnomalyInfo{}
		for _, anomaly := range anomalies {
			if anomaly.Timestamp.After(since) {
				filtered = append(filtered, anomaly)
			}
		}

		if len(filtered) > 0 {
			result[metricName] = filtered
		}
	}

	return result
}
