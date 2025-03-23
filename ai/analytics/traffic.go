// Package analytics provides data analysis capabilities for Sprawl
// This file contains implementation for traffic pattern analysis and detection

package analytics

import (
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

// PatternType defines the type of pattern being analyzed
type PatternType string

const (
	// DailyPattern represents daily traffic patterns (24-hour cycle)
	DailyPattern PatternType = "daily"
	// WeeklyPattern represents weekly traffic patterns (7-day cycle)
	WeeklyPattern PatternType = "weekly"
	// MonthlyPattern represents monthly traffic patterns
	MonthlyPattern PatternType = "monthly"
	// CustomPattern represents a custom time period pattern
	CustomPattern PatternType = "custom"
)

// TimeSeriesPoint is defined in timeseries.go

// PatternProfile represents detected patterns within time series data
type PatternProfile struct {
	// Type of pattern (daily, weekly, etc.)
	Type PatternType

	// Times identified as peaks (high points) in the pattern
	PeakTimes []time.Time

	// Times identified as troughs (low points) in the pattern
	TroughTimes []time.Time

	// Average value across the entire dataset
	AverageValue float64

	// Minimum value in the dataset
	MinValue float64

	// Maximum value in the dataset
	MaxValue float64

	// Standard deviation of the data
	StandardDeviation float64

	// Values normalized to 0-1 range for pattern comparison
	NormalizedPattern []float64

	// How confident we are in this pattern (0-1)
	// Higher values indicate more reliable patterns
	Confidence float64
}

// TrafficAnalyzer collects and analyzes message traffic patterns
// It identifies recurring patterns in data and provides predictions
// based on those patterns.
type TrafficAnalyzer struct {
	mu               sync.RWMutex
	topicData        map[string][]TimeSeriesPoint
	nodeData         map[string][]TimeSeriesPoint
	topicPatterns    map[string]map[PatternType]PatternProfile
	nodePatterns     map[string]map[PatternType]PatternProfile
	analysisInterval time.Duration
	maxDataPoints    int
	stopCh           chan struct{}
}

// NewTrafficAnalyzer creates a new traffic analyzer with the specified settings
// Parameters:
//   - analysisInterval: how often to run pattern analysis
//   - maxPoints: maximum number of data points to store per entity (topic/node)
func NewTrafficAnalyzer(analysisInterval time.Duration, maxPoints int) *TrafficAnalyzer {
	if maxPoints <= 0 {
		maxPoints = 10000 // Default: store up to 10K data points per entity
	}

	return &TrafficAnalyzer{
		topicData:        make(map[string][]TimeSeriesPoint),
		nodeData:         make(map[string][]TimeSeriesPoint),
		topicPatterns:    make(map[string]map[PatternType]PatternProfile),
		nodePatterns:     make(map[string]map[PatternType]PatternProfile),
		analysisInterval: analysisInterval,
		maxDataPoints:    maxPoints,
		stopCh:           make(chan struct{}),
	}
}

// Start begins periodic pattern analysis
// This starts a background goroutine that analyzes patterns at regular intervals
func (ta *TrafficAnalyzer) Start() {
	log.Println("Starting traffic pattern analyzer...")
	go ta.analysisLoop()
}

// Stop halts the analysis process
// This stops the background goroutine
func (ta *TrafficAnalyzer) Stop() {
	log.Println("Stopping traffic pattern analyzer...")
	close(ta.stopCh)
}

// analysisLoop periodically analyzes traffic patterns
// This is an internal function that runs in its own goroutine
func (ta *TrafficAnalyzer) analysisLoop() {
	ticker := time.NewTicker(ta.analysisInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ta.analyzeAllPatterns()
		case <-ta.stopCh:
			return
		}
	}
}

// analyzeAllPatterns analyzes patterns for all topics and nodes
// This is an internal function called periodically by the analysis loop
func (ta *TrafficAnalyzer) analyzeAllPatterns() {
	log.Println("Analyzing traffic patterns...")

	ta.mu.Lock()
	defer ta.mu.Unlock()

	// Analyze topic patterns
	for topic, data := range ta.topicData {
		if len(data) < 24 { // Need at least 24 data points for meaningful analysis
			continue
		}

		if _, exists := ta.topicPatterns[topic]; !exists {
			ta.topicPatterns[topic] = make(map[PatternType]PatternProfile)
		}

		// Analyze daily pattern
		dailyPattern := ta.analyzePattern(data, DailyPattern)
		ta.topicPatterns[topic][DailyPattern] = dailyPattern

		// If we have enough data, analyze weekly pattern too
		if len(data) >= 168 { // 24*7 = 168 hours in a week
			weeklyPattern := ta.analyzePattern(data, WeeklyPattern)
			ta.topicPatterns[topic][WeeklyPattern] = weeklyPattern
		}
	}

	// Analyze node patterns (similar approach)
	for node, data := range ta.nodeData {
		if len(data) < 24 {
			continue
		}

		if _, exists := ta.nodePatterns[node]; !exists {
			ta.nodePatterns[node] = make(map[PatternType]PatternProfile)
		}

		dailyPattern := ta.analyzePattern(data, DailyPattern)
		ta.nodePatterns[node][DailyPattern] = dailyPattern

		if len(data) >= 168 {
			weeklyPattern := ta.analyzePattern(data, WeeklyPattern)
			ta.nodePatterns[node][WeeklyPattern] = weeklyPattern
		}
	}

	log.Println("Traffic pattern analysis completed")
}

// analyzePattern analyzes time series data to detect patterns of the specified type
// This is the core pattern detection algorithm.
// Parameters:
//   - data: time series data points to analyze
//   - patternType: type of pattern to look for (daily, weekly, etc.)
//
// Returns:
//   - PatternProfile containing detected pattern characteristics
func (ta *TrafficAnalyzer) analyzePattern(data []TimeSeriesPoint, patternType PatternType) PatternProfile {
	profile := PatternProfile{
		Type:        patternType,
		PeakTimes:   []time.Time{},
		TroughTimes: []time.Time{},
		MinValue:    math.MaxFloat64,
		MaxValue:    -math.MaxFloat64,
		Confidence:  0.5, // Default medium confidence
	}

	// Extract values for statistical analysis
	values := make([]float64, len(data))
	sum := 0.0

	for i, point := range data {
		values[i] = point.Value
		sum += point.Value

		// Track min/max
		if point.Value < profile.MinValue {
			profile.MinValue = point.Value
		}
		if point.Value > profile.MaxValue {
			profile.MaxValue = point.Value
		}
	}

	// Calculate average
	profile.AverageValue = sum / float64(len(data))

	// Calculate standard deviation
	profile.StandardDeviation = calculateStdDev(values, profile.AverageValue)

	// Identify peaks and troughs (simplified algorithm)
	windowSize := 5 // Look at ±5 points to determine peaks/troughs

	for i := windowSize; i < len(data)-windowSize; i++ {
		isPeak := true
		isTrough := true

		for j := i - windowSize; j <= i+windowSize; j++ {
			if j == i {
				continue
			}

			if data[j].Value >= data[i].Value {
				isPeak = false
			}
			if data[j].Value <= data[i].Value {
				isTrough = false
			}
		}

		if isPeak {
			profile.PeakTimes = append(profile.PeakTimes, data[i].Timestamp)
		}
		if isTrough {
			profile.TroughTimes = append(profile.TroughTimes, data[i].Timestamp)
		}
	}

	// Create normalized pattern (values between 0-1)
	range_ := profile.MaxValue - profile.MinValue
	if range_ > 0 {
		profile.NormalizedPattern = make([]float64, len(data))
		for i, point := range data {
			profile.NormalizedPattern[i] = (point.Value - profile.MinValue) / range_
		}
	} else {
		// All values are the same
		profile.NormalizedPattern = make([]float64, len(data))
		for i := range profile.NormalizedPattern {
			profile.NormalizedPattern[i] = 0.5
		}
	}

	// Update confidence based on quality of data
	ta.calculatePatternConfidence(&profile, data)

	return profile
}

// calculatePatternConfidence calculates the confidence for a pattern profile
// This uses multiple statistical and signal processing techniques to assess
// how reliable the detected pattern is.
//
// Parameters:
//   - profile: the pattern profile to calculate confidence for
//   - data: the time series data used to detect the pattern
func (ta *TrafficAnalyzer) calculatePatternConfidence(profile *PatternProfile, data []TimeSeriesPoint) {
	if len(data) < 10 {
		profile.Confidence = 0.3 // Very low confidence with few data points
		return
	}

	// Calculate several quality metrics
	metrics := analyzeTimeSeriesQuality(data, profile.Type)

	// Combine metrics into a confidence score (0-1)
	// We weight different factors based on their importance
	confidence := 0.0

	// More data points gives higher confidence, up to a limit
	dataPointsWeight := 0.3
	dataPointsScore := math.Min(float64(len(data))/100.0, 1.0)
	confidence += dataPointsWeight * dataPointsScore

	// Signal-to-noise ratio (higher is better)
	snrWeight := 0.25
	confidence += snrWeight * metrics.SignalToNoise

	// Consistency of pattern (lower variance is better)
	consistencyWeight := 0.25
	consistencyScore := math.Max(0, 1.0-metrics.NormalizedVariance)
	confidence += consistencyWeight * consistencyScore

	// Stationarity of the time series (is the pattern stable)
	stationarityWeight := 0.2
	confidence += stationarityWeight * metrics.Stationarity

	// Clamp the final confidence value between 0.3 and 0.95
	// We never want 0 confidence (which would ignore the data)
	// and we never want 1.0 confidence (absolute certainty)
	profile.Confidence = math.Max(0.3, math.Min(0.95, confidence))
}

// TimeSeriesQualityMetrics holds various metrics about the quality of a time series
// These metrics are used to assess how reliable the pattern detection is.
type TimeSeriesQualityMetrics struct {
	// Ratio of signal power to noise power (higher is better)
	SignalToNoise float64

	// Normalized variance of the signal (lower is better for consistent patterns)
	NormalizedVariance float64

	// Measure of how stationary the time series is (higher is better for pattern detection)
	Stationarity float64

	// Strength of seasonality in the data (higher means stronger seasonal patterns)
	Seasonality float64

	// Strength of trend in the data (higher means stronger trend)
	TrendStrength float64
}

// analyzeTimeSeriesQuality calculates various quality metrics for a time series
// This is a comprehensive quality analysis that helps determine how reliable
// the pattern detection is.
//
// Parameters:
//   - data: time series data points to analyze
//   - patternType: type of pattern to look for (daily, weekly, etc.)
//
// Returns:
//   - TimeSeriesQualityMetrics with various quality measures
func analyzeTimeSeriesQuality(data []TimeSeriesPoint, patternType PatternType) TimeSeriesQualityMetrics {
	metrics := TimeSeriesQualityMetrics{}

	// Extract values
	values := make([]float64, len(data))
	for i, point := range data {
		values[i] = point.Value
	}

	// Calculate basic statistics
	mean := calculateMean(values)
	stdDev := calculateStdDev(values, mean)
	// No need to store variance as a separate variable

	// Normalize variance relative to mean (coefficient of variation)
	// This allows comparison between time series with different scales
	if mean != 0 {
		metrics.NormalizedVariance = stdDev / math.Abs(mean)
	} else {
		metrics.NormalizedVariance = 1.0
	}

	// Estimate signal to noise ratio using autocorrelation
	// Higher autocorrelation generally means higher signal-to-noise ratio
	// Choose lag based on the pattern type we're looking for
	lag := 1
	if patternType == DailyPattern {
		lag = 24 // For daily patterns, use 24-hour lag
	} else if patternType == WeeklyPattern {
		lag = 7 // For weekly patterns, use 7-day lag
	}

	autocorr := calculateAutocorrelation(values, lag)
	// Convert autocorrelation to a 0-1 scale
	metrics.SignalToNoise = (autocorr + 1) / 2

	// Estimate stationarity by comparing statistics in first and second half
	// A stationary time series has consistent statistical properties over time
	// This is important for pattern detection reliability
	halfSize := len(values) / 2
	if halfSize > 0 {
		firstHalf := values[:halfSize]
		secondHalf := values[halfSize:]

		firstMean := calculateMean(firstHalf)
		secondMean := calculateMean(secondHalf)

		// Use standard deviation and calculate variance
		firstStdDev := calculateStdDev(firstHalf, firstMean)
		secondStdDev := calculateStdDev(secondHalf, secondMean)

		firstVar := firstStdDev * firstStdDev
		secondVar := secondStdDev * secondStdDev

		// Compare means and variances - smaller difference means more stationary
		meanDiff := math.Abs(firstMean-secondMean) / math.Max(math.Abs(firstMean), math.Abs(secondMean))
		varDiff := math.Abs(firstVar-secondVar) / math.Max(firstVar, secondVar)

		// Convert to stationarity score (0-1)
		metrics.Stationarity = 1.0 - math.Min(1.0, (meanDiff+varDiff)/2)
	} else {
		metrics.Stationarity = 0.5 // Default for very short series
	}

	// Advanced: detect seasonality and trend strength
	seasonalityStrength := detectSeasonalityStrength(values, patternType)
	metrics.Seasonality = seasonalityStrength

	// If we have a strongly seasonal pattern, increase the signal-to-noise estimate
	if metrics.Seasonality > 0.7 {
		metrics.SignalToNoise = math.Max(metrics.SignalToNoise, 0.7)
	}

	return metrics
}

// detectSeasonalityStrength estimates the strength of seasonality in a time series
// Uses a simplified version of STL decomposition (Seasonal-Trend-Loess)
// to measure how strong the seasonal component is.
//
// Parameters:
//   - values: data values to analyze
//   - patternType: type of pattern to look for (daily, weekly, etc.)
//
// Returns:
//   - seasonality strength as a value from 0-1 (higher means stronger seasonal pattern)
func detectSeasonalityStrength(values []float64, patternType PatternType) float64 {
	if len(values) < 10 {
		return 0.5 // Not enough data to detect
	}

	// Choose period length based on pattern type
	period := 7
	if patternType == DailyPattern {
		period = 24
	} else if patternType == MonthlyPattern {
		period = 30
	}

	// Need at least 2 full periods
	if len(values) < period*2 {
		return 0.5
	}

	// Calculate seasonal strength using variance decomposition approach
	// This is a simplified version of STL decomposition

	// Calculate overall variance
	mean := calculateMean(values)
	stdDev := calculateStdDev(values, mean)
	totalVariance := stdDev * stdDev

	if totalVariance == 0 {
		return 0 // No variance, no pattern
	}

	// Calculate seasonal component by averaging values at the same phase
	seasonalComponent := make([]float64, period)

	// Estimate seasonal component by averaging values at the same phase
	for i := 0; i < period; i++ {
		sum := 0.0
		count := 0

		for j := i; j < len(values); j += period {
			sum += values[j]
			count++
		}

		if count > 0 {
			seasonalComponent[i] = sum / float64(count)
		}
	}

	// Calculate mean of seasonal component
	seasonalMean := calculateMean(seasonalComponent)

	// Center the seasonal component
	for i := range seasonalComponent {
		seasonalComponent[i] -= seasonalMean
	}

	// Calculate residuals after removing seasonal component
	residuals := make([]float64, len(values))
	for i := range values {
		phase := i % period
		residuals[i] = values[i] - mean - seasonalComponent[phase]
	}

	// Calculate residual variance
	residualMean := calculateMean(residuals)
	residualStdDev := calculateStdDev(residuals, residualMean)
	residualVariance := residualStdDev * residualStdDev

	// Strength = 1 - (residual variance / total variance)
	// Bounded between 0 and 1
	// This measures how much of the original variance is explained by the seasonal component
	strength := 1.0 - residualVariance/totalVariance
	return math.Max(0, math.Min(1, strength))
}

// AddTopicDataPoint adds a data point for a specific topic
func (ta *TrafficAnalyzer) AddTopicDataPoint(topic string, value float64, timestamp time.Time, labels map[string]string) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// Create data point
	point := TimeSeriesPoint{
		Timestamp: timestamp,
		Value:     value,
		Labels:    labels,
	}

	// Add to topic data, initializing if needed
	if _, exists := ta.topicData[topic]; !exists {
		ta.topicData[topic] = []TimeSeriesPoint{}
	}

	ta.topicData[topic] = append(ta.topicData[topic], point)

	// Enforce max points limit
	if len(ta.topicData[topic]) > ta.maxDataPoints {
		// Remove oldest data points
		ta.topicData[topic] = ta.topicData[topic][len(ta.topicData[topic])-ta.maxDataPoints:]
	}
}

// AddNodeDataPoint adds a data point for a specific node
func (ta *TrafficAnalyzer) AddNodeDataPoint(nodeID string, value float64, timestamp time.Time, labels map[string]string) {
	ta.mu.Lock()
	defer ta.mu.Unlock()

	// Create data point
	point := TimeSeriesPoint{
		Timestamp: timestamp,
		Value:     value,
		Labels:    labels,
	}

	// Add to node data, initializing if needed
	if _, exists := ta.nodeData[nodeID]; !exists {
		ta.nodeData[nodeID] = []TimeSeriesPoint{}
	}

	ta.nodeData[nodeID] = append(ta.nodeData[nodeID], point)

	// Enforce max points limit
	if len(ta.nodeData[nodeID]) > ta.maxDataPoints {
		// Remove oldest data points
		ta.nodeData[nodeID] = ta.nodeData[nodeID][len(ta.nodeData[nodeID])-ta.maxDataPoints:]
	}
}

// GetTopicPattern gets the pattern profile for a specific topic
func (ta *TrafficAnalyzer) GetTopicPattern(topic string, patternType PatternType) *PatternProfile {
	ta.mu.RLock()
	defer ta.mu.RUnlock()

	if patterns, exists := ta.topicPatterns[topic]; exists {
		if profile, exists := patterns[patternType]; exists {
			return &profile
		}
	}

	return nil
}

// GetNodePattern gets the pattern profile for a specific node
func (ta *TrafficAnalyzer) GetNodePattern(nodeID string, patternType PatternType) *PatternProfile {
	ta.mu.RLock()
	defer ta.mu.RUnlock()

	if patterns, exists := ta.nodePatterns[nodeID]; exists {
		if profile, exists := patterns[patternType]; exists {
			return &profile
		}
	}

	return nil
}

// GetBusiestTopics returns a list of topics with the highest average traffic
func (ta *TrafficAnalyzer) GetBusiestTopics(limit int) []string {
	ta.mu.RLock()
	defer ta.mu.RUnlock()

	type topicActivity struct {
		topic string
		avg   float64
	}

	activities := []topicActivity{}

	// Calculate average activity for each topic
	for topic, patterns := range ta.topicPatterns {
		if dailyPattern, exists := patterns[DailyPattern]; exists {
			activities = append(activities, topicActivity{
				topic: topic,
				avg:   dailyPattern.AverageValue,
			})
		}
	}

	// Sort by average activity (descending)
	sort.Slice(activities, func(i, j int) bool {
		return activities[i].avg > activities[j].avg
	})

	// Return top N topic names
	result := []string{}
	for i := 0; i < limit && i < len(activities); i++ {
		result = append(result, activities[i].topic)
	}

	return result
}

// PredictTopicActivity predicts future activity for a topic
// This uses the detected patterns to forecast expected activity
// at a specific time in the future.
//
// Parameters:
//   - topic: the topic to predict activity for
//   - futureTime: the time to predict activity for
//
// Returns:
//   - predicted activity value
func (ta *TrafficAnalyzer) PredictTopicActivity(topic string, futureTime time.Time) float64 {
	ta.mu.RLock()
	defer ta.mu.RUnlock()

	// Try to get patterns for prediction
	patterns, exists := ta.topicPatterns[topic]
	if !exists {
		return 0.0 // No data available
	}

	// Try daily pattern first
	dailyPattern, exists := patterns[DailyPattern]
	if !exists {
		return 0.0 // No pattern available
	}

	// Simple prediction using time of day pattern
	hourOfDay := futureTime.Hour()
	dayOfWeek := int(futureTime.Weekday())

	// Use our normalized pattern to predict
	if len(dailyPattern.NormalizedPattern) > 0 {
		// Map the hour to our pattern array
		idx := (hourOfDay * len(dailyPattern.NormalizedPattern)) / 24
		if idx >= len(dailyPattern.NormalizedPattern) {
			idx = len(dailyPattern.NormalizedPattern) - 1
		}

		// Get normalized value
		normalizedValue := dailyPattern.NormalizedPattern[idx]

		// Apply weekly adjustment if we have weekly pattern
		weeklyAdjustment := 1.0
		if weeklyPattern, exists := patterns[WeeklyPattern]; exists {
			// Map the day of week to our weekly pattern
			weekIdx := (dayOfWeek * len(weeklyPattern.NormalizedPattern)) / 7
			if weekIdx >= len(weeklyPattern.NormalizedPattern) {
				weekIdx = len(weeklyPattern.NormalizedPattern) - 1
			}
			weeklyValue := weeklyPattern.NormalizedPattern[weekIdx]
			weeklyAdjustment = 0.5 + weeklyValue/2 // Scale between 0.5-1.5
		}

		// Denormalize to get predicted value
		range_ := dailyPattern.MaxValue - dailyPattern.MinValue
		predictedValue := dailyPattern.MinValue + (normalizedValue * range_ * weeklyAdjustment)

		return predictedValue
	}

	// Fallback to average if we can't predict
	return dailyPattern.AverageValue
}
