// Package analytics provides data analysis capabilities for Sprawl
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
	Type              PatternType
	PeakTimes         []time.Time
	TroughTimes       []time.Time
	AverageValue      float64
	MinValue          float64
	MaxValue          float64
	StandardDeviation float64
	NormalizedPattern []float64 // Values normalized to 0-1 range
	Confidence        float64   // How confident we are in this pattern (0-1)
}

// TrafficAnalyzer collects and analyzes message traffic patterns
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
func (ta *TrafficAnalyzer) Start() {
	log.Println("Starting traffic pattern analyzer...")
	go ta.analysisLoop()
}

// Stop halts the analysis process
func (ta *TrafficAnalyzer) Stop() {
	log.Println("Stopping traffic pattern analyzer...")
	close(ta.stopCh)
}

// analysisLoop periodically analyzes traffic patterns
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
	varianceSum := 0.0
	for _, val := range values {
		diff := val - profile.AverageValue
		varianceSum += diff * diff
	}
	profile.StandardDeviation = math.Sqrt(varianceSum / float64(len(data)))

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
	// This would be more sophisticated in a real implementation
	if len(data) > 100 {
		profile.Confidence = 0.8
	} else if len(data) > 50 {
		profile.Confidence = 0.6
	}

	return profile
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
	// In a real implementation, this would be more sophisticated
	hourOfDay := futureTime.Hour()
	dayOfWeek := int(futureTime.Weekday())

	// Use our normalized pattern to predict
	// This is a simplified approach - real implementation would be more sophisticated
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
