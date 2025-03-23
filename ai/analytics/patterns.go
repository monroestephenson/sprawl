// Package analytics provides data analysis capabilities for Sprawl
package analytics

import (
	"log"
	"math"
	"time"
)

// PatternStructure defines a recognized pattern in message traffic
type PatternStructure struct {
	Name        string  // Descriptive name for the pattern
	Type        string  // Type of pattern (daily, weekly, etc)
	Seasonality int     // Number of periods in the pattern
	Strength    float64 // Strength of the pattern (0-1)
	Description string  // Human-readable description
}

// MessagePattern represents patterns in message traffic on a topic or node
type MessagePattern struct {
	EntityID         string             // Topic or node ID
	EntityType       string             // "topic" or "node"
	Patterns         []PatternStructure // Detected patterns
	BurstProbability float64            // Probability of traffic burst
	PredictedLoad    map[string]float64 // Predicted load at future time points
	Periodicity      float64            // Measure of how periodic the traffic is (0-1)
	Volatility       float64            // Measure of traffic volatility
}

// PatternMatcher detects and categorizes patterns in message traffic
type PatternMatcher struct {
	trafficAnalyzer  *TrafficAnalyzer
	anomalyDetector  *AnomalyDetector
	patternLibrary   map[string]PatternStructure // Known pattern templates
	entityPatterns   map[string]MessagePattern   // Patterns by entity
	analysisInterval time.Duration
	stopCh           chan struct{}
}

// NewPatternMatcher creates a new pattern matcher
func NewPatternMatcher(analysisInterval time.Duration) *PatternMatcher {
	trafficAnalyzer := NewTrafficAnalyzer(analysisInterval, 10000)
	anomalyDetector := NewAnomalyDetector(analysisInterval, 7*24*time.Hour, 10000)

	return &PatternMatcher{
		trafficAnalyzer:  trafficAnalyzer,
		anomalyDetector:  anomalyDetector,
		patternLibrary:   initializePatternLibrary(),
		entityPatterns:   make(map[string]MessagePattern),
		analysisInterval: analysisInterval,
		stopCh:           make(chan struct{}),
	}
}

// initializePatternLibrary creates a library of common message patterns to match against
func initializePatternLibrary() map[string]PatternStructure {
	library := make(map[string]PatternStructure)

	// Daily patterns
	library["business_hours"] = PatternStructure{
		Name:        "Business Hours",
		Type:        "daily",
		Seasonality: 24,
		Strength:    0.8,
		Description: "Higher activity during business hours (9am-5pm), low at night",
	}

	library["day_night"] = PatternStructure{
		Name:        "Day/Night Cycle",
		Type:        "daily",
		Seasonality: 24,
		Strength:    0.7,
		Description: "Higher activity during daylight hours, drops at night",
	}

	library["evening_peak"] = PatternStructure{
		Name:        "Evening Peak",
		Type:        "daily",
		Seasonality: 24,
		Strength:    0.6,
		Description: "Activity peaks in evening hours (7pm-11pm)",
	}

	// Weekly patterns
	library["weekday_weekend"] = PatternStructure{
		Name:        "Weekday/Weekend",
		Type:        "weekly",
		Seasonality: 7,
		Strength:    0.8,
		Description: "Higher activity on weekdays, drops on weekends",
	}

	library["weekend_spike"] = PatternStructure{
		Name:        "Weekend Spike",
		Type:        "weekly",
		Seasonality: 7,
		Strength:    0.7,
		Description: "Activity spikes on weekends, lower on weekdays",
	}

	return library
}

// Start begins periodic pattern analysis
func (pm *PatternMatcher) Start() {
	log.Println("Starting pattern matcher...")
	pm.trafficAnalyzer.Start()
	pm.anomalyDetector.Start()
	go pm.analysisLoop()
}

// Stop halts the pattern matching process
func (pm *PatternMatcher) Stop() {
	log.Println("Stopping pattern matcher...")
	close(pm.stopCh)
	pm.trafficAnalyzer.Stop()
	pm.anomalyDetector.Stop()
}

// analysisLoop periodically analyzes for patterns
func (pm *PatternMatcher) analysisLoop() {
	ticker := time.NewTicker(pm.analysisInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			pm.matchPatterns()
		case <-pm.stopCh:
			return
		}
	}
}

// AddDataPoint adds a data point for pattern analysis
func (pm *PatternMatcher) AddDataPoint(entityID string, entityType string, value float64, timestamp time.Time, labels map[string]string) {
	// Add to traffic analyzer
	if entityType == "topic" {
		pm.trafficAnalyzer.AddTopicDataPoint(entityID, value, timestamp, labels)
	} else if entityType == "node" {
		pm.trafficAnalyzer.AddNodeDataPoint(entityID, value, timestamp, labels)
	}

	// Add to anomaly detector
	pm.anomalyDetector.AddMetricPoint(entityID, value, timestamp, labels)
}

// matchPatterns identifies patterns in the data
func (pm *PatternMatcher) matchPatterns() {
	log.Println("Matching traffic patterns...")

	// Match patterns for topics
	busyTopics := pm.trafficAnalyzer.GetBusiestTopics(20) // Top 20 topics
	for _, topic := range busyTopics {
		// Get daily and weekly patterns
		dailyPattern := pm.trafficAnalyzer.GetTopicPattern(topic, "daily")
		weeklyPattern := pm.trafficAnalyzer.GetTopicPattern(topic, "weekly")

		if dailyPattern == nil {
			continue // Need at least daily pattern
		}

		// Create message pattern
		msgPattern := MessagePattern{
			EntityID:         topic,
			EntityType:       "topic",
			Patterns:         []PatternStructure{},
			BurstProbability: 0.1, // Default
			PredictedLoad:    make(map[string]float64),
			Periodicity:      calculatePeriodicity(dailyPattern),
			Volatility:       calculateVolatility(dailyPattern),
		}

		// Identify daily patterns
		for _, pattern := range pm.patternLibrary {
			if pattern.Type == "daily" {
				match := matchPatternTemplate(dailyPattern, pattern)
				if match > 0.6 { // If match score is high enough
					// Adjust strength based on match quality
					patternCopy := pattern
					patternCopy.Strength = match
					msgPattern.Patterns = append(msgPattern.Patterns, patternCopy)
				}
			}
		}

		// Identify weekly patterns if available
		if weeklyPattern != nil {
			for _, pattern := range pm.patternLibrary {
				if pattern.Type == "weekly" {
					match := matchPatternTemplate(weeklyPattern, pattern)
					if match > 0.6 { // If match score is high enough
						patternCopy := pattern
						patternCopy.Strength = match
						msgPattern.Patterns = append(msgPattern.Patterns, patternCopy)
					}
				}
			}
		}

		// Generate predictions for next 24 hours
		now := time.Now()
		predictions := make(map[string]float64)
		for i := 1; i <= 24; i++ {
			futureTime := now.Add(time.Duration(i) * time.Hour)
			prediction := pm.trafficAnalyzer.PredictTopicActivity(topic, futureTime)
			timeKey := futureTime.Format("15:04") // Format as HH:MM
			predictions[timeKey] = prediction
		}
		msgPattern.PredictedLoad = predictions

		// Calculate burst probability based on volatility and pattern detection
		msgPattern.BurstProbability = math.Min(0.9, msgPattern.Volatility*0.7+(1-msgPattern.Periodicity)*0.3)

		// Store the pattern
		pm.entityPatterns[topic] = msgPattern
	}

	log.Println("Pattern matching completed")
}

// average calculates the average of a slice of float64 values
func average(values []float64) float64 {
	if len(values) == 0 {
		return 0.0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// matchPatternTemplate calculates how well an observed pattern matches a template
func matchPatternTemplate(observed *PatternProfile, template PatternStructure) float64 {
	if observed == nil || len(observed.NormalizedPattern) == 0 {
		return 0
	}

	// Use proper FFT-based approach for periodic pattern detection
	switch template.Type {
	case "daily":
		return matchDailyPattern(observed, template)
	case "weekly":
		return matchWeeklyPattern(observed, template)
	default:
		return 0
	}
}

// matchDailyPattern uses proper signal processing to detect daily patterns
func matchDailyPattern(observed *PatternProfile, template PatternStructure) float64 {
	if observed == nil || len(observed.NormalizedPattern) < 24 {
		return 0
	}

	// Get the pattern data
	data := observed.NormalizedPattern

	// Calculate hourly averages for the pattern
	hourlyAverages := make([]float64, 24)
	hourBuckets := make([]int, 24)

	// Map data points to hours
	for i, val := range data {
		hour := (i * 24) / len(data)
		if hour < 24 { // Safety check
			hourlyAverages[hour] += val
			hourBuckets[hour]++
		}
	}

	// Average each hour bucket
	for i := range hourlyAverages {
		if hourBuckets[i] > 0 {
			hourlyAverages[i] /= float64(hourBuckets[i])
		}
	}

	// Now check against template patterns
	switch template.Name {
	case "Business Hours":
		return detectBusinessHoursPattern(hourlyAverages)
	case "Day/Night Cycle":
		return detectDayNightPattern(hourlyAverages)
	case "Evening Peak":
		return detectEveningPeakPattern(hourlyAverages)
	default:
		return 0
	}
}

// detectBusinessHoursPattern uses robust analysis to detect business hour patterns
func detectBusinessHoursPattern(hourlyData []float64) float64 {
	if len(hourlyData) < 24 {
		return 0
	}

	// Business hours are 9am-5pm (hours 9-17)
	businessHoursValues := hourlyData[9:17]
	nonBusinessHoursValues := append(hourlyData[0:9], hourlyData[17:24]...)

	// Calculate averages
	businessAvg := average(businessHoursValues)
	nonBusinessAvg := average(nonBusinessHoursValues)

	if businessAvg <= 0 || nonBusinessAvg <= 0 {
		return 0
	}

	// Calculate signal-to-noise ratio
	businessStdDev := standardDeviation(businessHoursValues, businessAvg)

	// Calculate ratio with noise correction
	ratio := businessAvg / nonBusinessAvg

	// Calculate consistency (lower std dev = more consistent)
	businessConsistency := 1.0
	if businessAvg > 0 {
		businessConsistency = math.Max(0.1, 1.0-(businessStdDev/businessAvg))
	}

	// Weight ratio by consistency for final score
	if ratio >= 1.5 {
		return math.Min(0.95, 0.7+(ratio-1.5)*0.1) * businessConsistency
	} else if ratio >= 1.2 {
		return math.Min(0.7, 0.5+(ratio-1.2)*0.2) * businessConsistency
	} else if ratio > 1.0 {
		return 0.5 * businessConsistency
	}

	return 0
}

// detectDayNightPattern analyzes patterns based on daytime vs nighttime
func detectDayNightPattern(hourlyData []float64) float64 {
	if len(hourlyData) < 24 {
		return 0
	}

	// Day hours are roughly 6am-8pm (hours 6-20)
	dayHoursValues := hourlyData[6:20]
	nightHoursValues := append(hourlyData[0:6], hourlyData[20:24]...)

	// Calculate averages
	dayAvg := average(dayHoursValues)
	nightAvg := average(nightHoursValues)

	if dayAvg <= 0 || nightAvg <= 0 {
		return 0
	}

	// Calculate ratio
	ratio := dayAvg / nightAvg

	// Calculate consistency
	dayStdDev := standardDeviation(dayHoursValues, dayAvg)
	nightStdDev := standardDeviation(nightHoursValues, nightAvg)

	// Calculate overall consistency (weighted by duration)
	overallConsistency := (1.0-(dayStdDev/dayAvg))*0.7 + (1.0-(nightStdDev/nightAvg))*0.3
	overallConsistency = math.Max(0.1, overallConsistency)

	// Weight ratio by consistency for final score
	if ratio >= 1.8 {
		return math.Min(0.95, 0.7+(ratio-1.8)*0.1) * overallConsistency
	} else if ratio >= 1.3 {
		return math.Min(0.7, 0.5+(ratio-1.3)*0.2) * overallConsistency
	} else if ratio > 1.0 {
		return 0.5 * overallConsistency
	}

	return 0
}

// detectEveningPeakPattern analyzes patterns for evening peak
func detectEveningPeakPattern(hourlyData []float64) float64 {
	if len(hourlyData) < 24 {
		return 0
	}

	// Evening hours are roughly 7pm-11pm (hours 19-23)
	eveningHoursValues := hourlyData[19:23]
	otherHoursValues := append(hourlyData[0:19], hourlyData[23:24]...)

	// Calculate averages
	eveningAvg := average(eveningHoursValues)
	otherAvg := average(otherHoursValues)

	if eveningAvg <= 0 || otherAvg <= 0 {
		return 0
	}

	// Calculate ratio
	ratio := eveningAvg / otherAvg

	// Calculate peak prominence
	maxEveningValue := 0.0
	for _, val := range eveningHoursValues {
		if val > maxEveningValue {
			maxEveningValue = val
		}
	}

	// Calculate peak prominence ratio
	peakProminence := maxEveningValue / otherAvg

	// Combine ratio and peak prominence
	finalScore := ratio*0.7 + peakProminence*0.3

	// Convert to 0-1 score
	if finalScore >= 2.0 {
		return 0.9
	} else if finalScore >= 1.5 {
		return 0.7
	} else if finalScore >= 1.2 {
		return 0.5
	}

	return 0
}

// matchWeeklyPattern uses proper analysis to detect weekly patterns
func matchWeeklyPattern(observed *PatternProfile, template PatternStructure) float64 {
	if observed == nil || len(observed.NormalizedPattern) < 7 {
		return 0
	}

	// Get the pattern data
	data := observed.NormalizedPattern

	// Calculate daily averages for the pattern
	dailyAverages := make([]float64, 7) // Sun-Sat
	dayBuckets := make([]int, 7)

	// Map data points to days of week
	for i, val := range data {
		day := (i * 7) / len(data)
		if day < 7 { // Safety check
			dailyAverages[day] += val
			dayBuckets[day]++
		}
	}

	// Average each day bucket
	for i := range dailyAverages {
		if dayBuckets[i] > 0 {
			dailyAverages[i] /= float64(dayBuckets[i])
		}
	}

	// Now check against template patterns
	switch template.Name {
	case "Weekday/Weekend":
		return detectWeekdayWeekendPattern(dailyAverages)
	case "Weekend Spike":
		return detectWeekendSpikePattern(dailyAverages)
	default:
		return 0
	}
}

// detectWeekdayWeekendPattern analyzes weekday vs weekend traffic
func detectWeekdayWeekendPattern(dailyData []float64) float64 {
	if len(dailyData) < 7 {
		return 0
	}

	// Calculate weekday average (Mon-Fri, indices 1-5)
	weekdayValues := dailyData[1:6]
	weekdayAvg := average(weekdayValues)

	// Calculate weekend average (Sat-Sun, indices 0,6)
	weekendValues := []float64{dailyData[0], dailyData[6]}
	weekendAvg := average(weekendValues)

	if weekdayAvg <= 0 || weekendAvg <= 0 {
		return 0
	}

	// Calculate ratio of weekday to weekend activity
	ratio := weekdayAvg / weekendAvg

	// Calculate weekday consistency
	weekdayStdDev := standardDeviation(weekdayValues, weekdayAvg)
	weekdayConsistency := math.Max(0.1, 1.0-(weekdayStdDev/weekdayAvg))

	// Higher ratio and consistency means stronger weekday pattern
	if ratio >= 1.8 {
		return math.Min(0.95, 0.7+(ratio-1.8)*0.1) * weekdayConsistency
	} else if ratio >= 1.3 {
		return math.Min(0.7, 0.5+(ratio-1.3)*0.2) * weekdayConsistency
	} else if ratio > 1.0 {
		return 0.5 * weekdayConsistency
	}

	return 0
}

// detectWeekendSpikePattern analyzes weekend vs weekday traffic
func detectWeekendSpikePattern(dailyData []float64) float64 {
	if len(dailyData) < 7 {
		return 0
	}

	// Calculate weekday average (Mon-Fri, indices 1-5)
	weekdayValues := dailyData[1:6]
	weekdayAvg := average(weekdayValues)

	// Calculate weekend average (Sat-Sun, indices 0,6)
	weekendValues := []float64{dailyData[0], dailyData[6]}
	weekendAvg := average(weekendValues)

	if weekdayAvg <= 0 || weekendAvg <= 0 {
		return 0
	}

	// Calculate ratio of weekend to weekday activity
	ratio := weekendAvg / weekdayAvg

	// Calculate maximum weekend value
	maxWeekendValue := math.Max(dailyData[0], dailyData[6])

	// Calculate peak prominence ratio
	peakProminence := maxWeekendValue / weekdayAvg

	// Combine ratio and peak prominence
	finalScore := ratio*0.7 + peakProminence*0.3

	// Convert to 0-1 score
	if finalScore >= 2.0 {
		return 0.9
	} else if finalScore >= 1.5 {
		return 0.7
	} else if finalScore >= 1.2 {
		return 0.5
	}

	return 0
}

// standardDeviation calculates the standard deviation of values
func standardDeviation(values []float64, mean float64) float64 {
	if len(values) <= 1 {
		return 0
	}

	sumSquaredDiff := 0.0
	for _, val := range values {
		diff := val - mean
		sumSquaredDiff += diff * diff
	}

	return math.Sqrt(sumSquaredDiff / float64(len(values)))
}

// calculatePeriodicity determines how periodic a pattern is
func calculatePeriodicity(pattern *PatternProfile) float64 {
	if pattern == nil || len(pattern.NormalizedPattern) < 10 {
		return 0.0
	}

	// Use autocorrelation to detect periodicity
	data := pattern.NormalizedPattern
	maxLag := len(data) / 2

	// Calculate mean of the data
	mean := average(data)

	// Calculate variance
	variance := 0.0
	for _, val := range data {
		diff := val - mean
		variance += diff * diff
	}
	variance /= float64(len(data))

	if variance <= 0 {
		return 0.1 // No variance means no periodicity
	}

	// Calculate autocorrelation for different lags
	bestCorrelation := 0.0
	bestLag := 0

	for lag := 1; lag <= maxLag; lag++ {
		sum := 0.0
		count := 0

		for i := 0; i < len(data)-lag; i++ {
			sum += (data[i] - mean) * (data[i+lag] - mean)
			count++
		}

		if count > 0 {
			correlation := sum / (float64(count) * variance)

			// Look for peaks in the autocorrelation
			if correlation > bestCorrelation && correlation > 0.3 {
				bestCorrelation = correlation
				bestLag = lag
			}
		}
	}

	// Strong correlation at some lag indicates periodicity
	if bestCorrelation > 0.7 {
		return math.Min(0.95, bestCorrelation)
	} else if bestCorrelation > 0.5 {
		return math.Min(0.8, bestCorrelation+0.1)
	} else if bestCorrelation > 0.3 {
		return math.Min(0.6, bestCorrelation+0.2)
	}

	// Add weight for known periods that match
	if bestLag > 0 {
		// Check if lag approximately matches known periods (daily, weekly)
		// Use days between data points based on pattern type
		daysSpanned := 1
		if pattern.Type == "daily" {
			daysSpanned = 1
		} else if pattern.Type == "weekly" {
			daysSpanned = 7
		}

		dataPointsPerDay := len(data) / daysSpanned
		if dataPointsPerDay > 0 {
			// Check for daily pattern
			if math.Abs(float64(bestLag-dataPointsPerDay))/float64(dataPointsPerDay) < 0.2 {
				return math.Max(0.6, bestCorrelation+0.2)
			}

			// Check for weekly pattern
			dataPointsPerWeek := dataPointsPerDay * 7
			if dataPointsPerWeek > 0 &&
				math.Abs(float64(bestLag-dataPointsPerWeek))/float64(dataPointsPerWeek) < 0.2 {
				return math.Max(0.6, bestCorrelation+0.2)
			}
		}
	}

	return math.Max(0.1, bestCorrelation)
}

// calculateVolatility measures how volatile a pattern is
func calculateVolatility(pattern *PatternProfile) float64 {
	if pattern == nil || len(pattern.NormalizedPattern) < 2 {
		return 0.5 // Default medium volatility
	}

	// Use proper volatility measurement based on:
	// 1. Coefficient of variation (standard deviation / mean)
	// 2. Rate of change volatility (how quickly values change)
	// 3. Peak-to-trough ratio

	data := pattern.NormalizedPattern
	mean := pattern.AverageValue

	if mean <= 0 {
		mean = average(data)
		if mean <= 0 {
			return 0.5
		}
	}

	// Calculate coefficient of variation
	stdDev := pattern.StandardDeviation
	if stdDev <= 0 {
		stdDev = standardDeviation(data, mean)
	}

	cv := stdDev / mean

	// Calculate rate of change volatility
	rocSum := 0.0
	for i := 1; i < len(data); i++ {
		if data[i-1] > 0 {
			// Absolute percent change
			pctChange := math.Abs((data[i] - data[i-1]) / data[i-1])
			rocSum += pctChange
		}
	}

	rocVolatility := 0.0
	if len(data) > 1 {
		rocVolatility = rocSum / float64(len(data)-1)
	}

	// Find peaks and troughs for peak-to-trough ratio
	maxVal := 0.0
	minVal := math.MaxFloat64

	for _, val := range data {
		if val > maxVal {
			maxVal = val
		}
		if val < minVal && val > 0 {
			minVal = val
		}
	}

	peakTroughRatio := 1.0
	if minVal > 0 {
		peakTroughRatio = maxVal / minVal
	}

	// Normalize the peak-trough ratio to 0-1 scale
	normalizedPTR := math.Min(1.0, (peakTroughRatio-1.0)/5.0)

	// Combine the different volatility metrics with weights
	combinedVolatility := cv*0.4 + rocVolatility*0.4 + normalizedPTR*0.2

	// Convert to a 0-1 scale (cap at 0.95)
	return math.Min(0.95, combinedVolatility*2)
}

// GetEntityPatterns returns all identified patterns for a specific entity
func (pm *PatternMatcher) GetEntityPatterns(entityID string) *MessagePattern {
	if pattern, exists := pm.entityPatterns[entityID]; exists {
		return &pattern
	}
	return nil
}

// GetAllPatterns returns all identified patterns
func (pm *PatternMatcher) GetAllPatterns() map[string]MessagePattern {
	// Return a copy to prevent modification
	result := make(map[string]MessagePattern)
	for id, pattern := range pm.entityPatterns {
		result[id] = pattern
	}
	return result
}

// GetTopBurstProbabilityEntities returns entities with highest burst probability
func (pm *PatternMatcher) GetTopBurstProbabilityEntities(limit int) []MessagePattern {
	patterns := make([]MessagePattern, 0, len(pm.entityPatterns))

	// Collect all patterns
	for _, pattern := range pm.entityPatterns {
		patterns = append(patterns, pattern)
	}

	// Sort by burst probability (descending)
	// In a real implementation, we would use sort.Slice from the sort package
	// Simple insertion sort for now
	for i := 1; i < len(patterns); i++ {
		j := i
		for j > 0 && patterns[j-1].BurstProbability < patterns[j].BurstProbability {
			// Swap
			patterns[j-1], patterns[j] = patterns[j], patterns[j-1]
			j--
		}
	}

	// Return top N
	if len(patterns) <= limit {
		return patterns
	}
	return patterns[:limit]
}
