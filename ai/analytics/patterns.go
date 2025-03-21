// Package analytics provides data analysis capabilities for Sprawl
package analytics

import (
	"log"
	"math"
	"time"
)

// PatternStructure defines a recognized pattern in message traffic
type PatternStructure struct {
	Name        string      // Descriptive name for the pattern
	Type        PatternType // Type of pattern (daily, weekly, etc)
	Seasonality int         // Number of periods in the pattern
	Strength    float64     // Strength of the pattern (0-1)
	Description string      // Human-readable description
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
		Type:        DailyPattern,
		Seasonality: 24,
		Strength:    0.8,
		Description: "Higher activity during business hours (9am-5pm), low at night",
	}

	library["day_night"] = PatternStructure{
		Name:        "Day/Night Cycle",
		Type:        DailyPattern,
		Seasonality: 24,
		Strength:    0.7,
		Description: "Higher activity during daylight hours, drops at night",
	}

	library["evening_peak"] = PatternStructure{
		Name:        "Evening Peak",
		Type:        DailyPattern,
		Seasonality: 24,
		Strength:    0.6,
		Description: "Activity peaks in evening hours (7pm-11pm)",
	}

	// Weekly patterns
	library["weekday_weekend"] = PatternStructure{
		Name:        "Weekday/Weekend",
		Type:        WeeklyPattern,
		Seasonality: 7,
		Strength:    0.8,
		Description: "Higher activity on weekdays, drops on weekends",
	}

	library["weekend_spike"] = PatternStructure{
		Name:        "Weekend Spike",
		Type:        WeeklyPattern,
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
		dailyPattern := pm.trafficAnalyzer.GetTopicPattern(topic, DailyPattern)
		weeklyPattern := pm.trafficAnalyzer.GetTopicPattern(topic, WeeklyPattern)

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
			if pattern.Type == DailyPattern {
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
				if pattern.Type == WeeklyPattern {
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

// matchPatternTemplate calculates how well an observed pattern matches a template
func matchPatternTemplate(observed *PatternProfile, template PatternStructure) float64 {
	// This would be a more sophisticated algorithm in a real implementation
	// For now, use a simplified approach based on normalized pattern
	if observed == nil || len(observed.NormalizedPattern) == 0 {
		return 0
	}

	// Different algorithms depending on pattern type
	switch template.Type {
	case DailyPattern:
		// For business hours pattern
		if template.Name == "Business Hours" {
			// Check if values are higher during 9-17 hours (9am-5pm)
			businessHoursAvg := 0.0
			nonBusinessHoursAvg := 0.0
			businessHoursCount := 0
			nonBusinessHoursCount := 0

			for i, val := range observed.NormalizedPattern {
				// Map index to hour of day (approximate)
				hour := (i * 24) / len(observed.NormalizedPattern)

				if hour >= 9 && hour < 17 {
					businessHoursAvg += val
					businessHoursCount++
				} else {
					nonBusinessHoursAvg += val
					nonBusinessHoursCount++
				}
			}

			if businessHoursCount > 0 && nonBusinessHoursCount > 0 {
				businessHoursAvg /= float64(businessHoursCount)
				nonBusinessHoursAvg /= float64(nonBusinessHoursCount)

				// Calculate ratio - higher means stronger business hours pattern
				ratio := businessHoursAvg / nonBusinessHoursAvg

				// Convert to match score (0-1)
				if ratio >= 1.5 {
					return 0.9 // Strong match
				} else if ratio >= 1.2 {
					return 0.7 // Medium match
				} else if ratio > 1.0 {
					return 0.5 // Weak match
				}
			}
		}

		// For evening peak pattern
		if template.Name == "Evening Peak" {
			// Check if values are higher during 19-23 hours (7pm-11pm)
			eveningAvg := 0.0
			otherAvg := 0.0
			eveningCount := 0
			otherCount := 0

			for i, val := range observed.NormalizedPattern {
				// Map index to hour of day
				hour := (i * 24) / len(observed.NormalizedPattern)

				if hour >= 19 && hour < 23 {
					eveningAvg += val
					eveningCount++
				} else {
					otherAvg += val
					otherCount++
				}
			}

			if eveningCount > 0 && otherCount > 0 {
				eveningAvg /= float64(eveningCount)
				otherAvg /= float64(otherCount)

				// Calculate ratio
				ratio := eveningAvg / otherAvg

				// Convert to match score
				if ratio >= 1.5 {
					return 0.9 // Strong match
				} else if ratio >= 1.2 {
					return 0.7 // Medium match
				} else if ratio > 1.0 {
					return 0.5 // Weak match
				}
			}
		}

	case WeeklyPattern:
		// For weekday/weekend pattern
		if template.Name == "Weekday/Weekend" {
			// Check if weekday values (1-5) are higher than weekend values (0,6)
			weekdayAvg := 0.0
			weekendAvg := 0.0
			weekdayCount := 0
			weekendCount := 0

			for i, val := range observed.NormalizedPattern {
				// Map index to day of week (0=Sunday, 6=Saturday)
				day := (i * 7) / len(observed.NormalizedPattern)

				if day >= 1 && day <= 5 {
					weekdayAvg += val
					weekdayCount++
				} else {
					weekendAvg += val
					weekendCount++
				}
			}

			if weekdayCount > 0 && weekendCount > 0 {
				weekdayAvg /= float64(weekdayCount)
				weekendAvg /= float64(weekendCount)

				// Calculate ratio
				ratio := weekdayAvg / weekendAvg

				// Convert to match score
				if ratio >= 1.5 {
					return 0.9 // Strong match
				} else if ratio >= 1.2 {
					return 0.7 // Medium match
				} else if ratio > 1.0 {
					return 0.5 // Weak match
				}
			}
		}

		// For weekend spike pattern
		if template.Name == "Weekend Spike" {
			// Check if weekend values (0,6) are higher than weekday values (1-5)
			weekdayAvg := 0.0
			weekendAvg := 0.0
			weekdayCount := 0
			weekendCount := 0

			for i, val := range observed.NormalizedPattern {
				// Map index to day of week
				day := (i * 7) / len(observed.NormalizedPattern)

				if day >= 1 && day <= 5 {
					weekdayAvg += val
					weekdayCount++
				} else {
					weekendAvg += val
					weekendCount++
				}
			}

			if weekdayCount > 0 && weekendCount > 0 {
				weekdayAvg /= float64(weekdayCount)
				weekendAvg /= float64(weekendCount)

				// Calculate ratio
				ratio := weekendAvg / weekdayAvg

				// Convert to match score
				if ratio >= 1.5 {
					return 0.9 // Strong match
				} else if ratio >= 1.2 {
					return 0.7 // Medium match
				} else if ratio > 1.0 {
					return 0.5 // Weak match
				}
			}
		}
	}

	return 0.0 // No match
}

// calculatePeriodicity determines how periodic a pattern is
func calculatePeriodicity(pattern *PatternProfile) float64 {
	if pattern == nil {
		return 0.0
	}

	// In a real implementation, this would use signal processing
	// techniques like FFT to find cyclical patterns

	// For simplicity, use the number of peaks as a proxy for periodicity
	if len(pattern.PeakTimes) == 0 {
		return 0.1
	}

	// More peaks and troughs indicates more periodic behavior
	// Cap at 0.9 for very periodic patterns
	return math.Min(0.9, 0.3+float64(len(pattern.PeakTimes)+len(pattern.TroughTimes))*0.05)
}

// calculateVolatility measures how volatile a pattern is
func calculateVolatility(pattern *PatternProfile) float64 {
	if pattern == nil {
		return 0.0
	}

	// Use standard deviation relative to average as volatility measure
	if pattern.AverageValue == 0 {
		return 0.5 // Default medium volatility
	}

	// Higher coefficient of variation means more volatile
	cv := pattern.StandardDeviation / pattern.AverageValue

	// Convert to a 0-1 scale (cap at 0.95)
	return math.Min(0.95, cv*2)
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
