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

	// Extract values for signal processing
	values := make([]float64, len(data))
	timestamps := make([]time.Time, len(data))
	for i, point := range data {
		values[i] = point.Value
		timestamps[i] = point.Timestamp
	}

	// Apply signal processing techniques for advanced pattern detection

	// 1. Calculate autocorrelation to detect repeating patterns
	acf := calculateTimeSeriesAutocorrelation(values, getAutocorrelationLag(profile.Type))

	// 2. Perform frequency domain analysis using FFT approximation
	// Find the dominant frequencies in the signal
	_, freqStrength := detectDominantFrequency(values, estimateSamplingRate(timestamps))

	// 3. Calculate periodogram to assess pattern strength at different frequencies
	_ = calculatePeriodogramStrength(values, profile.Type)

	// 4. Apply Seasonal-Trend decomposition for seasonal patterns
	// Extract seasonal component strength through STL-like decomposition
	seasonalStrength := extractSeasonalStrength(values, timestamps, profile.Type)

	// 5. Calculate signal-to-noise ratio with noise reduction
	snr := calculateSignalToNoiseRatio(values)

	// 6. Detect changepoints to identify pattern stability
	stabilityScore := assessPatternStability(values)

	// 7. Calculate detailed quality metrics
	metrics := analyzeTimeSeriesQuality(data, profile.Type)

	// Combine multiple metrics into a weighted confidence score (0-1)
	confidence := 0.0

	// More data points gives higher confidence, up to a limit
	dataPointsWeight := 0.15
	dataPointsScore := math.Min(float64(len(data))/100.0, 1.0)
	confidence += dataPointsWeight * dataPointsScore

	// Autocorrelation (higher is better)
	acfWeight := 0.20
	// Convert autocorrelation (-1 to 1) to a 0-1 scale
	acfScore := (acf + 1.0) / 2.0
	confidence += acfWeight * acfScore

	// Dominant frequency strength (higher is better)
	freqWeight := 0.15
	confidence += freqWeight * freqStrength

	// Seasonal component strength (higher is better)
	seasonalWeight := 0.15
	confidence += seasonalWeight * seasonalStrength

	// Signal to noise ratio (higher is better)
	snrWeight := 0.10
	// Normalize SNR from dB to 0-1 scale
	snrScore := math.Min(snr/20.0, 1.0) // 20dB is considered very good
	confidence += snrWeight * snrScore

	// Pattern stability (higher is better)
	stabilityWeight := 0.15
	confidence += stabilityWeight * stabilityScore

	// Include quality metrics from the analyzer
	metricsWeight := 0.10
	metricsScore := (metrics.SignalToNoise + metrics.Stationarity) / 2.0
	confidence += metricsWeight * metricsScore

	// Apply logistic function to scale the confidence between 0.3 and 0.95
	// This gives a nice S-curve that avoids extreme values
	// f(x) = 0.3 + 0.65/(1 + e^(-10(x-0.5)))
	scaledConfidence := 0.3 + 0.65/(1.0+math.Exp(-10.0*(confidence-0.5)))

	// Log detailed confidence components for debugging
	/* Enable when log.Debugf is available
	log.Debugf("Pattern confidence calculation for %v pattern:", profile.Type)
	log.Debugf("  - Data points score: %.2f (weight: %.2f)", dataPointsScore, dataPointsWeight)
	log.Debugf("  - Autocorrelation score: %.2f (weight: %.2f)", acfScore, acfWeight)
	log.Debugf("  - Frequency strength: %.2f (weight: %.2f)", freqStrength, freqWeight)
	log.Debugf("  - Seasonal strength: %.2f (weight: %.2f)", seasonalStrength, seasonalWeight)
	log.Debugf("  - SNR score: %.2f (weight: %.2f)", snrScore, snrWeight)
	log.Debugf("  - Stability score: %.2f (weight: %.2f)", stabilityScore, stabilityWeight)
	log.Debugf("  - Metrics score: %.2f (weight: %.2f)", metricsScore, metricsWeight)
	log.Debugf("  - Raw confidence: %.2f", confidence)
	log.Debugf("  - Scaled confidence: %.2f", scaledConfidence)
	*/

	profile.Confidence = scaledConfidence
}

// getAutocorrelationLag returns the appropriate lag for autocorrelation based on pattern type
func getAutocorrelationLag(patternType PatternType) int {
	switch patternType {
	case DailyPattern:
		return 24 // 24 hours for daily patterns
	case WeeklyPattern:
		return 7 // 7 days for weekly patterns
	case MonthlyPattern:
		return 30 // ~30 days for monthly patterns
	default:
		return 1 // Default to lag 1 for unknown patterns
	}
}

// estimateSamplingRate estimates the sampling rate from timestamps
func estimateSamplingRate(timestamps []time.Time) float64 {
	if len(timestamps) < 2 {
		return 1.0 // Default if not enough data
	}

	// Calculate average time difference between consecutive samples
	totalDuration := timestamps[len(timestamps)-1].Sub(timestamps[0])
	avgInterval := totalDuration.Seconds() / float64(len(timestamps)-1)

	// Return samples per second
	if avgInterval == 0 {
		return 1.0 // Avoid division by zero
	}
	return 1.0 / avgInterval
}

// calculateTimeSeriesAutocorrelation computes autocorrelation at a specific lag
func calculateTimeSeriesAutocorrelation(values []float64, lag int) float64 {
	if len(values) <= lag || lag <= 0 {
		return 0.0
	}

	// Calculate mean
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	// Calculate autocorrelation
	numerator := 0.0
	denominator := 0.0

	for i := 0; i < len(values)-lag; i++ {
		x1 := values[i] - mean
		x2 := values[i+lag] - mean
		numerator += x1 * x2
		denominator += x1 * x1
	}

	if denominator == 0 {
		return 0.0
	}

	return numerator / denominator
}

// detectSeasonalityStrength detects the strength of seasonality in the time series
// using a combination of frequency domain analysis and autocorrelation
func detectSeasonalityStrength(values []float64, patternType PatternType) float64 {
	// Need minimum data points for meaningful analysis
	if len(values) < 10 {
		return 0.5 // Default for short series
	}

	// Determine expected period based on pattern type
	period := 24 // Default to daily (24 hours)
	if patternType == WeeklyPattern {
		period = 7 // 7 days for weekly
	}

	// Check if we have enough data points
	if len(values) < period*2 {
		return 0.5
	}

	// First method: autocorrelation at the expected period lag
	autoCorr := calculateTimeSeriesAutocorrelation(values, period)

	// Second method: spectral analysis to detect strength of periodicity
	// We'll use the periodogram to identify power at the frequency corresponding to our expected period
	// For a sequence of length N, the frequency resolution is Fs/N where Fs is the sampling rate
	// Assuming 1 hour sampling rate for simplicity
	samplingRate := 1.0
	expectedFreq := samplingRate / float64(period)

	// Calculate periodogram
	n := len(values)
	periodogram := make([]float64, n/2+1)

	// Mean-center the signal
	mean := calculateMean(values)
	centeredValues := make([]float64, n)
	for i, v := range values {
		centeredValues[i] = v - mean
	}

	// Simple periodogram calculation using squared FFT magnitude
	// In a production system, we would use a specialized FFT library
	for k := 0; k < n/2+1; k++ {
		// Calculate frequency component at k
		real, imag := 0.0, 0.0
		for t := 0; t < n; t++ {
			angle := 2.0 * math.Pi * float64(k*t) / float64(n)
			real += centeredValues[t] * math.Cos(angle)
			imag += centeredValues[t] * math.Sin(angle)
		}
		periodogram[k] = (real*real + imag*imag) / float64(n)
	}

	// Find the peak closest to our expected frequency
	expectedIndex := int(math.Round(expectedFreq * float64(n) / samplingRate))
	searchRange := int(math.Max(float64(n)/20.0, 2.0)) // Search within 5% of expected freq

	// Search for peak near expected frequency
	startIdx := math.Max(0, float64(expectedIndex-searchRange))
	endIdx := math.Min(float64(len(periodogram)-1), float64(expectedIndex+searchRange))

	peakIndex := -1
	peakPower := 0.0

	for i := int(startIdx); i <= int(endIdx); i++ {
		if periodogram[i] > peakPower {
			peakPower = periodogram[i]
			peakIndex = i
		}
	}

	// Calculate the actual frequency found (in cycles per sample)
	actualFreq := float64(peakIndex) * samplingRate / float64(n)

	// Calculate how close the found peak is to the expected frequency
	// This can be used as an additional confidence measure
	frequencyMatch := 1.0
	if actualFreq > 0 && expectedFreq > 0 {
		actualPeriod := 1.0 / actualFreq
		periodRatio := actualPeriod / float64(period)
		// If the ratio is close to 1.0 or an integer multiple, it's a good match
		if periodRatio > 0.8 && periodRatio < 1.2 {
			frequencyMatch = 1.0 // Perfect match
		} else {
			// Discount for period mismatch
			frequencyMatch = 0.5
		}
	}

	// Calculate average power to normalize
	avgPower := 0.0
	for _, p := range periodogram {
		avgPower += p
	}
	avgPower /= float64(len(periodogram))

	// Calculate spectral density ratio (peak power / average power)
	// This is a measure of how much the signal power is concentrated at the seasonal frequency
	var spectrumStrength float64
	if avgPower > 0 {
		spectrumStrength = math.Min(1.0, peakPower/(avgPower*5.0)) // Cap at 1.0
	} else {
		spectrumStrength = 0.0
	}

	// Third method: Calculate variation between periods (low variation = strong seasonality)
	// Divide the time series into segments of length 'period' and calculate variance between them
	variationStrength := 0.0
	if len(values) >= period*3 {
		numPeriods := len(values) / period
		periodMeans := make([]float64, numPeriods)

		// Calculate mean for each period
		for i := 0; i < numPeriods; i++ {
			start := i * period
			end := start + period
			if end > len(values) {
				end = len(values)
			}

			periodMeans[i] = calculateMean(values[start:end])
		}

		// Calculate coefficient of variation for the period means
		periodMeansMean := calculateMean(periodMeans)
		periodMeansStdDev := calculateSeriesStdDev(periodMeans, periodMeansMean)

		if periodMeansMean > 0 {
			cv := periodMeansStdDev / periodMeansMean
			// Low CV means stable period-to-period pattern (strong seasonality)
			variationStrength = math.Max(0, 1.0-math.Min(1.0, cv))
		}
	}

	// Combine the three methods with weights
	// Autocorrelation is a strong indicator but can be misled by noise
	// Spectral density is good for detecting dominant frequencies
	// Variation gives insight into stability across periods
	// Include frequency match as a weight for the spectral component
	seasonalStrength := 0.5*((autoCorr+1.0)/2.0) + 0.3*spectrumStrength*frequencyMatch + 0.2*variationStrength

	return seasonalStrength
}

// Calculate maxFreqIndex correctly
func detectDominantFrequency(values []float64, samplingRate float64) (float64, float64) {
	if len(values) < 4 {
		return 0.0, 0.0
	}

	n := len(values)
	maxFreqIndex := 0
	maxPower := 0.0

	// Mean-center the signal
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(n)

	centered := make([]float64, n)
	for i, v := range values {
		centered[i] = v - mean
	}

	// Calculate power spectrum using Goertzel algorithm for selected frequencies
	// We test frequencies that correspond to common patterns
	// This is more efficient than a full FFT for our specific use case
	frequenciesToTest := []float64{
		1.0 / 24.0,  // Daily cycle (once per 24 hours)
		1.0 / 12.0,  // Twice daily cycle
		1.0 / 168.0, // Weekly cycle (once per 168 hours)
		1.0 / 8.0,   // 8-hour cycle (typical work shift)
		1.0 / 4.0,   // 4-hour cycle
	}

	for i, freq := range frequenciesToTest {
		// Normalized frequency
		normalizedFreq := freq / samplingRate * 2.0 * math.Pi

		// Apply Goertzel algorithm
		s1, s2 := 0.0, 0.0
		coeff := 2.0 * math.Cos(normalizedFreq)

		for i := 0; i < n; i++ {
			s0 := centered[i] + coeff*s1 - s2
			s2 = s1
			s1 = s0
		}

		// Calculate power
		power := s1*s1 + s2*s2 - coeff*s1*s2

		if power > maxPower {
			maxPower = power
			maxFreqIndex = i
		}
	}

	// Normalize the power to a 0-1 scale
	totalPower := 0.0
	for _, v := range centered {
		totalPower += v * v
	}

	normalizedStrength := 0.0
	if totalPower > 0 {
		normalizedStrength = math.Min(maxPower/totalPower*10.0, 1.0)
	}

	if maxFreqIndex < len(frequenciesToTest) {
		return frequenciesToTest[maxFreqIndex], normalizedStrength
	}

	return 0.0, 0.0
}

// calculatePeriodogramStrength calculates the strength of the pattern in the periodogram
func calculatePeriodogramStrength(values []float64, patternType PatternType) float64 {
	if len(values) < 10 {
		return 0.5 // Default for short series
	}

	// Determine period to test based on pattern type
	period := 24 // Default to daily (24 hours)
	switch patternType {
	case DailyPattern:
		period = 24
	case WeeklyPattern:
		period = 7 * 24
	case MonthlyPattern:
		period = 30 * 24
	}

	// Ensure period makes sense for the data length
	if period > len(values)/2 {
		period = len(values) / 2
		if period == 0 {
			period = 1
		}
	}

	// Mean-center the values
	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	centered := make([]float64, len(values))
	for i, v := range values {
		centered[i] = v - mean
	}

	// Calculate periodogram using correlation at the period
	correlation := 0.0
	validPoints := 0

	for i := 0; i < len(centered)-period; i++ {
		correlation += centered[i] * centered[i+period]
		validPoints++
	}

	// Calculate auto-correlation at lag 0 (variance)
	variance := 0.0
	for _, v := range centered {
		variance += v * v
	}

	if variance == 0 || validPoints == 0 {
		return 0.5
	}

	// Normalize to -1 to 1 range
	normalizedCorrelation := correlation / (variance * float64(validPoints) / float64(len(centered)))

	// Convert to a 0-1 strength scale
	strength := (normalizedCorrelation + 1.0) / 2.0

	return strength
}

// extractSeasonalStrength performs a simplified seasonal-trend decomposition
// and returns the strength of the seasonal component
func extractSeasonalStrength(values []float64, timestamps []time.Time, patternType PatternType) float64 {
	if len(values) < 2*24 { // Need at least 2 cycles for daily patterns
		return 0.5
	}

	// Determine period based on pattern type
	period := 24 // Default to daily (24 hours)
	switch patternType {
	case DailyPattern:
		period = 24
	case WeeklyPattern:
		period = 7 * 24
	case MonthlyPattern:
		period = 30 * 24
	}

	// Check if we have enough data for this period
	if len(values) < 2*period {
		return 0.5
	}

	// Calculate centered moving average (trend component)
	trendComponent := make([]float64, len(values))

	// Half the period, careful with even/odd periods
	halfPeriod := period / 2
	offset := 0
	if period%2 == 0 {
		// For even periods, need to offset by 1
		offset = 1
	}

	// Calculate centered moving average
	for i := halfPeriod; i < len(values)-halfPeriod-offset; i++ {
		sum := 0.0
		count := 0

		for j := i - halfPeriod; j <= i+halfPeriod+offset; j++ {
			if j >= 0 && j < len(values) {
				sum += values[j]
				count++
			}
		}

		if count > 0 {
			trendComponent[i] = sum / float64(count)
		}
	}

	// Fill in the edges with the nearest valid value
	for i := 0; i < halfPeriod; i++ {
		trendComponent[i] = trendComponent[halfPeriod]
	}
	for i := len(values) - halfPeriod - offset; i < len(values); i++ {
		trendComponent[i] = trendComponent[len(values)-halfPeriod-offset-1]
	}

	// Calculate detrended series (original - trend)
	detrended := make([]float64, len(values))
	for i := 0; i < len(values); i++ {
		detrended[i] = values[i] - trendComponent[i]
	}

	// Calculate seasonal component by averaging over periods
	seasonal := make([]float64, period)
	seasonalCounts := make([]int, period)

	for i := 0; i < len(detrended); i++ {
		idx := i % period
		seasonal[idx] += detrended[i]
		seasonalCounts[idx]++
	}

	// Calculate average seasonal component
	for i := 0; i < period; i++ {
		if seasonalCounts[i] > 0 {
			seasonal[i] /= float64(seasonalCounts[i])
		}
	}

	// Calculate the strength of the seasonal component
	// This is defined as variance(seasonal) / (variance(seasonal) + variance(residual))

	// Compute full seasonal component by repeating the pattern
	fullSeasonal := make([]float64, len(values))
	for i := 0; i < len(values); i++ {
		fullSeasonal[i] = seasonal[i%period]
	}

	// Calculate residual component
	residual := make([]float64, len(values))
	for i := 0; i < len(values); i++ {
		residual[i] = detrended[i] - fullSeasonal[i]
	}

	// Calculate variances
	varSeasonal := calculateVariance(fullSeasonal)
	varResidual := calculateVariance(residual)

	if varSeasonal+varResidual == 0 {
		return 0.5
	}

	strength := varSeasonal / (varSeasonal + varResidual)

	// Constrain to 0-1 range
	strength = math.Max(0.0, math.Min(1.0, strength))

	return strength
}

// calculateVariance computes the variance of a slice of values
func calculateVariance(values []float64) float64 {
	if len(values) < 2 {
		return 0.0
	}

	mean := 0.0
	for _, v := range values {
		mean += v
	}
	mean /= float64(len(values))

	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}

	return variance / float64(len(values))
}

// calculateSignalToNoiseRatio calculates the SNR in decibels
func calculateSignalToNoiseRatio(values []float64) float64 {
	if len(values) < 10 {
		return 10.0 // Default for short series
	}

	// Apply simple noise reduction with moving average
	windowSize := 3
	smoothed := make([]float64, len(values))

	for i := 0; i < len(values); i++ {
		sum := 0.0
		count := 0

		for j := i - windowSize; j <= i+windowSize; j++ {
			if j >= 0 && j < len(values) {
				sum += values[j]
				count++
			}
		}

		if count > 0 {
			smoothed[i] = sum / float64(count)
		}
	}

	// Calculate signal power (smoothed values)
	signalPower := 0.0
	for _, v := range smoothed {
		signalPower += v * v
	}
	signalPower /= float64(len(smoothed))

	// Calculate noise power (original - smoothed)
	noisePower := 0.0
	for i := 0; i < len(values); i++ {
		noise := values[i] - smoothed[i]
		noisePower += noise * noise
	}
	noisePower /= float64(len(values))

	// Avoid division by zero
	if noisePower < 1e-10 {
		return 30.0 // High SNR
	}

	// Calculate SNR in decibels
	snrDb := 10.0 * math.Log10(signalPower/noisePower)

	// Constrain to reasonable range
	snrDb = math.Max(0.0, math.Min(30.0, snrDb))

	return snrDb
}

// assessPatternStability detects changepoints and evaluates pattern stability
func assessPatternStability(values []float64) float64 {
	if len(values) < 20 {
		return 0.5 // Default for short series
	}

	// Calculate moving standard deviation with window size 5
	windowSize := 5
	movingStdDev := make([]float64, len(values))

	for i := windowSize; i < len(values)-windowSize; i++ {
		window := values[i-windowSize : i+windowSize+1]

		// Calculate mean for this window
		mean := 0.0
		for _, v := range window {
			mean += v
		}
		mean /= float64(len(window))

		// Calculate stddev
		sumSquares := 0.0
		for _, v := range window {
			diff := v - mean
			sumSquares += diff * diff
		}
		stdDev := math.Sqrt(sumSquares / float64(len(window)))

		movingStdDev[i] = stdDev
	}

	// Fill in edges
	for i := 0; i < windowSize; i++ {
		movingStdDev[i] = movingStdDev[windowSize]
	}
	for i := len(values) - windowSize; i < len(values); i++ {
		movingStdDev[i] = movingStdDev[len(values)-windowSize-1]
	}

	// Detect changepoints by looking for spikes in the moving stddev
	// A spike indicates a change in the pattern
	changepoints := 0
	threshold := 2.0 // Threshold for detecting a spike

	// Calculate baseline stddev
	avgStdDev := 0.0
	for _, v := range movingStdDev {
		avgStdDev += v
	}
	avgStdDev /= float64(len(movingStdDev))

	// Count number of points exceeding threshold
	for _, v := range movingStdDev {
		if v > threshold*avgStdDev {
			changepoints++
		}
	}

	// Calculate stability score (fewer changepoints means more stable)
	// We use an exponential decay function to map changepoint density to stability
	changepointDensity := float64(changepoints) / float64(len(values))
	stability := math.Exp(-5.0 * changepointDensity)

	return stability
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

	// Extract values and timestamps
	values := make([]float64, len(data))
	timestamps := make([]time.Time, len(data))
	for i, point := range data {
		values[i] = point.Value
		timestamps[i] = point.Timestamp
	}

	// Calculate basic statistics
	mean := calculateMean(values)
	stdDev := calculateStdDev(values, mean)

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

	autocorr := calculateTimeSeriesAutocorrelation(values, lag)
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

		firstStdDev := calculateStdDev(firstHalf, firstMean)
		secondStdDev := calculateStdDev(secondHalf, secondMean)

		// Calculate stationarity as similarity between halves
		// Both mean and stddev should be similar for stationary series
		meanDiff := math.Abs(firstMean-secondMean) / (math.Abs(firstMean) + math.Abs(secondMean) + 1e-10)
		stdDevDiff := math.Abs(firstStdDev-secondStdDev) / (firstStdDev + secondStdDev + 1e-10)

		// Combine into a stationarity score (1 = perfectly stationary)
		metrics.Stationarity = math.Max(0, 1-(meanDiff+stdDevDiff))
	} else {
		metrics.Stationarity = 0.5 // Default for very short series
	}

	// Assess seasonality strength
	metrics.Seasonality = detectSeasonalityStrength(values, patternType)

	// Assess trend strength using simple regression
	metrics.TrendStrength = detectTrendStrength(values, timestamps)

	return metrics
}

// detectTrendStrength calculates the strength of trend in the time series
// Returns a value between 0 and 1, where higher values indicate stronger trends
func detectTrendStrength(values []float64, timestamps []time.Time) float64 {
	if len(values) < 3 || len(timestamps) < 3 {
		return 0.0
	}

	// Convert timestamps to relative time in hours
	timeValues := make([]float64, len(timestamps))
	for i := range timestamps {
		if i == 0 {
			timeValues[i] = 0
		} else {
			// Hours since first timestamp
			timeValues[i] = timestamps[i].Sub(timestamps[0]).Hours()
		}
	}

	// Calculate linear regression using least squares
	n := float64(len(values))
	sumX := 0.0
	sumY := 0.0
	sumXY := 0.0
	sumX2 := 0.0
	sumY2 := 0.0

	for i := range values {
		x := timeValues[i]
		y := values[i]

		sumX += x
		sumY += y
		sumXY += x * y
		sumX2 += x * x
		sumY2 += y * y
	}

	// Calculate correlation coefficient
	numerator := n*sumXY - sumX*sumY
	denominator := math.Sqrt((n*sumX2 - sumX*sumX) * (n*sumY2 - sumY*sumY))

	correlation := 0.0
	if denominator != 0 {
		correlation = numerator / denominator
	}

	// Return absolute value of correlation as trend strength
	return math.Abs(correlation)
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
