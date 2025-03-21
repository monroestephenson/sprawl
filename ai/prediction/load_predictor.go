// Package prediction provides machine learning models for Sprawl
package prediction

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

// ResourceType defines the type of resource being predicted
type ResourceType string

const (
	// ResourceCPU represents CPU usage predictions
	ResourceCPU ResourceType = "cpu"
	// ResourceMemory represents memory usage predictions
	ResourceMemory ResourceType = "memory"
	// ResourceNetwork represents network bandwidth predictions
	ResourceNetwork ResourceType = "network"
	// ResourceDisk represents disk I/O predictions
	ResourceDisk ResourceType = "disk"
	// ResourceMessageRate represents message throughput predictions
	ResourceMessageRate ResourceType = "message_rate"
)

// LoadDataPoint represents a single data point for load prediction
type LoadDataPoint struct {
	Timestamp time.Time
	Value     float64
	Resource  ResourceType
	NodeID    string
	Labels    map[string]string
}

// PredictionInterval defines the time interval for predictions
type PredictionInterval string

const (
	// Interval5Min represents 5-minute prediction intervals
	Interval5Min PredictionInterval = "5min"
	// Interval15Min represents 15-minute prediction intervals
	Interval15Min PredictionInterval = "15min"
	// Interval1Hour represents 1-hour prediction intervals
	Interval1Hour PredictionInterval = "1hour"
	// Interval24Hour represents 24-hour prediction intervals
	Interval24Hour PredictionInterval = "24hour"
)

// PredictionResult represents the output of load prediction
type PredictionResult struct {
	Resource      ResourceType
	NodeID        string
	Timestamp     time.Time
	PredictedVal  float64
	LowerBound    float64 // Lower bound of prediction interval
	UpperBound    float64 // Upper bound of prediction interval
	Confidence    float64 // Confidence level (0-1)
	ProbBurstRisk float64 // Probability of a sudden burst (0-1)
}

// LoadPredictor predicts future resource usage
type LoadPredictor struct {
	mu                 sync.RWMutex
	historyData        map[ResourceType]map[string][]LoadDataPoint
	timeSeriesModels   map[ResourceType]map[string]timeSeriesModel
	seasonalModels     map[ResourceType]map[string]seasonalModel
	weightedPredictors map[ResourceType]map[string][]weightedPredictor
	modelWeights       map[string][]float64
	maxHistoryPoints   int
	trained            bool
}

// timeSeriesModel implements exponential smoothing for time series prediction
type timeSeriesModel struct {
	alpha        float64   // Smoothing factor
	lastValue    float64   // Last observed value
	lastForecast float64   // Last forecasted value
	trend        float64   // Current trend
	beta         float64   // Trend smoothing factor
	lastUpdated  time.Time // When model was last updated
}

// seasonalModel implements seasonal modeling for cyclical patterns
type seasonalModel struct {
	seasonLength    int       // Length of seasonal cycle (e.g., 24 for hourly in a day)
	seasonalIndices []float64 // Seasonal adjustment factors
	baseValue       float64   // Base value (non-seasonal component)
}

// weightedPredictor combines different prediction models with weights
type weightedPredictor struct {
	name       string
	predictFn  func(time.Time) float64
	errorStats struct {
		mse        float64   // Mean squared error
		lastErrors []float64 // Recent prediction errors
	}
}

// NewLoadPredictor creates a new load predictor instance
func NewLoadPredictor(maxHistoryPoints int) *LoadPredictor {
	if maxHistoryPoints <= 0 {
		maxHistoryPoints = 10000 // Default: 10K points per entity
	}

	return &LoadPredictor{
		historyData:        make(map[ResourceType]map[string][]LoadDataPoint),
		timeSeriesModels:   make(map[ResourceType]map[string]timeSeriesModel),
		seasonalModels:     make(map[ResourceType]map[string]seasonalModel),
		weightedPredictors: make(map[ResourceType]map[string][]weightedPredictor),
		modelWeights:       make(map[string][]float64),
		maxHistoryPoints:   maxHistoryPoints,
		trained:            false,
	}
}

// AddDataPoint adds a new data point for training
func (lp *LoadPredictor) AddDataPoint(point LoadDataPoint) {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	// Initialize maps if needed
	if _, exists := lp.historyData[point.Resource]; !exists {
		lp.historyData[point.Resource] = make(map[string][]LoadDataPoint)
	}

	// Initialize slice if needed
	identifier := point.NodeID
	if _, exists := lp.historyData[point.Resource][identifier]; !exists {
		lp.historyData[point.Resource][identifier] = []LoadDataPoint{}
	}

	// Add data point
	lp.historyData[point.Resource][identifier] = append(
		lp.historyData[point.Resource][identifier],
		point,
	)

	// Trim history if needed
	if len(lp.historyData[point.Resource][identifier]) > lp.maxHistoryPoints {
		// Keep only most recent points
		excess := len(lp.historyData[point.Resource][identifier]) - lp.maxHistoryPoints
		lp.historyData[point.Resource][identifier] = lp.historyData[point.Resource][identifier][excess:]
	}

	// Update models with new data point
	lp.updateModels(point)
}

// updateModels updates prediction models with new data
func (lp *LoadPredictor) updateModels(point LoadDataPoint) {
	resource := point.Resource
	identifier := point.NodeID

	// Initialize time series models if needed
	if _, exists := lp.timeSeriesModels[resource]; !exists {
		lp.timeSeriesModels[resource] = make(map[string]timeSeriesModel)
	}

	// Initialize seasonal models if needed
	if _, exists := lp.seasonalModels[resource]; !exists {
		lp.seasonalModels[resource] = make(map[string]seasonalModel)
	}

	// Initialize weighted predictors if needed
	if _, exists := lp.weightedPredictors[resource]; !exists {
		lp.weightedPredictors[resource] = make(map[string][]weightedPredictor)
	}

	// Update time series model
	if model, exists := lp.timeSeriesModels[resource][identifier]; exists {
		// If we have an existing model, update it
		if !model.lastUpdated.IsZero() {
			// Calculate time since last update
			elapsed := point.Timestamp.Sub(model.lastUpdated)

			// Apply exponential smoothing
			alpha := 0.3 // Default smoothing factor
			beta := 0.1  // Default trend smoothing factor

			// Adjust alpha based on time elapsed (increase for longer gaps)
			if elapsed > time.Hour {
				alpha = math.Min(0.8, alpha*(1.0+elapsed.Hours()/24.0))
			}

			// Calculate prediction error
			predictionError := point.Value - model.lastForecast

			// Update trend
			model.trend = beta*(point.Value-model.lastValue) + (1-beta)*model.trend

			// Update value with smoothing
			model.lastValue = point.Value
			model.lastForecast = alpha*point.Value + (1-alpha)*(model.lastForecast+model.trend)

			// Store error for model evaluation (would be used in a more complete implementation)
			_ = predictionError
		} else {
			// First data point for this model
			model.lastValue = point.Value
			model.lastForecast = point.Value
			model.trend = 0.0
		}

		model.lastUpdated = point.Timestamp
		lp.timeSeriesModels[resource][identifier] = model
	} else {
		// Create new model
		lp.timeSeriesModels[resource][identifier] = timeSeriesModel{
			alpha:        0.3,
			beta:         0.1,
			lastValue:    point.Value,
			lastForecast: point.Value,
			trend:        0.0,
			lastUpdated:  point.Timestamp,
		}
	}

	// Update seasonal model if we have enough data
	// This is simplified - a real implementation would be more complex
	if dataPoints, exists := lp.historyData[resource][identifier]; exists && len(dataPoints) >= 24 {
		// Try to update seasonal model with 24-hour seasonality
		if model, exists := lp.seasonalModels[resource][identifier]; exists {
			// Extract hour of day for seasonal indexing
			hour := point.Timestamp.Hour()

			// Update seasonal index for this hour
			if hour < len(model.seasonalIndices) {
				// Simple update: average of old value and new normalized value
				normalized := point.Value / model.baseValue
				model.seasonalIndices[hour] = (model.seasonalIndices[hour] + normalized) / 2.0

				// Update base value with slight weight to new value
				model.baseValue = 0.95*model.baseValue + 0.05*point.Value

				lp.seasonalModels[resource][identifier] = model
			}
		} else if len(dataPoints) >= 48 { // Need at least 2 days of data to initialize
			// Initialize a new seasonal model
			seasonalIndices := make([]float64, 24) // 24 hours in a day

			// Calculate average value as base
			sum := 0.0
			for _, dp := range dataPoints {
				sum += dp.Value
			}
			baseValue := sum / float64(len(dataPoints))

			// Initialize seasonal indices
			hourCounts := make([]int, 24)
			hourSums := make([]float64, 24)

			// Calculate sum and count for each hour
			for _, dp := range dataPoints {
				hour := dp.Timestamp.Hour()
				hourSums[hour] += dp.Value / baseValue
				hourCounts[hour]++
			}

			// Calculate average for each hour
			for i := 0; i < 24; i++ {
				if hourCounts[i] > 0 {
					seasonalIndices[i] = hourSums[i] / float64(hourCounts[i])
				} else {
					seasonalIndices[i] = 1.0 // Default: no seasonal effect
				}
			}

			// Store the model
			lp.seasonalModels[resource][identifier] = seasonalModel{
				seasonLength:    24,
				seasonalIndices: seasonalIndices,
				baseValue:       baseValue,
			}
		}
	}
}

// Train builds prediction models from historical data
func (lp *LoadPredictor) Train() error {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	// Check if we have data to train on
	if len(lp.historyData) == 0 {
		return errors.New("no historical data available for training")
	}

	log.Println("Training load prediction models...")

	// For each resource type and identifier
	for resource, resourceData := range lp.historyData {
		for identifier, dataPoints := range resourceData {
			if len(dataPoints) < 10 {
				// Need minimum data for training
				continue
			}

			// Initialize weighted predictors
			if _, exists := lp.weightedPredictors[resource]; !exists {
				lp.weightedPredictors[resource] = make(map[string][]weightedPredictor)
			}

			predictors := []weightedPredictor{}

			// Create time series predictor
			if tsModel, exists := lp.timeSeriesModels[resource][identifier]; exists {
				predictors = append(predictors, weightedPredictor{
					name: "exponential_smoothing",
					predictFn: func(t time.Time) float64 {
						// Simple forecast based on current value, trend, and time difference
						timeDiff := t.Sub(tsModel.lastUpdated).Hours()
						return tsModel.lastForecast + tsModel.trend*float64(timeDiff)
					},
					errorStats: struct {
						mse        float64
						lastErrors []float64
					}{
						mse:        1.0, // Start with default error
						lastErrors: make([]float64, 0),
					},
				})
			}

			// Create seasonal predictor
			if seasonModel, exists := lp.seasonalModels[resource][identifier]; exists {
				predictors = append(predictors, weightedPredictor{
					name: "seasonal",
					predictFn: func(t time.Time) float64 {
						// Get seasonal index for this hour
						hour := t.Hour()
						if hour >= len(seasonModel.seasonalIndices) {
							hour = hour % len(seasonModel.seasonalIndices)
						}

						// Apply seasonal adjustment to base value
						return seasonModel.baseValue * seasonModel.seasonalIndices[hour]
					},
					errorStats: struct {
						mse        float64
						lastErrors []float64
					}{
						mse:        1.0, // Start with default error
						lastErrors: make([]float64, 0),
					},
				})
			}

			// Add average baseline predictor
			sum := 0.0
			for _, dp := range dataPoints {
				sum += dp.Value
			}
			avgValue := sum / float64(len(dataPoints))

			predictors = append(predictors, weightedPredictor{
				name: "average_baseline",
				predictFn: func(t time.Time) float64 {
					return avgValue
				},
				errorStats: struct {
					mse        float64
					lastErrors []float64
				}{
					mse:        1.0, // Start with default error
					lastErrors: make([]float64, 0),
				},
			})

			// Store predictors
			lp.weightedPredictors[resource][identifier] = predictors

			// Initialize with equal weights
			weights := make([]float64, len(predictors))
			for i := range weights {
				weights[i] = 1.0 / float64(len(weights))
			}
			lp.modelWeights[identifier+string(resource)] = weights
		}
	}

	lp.trained = true
	log.Println("Load prediction models trained successfully")
	return nil
}

// TrainForResource trains the model for a specific resource type and node
// using data points collected since the cutoff time
func (lp *LoadPredictor) TrainForResource(resource ResourceType, nodeID string, cutoffTime time.Time) error {
	lp.mu.Lock()
	defer lp.mu.Unlock()

	// Check if we have data for this resource and node
	nodeData, exists := lp.historyData[resource]
	if !exists {
		return fmt.Errorf("no historical data for resource type: %s", resource)
	}

	dataPoints, exists := nodeData[nodeID]
	if !exists {
		return fmt.Errorf("no historical data for node: %s", nodeID)
	}

	// Filter data points to only those after the cutoff time
	filteredPoints := []LoadDataPoint{}
	for _, point := range dataPoints {
		if point.Timestamp.After(cutoffTime) {
			filteredPoints = append(filteredPoints, point)
		}
	}

	// Reduced minimum from 10 to 3 data points
	if len(filteredPoints) < 3 {
		return fmt.Errorf("insufficient data points for training (need at least 3, have %d)", len(filteredPoints))
	}

	// Create a time series model for this resource/node if it doesn't exist
	if _, exists := lp.timeSeriesModels[resource]; !exists {
		lp.timeSeriesModels[resource] = make(map[string]timeSeriesModel)
	}

	// Initialize or reset the time series model
	latestPoint := filteredPoints[len(filteredPoints)-1]
	lp.timeSeriesModels[resource][nodeID] = timeSeriesModel{
		alpha:        0.3, // Default alpha value for exponential smoothing
		beta:         0.1, // Default beta value for trend
		lastValue:    latestPoint.Value,
		lastForecast: latestPoint.Value,
		lastUpdated:  latestPoint.Timestamp,
	}

	// Initialize seasonal model if we have enough data points
	if len(filteredPoints) >= 24 {
		if _, exists := lp.seasonalModels[resource]; !exists {
			lp.seasonalModels[resource] = make(map[string]seasonalModel)
		}

		// For demonstration, assume a 24-hour seasonality pattern
		seasonLength := 24
		seasonalIndices := calculateSeasonalIndices(filteredPoints, seasonLength)

		lp.seasonalModels[resource][nodeID] = seasonalModel{
			seasonLength:    seasonLength,
			seasonalIndices: seasonalIndices,
			baseValue:       latestPoint.Value,
		}
	}

	// Initialize weighted predictors
	if _, exists := lp.weightedPredictors[resource]; !exists {
		lp.weightedPredictors[resource] = make(map[string][]weightedPredictor)
	}

	// Simple weighted predictors
	predictors := []weightedPredictor{
		{
			name: "exponential",
			predictFn: func(t time.Time) float64 {
				model := lp.timeSeriesModels[resource][nodeID]
				timeDiff := t.Sub(model.lastUpdated).Hours()
				return model.lastForecast + (model.trend * float64(timeDiff))
			},
			errorStats: struct {
				mse        float64
				lastErrors []float64
			}{
				mse:        0.0,
				lastErrors: make([]float64, 5),
			},
		},
	}

	// Add seasonal predictor if we have a seasonal model
	if _, exists := lp.seasonalModels[resource][nodeID]; exists {
		predictors = append(predictors, weightedPredictor{
			name: "seasonal",
			predictFn: func(t time.Time) float64 {
				model := lp.seasonalModels[resource][nodeID]
				hour := t.Hour()
				idx := hour % model.seasonLength
				if idx >= len(model.seasonalIndices) {
					idx = 0
				}
				return model.baseValue * model.seasonalIndices[idx]
			},
			errorStats: struct {
				mse        float64
				lastErrors []float64
			}{
				mse:        0.0,
				lastErrors: make([]float64, 5),
			},
		})
	}

	lp.weightedPredictors[resource][nodeID] = predictors

	// Initial equal weights for the predictors
	weights := make([]float64, len(predictors))
	for i := range weights {
		weights[i] = 1.0 / float64(len(weights))
	}
	lp.modelWeights[nodeID] = weights

	lp.trained = true
	return nil
}

// calculateSeasonalIndices calculates seasonal indices from historical data
func calculateSeasonalIndices(points []LoadDataPoint, seasonLength int) []float64 {
	if len(points) < seasonLength {
		// Not enough data, return neutral indices
		indices := make([]float64, seasonLength)
		for i := range indices {
			indices[i] = 1.0
		}
		return indices
	}

	// Group data by hour (or whatever seasonal period we're using)
	seasonalGroups := make(map[int][]float64)
	for _, point := range points {
		hour := point.Timestamp.Hour()
		seasonalGroups[hour] = append(seasonalGroups[hour], point.Value)
	}

	// Calculate the average for each hour
	seasonalAverages := make([]float64, seasonLength)
	for hour, values := range seasonalGroups {
		if hour >= seasonLength {
			continue
		}

		sum := 0.0
		for _, value := range values {
			sum += value
		}
		seasonalAverages[hour] = sum / float64(len(values))
	}

	// Calculate the overall average
	overallSum := 0.0
	validHours := 0
	for _, avg := range seasonalAverages {
		if avg > 0 {
			overallSum += avg
			validHours++
		}
	}

	overallAverage := 1.0
	if validHours > 0 {
		overallAverage = overallSum / float64(validHours)
	}

	// Calculate the seasonal indices
	seasonalIndices := make([]float64, seasonLength)
	for i, avg := range seasonalAverages {
		if avg > 0 {
			seasonalIndices[i] = avg / overallAverage
		} else {
			seasonalIndices[i] = 1.0 // Neutral index for hours with no data
		}
	}

	return seasonalIndices
}

// Predict forecasts future resource usage
func (lp *LoadPredictor) Predict(resource ResourceType, nodeID string, futureTime time.Time) (PredictionResult, error) {
	lp.mu.RLock()
	defer lp.mu.RUnlock()

	result := PredictionResult{
		Resource:      resource,
		NodeID:        nodeID,
		Timestamp:     futureTime,
		PredictedVal:  0.0,
		LowerBound:    0.0,
		UpperBound:    0.0,
		Confidence:    0.5,
		ProbBurstRisk: 0.1,
	}

	// Check if we have predictors for this resource and node
	predictors, ok := lp.weightedPredictors[resource][nodeID]
	if !ok || len(predictors) == 0 {
		return result, errors.New("no prediction models available")
	}

	// Get model weights
	weights, ok := lp.modelWeights[nodeID+string(resource)]
	if !ok || len(weights) != len(predictors) {
		// Default to equal weights
		weights = make([]float64, len(predictors))
		for i := range weights {
			weights[i] = 1.0 / float64(len(weights))
		}
	}

	// Make prediction by combining multiple models
	weightedSum := 0.0
	totalWeight := 0.0
	predictions := make([]float64, len(predictors))

	// Get predictions from each model and combine
	for i, predictor := range predictors {
		predictions[i] = predictor.predictFn(futureTime)
		weightedSum += predictions[i] * weights[i]
		totalWeight += weights[i]
	}

	if totalWeight > 0 {
		result.PredictedVal = weightedSum / totalWeight
	}

	// Calculate prediction bounds
	// In a real implementation, this would be more sophisticated
	varianceSum := 0.0
	for i, pred := range predictions {
		diff := pred - result.PredictedVal
		varianceSum += diff * diff * weights[i]
	}

	// Standard deviation of predictions
	stdDev := math.Sqrt(varianceSum / totalWeight)

	// Set confidence interval (roughly 95% confidence)
	result.LowerBound = result.PredictedVal - 1.96*stdDev
	if result.LowerBound < 0 {
		result.LowerBound = 0 // Ensure non-negative
	}
	result.UpperBound = result.PredictedVal + 1.96*stdDev

	// Calculate confidence based on consistency between models
	// High variance = lower confidence
	result.Confidence = 1.0 - math.Min(1.0, stdDev/result.PredictedVal)
	if result.Confidence < 0.1 {
		result.Confidence = 0.1 // Minimum confidence
	}

	// Burst risk calculation (simplified)
	// Check if we have sufficient history for this resource
	if data, exists := lp.historyData[resource][nodeID]; exists && len(data) > 10 {
		// Look for spikes in recent history
		recentData := data
		if len(data) > 100 {
			recentData = data[len(data)-100:]
		}

		// Calculate recent volatility
		sum := 0.0
		for _, point := range recentData {
			sum += point.Value
		}
		avg := sum / float64(len(recentData))

		varianceSum = 0.0
		maxJump := 0.0

		for i := 1; i < len(recentData); i++ {
			diff := recentData[i].Value - avg
			varianceSum += diff * diff

			// Calculate largest jump between consecutive points
			jump := math.Abs(recentData[i].Value - recentData[i-1].Value)
			if jump > maxJump {
				maxJump = jump
			}
		}

		volatility := math.Sqrt(varianceSum / float64(len(recentData)-1))
		jumpRatio := maxJump / avg

		// Higher volatility and jump ratio indicate higher burst risk
		result.ProbBurstRisk = math.Min(0.95, (volatility/avg)*0.5+jumpRatio*0.5)
	}

	return result, nil
}

// PredictMultiStep generates predictions for multiple time steps ahead
func (lp *LoadPredictor) PredictMultiStep(resource ResourceType, nodeID string, startTime time.Time, steps int, interval time.Duration) ([]PredictionResult, error) {
	if steps <= 0 {
		return nil, errors.New("number of steps must be positive")
	}

	results := make([]PredictionResult, steps)

	// Generate multi-step forecast
	for i := 0; i < steps; i++ {
		timePoint := startTime.Add(time.Duration(i) * interval)
		prediction, err := lp.Predict(resource, nodeID, timePoint)
		if err != nil {
			// Initialize with increasing uncertainty
			prediction = PredictionResult{
				Resource:     resource,
				NodeID:       nodeID,
				Timestamp:    timePoint,
				PredictedVal: 0.0,
				Confidence:   math.Max(0.1, 0.7-float64(i)*0.05), // Decreasing confidence
			}
		}

		// For long-term predictions, increase uncertainty
		if i > 0 {
			// Widen prediction interval
			intervalWidth := prediction.UpperBound - prediction.LowerBound
			growthFactor := 1.0 + float64(i)*0.05
			halfWidth := intervalWidth * growthFactor / 2.0

			prediction.LowerBound = prediction.PredictedVal - halfWidth
			if prediction.LowerBound < 0 {
				prediction.LowerBound = 0
			}
			prediction.UpperBound = prediction.PredictedVal + halfWidth

			// Reduce confidence for farther predictions
			prediction.Confidence = math.Max(0.1, prediction.Confidence-float64(i)*0.02)
		}

		results[i] = prediction
	}

	return results, nil
}

// DetectAnomaly checks if a data point is anomalous
func (lp *LoadPredictor) DetectAnomaly(point LoadDataPoint) (bool, float64) {
	lp.mu.RLock()
	defer lp.mu.RUnlock()

	// Get history for this resource and node
	history, exists := lp.historyData[point.Resource][point.NodeID]
	if !exists || len(history) < 10 {
		return false, 0.0 // Not enough data to detect anomalies
	}

	// Calculate mean and standard deviation
	sum := 0.0
	for _, hp := range history {
		sum += hp.Value
	}
	mean := sum / float64(len(history))

	varianceSum := 0.0
	for _, hp := range history {
		diff := hp.Value - mean
		varianceSum += diff * diff
	}
	stdDev := math.Sqrt(varianceSum / float64(len(history)))

	// Calculate z-score
	zScore := (point.Value - mean) / stdDev

	// Check if value is over 3 standard deviations from mean (common anomaly threshold)
	return math.Abs(zScore) > 3.0, math.Abs(zScore)
}
