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

// ensureInitialized ensures all maps are initialized for a resource and identifier
func (lp *LoadPredictor) ensureInitialized(resource ResourceType, identifier string) {
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

	// Initialize history data if needed
	if _, exists := lp.historyData[resource]; !exists {
		lp.historyData[resource] = make(map[string][]LoadDataPoint)
	}

	// Initialize the slice for this identifier if needed
	if _, exists := lp.historyData[resource][identifier]; !exists {
		lp.historyData[resource][identifier] = []LoadDataPoint{}
	}
}

// getIdentifier creates a unique identifier from node ID and resource
func getIdentifier(nodeID string, resource ResourceType) string {
	return nodeID + ":" + string(resource)
}

func (lp *LoadPredictor) updateModels(point LoadDataPoint) {
	// Ensure we have maps for this resource
	resource := point.Resource
	identifier := getIdentifier(point.NodeID, point.Resource)

	lp.ensureInitialized(resource, identifier)

	// Add to history
	if historyData, exists := lp.historyData[resource][identifier]; exists {
		lp.historyData[resource][identifier] = append(historyData, point)

		// Limit history size
		if len(lp.historyData[resource][identifier]) > lp.maxHistoryPoints {
			lp.historyData[resource][identifier] = lp.historyData[resource][identifier][1:]
		}
	}

	// Update time series model using double exponential smoothing
	if model, exists := lp.timeSeriesModels[resource][identifier]; exists {
		// Calculate time since last update to adjust alpha
		timeSinceLast := point.Timestamp.Sub(model.lastUpdated)

		// Dynamically adjust alpha based on time since last update
		// The longer the gap, the less weight we give to the previous forecast
		adjustedAlpha := model.alpha
		if timeSinceLast > time.Hour {
			// Increase alpha (more weight to new observation) for longer gaps
			adjustedAlpha = math.Min(0.8, model.alpha*2.0)
		}

		// Extract features that might influence the prediction
		features := extractTimeFeatures(point.Timestamp)
		hourOfDay := features["hour_of_day"]
		isWeekend := features["is_weekend"]

		// Apply seasonal adjustment if we have a seasonalModel
		seasonalAdjustment := 1.0
		if seasonalModel, hasSeasonalModel := lp.seasonalModels[resource][identifier]; hasSeasonalModel {
			// Adjust based on hour of day (hourly seasonality)
			hour := int(hourOfDay)
			if hour < len(seasonalModel.seasonalIndices) {
				seasonalAdjustment = seasonalModel.seasonalIndices[hour]
			}

			// Additional weekend adjustment if applicable
			if isWeekend > 0.5 { // Weekend
				// WeekendAdjustment would be calculated based on historical pattern differences
				weekendAdjustment := getWeekendAdjustment(lp.historyData[resource][identifier])
				seasonalAdjustment *= weekendAdjustment
			}
		}

		// Adjust the value based on seasonal factors
		seasonallyAdjustedValue := point.Value
		if seasonalAdjustment != 0 {
			seasonallyAdjustedValue = point.Value / seasonalAdjustment
		}

		// Calculate prediction error from previous forecast
		// Store for model evaluation and possible adaptive parameter tuning
		_ = seasonallyAdjustedValue - model.lastForecast

		// Update level (intercept) with exponential smoothing
		newLevel := adjustedAlpha*seasonallyAdjustedValue + (1-adjustedAlpha)*(model.lastValue+model.trend)

		// Update trend (slope) with exponential smoothing
		newTrend := model.beta*(newLevel-model.lastValue) + (1-model.beta)*model.trend

		// Update the model
		newModel := timeSeriesModel{
			alpha:        model.alpha, // Keep original alpha for next time
			lastValue:    newLevel,
			lastForecast: newLevel + newTrend,
			trend:        newTrend,
			beta:         model.beta,
			lastUpdated:  point.Timestamp,
		}

		// Store the updated model
		lp.timeSeriesModels[resource][identifier] = newModel
	}

	// Update seasonal model if we have enough data
	// Handle seasonality by properly capturing patterns
	if dataPoints, exists := lp.historyData[resource][identifier]; exists && len(dataPoints) >= 24 {
		// Initialize or update seasonal model
		if model, exists := lp.seasonalModels[resource][identifier]; exists {
			// Extract hour of day for seasonal indexing
			hour := point.Timestamp.Hour()

			// Calculate the detrended value
			baseValue := model.baseValue
			if baseValue == 0 {
				baseValue = 1.0 // Avoid division by zero
			}

			// Calculate ratio of actual value to expected baseline
			normalized := point.Value / baseValue

			// Exponential smoothing for seasonal index
			smoothingFactor := 0.1 // Low smoothing factor for seasonal components

			if hour < len(model.seasonalIndices) {
				// Update seasonal index for this hour with exponential smoothing
				oldIndex := model.seasonalIndices[hour]
				model.seasonalIndices[hour] = smoothingFactor*normalized + (1-smoothingFactor)*oldIndex

				// Rebalance indices to ensure they average to 1.0
				rebalanceSeasonalIndices(model.seasonalIndices)

				// Update base value with slight weight to new value (detrended)
				model.baseValue = 0.95*model.baseValue + 0.05*point.Value

				lp.seasonalModels[resource][identifier] = model
			}
		} else if len(dataPoints) >= 48 { // Need at least 2 days of data to initialize
			// Calculate seasonal indices based on historical data
			seasonalIndices := calculateSeasonalIndices(dataPoints, 24)

			// Calculate base value (average)
			sum := 0.0
			for _, dp := range dataPoints {
				sum += dp.Value
			}
			baseValue := sum / float64(len(dataPoints))
			if baseValue == 0 {
				baseValue = 1.0 // Avoid division by zero
			}

			// Initialize seasonal model
			lp.seasonalModels[resource][identifier] = seasonalModel{
				seasonLength:    24,
				seasonalIndices: seasonalIndices,
				baseValue:       baseValue,
			}
		}
	}
}

// rebalanceSeasonalIndices ensures seasonal indices average to 1.0
func rebalanceSeasonalIndices(indices []float64) {
	// Calculate sum of indices
	sum := 0.0
	for _, idx := range indices {
		sum += idx
	}

	// If sum is zero, set all to 1.0
	if sum == 0 {
		for i := range indices {
			indices[i] = 1.0
		}
		return
	}

	// Calculate average
	avg := sum / float64(len(indices))

	// Rebalance
	for i := range indices {
		indices[i] = indices[i] / avg
	}
}

// getWeekendAdjustment calculates how weekend values differ from weekday values
func getWeekendAdjustment(data []LoadDataPoint) float64 {
	if len(data) < 48 { // Need enough data to calculate pattern
		return 1.0
	}

	// Count weekend and weekday points
	var weekendSum, weekdaySum float64
	var weekendCount, weekdayCount int

	for _, point := range data {
		dayOfWeek := point.Timestamp.Weekday()
		if dayOfWeek == time.Saturday || dayOfWeek == time.Sunday {
			weekendSum += point.Value
			weekendCount++
		} else {
			weekdaySum += point.Value
			weekdayCount++
		}
	}

	// Calculate averages
	weekendAvg := 1.0
	weekdayAvg := 1.0

	if weekendCount > 0 {
		weekendAvg = weekendSum / float64(weekendCount)
	}

	if weekdayCount > 0 {
		weekdayAvg = weekdaySum / float64(weekdayCount)
	}

	// Calculate adjustment factor
	if weekdayAvg == 0 {
		return 1.0 // Avoid division by zero
	}

	return weekendAvg / weekdayAvg
}

// extractTimeFeatures extracts relevant time features for prediction
func extractTimeFeatures(timestamp time.Time) map[string]float64 {
	features := make(map[string]float64)

	// Basic time features
	features["hour_of_day"] = float64(timestamp.Hour())
	features["day_of_week"] = float64(timestamp.Weekday())
	features["month"] = float64(timestamp.Month())
	features["day_of_month"] = float64(timestamp.Day())

	// Derived features
	features["is_weekend"] = 0.0
	if timestamp.Weekday() == time.Saturday || timestamp.Weekday() == time.Sunday {
		features["is_weekend"] = 1.0
	}

	features["is_business_hours"] = 0.0
	hour := timestamp.Hour()
	if hour >= 9 && hour <= 17 && features["is_weekend"] == 0 {
		features["is_business_hours"] = 1.0
	}

	// Cyclic encoding of hour (to capture circular nature of time)
	features["hour_sin"] = math.Sin(2 * math.Pi * float64(hour) / 24.0)
	features["hour_cos"] = math.Cos(2 * math.Pi * float64(hour) / 24.0)

	// Cyclic encoding of day of week
	dayOfWeek := float64(timestamp.Weekday())
	features["day_sin"] = math.Sin(2 * math.Pi * dayOfWeek / 7.0)
	features["day_cos"] = math.Cos(2 * math.Pi * dayOfWeek / 7.0)

	return features
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

// calculateSeasonalIndices computes seasonal indices for a given period length
func calculateSeasonalIndices(dataPoints []LoadDataPoint, periodLength int) []float64 {
	if len(dataPoints) < periodLength*2 {
		// Default to no seasonality if we don't have enough data
		indices := make([]float64, periodLength)
		for i := range indices {
			indices[i] = 1.0
		}
		return indices
	}

	// Calculate the average value to use as a baseline
	sum := 0.0
	for _, dp := range dataPoints {
		sum += dp.Value
	}
	avgValue := sum / float64(len(dataPoints))
	if avgValue == 0 {
		avgValue = 1.0 // Avoid division by zero
	}

	// Initialize counters and sums for each period position
	periodSums := make([]float64, periodLength)
	periodCounts := make([]int, periodLength)

	// Group data by period position and calculate sums and counts
	for _, dp := range dataPoints {
		// Determine position in the seasonal cycle
		// For hourly data with 24-hour periodicity, this would be the hour of day
		position := dp.Timestamp.Hour() % periodLength

		// Add normalized value to the sum for this position
		periodSums[position] += dp.Value / avgValue
		periodCounts[position]++
	}

	// Calculate average for each position in the cycle
	indices := make([]float64, periodLength)
	for i := 0; i < periodLength; i++ {
		if periodCounts[i] > 0 {
			indices[i] = periodSums[i] / float64(periodCounts[i])
		} else {
			indices[i] = 1.0 // Default for positions with no data
		}
	}

	// Normalize indices to ensure they average to 1.0
	rebalanceSeasonalIndices(indices)

	return indices
}

// Predict forecasts resource usage for a future timestamp
func (lp *LoadPredictor) Predict(resource ResourceType, nodeID string, futureTime time.Time, interval PredictionInterval) (PredictionResult, error) {
	lp.mu.RLock()
	defer lp.mu.RUnlock()

	// Validate inputs
	if !lp.trained && len(lp.historyData) == 0 {
		return PredictionResult{}, errors.New("prediction model has not been trained")
	}

	identifier := getIdentifier(nodeID, resource)

	// Check if we have models for this resource
	if _, exists := lp.timeSeriesModels[resource]; !exists {
		return PredictionResult{}, fmt.Errorf("no models available for resource %s", resource)
	}

	// Check if we have models for this identifier
	if _, exists := lp.timeSeriesModels[resource][identifier]; !exists {
		// Try to find similar nodes to use as a fallback
		similarIdentifier := lp.findSimilarNode(resource, nodeID)
		if similarIdentifier == "" {
			return PredictionResult{}, fmt.Errorf("no models available for node %s and resource %s", nodeID, resource)
		}
		identifier = similarIdentifier
	}

	// Initialize prediction output
	result := PredictionResult{
		Resource:     resource,
		NodeID:       nodeID,
		Timestamp:    futureTime,
		PredictedVal: 0.0,
		Confidence:   0.0,
	}

	// Get time series model
	var tsModel timeSeriesModel
	var hasTSModel bool
	if model, exists := lp.timeSeriesModels[resource][identifier]; exists {
		tsModel = model
		hasTSModel = true
	}

	// Get seasonal model
	var seasonModel seasonalModel
	var hasSeasonalModel bool
	if model, exists := lp.seasonalModels[resource][identifier]; exists {
		seasonModel = model
		hasSeasonalModel = true
	}

	// Calculate base prediction using time series model
	if hasTSModel {
		// Time difference in hours (can be fractional)
		timeDiffHours := futureTime.Sub(tsModel.lastUpdated).Hours()

		// Apply trend projection based on time difference
		baseProjection := tsModel.lastValue + tsModel.trend*timeDiffHours

		// For long-term predictions, dampen the trend to avoid unrealistic forecasts
		if timeDiffHours > 24 {
			dampingFactor := math.Exp(-0.05 * (timeDiffHours - 24.0)) // Exponential damping
			dampedTrend := tsModel.trend * dampingFactor
			baseProjection = tsModel.lastValue + dampedTrend*timeDiffHours
		}

		result.PredictedVal = baseProjection

		// Apply seasonal adjustment if available
		if hasSeasonalModel {
			hour := futureTime.Hour()
			if hour < len(seasonModel.seasonalIndices) {
				seasonalFactor := seasonModel.seasonalIndices[hour]

				// Check for weekend effects
				features := extractTimeFeatures(futureTime)
				if features["is_weekend"] > 0.5 && len(lp.historyData[resource][identifier]) >= 48 {
					weekendAdjustment := getWeekendAdjustment(lp.historyData[resource][identifier])
					seasonalFactor *= weekendAdjustment
				}

				// Apply seasonal factor to the base projection
				result.PredictedVal = baseProjection * seasonalFactor
			}
		}

		// Determine prediction bounds based on historical error and forecast horizon
		if historyData, hasHistory := lp.historyData[resource][identifier]; hasHistory && len(historyData) > 10 {
			// Calculate prediction error statistics from recent history
			var errors []float64
			var sumSquaredErrors float64

			// Use last 20 data points or as many as available
			historySize := len(historyData)
			startIdx := max(0, historySize-20)

			for i := startIdx; i < historySize-1; i++ {
				// Calculate the error between actual and what would have been predicted
				current := historyData[i]
				next := historyData[i+1]
				timeDiff := next.Timestamp.Sub(current.Timestamp).Hours()

				// Simple projection from current point to next point
				projected := current.Value + tsModel.trend*timeDiff

				// Apply seasonal adjustments if available
				if hasSeasonalModel {
					currentHour := current.Timestamp.Hour()
					nextHour := next.Timestamp.Hour()

					if currentHour < len(seasonModel.seasonalIndices) && nextHour < len(seasonModel.seasonalIndices) {
						seasonalChange := seasonModel.seasonalIndices[nextHour] / seasonModel.seasonalIndices[currentHour]
						projected *= seasonalChange
					}
				}

				// Calculate prediction error
				error := next.Value - projected
				errors = append(errors, error)
				sumSquaredErrors += error * error
			}

			// Mean squared error
			mse := 0.0
			if len(errors) > 0 {
				mse = sumSquaredErrors / float64(len(errors))
			}

			// Standard deviation of errors
			stdDevError := math.Sqrt(mse)

			// Widen prediction interval for longer forecast horizons
			forecastHorizonFactor := math.Sqrt(timeDiffHours / 24.0)
			if forecastHorizonFactor < 1.0 {
				forecastHorizonFactor = 1.0
			}

			// Calculate confidence interval (95% CI is roughly ±2 standard deviations)
			marginOfError := 2.0 * stdDevError * forecastHorizonFactor

			// Set bounds
			result.LowerBound = result.PredictedVal - marginOfError
			if result.LowerBound < 0 && resource != ResourceNetwork { // Allow negative for network (can represent outbound)
				result.LowerBound = 0
			}
			result.UpperBound = result.PredictedVal + marginOfError

			// Calculate confidence (higher for shorter forecasts, lower for longer ones)
			confidenceFactor := 1.0 / (1.0 + forecastHorizonFactor*0.5)
			result.Confidence = math.Max(0.5, math.Min(0.95, confidenceFactor))

			// Calculate burst risk probability based on historical volatility and trend
			if len(errors) > 0 {
				// Estimate volatility as coefficient of variation
				meanValue := 0.0
				for _, dp := range historyData[startIdx:] {
					meanValue += dp.Value
				}
				meanValue /= float64(historySize - startIdx)

				if meanValue > 0 {
					volatility := stdDevError / meanValue
					trendStrength := math.Abs(tsModel.trend) / meanValue

					// Higher volatility and strong upward trends increase burst risk
					result.ProbBurstRisk = math.Min(0.95, volatility*0.5+trendStrength*0.5)
				}
			}
		} else {
			// Without enough history, use wider bounds and lower confidence
			result.LowerBound = result.PredictedVal * 0.5
			result.UpperBound = result.PredictedVal * 1.5
			result.Confidence = 0.5
			result.ProbBurstRisk = 0.5
		}
	} else {
		// Fallback: use average of historical data if available
		if historyData, exists := lp.historyData[resource][identifier]; exists && len(historyData) > 0 {
			// Calculate simple average
			sum := 0.0
			for _, point := range historyData {
				sum += point.Value
			}
			avg := sum / float64(len(historyData))

			result.PredictedVal = avg
			result.LowerBound = avg * 0.5
			result.UpperBound = avg * 1.5
			result.Confidence = 0.3    // Low confidence for this simple method
			result.ProbBurstRisk = 0.5 // Medium risk without trend information
		} else {
			// No data at all, return error
			return PredictionResult{}, fmt.Errorf("insufficient data for prediction")
		}
	}

	return result, nil
}

// findSimilarNode attempts to find a node with similar patterns for the given resource
func (lp *LoadPredictor) findSimilarNode(resource ResourceType, nodeID string) string {
	// If no data for this exact node, try to find a similar node
	if resourceData, exists := lp.historyData[resource]; exists {
		// For now, just return any other node we have data for
		for otherID := range resourceData {
			if otherID != nodeID && len(resourceData[otherID]) > 0 {
				return otherID
			}
		}
	}
	return "" // No similar node found
}

// Helper function for min/max operations
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// PredictMultiStep generates predictions for multiple time steps ahead
func (lp *LoadPredictor) PredictMultiStep(resource ResourceType, nodeID string, startTime time.Time, interval time.Duration, steps int) ([]PredictionResult, error) {
	lp.mu.RLock()
	defer lp.mu.RUnlock()

	if steps <= 0 {
		return nil, errors.New("steps must be positive")
	}

	// Convert the interval duration to a PredictionInterval
	var predInterval PredictionInterval
	if interval.Hours() >= 24 {
		predInterval = Interval24Hour
	} else if interval.Hours() >= 1 {
		predInterval = Interval1Hour
	} else if interval.Minutes() >= 15 {
		predInterval = Interval15Min
	} else {
		predInterval = Interval5Min
	}

	results := make([]PredictionResult, steps)

	// Generate predictions for each time step
	for i := 0; i < steps; i++ {
		timePoint := startTime.Add(time.Duration(i) * interval)
		prediction, err := lp.Predict(resource, nodeID, timePoint, predInterval)
		if err != nil {
			// Initialize with increasing uncertainty
			predictedVal := 0.0
			if i > 0 {
				// Use the last valid prediction with increased uncertainty
				predictedVal = results[i-1].PredictedVal
			}

			// Dummy prediction with high uncertainty
			results[i] = PredictionResult{
				Resource:      resource,
				NodeID:        nodeID,
				Timestamp:     timePoint,
				PredictedVal:  predictedVal,
				LowerBound:    predictedVal * 0.5,
				UpperBound:    predictedVal * 2.0,
				Confidence:    0.2,
				ProbBurstRisk: 0.5,
			}
		} else {
			results[i] = prediction

			// Increase uncertainty for further predictions
			if i > 0 {
				// Lower confidence for future predictions
				scaleFactor := 1.0 - float64(i)/float64(steps*2)
				if scaleFactor < 0.3 {
					scaleFactor = 0.3
				}
				results[i].Confidence *= scaleFactor

				// Widen prediction intervals
				margin := results[i].UpperBound - results[i].PredictedVal
				results[i].UpperBound = results[i].PredictedVal + margin*(1.0+float64(i)*0.1)
				results[i].LowerBound = results[i].PredictedVal - margin*(1.0+float64(i)*0.1)
				if results[i].LowerBound < 0 && resource != ResourceNetwork {
					results[i].LowerBound = 0
				}
			}
		}
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
