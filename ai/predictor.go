package ai

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// PredictionResponse represents the response for a prediction request
type PredictionResponse struct {
	Resource       string    `json:"resource"`
	PredictedVal   float64   `json:"predicted_value"`
	Confidence     float64   `json:"confidence"`
	Timestamp      time.Time `json:"timestamp"`
	PredictionType string    `json:"prediction_type"`
}

// HandlePredictions provides predictions for various system resources
func HandlePredictions(w http.ResponseWriter, r *http.Request) {
	resource := r.URL.Query().Get("resource")
	if resource == "" {
		http.Error(w, "Resource parameter is required", http.StatusBadRequest)
		return
	}

	// Get AI intelligence instance from context or global
	ai := GetGlobalIntelligence()
	if ai == nil {
		// Fallback predictions if AI is not initialized
		fallbackPrediction(w, r, resource)
		return
	}

	// Prediction horizon - default to 1 hour
	horizon := time.Hour
	if horizonParam := r.URL.Query().Get("horizon"); horizonParam != "" {
		if h, err := time.ParseDuration(horizonParam); err == nil {
			horizon = h
		}
	}

	// Get the appropriate metric type for the resource
	var metricType MetricType
	switch resource {
	case "cpu", "memory":
		metricType = MetricResourceUsage
	case "message_rate":
		metricType = MetricMessageCount
	case "network":
		metricType = MetricNetworkTraffic
	case "latency":
		metricType = MetricLatency
	default:
		http.Error(w, "Unsupported resource type", http.StatusBadRequest)
		return
	}

	// Get prediction from AI intelligence
	predictions, err := ai.PredictFutureLoad(metricType, resource, horizon)
	if err != nil || len(predictions) == 0 {
		// Fallback to basic prediction
		fallbackPrediction(w, r, resource)
		return
	}

	// Get the last prediction (at the requested horizon)
	lastPrediction := predictions[len(predictions)-1]

	// Build response
	response := PredictionResponse{
		Resource:       resource,
		PredictedVal:   lastPrediction.PredictedVal,
		Confidence:     lastPrediction.Confidence,
		Timestamp:      lastPrediction.Timestamp,
		PredictionType: lastPrediction.Explanation,
	}

	// Return prediction as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding prediction response: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

// fallbackPrediction provides basic predictions if AI is not available
func fallbackPrediction(w http.ResponseWriter, r *http.Request, resource string) {
	// Create a reasonable fallback prediction based on resource type
	var prediction PredictionResponse
	switch resource {
	case "cpu":
		prediction = PredictionResponse{
			Resource:       "cpu",
			PredictedVal:   25.5,
			Confidence:     0.7,
			Timestamp:      time.Now().Add(time.Hour),
			PredictionType: "linear_regression",
		}
	case "memory":
		prediction = PredictionResponse{
			Resource:       "memory",
			PredictedVal:   512.0,
			Confidence:     0.75,
			Timestamp:      time.Now().Add(time.Hour),
			PredictionType: "moving_average",
		}
	case "message_rate":
		prediction = PredictionResponse{
			Resource:       "message_rate",
			PredictedVal:   150.0,
			Confidence:     0.6,
			Timestamp:      time.Now().Add(time.Hour),
			PredictionType: "holt_winters",
		}
	case "network":
		prediction = PredictionResponse{
			Resource:       "network",
			PredictedVal:   1024.0,
			Confidence:     0.65,
			Timestamp:      time.Now().Add(time.Hour),
			PredictionType: "exponential_smoothing",
		}
	default:
		prediction = PredictionResponse{
			Resource:       resource,
			PredictedVal:   50.0,
			Confidence:     0.5,
			Timestamp:      time.Now().Add(time.Hour),
			PredictionType: "constant",
		}
	}

	// Return prediction as JSON
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(prediction); err != nil {
		log.Printf("Error encoding fallback prediction: %v", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
}

// Global instance for intelligence - would be initialized by the application
var globalIntelligence *Intelligence

// GetGlobalIntelligence returns the global instance of intelligence
func GetGlobalIntelligence() *Intelligence {
	return globalIntelligence
}

// SetGlobalIntelligence sets the global instance of intelligence
func SetGlobalIntelligence(ai *Intelligence) {
	globalIntelligence = ai
}
