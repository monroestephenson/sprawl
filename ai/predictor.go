package ai

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// PredictionResponse represents the response format for prediction requests
type PredictionResponse struct {
	Resource       string    `json:"resource"`
	PredictedVal   float64   `json:"predicted_val"`
	Confidence     float64   `json:"confidence"`
	Timestamp      time.Time `json:"timestamp"`
	PredictionType string    `json:"prediction_type"`
}

// HandlePredictions handles the /ai/predictions endpoint
func HandlePredictions(w http.ResponseWriter, r *http.Request) {
	resource := r.URL.Query().Get("resource")
	if resource == "" {
		http.Error(w, "Resource parameter is required", http.StatusBadRequest)
		return
	}

	// Simple mock predictions based on resource type
	var prediction PredictionResponse
	switch resource {
	case "cpu":
		prediction = PredictionResponse{
			Resource:       "cpu",
			PredictedVal:   25.5,
			Confidence:     0.85,
			Timestamp:      time.Now(),
			PredictionType: "linear",
		}
	case "memory":
		prediction = PredictionResponse{
			Resource:       "memory",
			PredictedVal:   512.0,
			Confidence:     0.9,
			Timestamp:      time.Now(),
			PredictionType: "ensemble",
		}
	case "message_rate":
		prediction = PredictionResponse{
			Resource:       "message_rate",
			PredictedVal:   150.0,
			Confidence:     0.75,
			Timestamp:      time.Now(),
			PredictionType: "moving_average",
		}
	case "network":
		prediction = PredictionResponse{
			Resource:       "network",
			PredictedVal:   1024.0,
			Confidence:     0.8,
			Timestamp:      time.Now(),
			PredictionType: "ensemble",
		}
	default:
		http.Error(w, fmt.Sprintf("Unsupported resource type: %s", resource), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(prediction)
}
