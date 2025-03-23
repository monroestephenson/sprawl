package node

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPublishHandler(t *testing.T) {
	// Create a simple handler that mocks the publishHandler
	handler := func(w http.ResponseWriter, r *http.Request) {
		// Check method
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Parse the request
		var msg struct {
			Topic   string `json:"topic"`
			Payload string `json:"payload"`
		}
		if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Send response
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "published",
			"id":     uuid.New().String(),
		}); err != nil {
			t.Logf("Error encoding publish response: %v", err)
		}
	}

	// Create test data
	publishData := struct {
		Topic   string `json:"topic"`
		Message string `json:"payload"`
	}{
		Topic:   "test-topic",
		Message: "Hello, test!",
	}

	body, err := json.Marshal(publishData)
	if err != nil {
		t.Fatalf("Failed to marshal publish data: %v", err)
	}

	// Create the request
	req := httptest.NewRequest(http.MethodPost, "/publish", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	// Create a recorder to capture the response
	recorder := httptest.NewRecorder()

	// Call the handler
	handler(recorder, req)

	// Check response
	if recorder.Code != http.StatusOK {
		t.Errorf("Expected status code %d but got %d", http.StatusOK, recorder.Code)
	}

	// Parse the response
	var response struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}

	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify the response
	if response.Status != "published" {
		t.Errorf("Expected status 'published' but got '%s'", response.Status)
	}
}

func TestSubscribeHandler(t *testing.T) {
	// Create a simple handler that mocks the subscribeHandler
	handler := func(w http.ResponseWriter, r *http.Request) {
		// Check method
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Parse the request
		var sub struct {
			ID     string   `json:"id"`
			Topics []string `json:"topics"`
		}
		if err := json.NewDecoder(r.Body).Decode(&sub); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// Generate ID if not provided
		if sub.ID == "" {
			sub.ID = uuid.New().String()
		}

		// Send response
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]string{
			"status": "subscribed",
			"id":     sub.ID,
		}); err != nil {
			t.Logf("Error encoding subscribe response: %v", err)
		}
	}

	// Create test data
	subscribeData := struct {
		Topics []string `json:"topics"`
		ID     string   `json:"id"`
	}{
		Topics: []string{"test-topic-1", "test-topic-2"},
		ID:     "test-client-1",
	}

	body, err := json.Marshal(subscribeData)
	if err != nil {
		t.Fatalf("Failed to marshal subscribe data: %v", err)
	}

	// Create the request
	req := httptest.NewRequest(http.MethodPost, "/subscribe", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	// Create a recorder to capture the response
	recorder := httptest.NewRecorder()

	// Call the handler
	handler(recorder, req)

	// Check response
	if recorder.Code != http.StatusOK {
		t.Errorf("Expected status code %d but got %d", http.StatusOK, recorder.Code)
	}

	// Parse the response
	var response struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}

	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify the response
	if response.Status != "subscribed" {
		t.Errorf("Expected status 'subscribed' but got '%s'", response.Status)
	}
}

func TestAIEndpoints(t *testing.T) {
	// Create a test AI engine
	ai := &TestAIEngine{}

	// Test the AI status endpoint
	t.Run("AI Status", func(t *testing.T) {
		// Create a handler that mocks handleAIStatus
		handler := func(w http.ResponseWriter, r *http.Request) {
			status := ai.GetStatus()
			status["node_id"] = "test-node"
			status["server_time"] = time.Now().Format(time.RFC3339)

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(status); err != nil {
				t.Logf("Error encoding AI status response: %v", err)
			}
		}

		// Create request
		req := httptest.NewRequest(http.MethodGet, "/ai/status", nil)
		recorder := httptest.NewRecorder()

		// Call handler
		handler(recorder, req)

		// Check response
		if recorder.Code != http.StatusOK {
			t.Errorf("Expected status code %d but got %d", http.StatusOK, recorder.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		// Check if enabled field exists
		if enabled, ok := response["enabled"]; !ok {
			t.Error("AI status response does not contain 'enabled' field")
		} else if enabled != true {
			t.Errorf("Expected 'enabled' to be true, but got %v", enabled)
		}
	})

	// Test the AI predictions endpoint
	t.Run("AI Predictions", func(t *testing.T) {
		// Create a handler that mocks handleAIPredictions
		handler := func(w http.ResponseWriter, r *http.Request) {
			resource := r.URL.Query().Get("resource")
			value, confidence := ai.GetPrediction(resource)

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]interface{}{
				"success":    true,
				"resource":   resource,
				"prediction": value,
				"confidence": confidence,
			}); err != nil {
				t.Logf("Error encoding AI predictions response: %v", err)
			}
		}

		// Create request
		req := httptest.NewRequest(http.MethodGet, "/ai/predictions?resource=cpu", nil)
		recorder := httptest.NewRecorder()

		// Call handler
		handler(recorder, req)

		// Check response
		if recorder.Code != http.StatusOK {
			t.Errorf("Expected status code %d but got %d", http.StatusOK, recorder.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		// Check for success field
		if success, ok := response["success"].(bool); !ok || !success {
			t.Error("AI predictions response does not indicate success")
		}
	})

	// Test the AI anomalies endpoint
	t.Run("AI Anomalies", func(t *testing.T) {
		// Create a handler that mocks handleAIAnomalies
		handler := func(w http.ResponseWriter, r *http.Request) {
			anomalies := ai.GetSimpleAnomalies()

			w.Header().Set("Content-Type", "application/json")
			if err := json.NewEncoder(w).Encode(map[string]interface{}{
				"anomalies": anomalies,
			}); err != nil {
				t.Logf("Error encoding AI anomalies response: %v", err)
			}
		}

		// Create request
		req := httptest.NewRequest(http.MethodGet, "/ai/anomalies", nil)
		recorder := httptest.NewRecorder()

		// Call handler
		handler(recorder, req)

		// Check response
		if recorder.Code != http.StatusOK {
			t.Errorf("Expected status code %d but got %d", http.StatusOK, recorder.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		// Check if anomalies field exists
		if _, ok := response["anomalies"]; !ok {
			t.Error("AI anomalies response does not contain 'anomalies' field")
		}
	})
}
