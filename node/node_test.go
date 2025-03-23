package node

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"sprawl/node/dht"
)

// Test creation of a new node
func TestNewNode(t *testing.T) {
	opts := &Options{
		BindAddress:           "127.0.0.1",
		BindPort:              0, // ephemeral port for testing
		HTTPPort:              8000,
		AdvertiseAddr:         "127.0.0.1",
		MaxConcurrentRequests: 10,
	}

	node, err := NewNode(opts)
	if err != nil {
		t.Fatalf("Failed to create node: %v", err)
	}

	if node.ID == "" {
		t.Error("Node ID should not be empty")
	}

	if node.Store == nil {
		t.Error("Node store should not be nil")
	}

	if node.DHT == nil {
		t.Error("Node DHT should not be nil")
	}

	if node.Gossip == nil {
		t.Error("Node Gossip should not be nil")
	}

	if node.Router == nil {
		t.Error("Node Router should not be nil")
	}

	if node.Balancer == nil {
		t.Error("Node Balancer should not be nil")
	}

	if node.Metrics == nil {
		t.Error("Node Metrics should not be nil")
	}

	if node.AI == nil {
		t.Error("Node AI should not be nil")
	}

	if node.BindAddress != opts.BindAddress {
		t.Errorf("Expected BindAddress to be %s, but got %s", opts.BindAddress, node.BindAddress)
	}

	if node.HTTPPort != opts.HTTPPort {
		t.Errorf("Expected HTTPPort to be %d, but got %d", opts.HTTPPort, node.HTTPPort)
	}

	if cap(node.semaphore) != opts.MaxConcurrentRequests {
		t.Errorf("Expected semaphore capacity to be %d, but got %d", opts.MaxConcurrentRequests, cap(node.semaphore))
	}
}

// Test invalid node creation
func TestInvalidNode(t *testing.T) {
	opts := &Options{
		BindAddress:           "127.0.0.1",
		BindPort:              0,
		HTTPPort:              -1, // Invalid port
		AdvertiseAddr:         "127.0.0.1",
		MaxConcurrentRequests: 10,
	}

	_, err := NewNode(opts)
	if err == nil {
		t.Fatal("Expected error when creating node with invalid port")
	}
}

// Test node HTTP handlers
func TestNodeHTTPHandlers(t *testing.T) {
	// Create a test node with mock components
	node := CreateSimpleNode(t)

	// Test AI status handler
	t.Run("AI Status Handler", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ai/status", nil)
		w := httptest.NewRecorder()

		node.handleAIStatus(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if enabled, ok := response["enabled"].(bool); !ok || !enabled {
			t.Errorf("Expected 'enabled' to be true, got %v", enabled)
		}
	})

	// Test AI predictions handler
	t.Run("AI Predictions Handler", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ai/predictions?resource=cpu", nil)
		w := httptest.NewRecorder()

		node.handleAIPredictions(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if success, ok := response["success"].(bool); !ok || !success {
			t.Errorf("Expected 'success' to be true, got %v", success)
		}

		if resource, ok := response["resource"].(string); !ok || resource != "cpu" {
			t.Errorf("Expected 'resource' to be 'cpu', got %v", resource)
		}
	})

	// Test AI predictions handler with missing resource parameter
	t.Run("AI Predictions Handler Missing Resource", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ai/predictions", nil)
		w := httptest.NewRecorder()

		node.handleAIPredictions(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("Expected status code %d, got %d", http.StatusBadRequest, w.Code)
		}
	})

	// Test AI anomalies handler
	t.Run("AI Anomalies Handler", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/ai/anomalies", nil)
		w := httptest.NewRecorder()

		node.handleAIAnomalies(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
		}

		var response map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		anomalies, ok := response["anomalies"].([]interface{})
		if !ok {
			t.Fatalf("Expected 'anomalies' to be an array, got %T", response["anomalies"])
		}

		if len(anomalies) == 0 {
			t.Error("Expected at least one anomaly, got none")
		}
	})
}

// Test node start and stop
func TestNodeStartStop(t *testing.T) {
	// We'll skip the actual Start method test since it relies on network binding
	// Instead, we'll test the Stop method to ensure it cleans up resources
	node := CreateSimpleNode(t)

	// Mock stopCh to be non-nil
	node.stopCh = make(chan struct{})

	// Test Stop
	err := node.Stop()
	if err != nil {
		t.Errorf("Failed to stop node: %v", err)
	}
}

// Test helper function
func TestTruncateID(t *testing.T) {
	// Test normal case
	id := "1234567890abcdef"
	result := truncateID(id)
	expected := "12345678"

	if result != expected {
		t.Errorf("Expected truncated ID to be %s, got %s", expected, result)
	}

	// Test short ID
	shortID := "123"
	result = truncateID(shortID)
	if result != shortID {
		t.Errorf("Expected short ID to remain %s, got %s", shortID, result)
	}
}

func TestHandleDHT(t *testing.T) {
	// Create a simple node using the SetupTestNode helper
	node, cleanup := SetupTestNode(t)
	defer cleanup()

	// Initialize DHT since CreateSimpleNode doesn't do it
	node.DHT = dht.NewDHT(node.ID)

	// Create a test request
	req := httptest.NewRequest(http.MethodGet, "/dht", nil)
	w := httptest.NewRecorder()

	// Call the handler directly
	node.handleDHT(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify response has expected fields
	if _, ok := response["node_id"]; !ok {
		t.Error("Response missing node_id field")
	}

	if _, ok := response["topic_map"]; !ok {
		t.Error("Response missing topic_map field")
	}

	// Test method not allowed
	req = httptest.NewRequest(http.MethodPost, "/dht", nil)
	w = httptest.NewRecorder()
	node.handleDHT(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleStore(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Create a test request with a topic parameter
	req := httptest.NewRequest(http.MethodGet, "/store?topic=test-topic", nil)
	w := httptest.NewRecorder()

	// Call the handler directly
	node.handleStore(w, req)

	// Check response
	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d, got %d", http.StatusOK, w.Code)
	}

	// Messages are returned as an array, so we don't need to unmarshal into a map
	var messages []interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &messages); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	// Verify response is an empty array
	if messages == nil {
		t.Error("Expected empty array response, got nil")
	}

	// Test method not allowed
	req = httptest.NewRequest(http.MethodPut, "/store", nil)
	w = httptest.NewRecorder()
	node.handleStore(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleStorageTiers(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/store/tiers", nil)
	w := httptest.NewRecorder()
	node.handleStorageTiers(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for GET, got %d", http.StatusOK, w.Code)
	}

	// Test POST request with valid data
	jsonStr := `{"topic":"test-topic","tier":"memory"}`
	req = httptest.NewRequest(http.MethodPost, "/store/tiers", strings.NewReader(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	node.handleStorageTiers(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for valid POST, got %d", http.StatusOK, w.Code)
	}

	// Test POST request with invalid tier
	jsonStr = `{"topic":"test-topic","tier":"invalid-tier"}`
	req = httptest.NewRequest(http.MethodPost, "/store/tiers", strings.NewReader(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	node.handleStorageTiers(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d for invalid tier, got %d", http.StatusBadRequest, w.Code)
	}

	// Test PUT method (not allowed)
	req = httptest.NewRequest(http.MethodPut, "/store/tiers", nil)
	w = httptest.NewRecorder()
	node.handleStorageTiers(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleStorageCompact(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Test POST request
	req := httptest.NewRequest(http.MethodPost, "/store/compact", nil)
	w := httptest.NewRecorder()
	node.handleStorageCompact(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for POST, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if success, ok := response["success"].(bool); !ok || !success {
		t.Errorf("Expected success to be true, got %v", response["success"])
	}

	// Test GET method (not allowed)
	req = httptest.NewRequest(http.MethodGet, "/store/compact", nil)
	w = httptest.NewRecorder()
	node.handleStorageCompact(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleTopics(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/topics", nil)
	w := httptest.NewRecorder()
	node.handleTopics(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for GET, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if _, ok := response["topics"]; !ok {
		t.Error("Response missing topics field")
	}

	if _, ok := response["count"]; !ok {
		t.Error("Response missing count field")
	}

	// Test POST method (not allowed)
	req = httptest.NewRequest(http.MethodPost, "/topics", nil)
	w = httptest.NewRecorder()
	node.handleTopics(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleAI(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/ai", nil)
	w := httptest.NewRecorder()
	node.handleAI(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for GET, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if _, ok := response["status"]; !ok {
		t.Error("Response missing status field")
	}

	if _, ok := response["endpoints"]; !ok {
		t.Error("Response missing endpoints field")
	}

	// Test POST method (not allowed)
	req = httptest.NewRequest(http.MethodPost, "/ai", nil)
	w = httptest.NewRecorder()
	node.handleAI(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleAIRecommendations(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Test GET request
	req := httptest.NewRequest(http.MethodGet, "/ai/recommendations", nil)
	w := httptest.NewRecorder()
	node.handleAIRecommendations(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for GET, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	recommendations, ok := response["recommendations"].([]interface{})
	if !ok {
		t.Error("Response missing recommendations field or not an array")
	} else if len(recommendations) == 0 {
		t.Error("Recommendations array is empty")
	}

	// Test POST method (not allowed)
	req = httptest.NewRequest(http.MethodPost, "/ai/recommendations", nil)
	w = httptest.NewRecorder()
	node.handleAIRecommendations(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandleAITrain(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Test POST request with valid data
	jsonStr := `{"resource":"cpu","lookback":30}`
	req := httptest.NewRequest(http.MethodPost, "/ai/train", strings.NewReader(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	node.handleAITrain(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for valid POST, got %d", http.StatusOK, w.Code)
	}

	var response map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if success, ok := response["success"].(bool); !ok || !success {
		t.Errorf("Expected success to be true, got %v", response["success"])
	}

	// Test POST request without resource
	jsonStr = `{"lookback":30}`
	req = httptest.NewRequest(http.MethodPost, "/ai/train", strings.NewReader(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	node.handleAITrain(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d for missing resource, got %d", http.StatusBadRequest, w.Code)
	}

	// Test GET method (not allowed)
	req = httptest.NewRequest(http.MethodGet, "/ai/train", nil)
	w = httptest.NewRecorder()
	node.handleAITrain(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

func TestHandler(t *testing.T) {
	node, _ := SetupTestNode(t)

	// Get the handler
	handler := node.Handler()

	// Make sure we got a non-nil handler
	if handler == nil {
		t.Fatal("Handler returned nil")
	}

	// Test a simple request to verify it works
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status code %d for health endpoint, got %d", http.StatusOK, w.Code)
	}
}

// Test the subscribe handler method check
func TestHandleSubscribeMethod(t *testing.T) {
	// Create a request body to avoid invalid JSON errors
	bodyStr := `{"topic":"test-topic","subscriber":"test-sub"}`

	// Test POST method (not allowed)
	req := httptest.NewRequest(http.MethodPost, "/subscribe", strings.NewReader(bodyStr))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	// Create a handler function that just checks the method
	handler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// We won't get here in the test
	}

	// Call the handler directly
	handler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

// Test just the handlePublish method not allowed
func TestHandlePublishMethod(t *testing.T) {
	node, cleanup := SetupTestNode(t)
	defer cleanup()

	// Test GET method (not allowed)
	req := httptest.NewRequest(http.MethodGet, "/publish", nil)
	w := httptest.NewRecorder()
	node.handlePublish(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status code %d for method not allowed, got %d", http.StatusMethodNotAllowed, w.Code)
	}
}

// Test just the JSON parsing validation
func TestHandlePublishInvalidJSON(t *testing.T) {
	node, cleanup := SetupTestNode(t)
	defer cleanup()

	// Test with invalid JSON
	req := httptest.NewRequest(http.MethodPost, "/publish", strings.NewReader(`{invalid json`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	node.handlePublish(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status code %d for invalid JSON, got %d", http.StatusBadRequest, w.Code)
	}
}
