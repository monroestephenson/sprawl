package node

import (
	"testing"
	"time"

	"sprawl/ai"
	"sprawl/ai/prediction"
	"sprawl/store"

	"github.com/google/uuid"
)

// GossipManagerInterface defines the methods that a gossip manager must implement
type GossipManagerInterface interface {
	Start() error
	Join(peers []string) (int, error)
	Leave() error
	Shutdown() error
	GetNodeName() string
	GetMemberCount() int
	Members() []string
	GetMembers() []string
	JoinCluster(seeds []string) error
	SendReliable(msg []byte, to string) error
	SendBroadcast(msg []byte) error
}

// TestAIEngine is a mock implementation of AIEngine for testing
type TestAIEngine struct {
	startCalled        bool
	stopCalled         bool
	recordedMetrics    map[string]float64
	statusResponse     map[string]interface{}
	predictionResponse float64
	confidenceResponse float64
	anomalies          []map[string]interface{}
	thresholds         ai.ThresholdConfig
}

// Start implements AIEngine.Start
func (t *TestAIEngine) Start() {
	t.startCalled = true
}

// Stop implements AIEngine.Stop
func (t *TestAIEngine) Stop() {
	t.stopCalled = true
}

// RecordMetric implements AIEngine.RecordMetric
func (t *TestAIEngine) RecordMetric(metricKind ai.MetricKind, entityID string, value float64, labels map[string]string) {
	if t.recordedMetrics == nil {
		t.recordedMetrics = make(map[string]float64)
	}
	key := string(metricKind) + ":" + entityID
	t.recordedMetrics[key] = value
}

// GetStatus implements AIEngine.GetStatus
func (t *TestAIEngine) GetStatus() map[string]interface{} {
	if t.statusResponse == nil {
		return map[string]interface{}{
			"status":  "test",
			"enabled": true,
		}
	}
	return t.statusResponse
}

// GetPrediction implements AIEngine.GetPrediction
func (t *TestAIEngine) GetPrediction(resource string) (float64, float64) {
	return t.predictionResponse, t.confidenceResponse
}

// GetSimpleAnomalies implements AIEngine.GetSimpleAnomalies
func (t *TestAIEngine) GetSimpleAnomalies() []map[string]interface{} {
	if t.anomalies == nil {
		return []map[string]interface{}{
			{
				"timestamp": time.Now().Unix(),
				"metric":    "test_metric",
				"value":     42.0,
				"severity":  "medium",
			},
		}
	}
	return t.anomalies
}

// GetThresholds implements AIEngine.GetThresholds
func (t *TestAIEngine) GetThresholds() ai.ThresholdConfig {
	if t.thresholds.CPUScaleUpThreshold == 0 {
		// Return default thresholds if not set
		return ai.DefaultThresholds()
	}
	return t.thresholds
}

// SetThresholds implements AIEngine.SetThresholds
func (t *TestAIEngine) SetThresholds(config ai.ThresholdConfig) error {
	t.thresholds = config
	return nil
}

// TriggerConfigReload implements AIEngine.TriggerConfigReload
func (t *TestAIEngine) TriggerConfigReload() {
	// No-op for test implementation
}

// GetRecommendations returns test scaling recommendations
func (e *TestAIEngine) GetRecommendations() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"resource":        "cpu",
			"current_value":   70.5,
			"predicted_value": 85.2,
			"recommendation":  "scale_up",
			"confidence":      0.85,
			"timestamp":       time.Now().Format(time.RFC3339),
			"reason":          "High CPU utilization predicted in next 30 minutes",
		},
		{
			"resource":        "memory",
			"current_value":   50.3,
			"predicted_value": 48.7,
			"recommendation":  "maintain",
			"confidence":      0.92,
			"timestamp":       time.Now().Format(time.RFC3339),
			"reason":          "Memory utilization stable",
		},
	}
}

// TrainResourceModel mocks the training functionality for tests
func (e *TestAIEngine) TrainResourceModel(resource prediction.ResourceType, nodeID string, lookback time.Duration) error {
	// Just return nil for tests
	return nil
}

// TestHashTopic provides a consistent hash function for testing
func TestHashTopic(topic string) string {
	return "test-hash-" + topic
}

// MockGossipManager is a simplified GossipManager for testing
type MockGossipManager struct {
	nodeID   string
	httpPort int
}

// NewMockGossipManager creates a new mock gossip manager
func NewMockGossipManager(nodeID string) *MockGossipManager {
	return &MockGossipManager{
		nodeID:   nodeID,
		httpPort: 9999,
	}
}

// GetNodeName returns the node's name
func (m *MockGossipManager) GetNodeName() string {
	return m.nodeID
}

// GetMemberCount returns the number of members
func (m *MockGossipManager) GetMemberCount() int {
	return 1
}

// Shutdown stops the gossip manager
func (m *MockGossipManager) Shutdown() error {
	return nil
}

// Start method for the mock
func (m *MockGossipManager) Start() error {
	return nil
}

// Join method for the mock
func (m *MockGossipManager) Join(peers []string) (int, error) {
	return 1, nil
}

// Leave method for the mock
func (m *MockGossipManager) Leave() error {
	return nil
}

// Members method for the mock
func (m *MockGossipManager) Members() []string {
	return []string{m.nodeID}
}

// GetMembers for the GossipManagerInterface
func (m *MockGossipManager) GetMembers() []string {
	return []string{m.nodeID}
}

// JoinCluster for the GossipManagerInterface
func (m *MockGossipManager) JoinCluster(seeds []string) error {
	return nil
}

// SendReliable method for the mock
func (m *MockGossipManager) SendReliable(msg []byte, to string) error {
	return nil
}

// SendBroadcast method for the mock
func (m *MockGossipManager) SendBroadcast(msg []byte) error {
	return nil
}

// CreateSimpleNode creates a very basic node for testing API handlers
func CreateSimpleNode(t *testing.T) *Node {
	// Generate node ID
	nodeID := uuid.New().String()

	// Create test storage
	testStore := store.NewStore()

	// Create test AI engine
	testAI := &TestAIEngine{}

	// Create a node with minimal components - leave Gossip as nil
	node := &Node{
		ID:        nodeID,
		Store:     testStore,
		AI:        testAI,
		HTTPPort:  9999,
		semaphore: make(chan struct{}, 10),
		stopCh:    make(chan struct{}),
	}

	return node
}

// SetupTestNode creates a node with test configuration
func SetupTestNode(t *testing.T) (*Node, func()) {
	node := CreateSimpleNode(t)

	// Return the node and a cleanup function
	return node, func() {
		// Clean up resources if needed
	}
}
