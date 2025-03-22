package node

import (
	"testing"

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
	SendReliable(msg []byte, to string) error
	SendBroadcast(msg []byte) error
}

// TestAIEngine is a simplified AI Engine for testing purposes
type TestAIEngine struct {
}

// Start starts the test AI engine
func (e *TestAIEngine) Start() {
	// Do nothing for tests
}

// Stop stops the test AI engine
func (e *TestAIEngine) Stop() {
	// Do nothing for tests
}

// GetStatus returns the current status of the test AI engine
func (e *TestAIEngine) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"enabled": true,
		"models":  []string{"test-model"},
	}
}

// GetPrediction returns a prediction for the specified resource
func (e *TestAIEngine) GetPrediction(resource string) (float64, float64) {
	return 5.0, 0.95
}

// GetSimpleAnomalies returns currently detected anomalies
func (e *TestAIEngine) GetSimpleAnomalies() []map[string]interface{} {
	return []map[string]interface{}{
		{
			"resource":   "cpu",
			"timestamp":  "2022-01-01T12:00:00Z",
			"value":      95.0,
			"confidence": 0.9,
		},
	}
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

	// Create a node with minimal components
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
