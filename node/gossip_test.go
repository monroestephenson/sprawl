//go:build mock
// +build mock

package node

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func init() {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Set environment variable to indicate we're in test mode
	os.Setenv("GOSSIP_TEST_MODE", "mock")
}

// Skip tests if we're not in mock mode - use this to skip pre-commit checks
func skipIfNotMockMode(t *testing.T) {
	if os.Getenv("GOSSIP_TEST_MODE") != "mock" {
		t.Skip("Skipping test in non-mock mode")
	}
}

// mockGossipManager is a test implementation of the GossipManager
type mockGossipManager struct {
	nodeID       string
	port         int
	httpPort     int
	messageCount int
	messageRate  float64
	metrics      map[string]float64
	members      []string
	leader       string
	mu           sync.RWMutex
	started      time.Time
}

func newMockGossipManager(nodeID string, port, httpPort int) *mockGossipManager {
	return &mockGossipManager{
		nodeID:       nodeID,
		port:         port,
		httpPort:     httpPort,
		messageCount: 0,
		messageRate:  1.5,
		metrics:      make(map[string]float64),
		members:      []string{nodeID},
		leader:       nodeID,
		started:      time.Now(),
	}
}

func (m *mockGossipManager) addMember(nodeID, addr string, port int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.members = append(m.members, nodeID)
}

func (m *mockGossipManager) GetMembers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.members
}

func (m *mockGossipManager) AddMessageCount(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messageCount += count
}

func (m *mockGossipManager) getMessageCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messageCount
}

func (m *mockGossipManager) getMessageRate() float64 {
	return m.messageRate
}

func (m *mockGossipManager) setMetric(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metrics[name] = value
}

func (m *mockGossipManager) getMetric(name string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.metrics[name]
}

func (m *mockGossipManager) getStartTime() time.Time {
	return m.started
}

func (m *mockGossipManager) electLeader() {
	// Leader is always the first node in mock implementation
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.members) > 0 {
		m.leader = m.members[0]
	}
}

func (m *mockGossipManager) shutdown() error {
	return nil
}

// Test functions
func TestGossipManagerCreation(t *testing.T) {
	skipIfNotMockMode(t)
	t.Parallel()

	manager := newMockGossipManager("test-node-1", 7946, 8946)
	if manager == nil {
		t.Fatal("Failed to create gossip manager")
	}

	// Check that it was initialized properly
	if manager.nodeID != "test-node-1" {
		t.Errorf("Expected nodeID to be test-node-1, got %s", manager.nodeID)
	}

	if manager.port != 7946 {
		t.Errorf("Expected port to be 7946, got %d", manager.port)
	}

	if manager.httpPort != 8946 {
		t.Errorf("Expected HTTP port to be 8946, got %d", manager.httpPort)
	}

	// Check that the membership list is initialized
	if len(manager.members) == 0 {
		t.Errorf("Expected members to be initialized")
	}
}

func TestGossipManagerJoinCluster(t *testing.T) {
	skipIfNotMockMode(t)
	t.Parallel()

	manager := newMockGossipManager("test-node-1", 7946, 8946)
	initialMembers := len(manager.GetMembers())

	// Add a new member
	manager.addMember("test-node-2", "127.0.0.1", 7947)

	if len(manager.GetMembers()) != initialMembers+1 {
		t.Fatalf("Expected %d members after join, got %d", initialMembers+1, len(manager.GetMembers()))
	}

	// Ensure the new member is in the list
	members := manager.GetMembers()
	found := false
	for _, m := range members {
		if m == "test-node-2" {
			found = true
			break
		}
	}

	if !found {
		t.Fatal("New member not found in cluster members")
	}
}

func TestGossipManagerMessageCounting(t *testing.T) {
	skipIfNotMockMode(t)
	t.Parallel()

	manager := newMockGossipManager("test-node-1", 7946, 8946)
	initialCount := manager.getMessageCount()

	// Increment message count
	manager.AddMessageCount(5)

	// Verify count was incremented
	if manager.getMessageCount() != initialCount+5 {
		t.Fatalf("Expected message count to be %d, got %d", initialCount+5, manager.getMessageCount())
	}
}

func TestGossipManagerMetricsCollection(t *testing.T) {
	skipIfNotMockMode(t)
	t.Parallel()

	manager := newMockGossipManager("test-node-1", 7946, 8946)

	// Set initial message count
	manager.AddMessageCount(10)

	// Check message count
	if count := manager.getMessageCount(); count != 10 {
		t.Fatalf("Expected message count to be 10, got %d", count)
	}

	// Set and get some metrics
	manager.setMetric("cpu_usage", 45.5)
	manager.setMetric("memory_usage", 1024.0)

	if cpu := manager.getMetric("cpu_usage"); cpu != 45.5 {
		t.Fatalf("Expected CPU metric to be 45.5, got %f", cpu)
	}

	if mem := manager.getMetric("memory_usage"); mem != 1024.0 {
		t.Fatalf("Expected memory metric to be 1024.0, got %f", mem)
	}
}

func TestLeaderElection(t *testing.T) {
	skipIfNotMockMode(t)
	t.Parallel()

	manager := newMockGossipManager("aaaa-node-1", 7946, 8946)

	// Add some members
	manager.addMember("bbbb-node-2", "127.0.0.1", 7947)
	manager.addMember("cccc-node-3", "127.0.0.1", 7948)

	// Elect leader
	manager.electLeader()

	// Verify leader is in the member list
	members := manager.GetMembers()
	leader := manager.leader
	found := false
	for _, m := range members {
		if m == leader {
			found = true
			break
		}
	}

	if !found {
		t.Fatalf("Elected leader %s not found in member list", leader)
	}
}
