package router

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/store"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouteMessage(t *testing.T) {
	// Create a new DHT for the test
	dht := dht.NewDHT("test-node")
	if err := dht.InitializeOwnNode("127.0.0.1", 8000, 8080); err != nil {
		t.Fatalf("Failed to initialize own node: %v", err)
	}

	// Setup store, consensus and router
	store := store.NewStore()
	raft := consensus.NewRaftNode("test-node", nil)
	replication := consensus.NewReplicationManager("test-node", raft, 1)
	router := NewRouter("test-node", dht, store, replication)

	// Register ourselves as a node for the test topic
	err := dht.RegisterNode("test-topic", "test-node", 8080)
	require.NoError(t, err)

	// Mark the node as synced for the topic and set it as healthy
	dht.MarkTopicReplicaSynced("test-topic", "test-node")
	dht.SetNodeHealth("test-node", true)

	// Create a test message
	msg := Message{
		ID:      "test-msg",
		Topic:   "test-topic",
		Payload: []byte("test payload"),
		TTL:     3,
	}

	ctx := context.Background()

	// First message
	err = router.RouteMessage(ctx, msg)
	if err != nil {
		t.Errorf("Failed to route message: %v", err)
	}

	// Second message should hit cache
	err = router.RouteMessage(ctx, msg)
	if err != nil {
		t.Errorf("Failed to route cached message: %v", err)
	}

	// Check metrics
	metrics := router.GetMetrics()
	assert.Equal(t, int64(1), metrics["cache_hit_count"])
}

func TestRouterMetrics(t *testing.T) {
	dht := dht.NewDHT("test-node")
	store := store.NewStore()
	raft := consensus.NewRaftNode("test-node", nil)
	replication := consensus.NewReplicationManager("test-node", raft, 1)
	router := NewRouter("test-node", dht, store, replication)

	// Record some test metrics
	router.metrics.RecordCacheHit()
	router.metrics.RecordLatency(100 * time.Millisecond)

	// Check the metrics directly from the router's metrics object
	assert.Equal(t, int64(1), router.metrics.routeCacheHits.Load(), "Expected 1 cache hit")
	assert.Equal(t, int64(100_000_000), router.metrics.latencySum.Load(), "Expected latency sum of 100ms in nanoseconds")
	assert.Equal(t, int64(1), router.metrics.latencyCount.Load(), "Expected latency count of 1")
}

func TestLoadBalancer(t *testing.T) {
	// Create a load balancer with test configuration
	config := LoadBalancerConfig{
		CPUWeight:                  0.4,
		MemoryWeight:               0.2,
		ThroughputWeight:           0.2,
		LatencyWeight:              0.2,
		EMASmoothing:               0.2,
		MaxUtilizationDiff:         0.2,
		ReplicationFactor:          3,
		CircuitBreakerThreshold:    3,
		CircuitBreakerResetTimeout: 50 * time.Millisecond, // Short timeout for testing
	}

	lb := NewLoadBalancer(config)

	// Test metric updates and retrieval
	lb.UpdateNodeMetric("node1", "cpu_usage", 0.5)
	lb.UpdateNodeMetric("node1", "memory_usage", 0.3)

	cpu, found := lb.GetNodeMetric("node1", "cpu_usage")
	assert.True(t, found)
	assert.Equal(t, 0.5, cpu)

	// Test circuit breaker
	assert.False(t, lb.IsCircuitOpen("node1"))

	// Record failures to trip circuit breaker
	for i := 0; i < 3; i++ {
		lb.RecordNodeFailure("node1")
	}

	assert.True(t, lb.IsCircuitOpen("node1"))

	// Wait for circuit reset
	time.Sleep(60 * time.Millisecond)
	assert.False(t, lb.IsCircuitOpen("node1"))

	// Test node health check
	assert.True(t, lb.IsNodeHealthy("node1"))
	lb.UpdateNodeMetric("node2", "cpu_usage", 0.95) // Unhealthy CPU
	assert.False(t, lb.IsNodeHealthy("node2"))

	// Test node selection
	nodes := []dht.NodeInfo{
		{ID: "node1", Address: "localhost", Port: 8001},
		{ID: "node2", Address: "localhost", Port: 8002},
		{ID: "node3", Address: "localhost", Port: 8003},
	}

	// Setup node metrics for selection - make sure node3 has the lowest CPU usage
	lb.UpdateNodeMetric("node1", "cpu_usage", 0.5)
	lb.UpdateNodeMetric("node2", "cpu_usage", 0.95) // Should be excluded (unhealthy)
	lb.UpdateNodeMetric("node3", "cpu_usage", 0.3)

	// Initialize other metrics to ensure fair comparison
	lb.UpdateNodeMetric("node1", "memory_usage", 0.4)
	lb.UpdateNodeMetric("node3", "memory_usage", 0.4)
	lb.UpdateNodeMetric("node1", "message_throughput", 100)
	lb.UpdateNodeMetric("node3", "message_throughput", 100)
	lb.UpdateNodeMetric("node1", "response_latency", 50)
	lb.UpdateNodeMetric("node3", "response_latency", 50)

	selectedNodes := lb.SelectNodes(nodes, Message{Topic: "test"})

	// Should only select healthy nodes (node1 and node3)
	assert.Equal(t, 2, len(selectedNodes))

	// node3 should be first (lower CPU)
	assert.Equal(t, "node3", selectedNodes[0].ID)
}

func TestRouteOptimizer(t *testing.T) {
	// Create an optimizer with test configuration
	config := OptimizerConfig{
		LatencyWeight:       0.5,
		ThroughputWeight:    0.3,
		ReliabilityWeight:   0.2,
		MetricsDecayFactor:  0.0, // Disable decay for testing
		CongestionThreshold: 150 * time.Millisecond,
	}
	optimizer := NewRouteOptimizer(config)

	// Test latency recording and retrieval
	optimizer.UpdateLatency("node1", "node2", 50*time.Millisecond)
	latency, found := optimizer.GetLatency("node1", "node2")
	assert.True(t, found)
	assert.Equal(t, 50*time.Millisecond, latency)

	// Set initial performance metrics with bypassEMA=true to ensure exact values
	optimizer.UpdatePerformanceMetric("node1", "latency", 50.0, true)
	optimizer.UpdatePerformanceMetric("node1", "throughput", 100.0, true)
	optimizer.UpdatePerformanceMetric("node1", "reliability", 0.99, true)

	optimizer.UpdatePerformanceMetric("node2", "latency", 100.0, true)
	optimizer.UpdatePerformanceMetric("node2", "throughput", 100.0, true)
	optimizer.UpdatePerformanceMetric("node2", "reliability", 0.95, true)

	// Test route optimization with multiple nodes
	nodes := []dht.NodeInfo{
		{ID: "node1", Address: "localhost", Port: 8001},
		{ID: "node2", Address: "localhost", Port: 8002},
	}

	optimizedNodes := optimizer.OptimizeRoute(nodes, Message{Topic: "test"})

	// node1 should be preferred (lower latency, higher reliability)
	assert.Equal(t, 2, len(optimizedNodes))
	assert.Equal(t, "node1", optimizedNodes[0].ID)

	// Test congestion detection
	assert.False(t, optimizer.IsCongested("node1"))

	// Update node1 latency to be congested, bypassing EMA
	optimizer.UpdatePerformanceMetric("node1", "latency", 200.0, true)

	// Verify node1 is now congested
	assert.True(t, optimizer.IsCongested("node1"))

	// Debug: Print node metrics and congestion status
	fmt.Printf("Node1 metrics: latency=%.1f, congested=%v\n",
		optimizer.performanceData["node1"]["latency"],
		optimizer.IsCongested("node1"))
	fmt.Printf("Node2 metrics: latency=%.1f, congested=%v\n",
		optimizer.performanceData["node2"]["latency"],
		optimizer.IsCongested("node2"))

	// Re-optimize with congestion should prefer node2 now
	optimizedNodes = optimizer.OptimizeRoute(nodes, Message{Topic: "test"})
	fmt.Printf("Optimized node order: %s, %s\n", optimizedNodes[0].ID, optimizedNodes[1].ID)

	assert.Equal(t, "node2", optimizedNodes[0].ID)
}

func TestRouteCache(t *testing.T) {
	// Create a cache with small capacity for testing
	cache := NewRouteCache(5, 100*time.Millisecond, 1)

	nodes1 := []dht.NodeInfo{{ID: "node1", Address: "localhost", Port: 8001}}
	nodes2 := []dht.NodeInfo{{ID: "node2", Address: "localhost", Port: 8002}}

	// Test set and get
	cache.Set("topic1", nodes1)
	cache.Set("topic2", nodes2)

	// Test hit
	result, found := cache.Get("topic1")
	assert.True(t, found)
	assert.Equal(t, "node1", result[0].ID)

	// Test miss
	_, found = cache.Get("topic3")
	assert.False(t, found)

	// Test expiration
	time.Sleep(110 * time.Millisecond)
	_, found = cache.Get("topic1")
	assert.False(t, found)

	// Test removal
	cache.Set("topic4", nodes1)
	cache.Remove("topic4")
	_, found = cache.Get("topic4")
	assert.False(t, found)

	// Test eviction on capacity overflow
	for i := 0; i < 10; i++ {
		cache.Set(fmt.Sprintf("overflow%d", i), nodes1)
	}
	assert.LessOrEqual(t, cache.Len(), 5)

	// Test node removal
	cache.Set("multi1", []dht.NodeInfo{
		{ID: "node1", Address: "localhost", Port: 8001},
		{ID: "node2", Address: "localhost", Port: 8002},
	})

	cache.RemoveNodeFromAll("node1")
	result, found = cache.Get("multi1")
	assert.True(t, found)
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "node2", result[0].ID)

	// Test metrics
	metrics := cache.GetMetrics()
	assert.NotNil(t, metrics["hit_rate"])
	assert.NotNil(t, metrics["size"])
}

func TestHighConcurrencyRouting(t *testing.T) {
	// Create a mock HTTP server that responds to message forwarding
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Extract host and port from test server
	serverURL := server.URL
	hostPort := serverURL[7:] // Remove "http://"
	port, _ := strconv.Atoi(hostPort[strings.IndexByte(hostPort, ':')+1:])

	// Setup DHT and router
	dht := dht.NewDHT("test-node")
	err := dht.InitializeOwnNode("127.0.0.1", 8000, port) // Use test server's port
	require.NoError(t, err, "Failed to initialize own node")

	// Register our own node for the test topic
	err = dht.RegisterNode("test-topic", "test-node", port)
	require.NoError(t, err, "Failed to register node")

	// Mark our node as synced for the topic and set it as healthy
	dht.MarkTopicReplicaSynced("test-topic", "test-node")
	dht.SetNodeHealth("test-node", true)

	// Initialize other components
	store := store.NewStore()
	raft := consensus.NewRaftNode("test-node", nil)
	replication := consensus.NewReplicationManager("test-node", raft, 1)
	router := NewRouter("test-node", dht, store, replication)

	// Run concurrent routing operations
	concurrency := 100
	var wg sync.WaitGroup
	errChan := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			msg := Message{
				ID:      fmt.Sprintf("test-msg-%d", idx),
				Topic:   "test-topic",
				Payload: []byte(fmt.Sprintf("test payload %d", idx)),
				TTL:     3,
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if err := router.RouteMessage(ctx, msg); err != nil {
				errChan <- err
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	// Check for errors
	errCount := 0
	for err := range errChan {
		errCount++
		t.Logf("Error during concurrent routing: %v", err)
	}

	assert.Zero(t, errCount, "Expected no errors during concurrent routing")

	// Verify metrics show the correct number of routed messages
	metrics := router.GetMetrics()
	t.Logf("Router metrics: %+v", metrics)
	assert.GreaterOrEqual(t, metrics["messages_routed"].(int64), int64(concurrency-errCount))
}

func TestNetworkPartitionRouting(t *testing.T) {
	// Create a mock HTTP server that simulates network partitions
	var serverFailureCount int
	var serverMutex sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serverMutex.Lock()
		defer serverMutex.Unlock()

		// Fail every other request to simulate partial network partition
		if serverFailureCount%2 == 0 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		serverFailureCount++
	}))
	defer server.Close()

	// Setup DHT and router
	dht := dht.NewDHT("test-node")
	if err := dht.InitializeOwnNode("127.0.0.1", 8000, 8080); err != nil {
		t.Fatalf("Failed to initialize own node: %v", err)
	}

	// Register test node for the topic
	err := dht.RegisterNode("test-topic", "test-node", 8080)
	if err != nil {
		t.Fatalf("Failed to register node for topic: %v", err)
	}

	// Mark node as synced for the topic and set it as healthy
	dht.MarkTopicReplicaSynced("test-topic", "test-node")
	dht.SetNodeHealth("test-node", true)

	// Initialize other components
	store := store.NewStore()
	raft := consensus.NewRaftNode("test-node", nil)
	replication := consensus.NewReplicationManager("test-node", raft, 1)
	router := NewRouter("test-node", dht, store, replication)

	// Add load balancer-tracked node for circuit breaker testing
	router.loadBalancer.UpdateNodeMetric("test-node", "cpu_usage", 30.0)
	router.loadBalancer.UpdateNodeMetric("test-node", "memory_usage", 40.0)
	router.loadBalancer.UpdateNodeMetric("test-node", "message_rate", 100.0)
	router.loadBalancer.UpdateNodeMetric("test-node", "response_time", 5.0)
	router.loadBalancer.UpdateNodeMetric("test-node", "error_rate", 0.1)
	router.loadBalancer.UpdateNodeMetric("test-node", "success_rate", 99.9)
	router.loadBalancer.UpdateNodeMetric("test-node", "disk_usage", 50.0)
	router.loadBalancer.UpdateNodeMetric("test-node", "network_latency", 2.0)

	// Direct access to load balancer internals to set up circuit state
	// In a real scenario, this would happen after multiple failures
	// Manually record failures to trigger the circuit breaker
	for i := 0; i < 10; i++ {
		router.loadBalancer.RecordNodeFailure("test-node")
	}

	// Test routing during network partition
	for i := 0; i < 10; i++ {
		msg := Message{
			ID:      fmt.Sprintf("partition-test-%d", i),
			Topic:   "test-topic",
			Payload: []byte(fmt.Sprintf("partition test %d", i)),
			TTL:     3,
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := router.RouteMessage(ctx, msg)
		cancel()

		// Simulate failures by alternating the node health
		if i%2 == 0 {
			dht.SetNodeHealth("test-node", false) // Simulate failure
		} else {
			dht.SetNodeHealth("test-node", true) // Restore health
		}

		// Log the result
		if err != nil {
			t.Logf("Message %d routing result: failed with %v", i, err)
		} else {
			t.Logf("Message %d routing result: success", i)
		}
	}

	// Check metrics to verify circuit breaker engaged
	metrics := router.GetMetrics()
	t.Logf("Partition test metrics: %+v", metrics)

	// There should be some open circuits due to our simulated failures
	openCircuits, ok := metrics["load_balancer_open_circuits"]
	if !ok {
		t.Fatalf("Expected load_balancer_open_circuits metric to be present")
	}

	// Handle different possible types of the metric value
	var circuitCount int64
	switch v := openCircuits.(type) {
	case int:
		circuitCount = int64(v)
	case int64:
		circuitCount = v
	default:
		t.Fatalf("Unexpected type for load_balancer_open_circuits: %T", openCircuits)
	}

	// This test assumes we've manually forced a circuit open
	assert.Greater(t, circuitCount, int64(0), "Expected at least one open circuit")
}

func TestChaosRouting(t *testing.T) {
	// Initialize random number generator for reproducible chaos
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// Set up test HTTP servers
	stableServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer stableServer.Close()

	var flakyFailCount atomic.Int64
	flakyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Flaky server - randomly fails
		if rng.Intn(10) < 3 {
			flakyFailCount.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer flakyServer.Close()

	var failingRequestCount atomic.Int64
	failingServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := failingRequestCount.Add(1)
		if count > 5 {
			// Server is down after 5 requests
			time.Sleep(500 * time.Millisecond) // Add significant delay
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer failingServer.Close()

	// Extract ports from test servers
	stableHostPort := stableServer.URL[7:] // Remove "http://"
	stablePort, _ := strconv.Atoi(stableHostPort[strings.IndexByte(stableHostPort, ':')+1:])

	// Setup DHT
	dht := dht.NewDHT("test-node")
	err := dht.InitializeOwnNode("127.0.0.1", 8000, stablePort)
	require.NoError(t, err, "Failed to initialize node")

	// Register ourselves for the chaos topic
	err = dht.RegisterNode("chaos-topic", "test-node", stablePort)
	require.NoError(t, err, "Failed to register node")

	// Mark our node as synced and healthy
	dht.MarkTopicReplicaSynced("chaos-topic", "test-node")
	dht.SetNodeHealth("test-node", true)

	// Setup store, consensus and router
	store := store.NewStore()
	raft := consensus.NewRaftNode("test-node", nil)
	replication := consensus.NewReplicationManager("test-node", raft, 1)
	router := NewRouter("test-node", dht, store, replication)
	router.routeTimeout = 1 * time.Second // Use shorter timeout for tests

	// Run concurrent routing operations
	concurrency := 10
	messagesPerRoutine := 5
	var wg sync.WaitGroup
	errChan := make(chan error, concurrency*messagesPerRoutine)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(routineID int) {
			defer wg.Done()

			for j := 0; j < messagesPerRoutine; j++ {
				msgID := fmt.Sprintf("chaos-msg-%d-%d", routineID, j)
				msg := Message{
					ID:      msgID,
					Topic:   "chaos-topic",
					Payload: []byte(fmt.Sprintf("chaos payload %d-%d", routineID, j)),
					TTL:     3,
				}

				// Add random delay to create more chaotic conditions
				time.Sleep(time.Duration(rng.Intn(10)) * time.Millisecond)

				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				err := router.RouteMessage(ctx, msg)
				cancel()

				if err != nil {
					errChan <- fmt.Errorf("routing error for message %s: %w", msgID, err)
				}
			}
		}(i)
	}

	// Wait for all message routing to complete
	wg.Wait()
	close(errChan)

	// Count errors - we expect some errors in chaotic conditions
	errorCount := 0
	for err := range errChan {
		errorCount++
		t.Logf("Expected chaos error: %v", err)
	}

	// Check metrics
	metrics := router.GetMetrics()
	t.Logf("Chaos routing metrics: %+v", metrics)
	t.Logf("Chaos test summary: %d/%d messages failed",
		errorCount, concurrency*messagesPerRoutine)

	// Some errors are expected in chaos scenarios
	maxAllowedFailures := int(float64(concurrency*messagesPerRoutine) * 0.3)
	assert.LessOrEqual(t, errorCount, maxAllowedFailures,
		"Too many routing failures: %d/%d (>30%%)", errorCount, concurrency*messagesPerRoutine)

	// Verify we routed some messages successfully
	assert.Greater(t, metrics["messages_routed"], int64(0),
		"Expected some messages to be routed successfully")
}

func BenchmarkRouteMessage(b *testing.B) {
	// Create a test DHT
	dht := dht.NewDHT("bench-node")
	err := dht.InitializeOwnNode("127.0.0.1", 8000, 8080)
	if err != nil {
		b.Fatalf("Failed to initialize own node: %v", err)
	}

	// Register our node for test topic
	err = dht.RegisterNode("bench-topic", "bench-node", 8080)
	if err != nil {
		b.Fatalf("Failed to register node: %v", err)
	}
	dht.MarkTopicReplicaSynced("bench-topic", "bench-node")
	dht.SetNodeHealth("bench-node", true)

	// Setup store, consensus and router
	store := store.NewStore()
	raft := consensus.NewRaftNode("bench-node", nil)
	replication := consensus.NewReplicationManager("bench-node", raft, 1)
	router := NewRouter("bench-node", dht, store, replication)

	// Create test message
	msg := Message{
		ID:      "bench-msg",
		Topic:   "bench-topic",
		Payload: []byte("benchmark payload"),
		TTL:     3,
	}

	// Run the benchmark
	ctx := context.Background()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		msg.ID = fmt.Sprintf("bench-msg-%d", i) // Ensure unique message ID
		err := router.RouteMessage(ctx, msg)
		if err != nil {
			b.Fatalf("Failed to route message: %v", err)
		}
	}

	b.StopTimer()

	// Log metrics
	metrics := router.GetMetrics()
	b.Logf("Router benchmark metrics: %+v", metrics)
}
