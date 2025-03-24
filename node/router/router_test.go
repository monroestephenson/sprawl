package router

import (
	"context"
	"testing"
	"time"

	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/store"

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
	if router.metrics.routeCacheHits.Load() != 1 {
		t.Errorf("Expected 1 cache hit, got %d", router.metrics.routeCacheHits.Load())
	}

	// Log average latency
	t.Logf("Average latency: %dms", router.metrics.GetAverageLatency()/time.Millisecond)
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

	metrics := router.GetMetrics()

	if metrics["route_cache_hits"].(int64) != 1 {
		t.Error("Expected 1 cache hit")
	}

	if metrics["avg_latency_ms"].(int64) != 100 {
		t.Errorf("Expected 100ms latency, got %d", metrics["avg_latency_ms"])
	}
}
