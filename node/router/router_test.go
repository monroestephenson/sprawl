package router

import (
	"context"
	"testing"
	"time"

	"sprawl/node/consensus"
	"sprawl/node/dht"
	"sprawl/store"
)

func TestRouteMessage(t *testing.T) {
	dht := dht.NewDHT("test-node")
	store := store.NewStore()
	raft := consensus.NewRaftNode("test-node", nil)
	replication := consensus.NewReplicationManager("test-node", raft, 1)
	router := NewRouter("test-node", dht, store, replication)

	// Register ourselves as a node for the test topic
	dht.RegisterNode("test-topic", "test-node", 8080)

	msg := Message{
		ID:      "test-msg",
		Topic:   "test-topic",
		Payload: []byte("test payload"),
		TTL:     3,
	}

	ctx := context.Background()

	// First message
	err := router.RouteMessage(ctx, msg)
	if err != nil {
		t.Errorf("Failed to route message: %v", err)
	}

	// Second message should hit cache
	err = router.RouteMessage(ctx, msg)
	if err != nil {
		t.Errorf("Failed to route cached message: %v", err)
	}

	metrics := router.GetMetrics()
	cacheHits := metrics["route_cache_hits"].(int64)
	if cacheHits != 1 {
		t.Errorf("Expected 1 cache hit, got %d", cacheHits)
	}

	// Wait a bit to ensure metrics are updated
	time.Sleep(20 * time.Millisecond)

	metrics = router.GetMetrics()
	avgLatency := metrics["avg_latency_ms"].(int64)
	if avgLatency < 0 {
		t.Errorf("Expected non-negative average latency, got %d", avgLatency)
	}

	t.Logf("Average latency: %dms", avgLatency)
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
