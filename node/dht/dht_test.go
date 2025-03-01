package dht

import (
	"crypto/sha256"
	"testing"
)

func TestDistance(t *testing.T) {
	a := sha256.Sum256([]byte("node1"))
	b := sha256.Sum256([]byte("node2"))

	dist := distance(a[:], b[:])
	if dist.Sign() <= 0 {
		t.Error("Distance should be positive")
	}

	// Distance to self should be 0
	selfDist := distance(a[:], a[:])
	if selfDist.Sign() != 0 {
		t.Error("Distance to self should be 0")
	}
}

func TestClosestNodes(t *testing.T) {
	dht := NewDHT("test")

	// Add some test nodes
	nodes := []NodeInfo{
		{ID: "node1", Address: "127.0.0.1", Port: 8001},
		{ID: "node2", Address: "127.0.0.1", Port: 8002},
		{ID: "node3", Address: "127.0.0.1", Port: 8003},
	}

	for _, node := range nodes {
		dht.AddNode(node)
	}

	target := sha256.Sum256([]byte("test-target"))
	closest := dht.closestNodes(target[:], 2)

	if len(closest) != 2 {
		t.Errorf("Expected 2 closest nodes, got %d", len(closest))
	}
}
