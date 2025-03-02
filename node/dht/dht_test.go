package dht

import (
	"crypto/sha256"
	"encoding/hex"
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

	// Add some test nodes with valid HTTP ports
	nodes := []NodeInfo{
		{ID: "node1", Address: "127.0.0.1", Port: 8001, HTTPPort: 8081},
		{ID: "node2", Address: "127.0.0.1", Port: 8002, HTTPPort: 8082},
		{ID: "node3", Address: "127.0.0.1", Port: 8003, HTTPPort: 8083},
	}

	for _, node := range nodes {
		dht.AddNode(node)
	}

	target := sha256.Sum256([]byte("test-target"))
	targetStr := hex.EncodeToString(target[:])
	closest := dht.findClosestNodes(targetStr, 2)

	if len(closest) != 2 {
		t.Errorf("Expected 2 closest nodes, got %d", len(closest))
	}
}
