package dht

import (
	"crypto/sha256"
	"math/big"
)

const (
	// K is the size of each k-bucket
	K = 20
	// Alpha is the number of parallel lookups
	Alpha = 3
	// IDLength is the length of node IDs in bytes
	IDLength = 32
)

// distance calculates the XOR distance between two hashes
func distance(a, b []byte) *big.Int {
	dist := new(big.Int)
	xor := make([]byte, len(a))
	for i := range a {
		xor[i] = a[i] ^ b[i]
	}
	dist.SetBytes(xor)
	return dist
}

// closestNodes returns the k closest nodes to a given hash
func (d *DHT) closestNodes(hash []byte, k int) []NodeInfo {
	distances := make(map[string]*big.Int)
	var nodes []NodeInfo

	d.mu.RLock()
	defer d.mu.RUnlock()

	// Calculate distances
	for id, node := range d.nodes {
		nodeHash := sha256.Sum256([]byte(id))
		dist := distance(hash, nodeHash[:])
		distances[id] = dist
		nodes = append(nodes, node)
	}

	// Sort by distance
	sortByDistance(nodes, distances)

	if len(nodes) > k {
		return nodes[:k]
	}
	return nodes
}

// sortByDistance sorts nodes by their distance to a target
func sortByDistance(nodes []NodeInfo, distances map[string]*big.Int) {
	for i := 0; i < len(nodes)-1; i++ {
		for j := i + 1; j < len(nodes); j++ {
			if distances[nodes[i].ID].Cmp(distances[nodes[j].ID]) > 0 {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}
}
