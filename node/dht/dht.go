package dht

import (
	"crypto/sha256"
	"encoding/hex"
	"log"
	"sync"
)

// NodeInfo represents information about a node in the DHT
type NodeInfo struct {
	ID       string
	Address  string
	Port     int
	HTTPPort int
	Topics   []string
}

// DHT manages the distributed hash table functionality
type DHT struct {
	mu          sync.RWMutex
	nodeID      string
	nodes       map[string]NodeInfo // Map of node ID to node info
	topicMap    map[string][]string // Map of topic to node IDs
	fingerTable map[string][]string // Kademlia-style finger table
}

// NewDHT creates a new DHT instance
func NewDHT(nodeID string) *DHT {
	return &DHT{
		nodeID:      nodeID,
		nodes:       make(map[string]NodeInfo),
		topicMap:    make(map[string][]string),
		fingerTable: make(map[string][]string),
	}
}

// HashTopic creates a consistent hash for a topic
func (d *DHT) HashTopic(topic string) string {
	hash := sha256.Sum256([]byte(topic))
	return hex.EncodeToString(hash[:])
}

// AddNode adds a new node to the DHT
func (d *DHT) AddNode(info NodeInfo) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Validate node info
	if info.HTTPPort <= 0 {
		log.Printf("[DHT] Warning: Attempted to add node %s with invalid HTTP port %d",
			info.ID[:8], info.HTTPPort)
		return
	}

	// Update existing node or add new one
	if existing, ok := d.nodes[info.ID]; ok {
		// Only update if new info has valid HTTP port
		if info.HTTPPort > 0 {
			existing.HTTPPort = info.HTTPPort
			existing.Address = info.Address
			d.nodes[info.ID] = existing
			log.Printf("[DHT] Updated node %s info (HTTP port: %d)", info.ID[:8], info.HTTPPort)
		}
	} else {
		d.nodes[info.ID] = info
		log.Printf("[DHT] Added new node %s (HTTP port: %d)", info.ID[:8], info.HTTPPort)
	}

	d.updateFingerTable()
}

// RemoveNode removes a node from the DHT
func (d *DHT) RemoveNode(nodeID string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.nodes, nodeID)
	d.updateFingerTable()

	// Redistribute topics from removed node
	for topic, nodes := range d.topicMap {
		newNodes := make([]string, 0)
		for _, id := range nodes {
			if id != nodeID {
				newNodes = append(newNodes, id)
			}
		}
		if len(newNodes) > 0 {
			d.topicMap[topic] = newNodes
		}
	}
}

// GetNodesForTopic returns the nodes responsible for a topic
func (d *DHT) GetNodesForTopic(topic string) []NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	hash := d.HashTopic(topic)
	log.Printf("[DHT] Looking up nodes for topic %s (hash: %s)", topic, hash[:8])

	// Log current DHT state
	log.Printf("[DHT] Current topic map has %d entries", len(d.topicMap))
	log.Printf("[DHT] Current node map has %d entries", len(d.nodes))

	if nodes, exists := d.topicMap[hash]; exists && len(nodes) > 0 {
		nodeInfos := make([]NodeInfo, 0, len(nodes))
		log.Printf("[DHT] Found %d registered nodes for topic %s", len(nodes), topic)

		for _, nodeID := range nodes {
			if info, ok := d.nodes[nodeID]; ok {
				nodeInfos = append(nodeInfos, info)
				log.Printf("[DHT] Including node %s (%s:%d) for topic %s",
					nodeID[:8], info.Address, info.HTTPPort, topic)
			} else {
				log.Printf("[DHT] Warning: Found nodeID %s in topic map but no node info", nodeID[:8])
			}
		}

		if len(nodeInfos) > 0 {
			log.Printf("[DHT] Returning %d nodes for topic %s", len(nodeInfos), topic)
			return nodeInfos
		}
		log.Printf("[DHT] Warning: Found topic mapping but no valid node info")
	}

	log.Printf("[DHT] No registered nodes found for topic %s", topic)
	return []NodeInfo{} // Return empty list instead of falling back to closest nodes
}

// findClosestNodes finds the N closest nodes to a hash
func (d *DHT) findClosestNodes(hash string, n int) []NodeInfo {
	// Simple implementation - in practice, you'd use XOR distance
	var nodes []NodeInfo
	for nodeID := range d.nodes {
		nodes = append(nodes, d.nodes[nodeID])
		if len(nodes) >= n {
			break
		}
	}
	return nodes
}

// updateFingerTable updates the Kademlia-style finger table
func (d *DHT) updateFingerTable() {
	// Simplified finger table update
	// In practice, you'd implement proper Kademlia finger table logic
	for nodeID := range d.nodes {
		d.fingerTable[nodeID] = make([]string, 0)
		for otherID := range d.nodes {
			if nodeID != otherID {
				d.fingerTable[nodeID] = append(d.fingerTable[nodeID], otherID)
			}
		}
	}
}

// ReassignTopic reassigns a topic to a new target node
func (d *DHT) ReassignTopic(topic string, targetNode string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hash := d.HashTopic(topic)

	// Create new mapping or update existing
	currentNodes := d.topicMap[hash]
	newNodes := make([]string, 0)

	// Add target node if not present
	hasTarget := false
	for _, node := range currentNodes {
		if node == targetNode {
			hasTarget = true
		}
		newNodes = append(newNodes, node)
	}

	if !hasTarget {
		newNodes = append(newNodes, targetNode)
	}

	d.topicMap[hash] = newNodes
}

// RegisterNode registers a node as responsible for a topic
func (d *DHT) RegisterNode(topic string, nodeID string, httpPort int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	hash := d.HashTopic(topic)
	log.Printf("[DHT] Registering node %s for topic %s (hash: %s) with HTTP port %d",
		nodeID[:8], topic, hash[:8], httpPort)

	// Update or create node info with complete information
	if existingNode, ok := d.nodes[nodeID]; ok {
		// Update existing node info
		existingNode.HTTPPort = httpPort
		if nodeID == d.nodeID {
			// Update our own node info
			existingNode.Address = "127.0.0.1" // Default for local testing
		}
		d.nodes[nodeID] = existingNode
		log.Printf("[DHT] Updated node info for %s (HTTP port: %d)", nodeID[:8], httpPort)
	} else {
		// Create new node info
		nodeInfo := NodeInfo{
			ID:       nodeID,
			Address:  "127.0.0.1", // Default for local testing
			HTTPPort: httpPort,
		}
		d.nodes[nodeID] = nodeInfo
		log.Printf("[DHT] Created new node info for %s (HTTP port: %d)", nodeID[:8], httpPort)
	}

	// Create or update the node list for this topic
	nodes := d.topicMap[hash]
	if nodes == nil {
		nodes = make([]string, 0)
	}

	// Add node if not already present
	found := false
	for _, n := range nodes {
		if n == nodeID {
			found = true
			break
		}
	}

	if !found {
		nodes = append(nodes, nodeID)
		d.topicMap[hash] = nodes
		log.Printf("[DHT] Registered node %s for topic %s (hash: %s)", nodeID[:8], topic, hash[:8])
		log.Printf("[DHT] Topic %s now has %d registered nodes", topic, len(nodes))

		// Log all nodes for this topic
		log.Printf("[DHT] Current nodes for topic %s:", topic)
		for _, n := range nodes {
			if info, ok := d.nodes[n]; ok {
				log.Printf("[DHT] - Node %s (HTTP port: %d)", n[:8], info.HTTPPort)
			}
		}
	}
}

func (d *DHT) GetTopicMap() map[string][]string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Make a copy to avoid concurrent access issues
	topicMapCopy := make(map[string][]string)
	for topic, nodes := range d.topicMap {
		nodesCopy := make([]string, len(nodes))
		copy(nodesCopy, nodes)
		topicMapCopy[topic] = nodesCopy
	}

	return topicMapCopy
}

func (d *DHT) MergeTopicMap(peerMap map[string][]string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	log.Printf("[DHT] Merging topic map with %d entries", len(peerMap))

	for topic, nodes := range peerMap {
		existing := d.topicMap[topic]
		merged := make([]string, 0)

		// Merge existing and new nodes
		nodeSet := make(map[string]bool)

		// Add existing nodes first
		for _, node := range existing {
			if !nodeSet[node] {
				// Only keep nodes that have valid info
				if info, ok := d.nodes[node]; ok && info.HTTPPort > 0 {
					nodeSet[node] = true
					merged = append(merged, node)
				}
			}
		}

		// Add new nodes
		for _, node := range nodes {
			if !nodeSet[node] {
				// Only add nodes that have valid info
				if info, ok := d.nodes[node]; ok && info.HTTPPort > 0 {
					nodeSet[node] = true
					merged = append(merged, node)
					log.Printf("[DHT] Added node %s with HTTP port %d to topic %s",
						node[:8], info.HTTPPort, topic)
				} else {
					log.Printf("[DHT] Skipping node %s for topic %s: missing or invalid HTTP port",
						node[:8], topic)
				}
			}
		}

		if len(merged) > 0 {
			d.topicMap[topic] = merged
			log.Printf("[DHT] Topic %s now has %d nodes after merge", topic, len(merged))

			// Log node info for debugging
			for _, nodeID := range merged {
				if info, ok := d.nodes[nodeID]; ok {
					log.Printf("[DHT] Node %s has HTTP port %d", nodeID[:8], info.HTTPPort)
				}
			}
		}
	}
}

// GetNodeInfo returns information about this node
func (d *DHT) GetNodeInfo() NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	if info, ok := d.nodes[d.nodeID]; ok {
		return info
	}

	// Return default info if not found
	return NodeInfo{
		ID:       d.nodeID,
		Address:  "127.0.0.1", // Default for local testing
		HTTPPort: 0,
	}
}
