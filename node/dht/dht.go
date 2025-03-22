package dht

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"sprawl/node/utils"
)

// Version is the current DHT implementation version for logging
const Version = "2.0"

// NodeInfo represents a node in the DHT
type NodeInfo struct {
	ID       string   `json:"id"`
	Address  string   `json:"address"`
	Port     int      `json:"port"`
	HTTPPort int      `json:"http_port"`
	Topics   []string `json:"topics,omitempty"`
}

// MarshalJSON implements custom JSON marshaling to ensure HTTPPort is always valid
func (n NodeInfo) MarshalJSON() ([]byte, error) {
	// Create a shadow type to avoid infinite recursion
	type NodeInfoAlias NodeInfo

	// If HTTPPort is invalid, fix it
	httpPort := n.HTTPPort
	if httpPort <= 0 {
		httpPort = 8080
		log.Printf("[DHT] AGGRESSIVE FIX: Fixed invalid HTTP port %d to 8080 during JSON marshaling for node %s",
			n.HTTPPort, utils.TruncateID(n.ID))
	}

	// Use a modified struct with the fixed HTTP port - ALWAYS include both int and string version for redundancy
	return json.Marshal(&struct {
		NodeInfoAlias
		HTTPPort    int      `json:"http_port"`
		HTTPPortStr string   `json:"http_port_str"`
		HTTPPorts   []string `json:"http_ports_array,omitempty"` // Additional redundant field
	}{
		NodeInfoAlias: NodeInfoAlias(n),
		HTTPPort:      httpPort,
		HTTPPortStr:   strconv.Itoa(httpPort),
		HTTPPorts:     []string{strconv.Itoa(httpPort), "port:" + strconv.Itoa(httpPort)}, // Extra redundancy
	})
}

// UnmarshalJSON implements custom JSON unmarshaling to ensure HTTPPort is always valid
func (n *NodeInfo) UnmarshalJSON(data []byte) error {
	// Create a shadow type to avoid infinite recursion
	type NodeInfoAlias NodeInfo

	// Create a temporary struct that can hold http_port_str and other fields too
	aux := &struct {
		*NodeInfoAlias
		HTTPPort    *int     `json:"http_port"`
		HTTPPortStr string   `json:"http_port_str"`
		HTTPPorts   []string `json:"http_ports_array,omitempty"`
	}{
		NodeInfoAlias: (*NodeInfoAlias)(n),
	}

	// First, log the incoming data for debugging
	log.Printf("[DHT] Unmarshaling JSON data: %s", string(data))

	// Try to decode with standard JSON unmarshaler
	if err := json.Unmarshal(data, &aux); err != nil {
		log.Printf("[DHT] ERROR unmarshaling JSON: %v. FORCING HTTPPort to 8080", err)
		n.HTTPPort = 8080
		return nil // Don't fail, just use the default port
	}

	// Track where we got the port from for logging
	portSource := "default"

	// Check if we got a valid HTTP port from http_port_str first
	if aux.HTTPPortStr != "" {
		if port, err := strconv.Atoi(aux.HTTPPortStr); err == nil && port > 0 {
			n.HTTPPort = port
			portSource = "http_port_str"
		}
	}

	// If http_port_str didn't work, check the direct field
	if n.HTTPPort <= 0 && aux.HTTPPort != nil && *aux.HTTPPort > 0 {
		n.HTTPPort = *aux.HTTPPort
		portSource = "http_port field"
	}

	// If we still have no valid port, check the array
	if n.HTTPPort <= 0 && len(aux.HTTPPorts) > 0 {
		// Try to extract from the first element
		portStr := aux.HTTPPorts[0]
		// Remove any "port:" prefix if present
		portStr = strings.TrimPrefix(portStr, "port:")
		if port, err := strconv.Atoi(portStr); err == nil && port > 0 {
			n.HTTPPort = port
			portSource = "http_ports_array"
		}
	}

	// LAST RESORT: If we still have an invalid port, force it to 8080
	if n.HTTPPort <= 0 {
		n.HTTPPort = 8080
		portSource = "forced default"
		log.Printf("[DHT] AGGRESSIVE FIX: No valid HTTP port found, forcing to 8080 for node %s",
			utils.TruncateID(n.ID))
	}

	log.Printf("[DHT] Successfully unmarshaled node %s with HTTP port %d (source: %s)",
		utils.TruncateID(n.ID), n.HTTPPort, portSource)

	return nil
}

// DHT represents a Distributed Hash Table
type DHT struct {
	mu               sync.RWMutex
	nodeID           string
	nodes            map[string]NodeInfo // Map of node ID to node info
	topicMap         map[string][]string // Map of topic to node IDs
	fingerTable      map[string][]string // Kademlia-style finger table
	validateHTTPPort bool                // Flag to enforce HTTP port validation
}

// NewDHT creates a new DHT instance
func NewDHT(nodeID string) *DHT {
	// Check for environment variable to enable strict HTTP port validation
	validateHTTPPort := false
	if val := os.Getenv("SPRAWL_HTTP_PORT_VALIDATE"); val == "true" {
		validateHTTPPort = true
		log.Printf("[DHT] SPRAWL_HTTP_PORT_VALIDATE=true: Strict HTTP port validation enabled (version %s)", Version)
	}

	return &DHT{
		nodeID:           nodeID,
		nodes:            make(map[string]NodeInfo),
		topicMap:         make(map[string][]string),
		fingerTable:      make(map[string][]string),
		validateHTTPPort: validateHTTPPort,
	}
}

// HashTopic hashes a topic string to get a consistent ID
func (d *DHT) HashTopic(topic string) string {
	hash := sha256.Sum256([]byte(topic))
	return hex.EncodeToString(hash[:])
}

// AddNode adds a new node to the DHT
func (d *DHT) AddNode(info NodeInfo) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Debug the input
	log.Printf("[DHT] Processing AddNode call for node %s with original HTTPPort=%d",
		utils.TruncateID(info.ID), info.HTTPPort)

	// Set a valid port if HTTPPort is missing
	if info.HTTPPort <= 0 {
		info.HTTPPort = 8080
		log.Printf("[DHT] Node %s had invalid HTTP port, setting to default 8080",
			utils.TruncateID(info.ID))
	}

	// Set a valid port if missing
	if info.Port <= 0 {
		log.Printf("[DHT] Node %s had invalid port, setting to 7946",
			utils.TruncateID(info.ID))
		info.Port = 7946
	}

	// Update or add the node
	if existingNode, exists := d.nodes[info.ID]; exists {
		log.Printf("[DHT] Updating existing node %s: old HTTPPort=%d, new HTTPPort=%d",
			utils.TruncateID(info.ID), existingNode.HTTPPort, info.HTTPPort)
	} else {
		log.Printf("[DHT] Adding new node %s with HTTPPort=%d",
			utils.TruncateID(info.ID), info.HTTPPort)
	}

	// Store the node
	d.nodes[info.ID] = info

	// Verify the node was stored correctly
	if storedNode, ok := d.nodes[info.ID]; ok {
		if storedNode.HTTPPort != info.HTTPPort {
			log.Printf("[DHT] Error: Node %s was stored with HTTPPort=%d instead of %d!",
				utils.TruncateID(info.ID), storedNode.HTTPPort, info.HTTPPort)

			// Try one more time to fix it
			storedNode.HTTPPort = info.HTTPPort
			d.nodes[info.ID] = storedNode

			log.Printf("[DHT] Made second attempt to fix HTTPPort for node %s",
				utils.TruncateID(info.ID))
		} else {
			log.Printf("[DHT] Node %s was stored with correct HTTPPort=%d",
				utils.TruncateID(info.ID), info.HTTPPort)
		}
	}

	// Update finger table with our changes
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
	log.Printf("[DHT] Looking up nodes for topic %s (hash: %s)", topic, utils.TruncateID(hash))

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
					utils.TruncateID(nodeID), info.Address, info.HTTPPort, topic)
			} else {
				log.Printf("[DHT] Warning: Found nodeID %s in topic map but no node info", utils.TruncateID(nodeID))
			}
		}

		if len(nodeInfos) > 0 {
			log.Printf("[DHT] Returning %d nodes for topic %s", len(nodeInfos), topic)
			return nodeInfos
		}
		log.Printf("[DHT] Warning: Found topic mapping but no valid node info")
	}

	// FALLBACK: If no nodes are found for the topic, register the current node and return it
	log.Printf("[DHT] No registered nodes found for topic %s, using current node as fallback", topic)

	// Get current node's info
	if info, ok := d.nodes[d.nodeID]; ok {
		// Auto-register current node for this topic
		d.mu.RUnlock() // Unlock for read before write
		d.RegisterNode(topic, d.nodeID, info.HTTPPort)
		d.mu.RLock() // Lock for read again

		log.Printf("[DHT] Auto-registered current node %s for topic %s as fallback",
			utils.TruncateID(d.nodeID), topic)
		return []NodeInfo{info}
	}

	return []NodeInfo{} // Return empty list as last resort
}

// findClosestNodes finds the N closest nodes to a hash
func (d *DHT) findClosestNodes(hash string, n int) []NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

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
	// Already under write lock from caller

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

	// Validate HTTP port first
	if httpPort <= 0 {
		log.Printf("[DHT] Warning: Attempted to register node %s with invalid HTTP port %d",
			utils.TruncateID(nodeID), httpPort)
		// Use default port 8080 as fallback instead of returning
		httpPort = 8080
		log.Printf("[DHT] Using fallback HTTP port %d for node %s", httpPort, utils.TruncateID(nodeID))
	}

	hash := d.HashTopic(topic)
	log.Printf("[DHT] Registering node %s for topic %s (hash: %s) with HTTP port %d",
		utils.TruncateID(nodeID), topic, utils.TruncateID(hash), httpPort)

	// Update or create node info with complete information
	if existingNode, ok := d.nodes[nodeID]; ok {
		// Update existing node info
		existingNode.HTTPPort = httpPort
		if nodeID == d.nodeID {
			// Update our own node info
			existingNode.Address = "127.0.0.1" // Default for local testing
		}

		// Add topic to the Topics array if not present
		var hasTopicInArray bool
		for _, t := range existingNode.Topics {
			if t == topic {
				hasTopicInArray = true
				break
			}
		}
		if !hasTopicInArray {
			existingNode.Topics = append(existingNode.Topics, topic)
		}

		d.nodes[nodeID] = existingNode
		log.Printf("[DHT] Updated node info for %s (HTTP port: %d, topics: %v)",
			utils.TruncateID(nodeID), httpPort, existingNode.Topics)
	} else {
		// Create new node info
		nodeInfo := NodeInfo{
			ID:       nodeID,
			Address:  "127.0.0.1", // Default for local testing
			HTTPPort: httpPort,
			Topics:   []string{topic}, // Add the topic to the Topics array
		}
		d.nodes[nodeID] = nodeInfo
		log.Printf("[DHT] Created new node info for %s (HTTP port: %d, topics: %v)",
			utils.TruncateID(nodeID), httpPort, nodeInfo.Topics)
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
		log.Printf("[DHT] Registered node %s for topic %s (hash: %s)", utils.TruncateID(nodeID), topic, utils.TruncateID(hash))
		log.Printf("[DHT] Topic %s now has %d registered nodes", topic, len(nodes))

		// Log all nodes for this topic
		log.Printf("[DHT] Current nodes for topic %s:", topic)
		for _, n := range nodes {
			if info, ok := d.nodes[n]; ok {
				log.Printf("[DHT] - Node %s (HTTP port: %d)", utils.TruncateID(n), info.HTTPPort)
			}
		}
	}
}

// GetTopicMap returns a copy of the topic map
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

// MergeTopicMap merges a peer's topic map with the local one
func (d *DHT) MergeTopicMap(peerMap map[string][]string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	log.Printf("[DHT] Merging topic map with %d entries", len(peerMap))

	for topic, nodes := range peerMap {
		existing := d.topicMap[topic]
		merged := make([]string, 0)

		// Merge existing and new nodes
		nodeSet := make(map[string]bool)

		// Add existing nodes first, but only if they have valid info
		for _, node := range existing {
			if !nodeSet[node] {
				if info, ok := d.nodes[node]; ok && info.HTTPPort > 0 {
					nodeSet[node] = true
					merged = append(merged, node)
					log.Printf("[DHT] Keeping existing node %s with HTTP port %d for topic %s",
						utils.TruncateID(node), info.HTTPPort, topic)
				} else {
					log.Printf("[DHT] Removing existing node %s from topic %s: invalid HTTP port",
						utils.TruncateID(node), topic)
				}
			}
		}

		// Add new nodes, but only if they have valid info
		for _, node := range nodes {
			if !nodeSet[node] {
				if info, ok := d.nodes[node]; ok && info.HTTPPort > 0 {
					nodeSet[node] = true
					merged = append(merged, node)
					log.Printf("[DHT] Added new node %s with HTTP port %d to topic %s",
						utils.TruncateID(node), info.HTTPPort, topic)
				} else {
					log.Printf("[DHT] Skipping new node %s for topic %s: invalid HTTP port",
						utils.TruncateID(node), topic)
				}
			}
		}

		if len(merged) > 0 {
			d.topicMap[topic] = merged
			log.Printf("[DHT] Topic %s now has %d valid nodes after merge", topic, len(merged))
		} else {
			// If no valid nodes remain, remove the topic
			delete(d.topicMap, topic)
			log.Printf("[DHT] Removed topic %s as it has no valid nodes", topic)
		}
	}
}

// GetNodeInfo gets info about the local node
func (d *DHT) GetNodeInfo() NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Return a copy of the node info for the current node
	nodeID := d.nodeID
	if node, ok := d.nodes[nodeID]; ok {
		return node
	}

	// Return default info if we somehow don't have our own info
	return NodeInfo{
		ID:       d.nodeID,
		Address:  "127.0.0.1",
		Port:     7946,
		HTTPPort: 8080,
	}
}

// GetNodeInfoByID gets info about a specific node by ID
func (d *DHT) GetNodeInfoByID(nodeID string) *NodeInfo {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Return the node info for the specified node ID
	if node, ok := d.nodes[nodeID]; ok {
		nodeCopy := node
		return &nodeCopy
	}

	return nil
}

// InitializeOwnNode initializes this node's own information in the DHT
func (d *DHT) InitializeOwnNode(address string, port int, httpPort int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.nodeID == "" {
		log.Printf("[DHT %s] CRITICAL ERROR: Cannot initialize own node with empty ID!", Version)
		return
	}

	// Check HTTP port and correct if needed based on validation setting
	if httpPort <= 0 {
		originalPort := httpPort
		if d.validateHTTPPort {
			// With strict validation, force the port to 8080
			httpPort = 8080
			log.Printf("[DHT %s] STRICT VALIDATION: Cannot initialize own node with invalid HTTP port %d. Forcing to %d!",
				Version, originalPort, httpPort)
		} else {
			// Without strict validation, just log a warning
			log.Printf("[DHT] Warning: Initializing own node with invalid HTTP port %d", httpPort)
		}
	}

	// Create or update full node info
	info := NodeInfo{
		ID:       d.nodeID,
		Address:  address,
		Port:     port,
		HTTPPort: httpPort,
	}

	// Log initialization action
	if existingInfo, exists := d.nodes[d.nodeID]; exists {
		// Check if we're changing the HTTP port
		if existingInfo.HTTPPort != httpPort {
			log.Printf("[DHT %s] Changing own node HTTP port from %d to %d (validation=%v)",
				Version, existingInfo.HTTPPort, httpPort, d.validateHTTPPort)
		}

		log.Printf("[DHT %s] Updating own node %s: Address=%s, Port=%d, HTTPPort=%d",
			Version, utils.TruncateID(d.nodeID), address, port, httpPort)
	} else {
		log.Printf("[DHT %s] Initializing own node %s: Address=%s, Port=%d, HTTPPort=%d",
			Version, utils.TruncateID(d.nodeID), address, port, httpPort)
	}

	// Update the node information
	d.nodes[d.nodeID] = info

	// Verify the node was stored correctly
	if storedInfo, exists := d.nodes[d.nodeID]; exists {
		if storedInfo.HTTPPort != httpPort {
			log.Printf("[DHT %s] CRITICAL ERROR: Node info HTTP port (%d) doesn't match expected (%d) after storing!",
				Version, storedInfo.HTTPPort, httpPort)
		}
	} else {
		log.Printf("[DHT %s] CRITICAL ERROR: Failed to store node info for %s!",
			Version, utils.TruncateID(d.nodeID))
	}
}

// DumpNodeInfo logs all node information for debugging purposes
func (d *DHT) DumpNodeInfo() {
	d.mu.RLock()
	defer d.mu.RUnlock()

	log.Printf("[DHT] === Node Information Dump ===")
	log.Printf("[DHT] Own node ID: %s", utils.TruncateID(d.nodeID))
	log.Printf("[DHT] Total nodes in DHT: %d", len(d.nodes))

	for id, info := range d.nodes {
		log.Printf("[DHT] Node %s: Address=%s, Port=%d, HTTPPort=%d",
			utils.TruncateID(id), info.Address, info.Port, info.HTTPPort)
	}

	log.Printf("[DHT] === End of Node Information Dump ===")
}

// GetTopicMapByName returns a copy of the topic map with original topic names as keys
func (d *DHT) GetTopicMapByName() map[string][]string {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Create a reverse map from hash -> topic name
	topicByHash := make(map[string]string)

	// Make a copy with topic names as keys
	topicMapByName := make(map[string][]string)

	// First pass: build hash->topic mapping
	for hash, nodes := range d.topicMap {
		// Extract topic name from the hash through node registrations
		// For now, we'll use a loop through all topics registered by nodes
		for _, info := range d.nodes {
			for _, topic := range info.Topics {
				if d.HashTopic(topic) == hash {
					topicByHash[hash] = topic
					break
				}
			}
			if _, found := topicByHash[hash]; found {
				break
			}
		}

		// If we can't find the original topic name, use the hash
		if _, found := topicByHash[hash]; !found {
			topicByHash[hash] = hash
		}

		// Copy the nodes
		nodesCopy := make([]string, len(nodes))
		copy(nodesCopy, nodes)

		// Store with the topic name as key
		topicName := topicByHash[hash]
		topicMapByName[topicName] = nodesCopy
	}

	return topicMapByName
}
