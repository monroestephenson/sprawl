package node

import (
	"bytes"
	"encoding/json"
	"log"
	"sprawl/node/dht"
	"strconv"
	"sync"
	"time"

	"sprawl/node/utils"
)

// gossipDelegate is a memberlist.Delegate implementation
type gossipDelegate struct {
	mu       sync.RWMutex
	metadata map[string][]byte
	dht      *dht.DHT
	httpPort int // Store the HTTP port
}

// NewGossipDelegate creates a new delegate for gossip protocol
func NewGossipDelegate(dht *dht.DHT, httpPort int) *gossipDelegate {
	log.Printf("[DELEGATE] Creating new gossipDelegate with HTTP port: %d", httpPort)

	// Validate HTTP port - this is critical for proper node communication
	originalPort := httpPort
	if httpPort <= 0 {
		httpPort = 8080
		log.Printf("[DELEGATE] WARNING: Invalid HTTP port %d provided to NewGossipDelegate. Setting to default port %d",
			originalPort, httpPort)
	}

	// Create the delegate with the validated port
	delegate := &gossipDelegate{
		metadata: make(map[string][]byte),
		dht:      dht,
		httpPort: httpPort,
	}

	// Verify that our own node info in DHT has the right HTTP port
	nodeInfo := dht.GetNodeInfo()
	if nodeInfo.HTTPPort != httpPort {
		log.Printf("[DELEGATE] CRITICAL: DHT's own node info has mismatched HTTP port. DHT=%d, Delegate=%d, fixing...",
			nodeInfo.HTTPPort, httpPort)

		// Update our own node info with the correct HTTP port
		nodeInfo.HTTPPort = httpPort
		dht.AddNode(nodeInfo)

		// Verify the fix was applied
		updatedInfo := dht.GetNodeInfo()
		if updatedInfo.HTTPPort != httpPort {
			log.Printf("[DELEGATE] ERROR: Failed to update HTTP port in DHT! Original=%d, Expected=%d, Got=%d",
				originalPort, httpPort, updatedInfo.HTTPPort)
		} else {
			log.Printf("[DELEGATE] Successfully fixed HTTP port in DHT: %d", updatedInfo.HTTPPort)
		}
	} else {
		log.Printf("[DELEGATE] DHT node info has correct HTTP port: %d", nodeInfo.HTTPPort)
	}

	log.Printf("[DELEGATE] gossipDelegate created successfully with HTTP port %d", httpPort)
	return delegate
}

// NodeMeta returns metadata about a node
func (d *gossipDelegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Get node info to access the node ID
	nodeInfo := d.dht.GetNodeInfo()

	// FORCE a valid HTTP port - this is critical for proper node communication
	httpPort := 8080 // Start with a valid default

	// Only use the delegate's port if it's valid
	if d.httpPort > 0 {
		httpPort = d.httpPort
		log.Printf("[DELEGATE] Using valid delegate HTTP port: %d", httpPort)
	} else {
		log.Printf("[DELEGATE] CRITICAL: Delegate has invalid HTTP port %d, forcing to 8080", d.httpPort)
		// Fix the delegate's port for future calls
		d.httpPort = httpPort
	}

	// Create metadata with node info - ALWAYS include the HTTP port
	meta := map[string]interface{}{
		"id":            nodeInfo.ID,
		"http_port":     httpPort,               // Use the validated port
		"http_port_str": strconv.Itoa(httpPort), // String format for reliable parsing
		"address":       nodeInfo.Address,
		"port":          nodeInfo.Port,
		"timestamp":     time.Now().UnixNano(), // Add timestamp to help with debugging
	}

	// Log the HTTP port being used in metadata
	log.Printf("[DELEGATE] Creating NodeMeta with HTTP port: %d", httpPort)

	// Encode metadata to JSON
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	if err := enc.Encode(meta); err != nil {
		log.Printf("[DELEGATE] Failed to encode node metadata: %v", err)
		return nil
	}

	data := buf.Bytes()

	// Check size limit and truncate if necessary
	if len(data) > limit {
		log.Printf("[DELEGATE] Warning: Node metadata size (%d) exceeds limit (%d), truncating",
			len(data), limit)
		data = data[:limit]
	}

	// Log the actual metadata being sent
	log.Printf("[DELEGATE] Sending node metadata: %s", string(data))
	return data
}

func (d *gossipDelegate) NotifyMsg(msg []byte) {
	// Skip empty messages
	if len(msg) == 0 {
		return
	}

	// Log the raw message for debugging
	log.Printf("[Gossip] AGGRESSIVE: Received raw message: %s", string(msg))

	// Try first to decode as a GossipMetadata structure (preferred)
	var metadata GossipMetadata
	if err := json.Unmarshal(msg, &metadata); err == nil {
		// Successfully parsed as GossipMetadata
		log.Printf("[Gossip] AGGRESSIVE: Successfully parsed message as GossipMetadata from node %s",
			utils.TruncateID(metadata.NodeID))

		// Check if we have a node_info field with a valid HTTP port
		if metadata.NodeInfo.ID != "" {
			log.Printf("[Gossip] AGGRESSIVE: NodeInfo found in GossipMetadata: ID=%s, HTTPPort=%d",
				utils.TruncateID(metadata.NodeInfo.ID), metadata.NodeInfo.HTTPPort)

			// Fix invalid HTTP port before adding to DHT
			if metadata.NodeInfo.HTTPPort <= 0 {
				metadata.NodeInfo.HTTPPort = 8080
				log.Printf("[Gossip] AGGRESSIVE: Fixed invalid HTTP port to 8080 for node %s in GossipMetadata",
					utils.TruncateID(metadata.NodeInfo.ID))
			}

			// Add the node to the DHT
			log.Printf("[Gossip] AGGRESSIVE: Adding node from GossipMetadata: ID=%s, HTTPPort=%d",
				utils.TruncateID(metadata.NodeInfo.ID), metadata.NodeInfo.HTTPPort)
			d.dht.AddNode(metadata.NodeInfo)
		}

		// Process DHT topic map if present
		if len(metadata.DHTPeers) > 0 {
			log.Printf("[Gossip] AGGRESSIVE: Processing DHT peers from GossipMetadata")
			d.dht.MergeTopicMap(metadata.DHTPeers)
		}

		return // Successfully processed as GossipMetadata
	}

	// If we get here, the message wasn't a GossipMetadata structure
	// Try to decode as a generic map and process as before
	var data map[string]interface{}
	if err := json.Unmarshal(msg, &data); err != nil {
		log.Printf("[Gossip] AGGRESSIVE: Failed to parse message as JSON: %v", err)
		return
	}

	log.Printf("[Gossip] AGGRESSIVE: Message decoded as map with keys: %v", mapKeys(data))

	// Process node info if present
	if nodeInfoData, ok := data["node_info"].(map[string]interface{}); ok {
		log.Printf("[Gossip] AGGRESSIVE: Found node_info field in message, processing it")
		d.processNodeInfo(nodeInfoData)
	} else {
		// Try to see if this is a direct node info object (without being nested)
		if nodeID, hasID := data["id"].(string); hasID && nodeID != "" {
			log.Printf("[Gossip] AGGRESSIVE: Message appears to be a direct node info object, processing it")
			d.processNodeInfo(data)
		}
	}

	// Process DHT topic map if present
	if topicMap, ok := data["dht_peers"].(map[string]interface{}); ok {
		log.Printf("[Gossip] AGGRESSIVE: Found dht_peers field in message, processing it")
		d.processTopicMap(topicMap)
	}
}

func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func (d *gossipDelegate) processNodeInfo(nodeData map[string]interface{}) {
	// Extract node ID
	nodeID, ok := nodeData["id"].(string)
	if !ok || nodeID == "" {
		log.Printf("[Gossip] Received node info without valid ID")
		return
	}

	// Log the raw data for debugging
	dataBytes, _ := json.Marshal(nodeData)
	log.Printf("[Gossip] Processing node data for %s: %s", utils.TruncateID(nodeID), string(dataBytes))

	// Create node info structure
	nodeInfo := dht.NodeInfo{
		ID: nodeID,
	}

	// Extract HTTP port from node data or use default
	if httpPortVal, ok := nodeData["http_port"].(float64); ok && httpPortVal > 0 {
		nodeInfo.HTTPPort = int(httpPortVal)
		log.Printf("[Gossip] Using extracted HTTP port %d for node %s", nodeInfo.HTTPPort, utils.TruncateID(nodeID))
	} else {
		// Fallback to delegate's configured HTTP port
		nodeInfo.HTTPPort = d.httpPort
		log.Printf("[Gossip] Using delegate's HTTP port %d for node %s", nodeInfo.HTTPPort, utils.TruncateID(nodeID))
	}

	// Extract other fields for completeness
	if address, ok := nodeData["address"].(string); ok && address != "" {
		nodeInfo.Address = address
	} else {
		nodeInfo.Address = "127.0.0.1" // Default
	}

	if portVal, ok := nodeData["port"].(float64); ok && portVal > 0 {
		nodeInfo.Port = int(portVal)
	} else {
		nodeInfo.Port = 7946 // Default memberlist port
	}

	// Add the node to DHT with the correct HTTP port
	log.Printf("[Gossip] Adding node %s to DHT with HTTP port %d", utils.TruncateID(nodeID), nodeInfo.HTTPPort)
	d.dht.AddNode(nodeInfo)
}

// processTopicMap handles topic mapping information
func (d *gossipDelegate) processTopicMap(topicMap map[string]interface{}) {
	// Convert to the expected format for MergeTopicMap
	convertedMap := make(map[string][]string)
	for topic, nodesArr := range topicMap {
		if nodes, ok := nodesArr.([]interface{}); ok {
			nodeIDs := make([]string, 0, len(nodes))
			for _, n := range nodes {
				if nodeID, ok := n.(string); ok {
					nodeIDs = append(nodeIDs, nodeID)
				}
			}
			convertedMap[topic] = nodeIDs
		}
	}

	// Merge into DHT
	if len(convertedMap) > 0 {
		log.Printf("[Gossip] Merging topic map with %d entries", len(convertedMap))
		d.dht.MergeTopicMap(convertedMap)
	}
}

func (d *gossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// Return all stored metadata messages that need to be broadcast
	d.mu.RLock()
	defer d.mu.RUnlock()

	var broadcasts [][]byte
	for _, msg := range d.metadata {
		if len(msg) <= limit {
			broadcasts = append(broadcasts, msg)
		}
	}
	return broadcasts
}

func (d *gossipDelegate) LocalState(join bool) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Get our local DHT state to share with joining nodes
	nodeInfo := d.dht.GetNodeInfo()

	// Ensure HTTP port is valid - this is critical for proper node communication
	if nodeInfo.HTTPPort <= 0 {
		log.Printf("[Gossip] CRITICAL: Local node has invalid HTTP port %d, fixing to %d",
			nodeInfo.HTTPPort, d.httpPort)
		nodeInfo.HTTPPort = d.httpPort

		// Update the DHT with corrected information
		d.dht.AddNode(nodeInfo)
	}

	// Create a message with our DHT state
	state := map[string]interface{}{
		"node_info": map[string]interface{}{
			"id":        nodeInfo.ID,
			"address":   nodeInfo.Address,
			"port":      nodeInfo.Port,
			"http_port": nodeInfo.HTTPPort, // Make sure HTTP port is included
			"topics":    nodeInfo.Topics,
		},
		"topic_map": d.dht.GetTopicMap(),
		"sync_type": "full",
		"timestamp": time.Now().UnixNano(),
	}

	// Log what we're sending
	log.Printf("[Gossip] Sending local state with node %s (HTTP port: %d) and %d topic mappings",
		nodeInfo.ID[:8], nodeInfo.HTTPPort, len(d.dht.GetTopicMap()))

	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("[Gossip] Failed to marshal local state: %v", err)
		return nil
	}

	return data
}

func (d *gossipDelegate) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}

	// Use decoder to preserve number types
	dec := json.NewDecoder(bytes.NewReader(buf))
	dec.UseNumber()

	var remoteState map[string]interface{}
	if err := dec.Decode(&remoteState); err != nil {
		log.Printf("[Gossip] Failed to decode remote state: %v", err)
		return
	}

	log.Printf("[Gossip] Received remote state of %d bytes", len(buf))

	// Process node info from remote state
	if nodeData, ok := remoteState["node_info"].(map[string]interface{}); ok {
		d.processNodeInfo(nodeData)
	}

	// Process topic map from remote state
	if topicMap, ok := remoteState["topic_map"].(map[string]interface{}); ok {
		d.processTopicMap(topicMap)
	}

	log.Printf("[Gossip] Merged remote DHT state with %d topic mappings",
		len(remoteState["topic_map"].(map[string]interface{})))
}
