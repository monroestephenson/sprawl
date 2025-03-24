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

	"github.com/hashicorp/memberlist"
)

// gossipDelegate is a memberlist.Delegate implementation
type gossipDelegate struct {
	mu       sync.RWMutex
	metadata map[string][]byte
	dht      *dht.DHT
	httpPort int // Store the HTTP port
}

// NewGossipDelegate creates a new gossip delegate with DHT integration
func NewGossipDelegate(dht *dht.DHT, httpPort int) *gossipDelegate {
	log.Printf("[Delegate] Creating new gossip delegate with HTTP port: %d", httpPort)

	// Use default HTTP port if none specified
	if httpPort <= 0 {
		httpPort = 8080
		log.Printf("[Delegate] Using default HTTP port: %d", httpPort)
	}

	delegate := &gossipDelegate{
		dht:      dht,
		httpPort: httpPort,
		mu:       sync.RWMutex{},
	}

	// Ensure our node information is consistent with the delegate's HTTP port
	nodeInfo := dht.GetNodeInfo()
	if nodeInfo != nil && nodeInfo.HTTPPort != httpPort {
		log.Printf("[Delegate] Updating node's HTTP port from %d to %d", nodeInfo.HTTPPort, httpPort)

		nodeInfo.HTTPPort = httpPort
		if err := dht.AddNode(nodeInfo); err != nil {
			log.Printf("[Delegate] Warning: Could not update node HTTP port: %v", err)
		}
	}

	return delegate
}

// NodeMeta returns metadata about a node
func (d *gossipDelegate) NodeMeta(limit int) []byte {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Get node info to access the node ID
	nodeInfo := d.dht.GetNodeInfo()
	if nodeInfo == nil {
		log.Printf("[Delegate] Warning: Could not retrieve node info")
		return []byte{}
	}

	// Prepare metadata with HTTP port information
	metadata := map[string]interface{}{
		"node_id":       nodeInfo.ID,
		"http_port":     d.httpPort,
		"http_port_str": strconv.Itoa(d.httpPort), // String format for reliable parsing
		"version":       "1.0",
	}

	data, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("[Delegate] Error marshaling node metadata: %v", err)
		return []byte{}
	}

	if len(data) > limit {
		log.Printf("[Delegate] Warning: Metadata size %d exceeds limit %d", len(data), limit)
		return data[:limit]
	}

	return data
}

// NotifyMsg is invoked when a user-data message is received
func (d *gossipDelegate) NotifyMsg(msg []byte) {
	if len(msg) == 0 {
		return
	}

	// First, try to parse as GossipMetadata
	var metadata GossipMetadata
	if err := json.Unmarshal(msg, &metadata); err == nil && metadata.NodeInfo.ID != "" {
		// Process NodeInfo
		if metadata.NodeInfo.ID != "" {
			log.Printf("[Delegate] Received metadata from node %s", utils.TruncateID(metadata.NodeInfo.ID))

			// Ensure HTTP port is valid
			if metadata.NodeInfo.HTTPPort <= 0 {
				metadata.NodeInfo.HTTPPort = 8080
				log.Printf("[Delegate] Using default HTTP port 8080 for node %s", utils.TruncateID(metadata.NodeInfo.ID))
			}

			// Add node to DHT
			if err := d.dht.AddNode(&metadata.NodeInfo); err != nil {
				log.Printf("[Delegate] Error adding node to DHT: %v", err)
			}
		}

		// Process DHT topic map if present
		if len(metadata.DHTPeers) > 0 {
			log.Printf("[Delegate] Processing topic map with %d topics", len(metadata.DHTPeers))
			d.dht.MergeTopicMap(metadata.DHTPeers)
		}

		return
	}

	// Fall back to generic JSON processing for compatibility
	var data map[string]interface{}
	if err := json.Unmarshal(msg, &data); err != nil {
		log.Printf("[Delegate] Received non-JSON message, ignoring")
		return
	}

	// Process node info if present
	if nodeInfoData, ok := data["node_info"].(map[string]interface{}); ok {
		d.processNodeInfo(nodeInfoData)
	} else if nodeID, hasID := data["id"].(string); hasID && nodeID != "" {
		// Try to see if this is a direct node info object
		d.processNodeInfo(data)
	}

	// Process DHT topic map if present
	if topicMap, ok := data["dht_peers"].(map[string]interface{}); ok {
		d.processTopicMap(topicMap)
	}
}

// mapKeys returns a slice of all keys in a map
//
//nolint:unused // Kept for debugging and future use
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
		log.Printf("[Delegate] Received node info without valid ID")
		return
	}

	// Extract other node data with sensible defaults
	address, _ := nodeData["address"].(string)
	if address == "" {
		address = "127.0.0.1" // Default to localhost if missing
	}

	// Extract ports with validation
	port := 7946 // Default memberlist port
	if portVal, ok := nodeData["port"].(float64); ok && portVal > 0 {
		port = int(portVal)
	}

	// Extract HTTP port, use different sources with fallbacks
	httpPort := d.httpPort // Default to our port as a reasonable guess

	if httpPortVal, ok := nodeData["http_port"].(float64); ok && httpPortVal > 0 {
		httpPort = int(httpPortVal)
	} else if httpPortStr, ok := nodeData["http_port_str"].(string); ok {
		if parsedPort, err := strconv.Atoi(httpPortStr); err == nil && parsedPort > 0 {
			httpPort = parsedPort
		}
	}

	// Create node info
	nodeInfo := dht.NodeInfo{
		ID:       nodeID,
		Address:  address,
		Port:     port,
		HTTPPort: httpPort,
	}

	// Add node to DHT
	if err := d.dht.AddNode(&nodeInfo); err != nil {
		log.Printf("[Delegate] Error adding node %s: %v", utils.TruncateID(nodeID), err)
		return
	}

	log.Printf("[Delegate] Added node %s with HTTP port %d", utils.TruncateID(nodeID), httpPort)
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
		d.dht.MergeTopicMap(convertedMap)
	}
}

func (d *gossipDelegate) GetBroadcasts(overhead, limit int) [][]byte {
	// Return all stored metadata messages that need to be broadcast
	d.mu.RLock()
	defer d.mu.RUnlock()

	if len(d.metadata) == 0 {
		return nil
	}

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
	if nodeInfo == nil {
		log.Printf("[Delegate] Warning: Unable to get node info for local state")
		return nil
	}

	// Ensure HTTP port is valid
	if nodeInfo.HTTPPort <= 0 {
		nodeInfo.HTTPPort = d.httpPort
		if err := d.dht.AddNode(nodeInfo); err != nil {
			log.Printf("[Delegate] Error updating node info: %v", err)
		}
	}

	// Create a message with our DHT state
	state := map[string]interface{}{
		"node_info": map[string]interface{}{
			"id":        nodeInfo.ID,
			"address":   nodeInfo.Address,
			"port":      nodeInfo.Port,
			"http_port": nodeInfo.HTTPPort,
			"topics":    nodeInfo.Topics,
		},
		"topic_map": d.dht.GetTopicMap(),
		"sync_type": "full",
		"timestamp": time.Now().UnixNano(),
	}

	data, err := json.Marshal(state)
	if err != nil {
		log.Printf("[Delegate] Failed to marshal local state: %v", err)
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
		log.Printf("[Delegate] Failed to decode remote state: %v", err)
		return
	}

	// Process node info from remote state
	if nodeData, ok := remoteState["node_info"].(map[string]interface{}); ok {
		d.processNodeInfo(nodeData)
	}

	// Process topic map from remote state
	if topicMap, ok := remoteState["topic_map"].(map[string]interface{}); ok {
		d.processTopicMap(topicMap)
	}
}

// NotifyJoin is invoked when a node joins the cluster
func (d *gossipDelegate) NotifyJoin(node *memberlist.Node) {
	log.Printf("[Delegate] Node joined: %s", node.Name)
}

// NotifyLeave is invoked when a node leaves the cluster
func (d *gossipDelegate) NotifyLeave(node *memberlist.Node) {
	log.Printf("[Delegate] Node left: %s", node.Name)
}

// NotifyUpdate is invoked when a node's metadata changes
func (d *gossipDelegate) NotifyUpdate(node *memberlist.Node) {
	log.Printf("[Delegate] Node updated: %s", node.Name)
}
