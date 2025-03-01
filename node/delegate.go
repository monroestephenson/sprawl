package node

import (
	"encoding/json"
	"log"
	"sprawl/node/dht"
	"sync"
	"time"
)

type gossipDelegate struct {
	mu       sync.RWMutex
	metadata map[string][]byte
	dht      *dht.DHT
}

func NewGossipDelegate(dht *dht.DHT) *gossipDelegate {
	return &gossipDelegate{
		metadata: make(map[string][]byte),
		dht:      dht,
	}
}

func (d *gossipDelegate) NodeMeta(limit int) []byte {
	// Return node metadata within size limit
	return []byte{}
}

func (d *gossipDelegate) NotifyMsg(msg []byte) {
	var meta GossipMetadata
	if err := json.Unmarshal(msg, &meta); err != nil {
		log.Printf("[Gossip] Failed to unmarshal metadata: %v", err)
		return
	}

	// Update DHT with peer information
	if meta.DHTPeers != nil {
		log.Printf("[Gossip] Received DHT update from node %s with %d topic mappings",
			meta.NodeID[:8], len(meta.DHTPeers))

		// Store the raw message before merging
		d.mu.Lock()
		d.metadata[meta.NodeID] = msg
		d.mu.Unlock()

		// Update node info first
		if meta.NodeInfo.ID != "" {
			log.Printf("[Gossip] Updating node info for %s (HTTP port: %d)",
				meta.NodeInfo.ID[:8], meta.NodeInfo.HTTPPort)
			d.dht.AddNode(meta.NodeInfo)
		}

		// Merge the DHT information
		d.dht.MergeTopicMap(meta.DHTPeers)

		// Log the successful merge
		log.Printf("[Gossip] Successfully merged DHT information from node %s", meta.NodeID[:8])
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
	// Return current DHT state for new nodes
	meta := GossipMetadata{
		DHTPeers:  d.dht.GetTopicMap(),
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("[Gossip] Failed to marshal local state: %v", err)
		return nil
	}

	return data
}

func (d *gossipDelegate) MergeRemoteState(buf []byte, join bool) {
	// Parse and merge remote state
	var meta GossipMetadata
	if err := json.Unmarshal(buf, &meta); err != nil {
		log.Printf("[Gossip] Failed to unmarshal remote state: %v", err)
		return
	}

	// Merge DHT information from remote state
	if meta.DHTPeers != nil {
		log.Printf("[Gossip] Merging remote DHT state with %d topic mappings", len(meta.DHTPeers))
		d.dht.MergeTopicMap(meta.DHTPeers)
	}
}
