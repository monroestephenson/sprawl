package node

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"sprawl/node/dht"

	"github.com/hashicorp/memberlist"
)

type GossipManager struct {
	list     *memberlist.Memberlist
	config   *memberlist.Config
	dht      *dht.DHT
	metadata map[string]interface{}
	stopCh   chan struct{}
	done     chan struct{}
}

// GossipMetadata represents the metadata shared between nodes
type GossipMetadata struct {
	NodeID    string              `json:"node_id"`
	Topics    []string            `json:"topics"`
	LoadStats LoadStats           `json:"load_stats"`
	DHTPeers  map[string][]string `json:"dht_peers"` // topic -> []nodeID
	NodeInfo  dht.NodeInfo        `json:"node_info"` // Information about this node
	Timestamp time.Time           `json:"timestamp"`
}

type LoadStats struct {
	CPUUsage    float64 `json:"cpu_usage"`
	MemoryUsage float64 `json:"memory_usage"`
	MsgCount    int64   `json:"msg_count"`
}

// NewGossipManager configures a new gossip manager
func NewGossipManager(nodeID, bindAddr string, bindPort int, dhtInstance *dht.DHT) (*GossipManager, error) {
	cfg := memberlist.DefaultLocalConfig()
	cfg.Name = nodeID
	cfg.BindAddr = bindAddr
	cfg.BindPort = bindPort

	// Add delegate for metadata exchange
	cfg.Delegate = NewGossipDelegate(dhtInstance)

	ml, err := memberlist.Create(cfg)
	if err != nil {
		return nil, err
	}

	gm := &GossipManager{
		list:     ml,
		config:   cfg,
		dht:      dhtInstance,
		metadata: make(map[string]interface{}),
		stopCh:   make(chan struct{}),
		done:     make(chan struct{}),
	}

	// Start periodic metadata broadcast
	go gm.startMetadataBroadcast()

	return gm, nil
}

// startMetadataBroadcast periodically broadcasts node metadata
func (g *GossipManager) startMetadataBroadcast() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer close(g.done)

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.broadcastMetadata()
		}
	}
}

// broadcastMetadata shares this node's metadata with the cluster
func (g *GossipManager) broadcastMetadata() {
	// Get current topic map from DHT
	topicMap := g.dht.GetTopicMap()

	meta := GossipMetadata{
		NodeID:    g.config.Name,
		Topics:    g.getLocalTopics(),
		LoadStats: g.getLoadStats(),
		DHTPeers:  topicMap,
		NodeInfo:  g.dht.GetNodeInfo(),
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(meta)
	if err != nil {
		log.Printf("[Gossip] Failed to marshal metadata: %v", err)
		return
	}

	// Log the DHT information being shared
	log.Printf("[Gossip] Broadcasting DHT info with %d topic mappings", len(topicMap))

	// Send to all members
	for _, node := range g.list.Members() {
		if node.Name != g.config.Name { // Don't send to self
			if err := g.list.SendReliable(node, data); err != nil {
				log.Printf("[Gossip] Failed to send metadata to %s: %v", node.Name, err)
			}
		}
	}
}

// getLoadStats collects current node metrics
func (g *GossipManager) getLoadStats() LoadStats {
	// In practice, implement real metrics collection
	return LoadStats{
		CPUUsage:    0.5, // Example values
		MemoryUsage: 0.4,
		MsgCount:    1000,
	}
}

// getLocalTopics returns topics this node is responsible for
func (g *GossipManager) getLocalTopics() []string {
	// Get all topics from DHT where this node is registered
	topicMap := g.dht.GetTopicMap()
	localTopics := make([]string, 0)

	for topic, nodes := range topicMap {
		for _, nodeID := range nodes {
			if nodeID == g.config.Name {
				localTopics = append(localTopics, topic)
				break
			}
		}
	}

	return localTopics
}

// JoinCluster tries to join the existing cluster via the given seed nodes
func (g *GossipManager) JoinCluster(seeds []string) error {
	if len(seeds) == 0 {
		log.Println("No seeds provided, running as initial node in cluster.")
		return nil
	}

	joined, err := g.list.Join(seeds)
	if err != nil {
		return fmt.Errorf("failed to join cluster: %w", err)
	}
	log.Printf("Joined %d nodes\n", joined)
	return nil
}

// GetMembers returns the current gossip cluster members
func (g *GossipManager) GetMembers() []string {
	members := g.list.Members()
	var names []string
	for _, m := range members {
		names = append(names, fmt.Sprintf("%s (%s:%d)", m.Name, m.Address(), m.Port))
	}
	return names
}

// Shutdown gracefully leaves the cluster
func (g *GossipManager) Shutdown() {
	close(g.stopCh)
	<-g.done // Wait for broadcast to stop
	g.list.Shutdown()
}
