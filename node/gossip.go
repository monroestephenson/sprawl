package node

import (
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"sprawl/node/dht"
	"sprawl/node/utils"

	"github.com/hashicorp/memberlist"
	"github.com/shirou/gopsutil/v3/cpu"
)

// GossipManager handles cluster membership and metadata gossip
type GossipManager struct {
	list      *memberlist.Memberlist
	config    *memberlist.Config
	dht       *dht.DHT
	metadata  map[string]interface{}
	stopCh    chan struct{}
	done      chan struct{}
	httpPort  int
	hostname  string
	port      int
	state     string
	startTime time.Time
	isLeader  bool
	bindAddr  string
}

// GossipMetadata represents the metadata gossiped between nodes
type GossipMetadata struct {
	NodeID     string              `json:"node_id"`
	TopicList  []string            `json:"topics_list"` // List of topics this node handles
	LoadStats  LoadStats           `json:"load_stats"`
	DHTPeers   map[string][]string `json:"dht_peers"` // topic -> []nodeID
	NodeInfo   dht.NodeInfo        `json:"node_info"` // Information about this node
	Timestamp  time.Time           `json:"timestamp"`
	Hostname   string              `json:"hostname"`
	Port       int                 `json:"port"`
	HTTPPort   int                 `json:"http_port"`
	State      string              `json:"state"`
	StartTime  int64               `json:"start_time"`
	Uptime     float64             `json:"uptime"`
	DHTNodes   int                 `json:"dht_nodes"`
	TopicCount int                 `json:"topic_count"` // Number of topics
	CPUUsage   float64             `json:"cpu_usage"`
	MemUsage   float64             `json:"memory_usage"`
	MsgCount   int                 `json:"msg_count"`
	LastSeen   int64               `json:"last_seen"`
	IsLeader   bool                `json:"is_leader"`
}

// LoadStats represents node load metrics
type LoadStats struct {
	CPUUsage    float64 `json:"cpu_usage"`
	MemoryUsage float64 `json:"memory_usage"`
	MsgCount    int64   `json:"msg_count"`
}

// Helper function to log a shortened node ID
func logID(id string) string {
	return utils.TruncateID(id)
}

// NewGossipManager creates a new gossip manager
func NewGossipManager(nodeID, bindAddr string, bindPort int, dhtInstance *dht.DHT, httpPort int) (*GossipManager, error) {
	// Validate the HTTP port
	if httpPort <= 0 {
		defaultPort := 8080
		log.Printf("[Gossip] CRITICAL: Invalid HTTP port %d provided to NewGossipManager, defaulting to %d",
			httpPort, defaultPort)
		httpPort = defaultPort
	}

	log.Printf("[Gossip] Creating new GossipManager for node %s with HTTP port %d",
		logID(nodeID), httpPort)

	// Create a new manager
	g := &GossipManager{
		dht:       dhtInstance,
		metadata:  make(map[string]interface{}),
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
		httpPort:  httpPort,
		hostname:  bindAddr,
		port:      bindPort,
		state:     "active",
		startTime: time.Now(),
		isLeader:  false,
		bindAddr:  bindAddr,
	}

	// Configure memberlist
	config := memberlist.DefaultLANConfig()
	config.Name = nodeID
	config.BindAddr = bindAddr
	config.BindPort = bindPort
	config.LogOutput = &logWriter{prefix: "[Memberlist] "}

	// Enhance metadata exchange reliability
	config.EnableCompression = true                // Enable compression for more efficient metadata transfer
	config.TCPTimeout = 5 * time.Second            // Increase TCP timeout for better reliability
	config.PushPullInterval = 15 * time.Second     // More frequent state exchange
	config.GossipInterval = 100 * time.Millisecond // More frequent gossiping
	config.ProbeTimeout = 2 * time.Second          // Shorter probe timeout
	config.ProbeInterval = 1 * time.Second         // More frequent probing

	// Improve failure detection
	config.SuspicionMult = 3            // Lower suspicion multiplier (default is 4)
	config.SuspicionMaxTimeoutMult = 4  // Lower max timeout multiplier (default is 6)
	config.TCPTimeout = 2 * time.Second // Shorter TCP timeout
	config.RetransmitMult = 2           // Lower retransmit multiplier
	config.IndirectChecks = 2           // Fewer indirect checks for faster failure detection

	// Use our custom delegate
	delegate := NewGossipDelegate(dhtInstance, httpPort)
	config.Delegate = delegate

	// Add events delegate to track membership changes
	config.Events = g // GossipManager implements NotifyJoin/Leave/Update

	// Create memberlist
	list, err := memberlist.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}

	// Log success
	log.Printf("[Gossip] Successfully created memberlist with node ID %s and HTTP port %d",
		logID(nodeID), httpPort)

	// Initialize fields
	g.list = list
	g.config = config

	// Start metadata broadcast
	go g.startMetadataBroadcast()

	return g, nil
}

// logWriter implements io.Writer for memberlist logging
type logWriter struct {
	prefix string
}

// Write satisfies the io.Writer interface
func (w *logWriter) Write(p []byte) (n int, err error) {
	log.Printf("%s%s", w.prefix, string(p))
	return len(p), nil
}

// startMetadataBroadcast periodically broadcasts node metadata
func (g *GossipManager) startMetadataBroadcast() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer close(g.done)

	for {
		select {
		case <-ticker.C:
			g.broadcastMetadata()
		case <-g.stopCh:
			return
		}
	}
}

// broadcastMetadata sends a metadata update to other nodes
func (g *GossipManager) broadcastMetadata() {
	// Get own node info from DHT
	nodeInfo := g.dht.GetNodeInfo()

	// Create metadata with our current topic map and node info
	metadata := GossipMetadata{
		NodeID:     g.list.LocalNode().Name,
		TopicList:  g.getLocalTopics(),
		LoadStats:  g.getLoadStats(),
		DHTPeers:   g.dht.GetTopicMap(),
		NodeInfo:   nodeInfo, // Use the node info with actual HTTP port
		Timestamp:  time.Now(),
		Hostname:   g.hostname,
		Port:       g.port,
		HTTPPort:   g.httpPort,
		State:      g.state,
		StartTime:  g.startTime.UnixNano(),
		Uptime:     time.Since(g.startTime).Seconds(),
		DHTNodes:   len(g.dht.GetTopicMap()), // Count topic maps as a proxy for node count
		TopicCount: len(g.dht.GetTopicMap()),
		CPUUsage:   g.getCPUUsage(),
		MemUsage:   g.getMemUsage(),
		MsgCount:   g.getMessageCount(),
		LastSeen:   time.Now().UnixNano(),
		IsLeader:   g.isLeader,
	}

	log.Printf("[Gossip] Broadcasting metadata with HTTP port %d",
		metadata.NodeInfo.HTTPPort)

	// Convert to JSON and broadcast as a gossip message
	data, err := json.Marshal(metadata)
	if err != nil {
		log.Printf("[Gossip] Failed to marshal metadata: %v", err)
		return
	}

	// Log what we're broadcasting
	log.Printf("[Gossip] Broadcasting metadata (%d bytes)", len(data))

	// Broadcast to all members
	for _, node := range g.list.Members() {
		if node.Name == g.list.LocalNode().Name {
			continue // Skip self
		}

		err := g.list.SendReliable(node, data)
		if err != nil {
			log.Printf("[Gossip] Failed to send metadata to %s: %v", logID(node.Name), err)
		}
	}
}

// getLoadStats collects system load statistics
func (g *GossipManager) getLoadStats() LoadStats {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)

	return LoadStats{
		CPUUsage:    g.getCPUUsage(),
		MemoryUsage: float64(stats.Alloc) / 1024 / 1024,
		MsgCount:    int64(g.getMessageCount()),
	}
}

// getLocalTopics retrieves topics this node is responsible for
func (g *GossipManager) getLocalTopics() []string {
	// If DHT is not available, return empty list
	if g.dht == nil {
		return []string{}
	}

	// Get the topic map
	topicMap := g.dht.GetTopicMap()
	if topicMap == nil {
		return []string{}
	}

	// Get the local node ID
	nodeID := g.list.LocalNode().Name

	// Find all topics that this node is responsible for
	var localTopics []string
	for topic, nodes := range topicMap {
		// Check if this node is in the list of nodes for this topic
		for _, node := range nodes {
			if node == nodeID {
				localTopics = append(localTopics, topic)
				break
			}
		}
	}

	return localTopics
}

// JoinCluster joins a cluster with the given seed nodes
func (g *GossipManager) JoinCluster(seeds []string) error {
	if len(seeds) == 0 {
		log.Printf("[Gossip] No seeds provided, not joining any cluster")
		return nil
	}

	log.Printf("[Gossip] Joining cluster with seeds: %v", seeds)

	// Convert string addresses to IP addresses
	addrs := make([]string, 0, len(seeds))
	addrs = append(addrs, seeds...)

	// Join the cluster
	n, err := g.list.Join(addrs)
	if err != nil {
		return err
	}

	log.Printf("[Gossip] Joined %d nodes", n)
	return nil
}

// GetMembers returns all members in the cluster
func (g *GossipManager) GetMembers() []string {
	members := g.list.Members()
	result := make([]string, 0, len(members))

	for _, m := range members {
		result = append(result, m.Name)
	}

	return result
}

// Shutdown stops the gossip manager
func (g *GossipManager) Shutdown() {
	log.Printf("[Gossip] Shutting down gossip manager")
	close(g.stopCh)
	<-g.done
	if err := g.list.Shutdown(); err != nil {
		log.Printf("[Gossip] Error shutting down memberlist: %v", err)
	}
}

// NotifyJoin is called when a node joins the cluster
func (g *GossipManager) NotifyJoin(node *memberlist.Node) {
	log.Printf("[Gossip] Node %s joined at %s", logID(node.Name), node.Addr)
}

// NotifyLeave is called when a node leaves the cluster
func (g *GossipManager) NotifyLeave(node *memberlist.Node) {
	log.Printf("[Gossip] Node %s left, removing from DHT and notifying all peers", logID(node.Name))

	// Remove the node from the DHT immediately
	g.dht.RemoveNode(node.Name)

	// Broadcast updated state to all remaining nodes immediately
	g.broadcastMetadata()

	// Force a membership update to all peers to ensure the change propagates
	go func() {
		// Broadcast multiple times to ensure delivery
		for i := 0; i < 3; i++ {
			g.broadcastMetadata()
			time.Sleep(500 * time.Millisecond)
		}
	}()
}

// NotifyUpdate is called when a node information is updated
func (g *GossipManager) NotifyUpdate(node *memberlist.Node) {
	log.Printf("[Gossip] Node %s updated", logID(node.Name))
}

// getCPUUsage returns the current CPU usage for this node
func (g *GossipManager) getCPUUsage() float64 {
	// Get CPU percentages using gopsutil
	percents, err := cpu.Percent(0, false)
	if err != nil {
		log.Printf("[Gossip] Error getting CPU usage: %v", err)
		return 0.0
	}

	if len(percents) > 0 {
		return percents[0]
	}

	return 0.0
}

// getMemUsage returns the current memory usage
func (g *GossipManager) getMemUsage() float64 {
	// Get memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// Calculate memory usage as percentage of total allocated memory
	memoryUsagePercent := float64(memStats.Alloc) / float64(memStats.Sys) * 100.0
	return memoryUsagePercent
}

// getMessageCount returns the total message count processed by this node
func (g *GossipManager) getMessageCount() int {
	// If dht is nil, return 0
	if g.dht == nil {
		return 0
	}

	// In a real implementation, we would track message counts
	// For now, return an estimate based on topic count
	topicMap := g.dht.GetTopicMap()
	if topicMap == nil {
		return 0
	}

	// Estimate 10 messages per topic
	return len(topicMap) * 10
}
