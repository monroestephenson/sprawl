package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"sprawl/node/dht"
	"sprawl/node/utils"

	"github.com/hashicorp/memberlist"
	"github.com/shirou/gopsutil/v3/cpu"
)

// ProbeNodeFunc defines a function signature for probing a node
type ProbeNodeFunc func(nodeID string)

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
	nodeID    string

	// Message counting
	messageCountMu sync.RWMutex
	messageCount   int64

	// Health monitoring
	failureDetector *FailureDetector
	metricsManager  *MetricsManager

	// Membership management
	membershipMu sync.RWMutex
	members      map[string]*MemberInfo

	// Node state and leader election
	stateMu      sync.RWMutex
	stateHistory []StateChange
	leaderID     string

	// Probing
	directProbeMu sync.Mutex
	probeResults  map[string][]ProbeResult
	probeTimeout  time.Duration

	// Context for graceful shutdown
	ctx    context.Context
	cancel context.CancelFunc

	// Function to probe a node (used by failure detector)
	ProbeNodeFn ProbeNodeFunc
}

// MemberInfo stores information about a cluster member
type MemberInfo struct {
	NodeID          string
	Address         string
	Port            int
	HTTPPort        int
	State           string
	LastSeen        time.Time
	JoinTime        time.Time
	Tags            map[string]string
	Version         string
	ProtocolVersion uint8
	DelegateVersion uint8
	Metadata        map[string]interface{}
}

// StateChange records a node state transition
type StateChange struct {
	Timestamp time.Time
	OldState  string
	NewState  string
	Reason    string
}

// ProbeResult represents the outcome of a direct node probe
type ProbeResult struct {
	Success      bool
	ResponseTime time.Duration
	Error        string
	Timestamp    time.Time
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

	// Added fields for enhanced monitoring
	ClusterHealth         map[string]interface{} `json:"cluster_health"`
	SystemMetrics         map[string]interface{} `json:"system_metrics"`
	NodeState             string                 `json:"node_state"`
	FailedNodeDetections  []string               `json:"failed_node_detections"`
	SuspectNodeDetections []string               `json:"suspect_node_detections"`
	MembershipVersion     int64                  `json:"membership_version"`
	FeatureFlags          map[string]bool        `json:"feature_flags"`
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

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Create a new manager
	g := &GossipManager{
		dht:          dhtInstance,
		metadata:     make(map[string]interface{}),
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
		httpPort:     httpPort,
		hostname:     bindAddr,
		port:         bindPort,
		state:        "active",
		startTime:    time.Now(),
		isLeader:     false,
		bindAddr:     bindAddr,
		nodeID:       nodeID,
		members:      make(map[string]*MemberInfo),
		probeResults: make(map[string][]ProbeResult),
		probeTimeout: 5 * time.Second,
		stateHistory: make([]StateChange, 0, 100),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Initialize ProbeNodeFn to point to our probeNode method
	g.ProbeNodeFn = g.probeNode

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
		cancel() // Clean up context
		return nil, fmt.Errorf("failed to create memberlist: %v", err)
	}

	// Log success
	log.Printf("[Gossip] Successfully created memberlist with node ID %s and HTTP port %d",
		logID(nodeID), httpPort)

	// Initialize fields
	g.list = list
	g.config = config

	// Create and configure the failure detector
	g.failureDetector = NewFailureDetector(dhtInstance)
	g.failureDetector.SetGossipManager(g)

	// Create and configure the metrics manager
	g.metricsManager = NewMetricsManager(nodeID)

	// Record initial state
	g.recordStateChange("", "active", "Initial state")

	// Start subsystems
	g.failureDetector.Start()
	g.metricsManager.Start()

	// Start metadata broadcast
	go g.startMetadataBroadcast()

	// Start periodic health check
	go g.startHealthCheck()

	return g, nil
}

// startHealthCheck periodically monitors the health of the cluster
func (g *GossipManager) startHealthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-g.stopCh:
			return
		case <-ticker.C:
			g.performHealthCheck()
		}
	}
}

// performHealthCheck evaluates the cluster's health
func (g *GossipManager) performHealthCheck() {
	// Get list of all members
	members := g.list.Members()

	// Log current membership
	log.Printf("[Gossip] Health check: Cluster has %d members", len(members))

	// Check for isolated nodes (nodes that haven't sent updates recently)
	g.membershipMu.RLock()
	now := time.Now()
	for nodeID, member := range g.members {
		if now.Sub(member.LastSeen) > 60*time.Second {
			log.Printf("[Gossip] Health alert: Node %s hasn't been seen for %s",
				logID(nodeID), now.Sub(member.LastSeen))
		}
	}
	g.membershipMu.RUnlock()

	// Check for leader
	g.stateMu.RLock()
	hasLeader := g.leaderID != ""
	g.stateMu.RUnlock()

	if !hasLeader {
		log.Printf("[Gossip] Health alert: No leader elected in the cluster")
	}

	// Update metrics
	g.metricsManager.AddCustomMetric("cluster_size", len(members))
	g.metricsManager.AddCustomMetric("has_leader", hasLeader)
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

	// Get failure detector health data
	var suspectNodes, failedNodes []string
	if g.failureDetector != nil {
		suspectNodes = g.failureDetector.GetSuspectNodes()
		failedNodes = g.failureDetector.GetFailedNodes()
	}

	// Get metrics data
	var systemMetrics map[string]interface{}
	if g.metricsManager != nil {
		systemMetrics = g.metricsManager.GetMetricsAsMap()
	} else {
		systemMetrics = make(map[string]interface{})
	}

	// Get cluster health
	var clusterHealth map[string]interface{}
	if g.failureDetector != nil {
		clusterHealth = g.failureDetector.CheckClusterHealth()
	} else {
		clusterHealth = make(map[string]interface{})
	}

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

		// Enhanced monitoring fields
		ClusterHealth:         clusterHealth,
		SystemMetrics:         systemMetrics,
		NodeState:             g.state,
		SuspectNodeDetections: suspectNodes,
		FailedNodeDetections:  failedNodes,
		MembershipVersion:     time.Now().UnixNano(), // Simple version counter
		FeatureFlags:          map[string]bool{"enhanced_metrics": true, "failure_detection": true},
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

			// Record error for failure detection
			if g.failureDetector != nil {
				g.failureDetector.RecordError(node.Name)
			}
		} else {
			// Record heartbeat for failure detection
			if g.failureDetector != nil {
				g.failureDetector.RecordHeartbeat(node.Name)
			}
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

	// Get topic map and extract topics this node handles
	topicMap := g.dht.GetTopicMap()
	localTopics := make([]string, 0)
	localNodeID := g.nodeID

	for topic, nodes := range topicMap {
		for _, nodeID := range nodes {
			if nodeID == localNodeID {
				localTopics = append(localTopics, topic)
				break
			}
		}
	}

	return localTopics
}

// JoinCluster joins an existing cluster
func (g *GossipManager) JoinCluster(seeds []string) error {
	if len(seeds) == 0 {
		log.Printf("[Gossip] No seeds provided, starting a new cluster")
		return nil
	}

	log.Printf("[Gossip] Joining cluster with %d seed nodes", len(seeds))

	// Try to join the cluster
	n, err := g.list.Join(seeds)
	if err != nil {
		return fmt.Errorf("failed to join cluster: %v", err)
	}

	log.Printf("[Gossip] Successfully joined cluster with %d nodes", n)

	// Record state change
	g.recordStateChange("active", "joined", "Joined cluster")

	return nil
}

// GetMembers returns a list of member IDs
func (g *GossipManager) GetMembers() []string {
	members := g.list.Members()
	result := make([]string, len(members))

	for i, member := range members {
		result[i] = member.Name
	}

	return result
}

// GetMemberInfo returns detailed information about a specific member
func (g *GossipManager) GetMemberInfo(nodeID string) *MemberInfo {
	g.membershipMu.RLock()
	defer g.membershipMu.RUnlock()

	if info, exists := g.members[nodeID]; exists {
		// Return a copy to avoid race conditions
		infoCopy := *info
		return &infoCopy
	}

	return nil
}

// GetAllMemberInfo returns detailed information about all members
func (g *GossipManager) GetAllMemberInfo() map[string]*MemberInfo {
	g.membershipMu.RLock()
	defer g.membershipMu.RUnlock()

	// Create a copy to avoid race conditions
	result := make(map[string]*MemberInfo, len(g.members))
	for nodeID, info := range g.members {
		infoCopy := *info
		result[nodeID] = &infoCopy
	}

	return result
}

// Shutdown gracefully shuts down the gossip manager
func (g *GossipManager) Shutdown() {
	log.Printf("[Gossip] Shutting down gossip manager")

	// Cancel context to signal shutdown
	g.cancel()

	// Signal broadcast goroutine to stop
	close(g.stopCh)

	// Wait for broadcast to complete
	<-g.done

	// Stop failure detector
	if g.failureDetector != nil {
		g.failureDetector.Stop()
	}

	// Stop metrics manager
	if g.metricsManager != nil {
		g.metricsManager.Stop()
	}

	// Leave the memberlist
	err := g.list.Leave(time.Second * 5)
	if err != nil {
		log.Printf("[Gossip] Error leaving memberlist: %v", err)
	}

	// Shutdown memberlist
	err = g.list.Shutdown()
	if err != nil {
		log.Printf("[Gossip] Error shutting down memberlist: %v", err)
	}

	log.Printf("[Gossip] Gossip manager shutdown complete")
}

// NotifyJoin is called when a node joins the cluster
func (g *GossipManager) NotifyJoin(node *memberlist.Node) {
	log.Printf("[Gossip] Node %s joined the cluster", logID(node.Name))

	// Create or update member info
	g.membershipMu.Lock()
	g.members[node.Name] = &MemberInfo{
		NodeID:   node.Name,
		Address:  node.Addr.String(),
		Port:     int(node.Port),
		State:    "active",
		LastSeen: time.Now(),
		JoinTime: time.Now(),
	}
	g.membershipMu.Unlock()

	// Record heartbeat in failure detector
	if g.failureDetector != nil {
		g.failureDetector.RecordHeartbeat(node.Name)
	}

	// Update metrics
	if g.metricsManager != nil {
		g.metricsManager.AddCustomMetric("cluster_size", len(g.list.Members()))
	}
}

// NotifyLeave is called when a node leaves the cluster
func (g *GossipManager) NotifyLeave(node *memberlist.Node) {
	log.Printf("[Gossip] Node %s left the cluster", logID(node.Name))

	// Update member state
	g.membershipMu.Lock()
	if member, exists := g.members[node.Name]; exists {
		member.State = "left"
		member.LastSeen = time.Now()
	}
	g.membershipMu.Unlock()

	// Check if this was the leader
	g.stateMu.Lock()
	if g.leaderID == node.Name {
		log.Printf("[Gossip] Leader node %s left, triggering re-election", logID(node.Name))
		g.leaderID = ""
		go g.electLeader() // Trigger leader election in a goroutine
	}
	g.stateMu.Unlock()

	// This node has left gracefully, so we don't need to mark it as failed
	// but we should clean up any cached data

	// Remove the node from DHT
	if g.dht != nil {
		g.dht.RemoveNode(node.Name)
	}

	// Update metrics
	if g.metricsManager != nil {
		g.metricsManager.AddCustomMetric("cluster_size", len(g.list.Members()))
	}
}

// NotifyUpdate is called when a node is updated
func (g *GossipManager) NotifyUpdate(node *memberlist.Node) {
	log.Printf("[Gossip] Node %s updated", logID(node.Name))

	// Update last seen time
	g.membershipMu.Lock()
	if member, exists := g.members[node.Name]; exists {
		member.LastSeen = time.Now()
	} else {
		// We don't have this member yet, create it
		g.members[node.Name] = &MemberInfo{
			NodeID:   node.Name,
			Address:  node.Addr.String(),
			Port:     int(node.Port),
			State:    "active",
			LastSeen: time.Now(),
			JoinTime: time.Now(),
		}
	}
	g.membershipMu.Unlock()

	// Record heartbeat in failure detector
	if g.failureDetector != nil {
		g.failureDetector.RecordHeartbeat(node.Name)
	}
}

// getCPUUsage retrieves the current CPU usage
func (g *GossipManager) getCPUUsage() float64 {
	percent, err := cpu.Percent(time.Second, false) // Get overall CPU percentage
	if err != nil {
		log.Printf("[Gossip] Error getting CPU usage: %v", err)
		return 0
	}

	if len(percent) == 0 {
		return 0
	}

	return percent[0]
}

// getMemUsage retrieves current memory usage
func (g *GossipManager) getMemUsage() float64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.Alloc) / float64(m.Sys) * 100
}

// AddMessageCount increments the message counter
func (g *GossipManager) AddMessageCount(count int) {
	g.messageCountMu.Lock()
	defer g.messageCountMu.Unlock()
	g.messageCount += int64(count)

	// Update metrics manager as well
	if g.metricsManager != nil {
		g.metricsManager.AddMessageCount(count)
	}
}

// getMessageCount returns the current message count
func (g *GossipManager) getMessageCount() int {
	g.messageCountMu.RLock()
	defer g.messageCountMu.RUnlock()
	return int(g.messageCount)
}

// electLeader performs leader election
func (g *GossipManager) electLeader() {
	// Simple leader election: the node with the lowest ID becomes leader
	members := g.list.Members()
	if len(members) == 0 {
		log.Printf("[Gossip] No members in cluster for leader election")
		return
	}

	lowestID := members[0].Name
	for _, member := range members {
		if member.Name < lowestID {
			lowestID = member.Name
		}
	}

	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	g.leaderID = lowestID
	g.isLeader = (lowestID == g.nodeID)

	if g.isLeader {
		log.Printf("[Gossip] This node has been elected as leader")
		g.recordStateChange(g.state, "leader", "Elected as leader")
		g.state = "leader"
	} else {
		log.Printf("[Gossip] Node %s has been elected as leader", logID(lowestID))
	}
}

// recordStateChange adds a state change to history
func (g *GossipManager) recordStateChange(oldState, newState, reason string) {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()

	change := StateChange{
		Timestamp: time.Now(),
		OldState:  oldState,
		NewState:  newState,
		Reason:    reason,
	}

	g.stateHistory = append(g.stateHistory, change)

	// Keep history at a reasonable size
	maxHistorySize := 100
	if len(g.stateHistory) > maxHistorySize {
		g.stateHistory = g.stateHistory[len(g.stateHistory)-maxHistorySize:]
	}
}

// probeNode performs a direct health check on a specific node
func (g *GossipManager) probeNode(nodeID string) {
	g.directProbeMu.Lock()
	defer g.directProbeMu.Unlock()

	// Get node info
	nodeInfo := g.dht.GetNodeInfoByID(nodeID)
	if nodeInfo == nil {
		log.Printf("[Gossip] Cannot probe node %s: no info available", logID(nodeID))
		g.recordProbeResult(nodeID, ProbeResult{
			Success:   false,
			Error:     "No node info available",
			Timestamp: time.Now(),
		})
		return
	}

	// Build URL for health check
	url := fmt.Sprintf("http://%s:%d/health", nodeInfo.Address, nodeInfo.HTTPPort)

	// Create context with timeout
	ctx, cancel := context.WithTimeout(g.ctx, g.probeTimeout)
	defer cancel()

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[Gossip] Error creating probe request for node %s: %v", logID(nodeID), err)
		g.recordProbeResult(nodeID, ProbeResult{
			Success:   false,
			Error:     fmt.Sprintf("Request creation error: %v", err),
			Timestamp: time.Now(),
		})
		return
	}

	// Perform the probe
	startTime := time.Now()
	resp, err := http.DefaultClient.Do(req)
	responseTime := time.Since(startTime)

	if err != nil {
		log.Printf("[Gossip] Probe failed for node %s: %v", logID(nodeID), err)
		g.recordProbeResult(nodeID, ProbeResult{
			Success:      false,
			ResponseTime: responseTime,
			Error:        fmt.Sprintf("HTTP error: %v", err),
			Timestamp:    time.Now(),
		})

		// Record failure in failure detector
		if g.failureDetector != nil {
			g.failureDetector.RecordError(nodeID)
		}

		return
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[Gossip] Error reading probe response from node %s: %v", logID(nodeID), err)
		g.recordProbeResult(nodeID, ProbeResult{
			Success:      false,
			ResponseTime: responseTime,
			Error:        fmt.Sprintf("Response read error: %v", err),
			Timestamp:    time.Now(),
		})
		return
	}

	// Check response status
	if resp.StatusCode != http.StatusOK {
		log.Printf("[Gossip] Unhealthy response from node %s: %d %s",
			logID(nodeID), resp.StatusCode, string(body))
		g.recordProbeResult(nodeID, ProbeResult{
			Success:      false,
			ResponseTime: responseTime,
			Error:        fmt.Sprintf("Unhealthy status: %d", resp.StatusCode),
			Timestamp:    time.Now(),
		})

		// Record error in failure detector
		if g.failureDetector != nil {
			g.failureDetector.RecordError(nodeID)
		}

		return
	}

	// Successful probe
	log.Printf("[Gossip] Successful probe of node %s: %s (took %s)",
		logID(nodeID), string(body), responseTime)
	g.recordProbeResult(nodeID, ProbeResult{
		Success:      true,
		ResponseTime: responseTime,
		Timestamp:    time.Now(),
	})

	// Record heartbeat and response time
	if g.failureDetector != nil {
		g.failureDetector.RecordHeartbeat(nodeID)
		g.failureDetector.RecordResponseTime(nodeID, responseTime)
	}
}

// recordProbeResult stores the result of a node probe
func (g *GossipManager) recordProbeResult(nodeID string, result ProbeResult) {
	results, ok := g.probeResults[nodeID]
	if !ok {
		results = make([]ProbeResult, 0, 10)
	}

	// Add new result
	results = append(results, result)

	// Keep only the most recent 10 results
	if len(results) > 10 {
		results = results[len(results)-10:]
	}

	g.probeResults[nodeID] = results
}

// GetNodeState returns the current state of a node
func (g *GossipManager) GetNodeState(nodeID string) string {
	// First check our internal state
	g.membershipMu.RLock()
	if member, exists := g.members[nodeID]; exists {
		state := member.State
		g.membershipMu.RUnlock()
		return state
	}
	g.membershipMu.RUnlock()

	// If we don't have it in our member list, check failure detector
	if g.failureDetector != nil {
		state := g.failureDetector.GetNodeState(nodeID)
		switch state {
		case StateHealthy:
			return "healthy"
		case StateSuspect:
			return "suspect"
		case StateFailed:
			return "failed"
		}
	}

	// Default if we don't know
	return "unknown"
}

// ProbeAllNodes triggers health checks for all nodes
func (g *GossipManager) ProbeAllNodes() map[string]bool {
	members := g.list.Members()
	results := make(map[string]bool)

	for _, member := range members {
		if member.Name == g.nodeID {
			// Skip self
			results[member.Name] = true
			continue
		}

		// Probe in a separate goroutine to avoid blocking
		go g.probeNode(member.Name)

		// For immediate return, mark as pending
		results[member.Name] = true
	}

	return results
}

// GetMetricsManager returns the metrics manager
func (g *GossipManager) GetMetricsManager() *MetricsManager {
	return g.metricsManager
}

// GetStartTime returns the time when the gossip manager was started
func (g *GossipManager) GetStartTime() time.Time {
	return g.startTime
}
