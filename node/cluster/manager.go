// Package cluster provides node discovery and cluster management
package cluster

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"sprawl/node/client"
	"sprawl/node/dht"
)

// ClusterManager handles node discovery and manages connections to other nodes
type ClusterManager struct {
	mu            sync.RWMutex
	nodeID        string
	nodes         map[string]*NodeInfo
	clientCache   map[string]*client.NodeClient
	dht           *dht.DHT
	refreshTicker *time.Ticker
	stopCh        chan struct{}
}

// NodeInfo represents information about a node in the cluster
type NodeInfo struct {
	ID            string    `json:"id"`
	Hostname      string    `json:"hostname"`
	Port          int       `json:"port"`
	HTTPPort      int       `json:"http_port"`
	LastSeen      time.Time `json:"last_seen"`
	IsActive      bool      `json:"is_active"`
	FailureCount  int       `json:"failure_count"`
	Topics        []string  `json:"topics"`
	IsReachable   bool      `json:"is_reachable"`
	IsCoordinator bool      `json:"is_coordinator"`

	// Health check data
	LastHealthCheck     time.Time          `json:"last_health_check"`
	HealthStatus        string             `json:"health_status"` // "healthy", "degraded", "unhealthy"
	HealthCheckFailures int                `json:"health_check_failures"`
	ResourceUtilization map[string]float64 `json:"resource_utilization"`
	Capabilities        []string           `json:"capabilities"`
	Tags                map[string]string  `json:"tags"`
	LastError           string             `json:"last_error,omitempty"`
}

// NewClusterManager creates a new cluster manager
func NewClusterManager(nodeID string, dhtInstance *dht.DHT) *ClusterManager {
	if dhtInstance == nil {
		log.Printf("[WARN] Creating ClusterManager with nil DHT instance")
	}

	cm := &ClusterManager{
		nodeID:      nodeID,
		nodes:       make(map[string]*NodeInfo),
		clientCache: make(map[string]*client.NodeClient),
		dht:         dhtInstance,
		stopCh:      make(chan struct{}),
	}

	// Start periodic node discovery with an initial refresh
	cm.refreshNodeList() // Initial refresh

	// Set shorter refresh interval for development/testing
	refreshInterval := 30 * time.Second
	if os.Getenv("CLUSTER_REFRESH_INTERVAL") != "" {
		if i, err := strconv.Atoi(os.Getenv("CLUSTER_REFRESH_INTERVAL")); err == nil {
			refreshInterval = time.Duration(i) * time.Second
			log.Printf("[ClusterManager] Using custom refresh interval: %v", refreshInterval)
		}
	}

	cm.refreshTicker = time.NewTicker(refreshInterval)
	go cm.periodicRefresh()

	// Start health check loop with a different interval
	go cm.healthCheckLoop(1 * time.Minute)

	log.Printf("[ClusterManager] Initialized with node ID %s", nodeID)
	return cm
}

// periodicRefresh regularly updates the node list from gossip and DHT
func (cm *ClusterManager) periodicRefresh() {
	for {
		select {
		case <-cm.refreshTicker.C:
			cm.refreshNodeList()
		case <-cm.stopCh:
			cm.refreshTicker.Stop()
			return
		}
	}
}

// refreshNodeList updates the list of nodes from DHT
func (cm *ClusterManager) refreshNodeList() {
	// Get all nodes from DHT
	allNodes := cm.dht.GetAllNodes()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Track which nodes were updated
	updated := make(map[string]bool)

	// Update existing nodes and add new ones
	for _, dhtNode := range allNodes {
		updated[dhtNode.ID] = true

		// Skip self
		if dhtNode.ID == cm.nodeID {
			continue
		}

		// Update or add node
		if node, exists := cm.nodes[dhtNode.ID]; exists {
			// Update existing node
			node.Hostname = dhtNode.Address
			node.Port = dhtNode.Port
			node.HTTPPort = dhtNode.HTTPPort
			node.LastSeen = time.Now()
		} else {
			// Add new node
			cm.nodes[dhtNode.ID] = &NodeInfo{
				ID:            dhtNode.ID,
				Hostname:      dhtNode.Address,
				Port:          dhtNode.Port,
				HTTPPort:      dhtNode.HTTPPort,
				LastSeen:      time.Now(),
				IsActive:      true,
				FailureCount:  0,
				IsReachable:   true, // Assume reachable until proven otherwise
				IsCoordinator: false,
			}

			log.Printf("[Cluster] Discovered new node: %s (%s:%d)", dhtNode.ID[:8], dhtNode.Address, dhtNode.HTTPPort)
		}
	}

	// Remove nodes that are no longer in DHT
	for nodeID, node := range cm.nodes {
		if !updated[nodeID] {
			// If not seen for a while, mark as inactive
			if time.Since(node.LastSeen) > 5*time.Minute {
				delete(cm.nodes, nodeID)
				delete(cm.clientCache, nodeID)
				log.Printf("[Cluster] Removed inactive node: %s", nodeID[:8])
			}
		}
	}

	// Verify reachability of nodes
	go cm.verifyNodeReachability()
}

// verifyNodeReachability checks which nodes are actually reachable
func (cm *ClusterManager) verifyNodeReachability() {
	cm.mu.RLock()
	nodesToCheck := make([]*NodeInfo, 0, len(cm.nodes))
	for _, node := range cm.nodes {
		nodesToCheck = append(nodesToCheck, node)
	}
	cm.mu.RUnlock()

	// Check each node in parallel
	var wg sync.WaitGroup
	for _, node := range nodesToCheck {
		wg.Add(1)
		go func(n *NodeInfo) {
			defer wg.Done()
			cm.pingNode(n)
		}(node)
	}

	wg.Wait()
}

// pingNode checks if a node is reachable
func (cm *ClusterManager) pingNode(node *NodeInfo) {
	nodeClient, err := cm.getClient(node.ID)
	if err != nil {
		cm.mu.Lock()
		node.IsReachable = false
		node.FailureCount++
		cm.mu.Unlock()
		return
	}

	// Try to ping the node
	err = nodeClient.Ping()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	if err != nil {
		node.IsReachable = false
		node.FailureCount++
		if node.FailureCount > 5 {
			log.Printf("[Cluster] Node %s unreachable (%d failures)", node.ID[:8], node.FailureCount)
		}
	} else {
		if !node.IsReachable {
			log.Printf("[Cluster] Node %s is reachable again", node.ID[:8])
		}
		node.IsReachable = true
		node.FailureCount = 0
	}
}

// healthCheckLoop periodically checks the health of all nodes
func (cm *ClusterManager) healthCheckLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			cm.checkNodesHealth()
		case <-cm.stopCh:
			return
		}
	}
}

// checkNodesHealth performs health checks on all known nodes
func (cm *ClusterManager) checkNodesHealth() {
	log.Printf("[ClusterManager] Running health check on %d nodes", len(cm.nodes))

	cm.mu.RLock()
	nodeIDs := make([]string, 0, len(cm.nodes))
	for id := range cm.nodes {
		nodeIDs = append(nodeIDs, id)
	}
	cm.mu.RUnlock()

	var healthyNodes, degradedNodes, unhealthyNodes int

	for _, id := range nodeIDs {
		if id == cm.nodeID {
			continue // Skip self
		}

		status, err := cm.checkNodeHealth(id)
		if err != nil {
			log.Printf("[ClusterManager] Health check failed for node %s: %v", id, err)
		}

		switch status {
		case "healthy":
			healthyNodes++
		case "degraded":
			degradedNodes++
		case "unhealthy":
			unhealthyNodes++
		}
	}

	log.Printf("[ClusterManager] Health check complete: %d healthy, %d degraded, %d unhealthy nodes",
		healthyNodes, degradedNodes, unhealthyNodes)
}

// checkNodeHealth checks a single node's health and updates its status
func (cm *ClusterManager) checkNodeHealth(nodeID string) (string, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	node, exists := cm.nodes[nodeID]
	if !exists {
		return "", fmt.Errorf("node %s not found", nodeID)
	}

	// Get node client with error handling and retry
	nodeClient, err := cm.getClient(nodeID)
	if err != nil {
		node.HealthStatus = "unhealthy"
		node.HealthCheckFailures++
		node.LastError = err.Error()
		node.LastHealthCheck = time.Now()
		return node.HealthStatus, err
	}

	// Ping the node
	err = nodeClient.Ping()
	if err != nil {
		node.HealthStatus = "unhealthy"
		node.HealthCheckFailures++
		node.LastError = err.Error()
		node.IsReachable = false
		node.LastHealthCheck = time.Now()
		return node.HealthStatus, err
	}

	// Try to get node status
	status, err := nodeClient.GetNodeStatus()
	if err != nil {
		node.HealthStatus = "degraded"
		node.LastError = fmt.Sprintf("Ping succeeded but status check failed: %v", err)
		node.LastHealthCheck = time.Now()
		return node.HealthStatus, nil
	}

	// Update node information from status
	node.IsReachable = true
	node.LastSeen = time.Now()
	node.LastHealthCheck = time.Now()
	node.HealthStatus = "healthy"
	node.HealthCheckFailures = 0
	node.LastError = ""

	// Extract resource utilization if available
	if metrics, ok := status["metrics"].(map[string]interface{}); ok {
		resourceUtil := make(map[string]float64)
		for k, v := range metrics {
			if floatVal, ok := v.(float64); ok {
				resourceUtil[k] = floatVal
			}
		}
		node.ResourceUtilization = resourceUtil
	}

	// Extract capabilities if available
	if caps, ok := status["capabilities"].([]interface{}); ok {
		capabilities := make([]string, 0, len(caps))
		for _, cap := range caps {
			if strCap, ok := cap.(string); ok {
				capabilities = append(capabilities, strCap)
			}
		}
		node.Capabilities = capabilities
	}

	return node.HealthStatus, nil
}

// GetNodeClient returns a client for communicating with a node
func (cm *ClusterManager) GetNodeClient(nodeID string) (*client.NodeClient, error) {
	client, err := cm.getClient(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for node %s: %w", nodeID, err)
	}
	return client, nil
}

// getClient returns a cached client or creates a new one with retry logic
func (cm *ClusterManager) getClient(nodeID string) (*client.NodeClient, error) {
	cm.mu.RLock()
	// Check for existing client in cache
	if client, exists := cm.clientCache[nodeID]; exists {
		cm.mu.RUnlock()
		return client, nil
	}
	cm.mu.RUnlock()

	// Need write lock to create new client
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists := cm.clientCache[nodeID]; exists {
		return client, nil
	}

	// Get node info
	nodeInfo, exists := cm.nodes[nodeID]
	if !exists {
		// If node not in our list, try to get it from DHT
		if cm.dht == nil {
			return nil, fmt.Errorf("node %s not found and DHT not available", nodeID)
		}

		dhtNode := cm.dht.GetNode(nodeID)
		if dhtNode == nil {
			return nil, fmt.Errorf("node %s not found in DHT", nodeID)
		}

		// Add node to our list
		nodeInfo = &NodeInfo{
			ID:       dhtNode.ID,
			Hostname: dhtNode.Address,
			Port:     dhtNode.Port,
			HTTPPort: dhtNode.HTTPPort,
			LastSeen: time.Now(),
			IsActive: true,
		}
		cm.nodes[nodeID] = nodeInfo
	}

	// Validate node info
	if nodeInfo.Hostname == "" {
		return nil, fmt.Errorf("node %s has no hostname", nodeID)
	}

	if nodeInfo.HTTPPort <= 0 {
		return nil, fmt.Errorf("node %s has invalid HTTP port: %d", nodeID, nodeInfo.HTTPPort)
	}

	// Create client with retry options
	options := client.DefaultNodeClientOptions()
	options.Retries = 3
	options.Timeout = 5 * time.Second

	client := client.NewNodeClient(nodeInfo.Hostname, nodeInfo.HTTPPort, options)

	// Add to cache
	cm.clientCache[nodeID] = client
	return client, nil
}

// GetAllNodeIDs returns the IDs of all known nodes
func (cm *ClusterManager) GetAllNodeIDs() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodeIDs := make([]string, 0, len(cm.nodes))
	for nodeID := range cm.nodes {
		if nodeID != cm.nodeID { // Exclude self
			nodeIDs = append(nodeIDs, nodeID)
		}
	}

	return nodeIDs
}

// GetActiveNodeIDs returns the IDs of all active nodes
func (cm *ClusterManager) GetActiveNodeIDs() []string {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	nodeIDs := make([]string, 0, len(cm.nodes))
	for nodeID, node := range cm.nodes {
		if nodeID != cm.nodeID && node.IsReachable { // Exclude self and unreachable nodes
			nodeIDs = append(nodeIDs, nodeID)
		}
	}

	return nodeIDs
}

// GetNodeInfo returns information about a specific node
func (cm *ClusterManager) GetNodeInfo(nodeID string) *NodeInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if node, exists := cm.nodes[nodeID]; exists {
		return node
	}

	return nil
}

// Stop stops the cluster manager
