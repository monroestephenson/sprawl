// Package metrics provides cluster-wide metrics collection capabilities
package metrics

import (
	"fmt"
	"log"
	"sync"
	"time"

	"sprawl/node/cluster"
)

// ClusterMetricsCollector implements the ai.MetricsCollector interface for cluster-wide metrics
type ClusterMetricsCollector struct {
	clusterManager    ClusterManagerInterface
	store             StoreInterface
	metricsCache      map[string]map[string]float64
	metricsCacheTime  map[string]time.Time
	cacheDuration     time.Duration
	mu                sync.RWMutex
	metricsAggregated map[string]float64
	refreshTicker     *time.Ticker
	stopCh            chan struct{}
}

// StoreInterface defines the subset of store methods needed by the collector
type StoreInterface interface {
	GetMemoryUsage() int64
	GetClusterNodeIDs() []string
	GetMessageCount() int
	GetNodeMetrics(nodeID string) map[string]float64
	GetMessageCountForTopic(topic string) int
}

// ClusterManagerInterface defines the subset of cluster manager methods needed
type ClusterManagerInterface interface {
	GetAllNodeIDs() []string
	GetNodeClient(nodeID string) (*cluster.NodeClientAdapter, error)
}

// NodeClientInterface defines required node client functionality
type NodeClientInterface interface {
	GetMetrics() (map[string]float64, error)
}

// NewClusterMetricsCollector creates a new metrics collector for cluster operations
func NewClusterMetricsCollector(clusterManager ClusterManagerInterface, store StoreInterface) *ClusterMetricsCollector {
	collector := &ClusterMetricsCollector{
		clusterManager:    clusterManager,
		store:             store,
		metricsCache:      make(map[string]map[string]float64),
		metricsCacheTime:  make(map[string]time.Time),
		cacheDuration:     30 * time.Second,
		metricsAggregated: make(map[string]float64),
		stopCh:            make(chan struct{}),
	}

	// Start background refresh
	collector.refreshTicker = time.NewTicker(1 * time.Minute)
	go collector.periodicRefresh()

	log.Printf("[ClusterMetricsCollector] Initialized")
	return collector
}

// periodicRefresh refreshes metrics in the background
func (c *ClusterMetricsCollector) periodicRefresh() {
	for {
		select {
		case <-c.refreshTicker.C:
			c.refreshClusterMetrics()
		case <-c.stopCh:
			c.refreshTicker.Stop()
			return
		}
	}
}

// Stop halts the background refresh
func (c *ClusterMetricsCollector) Stop() {
	close(c.stopCh)
}

// refreshClusterMetrics refreshes metrics for all nodes
func (c *ClusterMetricsCollector) refreshClusterMetrics() {
	nodeIDs := c.clusterManager.GetAllNodeIDs()
	log.Printf("[ClusterMetricsCollector] Refreshing metrics for %d nodes", len(nodeIDs))

	var wg sync.WaitGroup
	for _, id := range nodeIDs {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, err := c.CollectNodeMetrics(id)
			if err != nil {
				log.Printf("[ClusterMetricsCollector] Error refreshing metrics for node %s: %v", id, err)
			}
		}(id)
	}
	wg.Wait()

	// Aggregate metrics
	c.aggregateClusterMetrics()
}

// aggregateClusterMetrics computes aggregate metrics across the cluster
func (c *ClusterMetricsCollector) aggregateClusterMetrics() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reset aggregated metrics
	c.metricsAggregated = make(map[string]float64)

	// Track aggregation stats
	var totalCPU, totalMem, totalNodes float64

	// Collect from all nodes
	for _, metrics := range c.metricsCache {
		if cpu, ok := metrics["cpu_usage"]; ok {
			totalCPU += cpu
		}
		if mem, ok := metrics["memory_usage"]; ok {
			totalMem += mem
		}
		totalNodes++
	}

	// Calculate averages if we have nodes
	if totalNodes > 0 {
		c.metricsAggregated["average_cpu_usage"] = totalCPU / totalNodes
		c.metricsAggregated["average_memory_usage"] = totalMem / totalNodes
	}

	// Add total node count
	c.metricsAggregated["node_count"] = totalNodes

	// Add store metrics
	c.metricsAggregated["total_messages"] = float64(c.store.GetMessageCount())
	c.metricsAggregated["memory_usage_bytes"] = float64(c.store.GetMemoryUsage())

	log.Printf("[ClusterMetricsCollector] Aggregated metrics for %d nodes", int(totalNodes))
}

// GetSystemMetrics returns metrics for the local system
func (c *ClusterMetricsCollector) GetSystemMetrics() (float64, float64, float64, error) {
	// Get metrics from local store
	cpuUsage := 0.0
	memoryUsage := 0.0
	networkUsage := 0.0

	// Check if we have local metrics in cache
	c.mu.RLock()
	if metrics, ok := c.metricsCache["local"]; ok {
		if cpu, ok := metrics["cpu_usage"]; ok {
			cpuUsage = cpu
		}
		if mem, ok := metrics["memory_usage"]; ok {
			memoryUsage = mem
		}
		if net, ok := metrics["network_bytes_per_sec"]; ok {
			networkUsage = net
		}
	}
	c.mu.RUnlock()

	// If we don't have cached metrics, try to get from store
	if cpuUsage == 0 && memoryUsage == 0 {
		storeMetrics := c.store.GetNodeMetrics("local")
		if storeMetrics != nil {
			if cpu, ok := storeMetrics["cpu_usage"]; ok {
				cpuUsage = cpu
			}
			if mem, ok := storeMetrics["memory_usage"]; ok {
				memoryUsage = mem
			}
			if net, ok := storeMetrics["network_bytes_per_sec"]; ok {
				networkUsage = net
			}
		}
	}

	return cpuUsage, memoryUsage, networkUsage, nil
}

// GetClusterNodes returns a list of node IDs in the cluster
func (c *ClusterMetricsCollector) GetClusterNodes() []string {
	return c.store.GetClusterNodeIDs()
}

// CollectNodeMetrics retrieves metrics from a specific node
func (c *ClusterMetricsCollector) CollectNodeMetrics(nodeID string) (map[string]float64, error) {
	// Check cache first
	c.mu.RLock()
	cacheTime, hasCacheTime := c.metricsCacheTime[nodeID]
	if hasCacheTime && time.Since(cacheTime) < c.cacheDuration {
		metrics := c.metricsCache[nodeID]
		c.mu.RUnlock()
		return metrics, nil
	}
	c.mu.RUnlock()

	var metrics map[string]float64
	var err error

	// If it's the local node, get from store directly
	if nodeID == "local" {
		metrics = c.store.GetNodeMetrics("local")
		if metrics == nil {
			return nil, fmt.Errorf("no metrics available for local node")
		}
	} else {
		// Get metrics from remote node
		var client *cluster.NodeClientAdapter
		client, err = c.clusterManager.GetNodeClient(nodeID)
		if err != nil {
			return nil, fmt.Errorf("failed to get client for node %s: %w", nodeID, err)
		}

		metrics, err = client.GetMetrics()
		if err != nil {
			return nil, fmt.Errorf("failed to collect metrics from node %s: %w", nodeID, err)
		}
	}

	// Update cache
	c.mu.Lock()
	c.metricsCache[nodeID] = metrics
	c.metricsCacheTime[nodeID] = time.Now()
	c.mu.Unlock()

	return metrics, nil
}
