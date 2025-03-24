package router

import (
	"math"
	"sort"
	"sync"
	"time"

	"sprawl/node/dht"
)

// RouteOptimizer handles network-aware routing decisions and path optimization
type RouteOptimizer struct {
	// network topology map: nodeID -> {targetNodeID -> latency}
	topologyMap map[string]map[string]time.Duration
	// historical performance tracking: nodeID -> {metric -> value}
	performanceData map[string]map[string]float64
	// lock for concurrent access
	mu sync.RWMutex
	// configuration parameters
	config OptimizerConfig
}

// OptimizerConfig contains configuration parameters for the route optimizer
type OptimizerConfig struct {
	// Weight factors for different optimization criteria
	LatencyWeight     float64
	ThroughputWeight  float64
	ReliabilityWeight float64

	// How frequently to decay historical data
	MetricsDecayFactor float64

	// Thresholds for congestion detection
	CongestionThreshold time.Duration

	// How long to keep latency measurements before considering them stale
	LatencyTTL time.Duration
}

// NewRouteOptimizer creates a new RouteOptimizer with the provided configuration
func NewRouteOptimizer(config OptimizerConfig) *RouteOptimizer {
	// Apply defaults for any zero values
	if config.LatencyWeight == 0 {
		config.LatencyWeight = 0.5
	}
	if config.ThroughputWeight == 0 {
		config.ThroughputWeight = 0.3
	}
	if config.ReliabilityWeight == 0 {
		config.ReliabilityWeight = 0.2
	}
	if config.MetricsDecayFactor == 0 {
		config.MetricsDecayFactor = 0.95 // 5% decay per update
	}
	if config.CongestionThreshold == 0 {
		config.CongestionThreshold = 100 * time.Millisecond
	}
	if config.LatencyTTL == 0 {
		config.LatencyTTL = 5 * time.Minute
	}

	return &RouteOptimizer{
		topologyMap:     make(map[string]map[string]time.Duration),
		performanceData: make(map[string]map[string]float64),
		config:          config,
	}
}

// defaultOptimizerConfig returns a default configuration for the route optimizer
func defaultOptimizerConfig() OptimizerConfig {
	return OptimizerConfig{
		LatencyWeight:       0.5,
		ThroughputWeight:    0.3,
		ReliabilityWeight:   0.2,
		MetricsDecayFactor:  0.95,
		CongestionThreshold: 100 * time.Millisecond,
		LatencyTTL:          5 * time.Minute,
	}
}

// UpdateLatency records a latency measurement between source and target nodes
func (o *RouteOptimizer) UpdateLatency(sourceID, targetID string, latency time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Ensure map entries exist
	if _, exists := o.topologyMap[sourceID]; !exists {
		o.topologyMap[sourceID] = make(map[string]time.Duration)
	}

	// Update the latency measurement
	o.topologyMap[sourceID][targetID] = latency
}

// UpdatePerformanceMetric updates a performance metric for a node
func (o *RouteOptimizer) UpdatePerformanceMetric(nodeID, metric string, value float64, bypassEMA ...bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Ensure map entries exist
	if _, exists := o.performanceData[nodeID]; !exists {
		o.performanceData[nodeID] = make(map[string]float64)
	}

	// Apply exponential moving average if we already have data
	// and bypassEMA is not provided or false
	skipEMA := len(bypassEMA) > 0 && bypassEMA[0]
	if !skipEMA {
		if existingValue, exists := o.performanceData[nodeID][metric]; exists {
			value = existingValue*o.config.MetricsDecayFactor + value*(1-o.config.MetricsDecayFactor)
		}
	}

	o.performanceData[nodeID][metric] = value
}

// GetNodeScore calculates a composite score for a node based on various metrics
// Lower score is better
func (o *RouteOptimizer) GetNodeScore(nodeID string) float64 {
	o.mu.RLock()
	defer o.mu.RUnlock()

	// Default to a high score if we have no data
	if _, exists := o.performanceData[nodeID]; !exists {
		return 1000.0
	}

	metrics := o.performanceData[nodeID]

	// Calculate weighted score components
	latencyScore := metrics["latency"] * o.config.LatencyWeight
	throughputScore := (1.0 / math.Max(1.0, metrics["throughput"])) * o.config.ThroughputWeight
	reliabilityScore := (1.0 - metrics["reliability"]) * o.config.ReliabilityWeight

	// Return composite score (lower is better)
	return latencyScore + throughputScore + reliabilityScore
}

// IsCongested determines if a path to a node is congested
func (o *RouteOptimizer) IsCongested(nodeID string) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if metrics, exists := o.performanceData[nodeID]; exists {
		latency := metrics["latency"]
		threshold := float64(o.config.CongestionThreshold / time.Millisecond) // Convert to ms for comparison

		// Return true if latency exceeds threshold
		return latency >= threshold
	}
	return false // Assume not congested if we have no data
}

// OptimizeRoute takes a set of candidate nodes and returns them in optimized order
func (o *RouteOptimizer) OptimizeRoute(nodes []dht.NodeInfo, msg Message) []dht.NodeInfo {
	if len(nodes) <= 1 {
		return nodes // Nothing to optimize
	}

	type scoredNode struct {
		node  dht.NodeInfo
		score float64
	}

	// Score each node
	scoredNodes := make([]scoredNode, 0, len(nodes))
	for _, node := range nodes {
		score := o.GetNodeScore(node.ID)

		// Check if node is congested and penalize it heavily
		if o.IsCongested(node.ID) {
			score *= 10.0 // Apply a much higher penalty to ensure congested nodes are avoided
		}

		scoredNodes = append(scoredNodes, scoredNode{
			node:  node,
			score: score,
		})
	}

	// Sort by score (lower is better)
	sort.Slice(scoredNodes, func(i, j int) bool {
		return scoredNodes[i].score < scoredNodes[j].score
	})

	// Convert back to node list
	result := make([]dht.NodeInfo, len(scoredNodes))
	for i, sn := range scoredNodes {
		result[i] = sn.node
	}

	return result
}

// GetLatency retrieves the recorded latency between two nodes
func (o *RouteOptimizer) GetLatency(sourceID, targetID string) (time.Duration, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if sourceMap, exists := o.topologyMap[sourceID]; exists {
		if latency, found := sourceMap[targetID]; found {
			return latency, true
		}
	}
	return 0, false
}

// GetMetrics returns metrics about the optimizer for monitoring
func (o *RouteOptimizer) GetMetrics() map[string]interface{} {
	o.mu.RLock()
	defer o.mu.RUnlock()

	metrics := map[string]interface{}{
		"topology_node_count":    len(o.topologyMap),
		"performance_node_count": len(o.performanceData),
	}

	// Calculate average latency across all paths
	var totalLatency time.Duration
	var pathCount int
	for _, targetMap := range o.topologyMap {
		for _, latency := range targetMap {
			totalLatency += latency
			pathCount++
		}
	}

	if pathCount > 0 {
		metrics["avg_path_latency_ms"] = totalLatency.Milliseconds() / int64(pathCount)
	} else {
		metrics["avg_path_latency_ms"] = 0
	}

	return metrics
}
