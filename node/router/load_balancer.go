package router

import (
	"sync"
	"time"

	"sprawl/node/dht"
)

// scoredNode represents a node with its calculated load score
type scoredNode struct {
	node  dht.NodeInfo
	score float64
}

// LoadBalancer manages node selection based on load metrics
type LoadBalancer struct {
	// Node metrics storage: nodeID -> metricName -> value
	nodeMetrics map[string]map[string]float64
	// Exponential moving averages for each metric
	metricEMAs map[string]map[string]float64
	// Lock for concurrent access
	mu sync.RWMutex
	// Configuration parameters
	config LoadBalancerConfig
	// Last update timestamp per node
	lastUpdate map[string]time.Time
	// Circuit breaker status per node: nodeID -> circuit open?
	circuitBreakers map[string]bool
}

// LoadBalancerConfig contains configuration parameters for the load balancer
type LoadBalancerConfig struct {
	// Weight of each metric in the final scoring calculation
	CPUWeight        float64
	MemoryWeight     float64
	ThroughputWeight float64
	LatencyWeight    float64

	// For exponential moving average calculations
	EMASmoothing float64

	// Maximum allowed utilization difference between nodes
	MaxUtilizationDiff float64

	// Number of nodes to select for replication
	ReplicationFactor int

	// Circuit breaker settings
	CircuitBreakerThreshold    int           // Failures before opening circuit
	CircuitBreakerResetTimeout time.Duration // Time to wait before testing circuit again
}

// NewLoadBalancer creates a new LoadBalancer with the provided configuration
func NewLoadBalancer(config LoadBalancerConfig) *LoadBalancer {
	// Set defaults for any zero values
	if config.CPUWeight == 0 {
		config.CPUWeight = 0.4
	}
	if config.MemoryWeight == 0 {
		config.MemoryWeight = 0.2
	}
	if config.ThroughputWeight == 0 {
		config.ThroughputWeight = 0.2
	}
	if config.LatencyWeight == 0 {
		config.LatencyWeight = 0.2
	}
	if config.EMASmoothing == 0 {
		config.EMASmoothing = 0.2 // 20% weight to new values
	}
	if config.MaxUtilizationDiff == 0 {
		config.MaxUtilizationDiff = 0.2 // 20% max difference
	}
	if config.ReplicationFactor == 0 {
		config.ReplicationFactor = 3 // Default replication factor
	}
	if config.CircuitBreakerThreshold == 0 {
		config.CircuitBreakerThreshold = 3
	}
	if config.CircuitBreakerResetTimeout == 0 {
		config.CircuitBreakerResetTimeout = 30 * time.Second
	}

	return &LoadBalancer{
		nodeMetrics:     make(map[string]map[string]float64),
		metricEMAs:      make(map[string]map[string]float64),
		lastUpdate:      make(map[string]time.Time),
		circuitBreakers: make(map[string]bool),
		config:          config,
	}
}

// defaultLoadBalancerConfig returns a default configuration for the load balancer
func defaultLoadBalancerConfig() LoadBalancerConfig {
	return LoadBalancerConfig{
		CPUWeight:                  0.4,
		MemoryWeight:               0.2,
		ThroughputWeight:           0.2,
		LatencyWeight:              0.2,
		EMASmoothing:               0.2,
		MaxUtilizationDiff:         0.2,
		ReplicationFactor:          3,
		CircuitBreakerThreshold:    3,
		CircuitBreakerResetTimeout: 30 * time.Second,
	}
}

// UpdateNodeMetric updates a single metric for a node and recalculates the EMA
func (lb *LoadBalancer) UpdateNodeMetric(nodeID, metric string, value float64) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Create maps if they don't exist
	if _, exists := lb.nodeMetrics[nodeID]; !exists {
		lb.nodeMetrics[nodeID] = make(map[string]float64)
		lb.metricEMAs[nodeID] = make(map[string]float64)
	}

	// Update raw metric value
	lb.nodeMetrics[nodeID][metric] = value

	// Update EMA
	if currentEMA, exists := lb.metricEMAs[nodeID][metric]; exists {
		// Apply exponential moving average formula
		lb.metricEMAs[nodeID][metric] = currentEMA*(1-lb.config.EMASmoothing) + value*lb.config.EMASmoothing
	} else {
		// First value, just set it directly
		lb.metricEMAs[nodeID][metric] = value
	}

	// Update last update timestamp
	lb.lastUpdate[nodeID] = time.Now()
}

// GetNodeMetric retrieves a specific metric for a node
func (lb *LoadBalancer) GetNodeMetric(nodeID, metric string) (float64, bool) {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if metrics, exists := lb.metricEMAs[nodeID]; exists {
		if value, found := metrics[metric]; found {
			return value, true
		}
	}
	return 0, false
}

// RecordNodeSuccess marks a successful operation for a node
func (lb *LoadBalancer) RecordNodeSuccess(nodeID string) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Reset circuit breaker if it was open
	lb.circuitBreakers[nodeID] = false
}

// RecordNodeFailure records a failure for a node and possibly trips the circuit breaker
func (lb *LoadBalancer) RecordNodeFailure(nodeID string) bool {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Initialize failure count if not present
	if _, exists := lb.nodeMetrics[nodeID]; !exists {
		lb.nodeMetrics[nodeID] = make(map[string]float64)
		lb.metricEMAs[nodeID] = make(map[string]float64)
	}

	// Increment failure count
	failCount := lb.nodeMetrics[nodeID]["failures"] + 1
	lb.nodeMetrics[nodeID]["failures"] = failCount

	// Update EMA for failures
	if currentEMA, exists := lb.metricEMAs[nodeID]["failures"]; exists {
		lb.metricEMAs[nodeID]["failures"] = currentEMA*(1-lb.config.EMASmoothing) + failCount*lb.config.EMASmoothing
	} else {
		lb.metricEMAs[nodeID]["failures"] = failCount
	}

	// Trip circuit breaker if threshold exceeded
	if failCount >= float64(lb.config.CircuitBreakerThreshold) {
		lb.circuitBreakers[nodeID] = true
		// Reset failure counter
		lb.nodeMetrics[nodeID]["failures"] = 0
		// Schedule circuit reset
		go lb.scheduleCircuitReset(nodeID)
		return true
	}

	return false
}

// scheduleCircuitReset schedules the resetting of a circuit breaker after the timeout
func (lb *LoadBalancer) scheduleCircuitReset(nodeID string) {
	time.Sleep(lb.config.CircuitBreakerResetTimeout)

	lb.mu.Lock()
	defer lb.mu.Unlock()

	// Set to half-open state
	lb.circuitBreakers[nodeID] = false
}

// IsCircuitOpen checks if a node's circuit breaker is open
func (lb *LoadBalancer) IsCircuitOpen(nodeID string) bool {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	if open, exists := lb.circuitBreakers[nodeID]; exists && open {
		return true
	}
	return false
}

// CalculateNodeScore computes a score for a node based on its metrics
// Lower score is better
func (lb *LoadBalancer) CalculateNodeScore(nodeID string) float64 {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	// Default to a high score if no metrics available
	if _, exists := lb.metricEMAs[nodeID]; !exists {
		return 1000.0
	}

	metrics := lb.metricEMAs[nodeID]

	// Get metric values with defaults
	cpuUsage := metrics["cpu_usage"]
	memoryUsage := metrics["memory_usage"]
	throughput := metrics["message_throughput"]
	latency := metrics["response_latency"]

	// Calculate weighted score (lower is better)
	score := cpuUsage*lb.config.CPUWeight +
		memoryUsage*lb.config.MemoryWeight +
		(1.0/throughput)*lb.config.ThroughputWeight +
		latency*lb.config.LatencyWeight

	return score
}

// IsNodeHealthy determines if a node is healthy enough for routing
func (lb *LoadBalancer) IsNodeHealthy(nodeID string) bool {
	// Check if circuit breaker is open
	if lb.IsCircuitOpen(nodeID) {
		return false
	}

	lb.mu.RLock()
	defer lb.mu.RUnlock()

	// Check if metrics are stale (no updates in last minute)
	if lastUpdate, exists := lb.lastUpdate[nodeID]; exists {
		if time.Since(lastUpdate) > time.Minute {
			return false
		}
	}

	// Consider healthy by default if we have no metrics
	if _, exists := lb.metricEMAs[nodeID]; !exists {
		return true
	}

	metrics := lb.metricEMAs[nodeID]

	// Check CPU and memory thresholds
	if metrics["cpu_usage"] > 0.9 || metrics["memory_usage"] > 0.9 {
		return false
	}

	return true
}

// SelectNodes chooses the best nodes for handling a message based on load metrics
func (lb *LoadBalancer) SelectNodes(nodes []dht.NodeInfo, msg Message) []dht.NodeInfo {
	if len(nodes) == 0 {
		return nodes
	}

	// If only one node, return it directly
	if len(nodes) == 1 {
		return nodes
	}

	// Score all nodes
	scoredNodes := make([]scoredNode, 0, len(nodes))
	for _, node := range nodes {
		// Skip unhealthy nodes
		if !lb.IsNodeHealthy(node.ID) {
			continue
		}

		score := lb.CalculateNodeScore(node.ID)
		scoredNodes = append(scoredNodes, scoredNode{
			node:  node,
			score: score,
		})
	}

	// If all nodes unhealthy, use all of them with default ordering
	if len(scoredNodes) == 0 {
		return nodes
	}

	// Sort by score (lowest is best)
	scoredNodes = lb.sortNodesByScore(scoredNodes)

	// Get the replication factor or available nodes, whichever is smaller
	count := lb.config.ReplicationFactor
	if count > len(scoredNodes) {
		count = len(scoredNodes)
	}

	// Build result set
	result := make([]dht.NodeInfo, count)
	for i := 0; i < count; i++ {
		result[i] = scoredNodes[i].node
	}

	return result
}

// sortNodesByScore sorts nodes by their score and implements load balancing
func (lb *LoadBalancer) sortNodesByScore(nodes []scoredNode) []scoredNode {
	// First sort by raw score
	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			if nodes[i].score > nodes[j].score {
				nodes[i], nodes[j] = nodes[j], nodes[i]
			}
		}
	}

	// If we have enough nodes, check utilization difference
	if len(nodes) > 1 {
		// Get best and worst node
		bestScore := nodes[0].score
		worstScore := nodes[len(nodes)-1].score

		// Calculate normalized difference
		diff := (worstScore - bestScore) / bestScore

		// If difference is too large, rebalance
		if diff > lb.config.MaxUtilizationDiff {
			// Flatten scores to reduce the difference
			for i := 0; i < len(nodes); i++ {
				// Apply smoothing to reduce extreme differences
				nodes[i].score = bestScore + (nodes[i].score-bestScore)*0.5
			}

			// Re-sort with adjusted scores
			for i := 0; i < len(nodes); i++ {
				for j := i + 1; j < len(nodes); j++ {
					if nodes[i].score > nodes[j].score {
						nodes[i], nodes[j] = nodes[j], nodes[i]
					}
				}
			}
		}
	}

	return nodes
}

// GetMetrics returns load balancer metrics for monitoring
func (lb *LoadBalancer) GetMetrics() map[string]interface{} {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	metrics := map[string]interface{}{
		"tracked_nodes": len(lb.nodeMetrics),
		"open_circuits": 0,
	}

	// Count open circuit breakers
	for _, open := range lb.circuitBreakers {
		if open {
			metrics["open_circuits"] = metrics["open_circuits"].(int) + 1
		}
	}

	// Calculate average metrics across nodes
	var totalCPU, totalMemory, totalLatency float64
	var nodeCount int

	for _, nodeMetrics := range lb.metricEMAs {
		if cpu, exists := nodeMetrics["cpu_usage"]; exists {
			totalCPU += cpu
			nodeCount++
		}
		if mem, exists := nodeMetrics["memory_usage"]; exists {
			totalMemory += mem
		}
		if latency, exists := nodeMetrics["response_latency"]; exists {
			totalLatency += latency
		}
	}

	// Add averages to metrics if we have data
	if nodeCount > 0 {
		metrics["avg_cpu_usage"] = totalCPU / float64(nodeCount)
		metrics["avg_memory_usage"] = totalMemory / float64(nodeCount)
		metrics["avg_response_latency_ms"] = totalLatency / float64(nodeCount)
	}

	return metrics
}
