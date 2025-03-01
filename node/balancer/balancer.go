package balancer

import (
	"sync"
	"time"

	"sprawl/node/dht"
)

type LoadBalancer struct {
	dht        *dht.DHT
	mu         sync.RWMutex
	nodeLoads  map[string]NodeLoad
	thresholds Thresholds
}

type NodeLoad struct {
	CPU       float64
	Memory    float64
	MsgCount  int64
	Topics    []string
	Timestamp time.Time
}

type Thresholds struct {
	CPUHigh    float64
	MemoryHigh float64
	MsgHigh    int64
}

func NewLoadBalancer(dht *dht.DHT) *LoadBalancer {
	return &LoadBalancer{
		dht:       dht,
		nodeLoads: make(map[string]NodeLoad),
		thresholds: Thresholds{
			CPUHigh:    0.8,
			MemoryHigh: 0.8,
			MsgHigh:    10000,
		},
	}
}

// UpdateNodeLoad updates the load information for a node
func (lb *LoadBalancer) UpdateNodeLoad(nodeID string, load NodeLoad) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	lb.nodeLoads[nodeID] = load

	// Check if rebalancing is needed
	if lb.needsRebalancing(nodeID) {
		lb.rebalanceTopics(nodeID)
	}
}

func (lb *LoadBalancer) needsRebalancing(nodeID string) bool {
	load := lb.nodeLoads[nodeID]
	return load.CPU > lb.thresholds.CPUHigh ||
		load.Memory > lb.thresholds.MemoryHigh ||
		load.MsgCount > lb.thresholds.MsgHigh
}

func (lb *LoadBalancer) rebalanceTopics(overloadedNode string) {
	// Find least loaded nodes
	var targetNode string
	var minLoad float64 = 1.0

	for nodeID, load := range lb.nodeLoads {
		if nodeID != overloadedNode && load.CPU < minLoad {
			minLoad = load.CPU
			targetNode = nodeID
		}
	}

	if targetNode != "" {
		// Move some topics to the target node
		load := lb.nodeLoads[overloadedNode]
		if len(load.Topics) > 0 {
			// Move half of the topics
			moveCount := len(load.Topics) / 2
			for i := 0; i < moveCount; i++ {
				topic := load.Topics[i]
				// Update DHT mappings
				lb.dht.ReassignTopic(topic, targetNode)
			}
		}
	}
}
