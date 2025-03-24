package cluster

import (
	"fmt"
	"log"
)

// ClusterProviderAdapter adapts ClusterManager to the ClusterProvider interface
type ClusterProviderAdapter struct {
	manager *ClusterManager
}

// NodeClientAdapter adapts client.NodeClient to the NodeClient interface
type NodeClientAdapter struct {
	client interface {
		GetMetrics() (map[string]float64, error)
	}
}

// NewClusterProviderAdapter creates a new adapter for ClusterManager
func NewClusterProviderAdapter(cm *ClusterManager) *ClusterProviderAdapter {
	return &ClusterProviderAdapter{
		manager: cm,
	}
}

// GetAllNodeIDs implements the ClusterProvider interface
func (a *ClusterProviderAdapter) GetAllNodeIDs() []string {
	return a.manager.GetAllNodeIDs()
}

// GetNodeClient implements the ClusterProvider interface
func (a *ClusterProviderAdapter) GetNodeClient(nodeID string) (NodeClientAdapter, error) {
	client, err := a.manager.GetNodeClient(nodeID)
	if err != nil {
		log.Printf("[Adapter] Failed to get client for node %s: %v", nodeID, err)
		return NodeClientAdapter{}, fmt.Errorf("failed to get node client: %w", err)
	}

	return NodeClientAdapter{client: client}, nil
}

// GetMetrics implements the NodeClient interface
func (a *NodeClientAdapter) GetMetrics() (map[string]float64, error) {
	return a.client.GetMetrics()
}
