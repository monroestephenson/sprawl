package node

import (
	"log"
	"net/http"

	"sprawl/ai"
)

// RegisterHandlers sets up all HTTP handlers
func (n *Node) RegisterHandlers() {
	// Register the health check endpoints
	// Health check endpoints are registered in Handler() function

	// Register core API endpoints
	http.HandleFunc("/publish", n.handlePublish)
	http.HandleFunc("/subscribe", n.handleSubscribe)
	http.HandleFunc("/store", n.handleStore)
	http.HandleFunc("/metrics", n.handleMetrics)

	// AI endpoints
	http.HandleFunc("/ai/predictions", ai.HandlePredictions)
	http.HandleFunc("/ai/status", n.handleAIStatus)
	http.HandleFunc("/ai/anomalies", n.handleAIAnomalies)

	// Add other handlers as needed
	log.Printf("[Node %s] Registered API endpoints for publish, subscribe, metrics, etc.", n.ID[:8])
}
