package node

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// HealthStatus represents the health status response
type HealthStatus struct {
	Status       string                 `json:"status"`
	NodeID       string                 `json:"node_id"`
	Timestamp    int64                  `json:"timestamp"`
	Uptime       float64                `json:"uptime"`
	SystemHealth map[string]interface{} `json:"system_health"`
	ClusterState map[string]interface{} `json:"cluster_state"`
	Version      string                 `json:"version"`
	Features     map[string]bool        `json:"features"`
}

// HealthHandler handles health check requests
type HealthHandler struct {
	gossipManager  *GossipManager
	metricsManager *MetricsManager
	version        string
	startTime      time.Time
	nodeID         string
}

// NewHealthHandler creates a new health handler
func NewHealthHandler(g *GossipManager, m *MetricsManager, nodeID, version string) *HealthHandler {
	return &HealthHandler{
		gossipManager:  g,
		metricsManager: m,
		version:        version,
		startTime:      time.Now(),
		nodeID:         nodeID,
	}
}

// ServeHTTP handles HTTP requests to the health endpoint
func (h *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Set content type and headers
	w.Header().Set("Content-Type", "application/json")

	// Gather system health metrics
	systemHealth := make(map[string]interface{})
	if h.metricsManager != nil {
		metrics := h.metricsManager.GetCurrentMetrics()
		systemHealth["cpu_usage"] = metrics.CPUUsage
		systemHealth["memory_usage"] = metrics.MemoryUsage
		systemHealth["disk_usage"] = metrics.DiskUsage
		systemHealth["message_rate"] = metrics.MessageRate
	}

	// Gather cluster state
	clusterState := make(map[string]interface{})
	if h.gossipManager != nil {
		clusterState["member_count"] = len(h.gossipManager.GetMembers())

		// Get failure detector status if available
		if h.gossipManager.failureDetector != nil {
			health := h.gossipManager.failureDetector.CheckClusterHealth()
			for k, v := range health {
				clusterState[k] = v
			}
		}

		// Get current node state
		clusterState["node_state"] = h.gossipManager.state
		clusterState["is_leader"] = h.gossipManager.isLeader
	}

	// Create health status response
	status := HealthStatus{
		Status:       "ok", // Default to ok
		NodeID:       h.nodeID,
		Timestamp:    time.Now().Unix(),
		Uptime:       time.Since(h.startTime).Seconds(),
		SystemHealth: systemHealth,
		ClusterState: clusterState,
		Version:      h.version,
		Features: map[string]bool{
			"metrics":           h.metricsManager != nil,
			"failure_detection": h.gossipManager != nil && h.gossipManager.failureDetector != nil,
			"health_monitoring": true,
		},
	}

	// Check if the system should be considered unhealthy
	if systemHealth["cpu_usage"] != nil && systemHealth["cpu_usage"].(float64) > 90 {
		status.Status = "warning"
	}
	if systemHealth["memory_usage"] != nil && systemHealth["memory_usage"].(float64) > 90 {
		status.Status = "warning"
	}
	if clusterState["healthy_ratio"] != nil && clusterState["healthy_ratio"].(float64) < 0.5 {
		status.Status = "critical"
	}

	// Return the health status as JSON
	if err := json.NewEncoder(w).Encode(status); err != nil {
		log.Printf("[Health] Error encoding health status: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// RegisterHealthEndpoint registers the health handler with the given HTTP server
func RegisterHealthEndpoint(mux *http.ServeMux, gossipManager *GossipManager, metricsManager *MetricsManager, nodeID, version string) {
	handler := NewHealthHandler(gossipManager, metricsManager, nodeID, version)
	mux.Handle("/health", handler)

	// Also register at /health/check for compatibility with cloud providers
	mux.Handle("/health/check", handler)

	// Register detailed endpoints
	mux.HandleFunc("/health/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var metrics interface{}
		if metricsManager != nil {
			metrics = metricsManager.GetMetricsAsMap()
		} else {
			metrics = map[string]string{"status": "metrics not available"}
		}

		if err := json.NewEncoder(w).Encode(metrics); err != nil {
			log.Printf("[Health] Error encoding metrics: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	})

	mux.HandleFunc("/health/membership", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var membership interface{}
		if gossipManager != nil {
			membership = gossipManager.GetAllMemberInfo()
		} else {
			membership = map[string]string{"status": "membership not available"}
		}

		if err := json.NewEncoder(w).Encode(membership); err != nil {
			log.Printf("[Health] Error encoding membership: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	})
}
