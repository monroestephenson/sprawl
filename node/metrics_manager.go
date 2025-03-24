package node

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/net"
)

// MetricsType represents different categories of metrics
type MetricsType string

const (
	// MetricsTypeSystem represents system-level metrics (CPU, memory, etc.)
	MetricsTypeSystem MetricsType = "system"
	// MetricsTypeNode represents node-specific metrics
	MetricsTypeNode MetricsType = "node"
	// MetricsTypeNetwork represents network activity metrics
	MetricsTypeNetwork MetricsType = "network"
	// MetricsTypeApplication represents application-specific metrics
	MetricsTypeApplication MetricsType = "application"
)

// systemMetricsInterval is the interval for collecting system metrics
const systemMetricsInterval = 5 * time.Second

// networkMetricsInterval is the interval for collecting network metrics
const networkMetricsInterval = 10 * time.Second

// applicationMetricsInterval is the interval for collecting application metrics
const applicationMetricsInterval = 15 * time.Second

// NodeMetrics represents all metrics associated with a node
type NodeMetrics struct {
	// System metrics
	CPUUsage       float64 `json:"cpu_usage"`        // Percentage CPU usage (0-100)
	MemoryUsage    float64 `json:"memory_usage"`     // Percentage memory usage (0-100)
	DiskUsage      float64 `json:"disk_usage"`       // Percentage disk usage (0-100)
	DiskIOPS       float64 `json:"disk_iops"`        // Disk IOPS
	LoadAverage    float64 `json:"load_average"`     // System load average
	NetworkRxBytes int64   `json:"network_rx_bytes"` // Network bytes received per second
	NetworkTxBytes int64   `json:"network_tx_bytes"` // Network bytes transmitted per second

	// Node operational metrics
	MessageCount      int64    `json:"message_count"`      // Total message count processed
	MessageRate       float64  `json:"message_rate"`       // Messages per second
	ActiveTopics      int      `json:"active_topics"`      // Number of active topics
	ActiveSubscribers int      `json:"active_subscribers"` // Number of active subscribers
	RoutingTableSize  int      `json:"routing_table_size"` // Size of the routing table
	PeerCount         int      `json:"peer_count"`         // Number of connected peers
	UpTime            float64  `json:"uptime"`             // Uptime in seconds
	LastSeen          int64    `json:"last_seen"`          // Unix timestamp of last seen
	FailoverCount     int      `json:"failover_count"`     // Number of failovers handled
	RoutingLatency    int64    `json:"routing_latency"`    // Average routing latency in microseconds
	QueueDepth        int      `json:"queue_depth"`        // Current queue depth
	ErrorRate         float64  `json:"error_rate"`         // Errors per second
	SuccessRate       float64  `json:"success_rate"`       // Success rate (0-100)
	HealthScore       float64  `json:"health_score"`       // Overall health score (0-100)
	StateChanges      int      `json:"state_changes"`      // Number of state changes since start
	JoinTimestamp     int64    `json:"join_timestamp"`     // When the node joined the cluster
	SuspectCount      int      `json:"suspect_count"`      // Number of times node was suspect
	RecoveryTime      int64    `json:"recovery_time"`      // Average time to recover in ms
	Warnings          []string `json:"warnings"`           // Warnings or alerts
}

// MetricsSnapshot contains a point-in-time collection of metrics
type MetricsSnapshot struct {
	NodeID         string                 `json:"node_id"`
	Timestamp      int64                  `json:"timestamp"`
	SystemMetrics  map[string]interface{} `json:"system_metrics"`
	NodeMetrics    map[string]interface{} `json:"node_metrics"`
	NetworkMetrics map[string]interface{} `json:"network_metrics"`
	AppMetrics     map[string]interface{} `json:"app_metrics"`
}

// MetricsManager handles collection and distribution of metrics
type MetricsManager struct {
	mu                   sync.RWMutex
	nodeID               string
	currentMetrics       NodeMetrics
	metricsHistory       []MetricsSnapshot             // Rolling history of metrics
	historyMaxSize       int                           // Maximum number of snapshots to retain
	metricsCollected     int64                         // Total number of metrics collections performed
	lastNetworkIO        map[string]net.IOCountersStat // Previous network stats for rate calculation
	lastNetworkStatsTime time.Time                     // When last network stats were collected
	stopCh               chan struct{}
	wg                   sync.WaitGroup

	// Message tracking
	messageCountMu    sync.RWMutex
	messageCount      int64
	messageRateWindow []int64 // Store recent message counts for rate calculation
	lastMessageTime   time.Time

	// Custom metrics
	customMetrics   map[string]interface{}
	customMetricsMu sync.RWMutex
}

// NewMetricsManager creates a new metrics manager
func NewMetricsManager(nodeID string) *MetricsManager {
	mm := &MetricsManager{
		nodeID:            nodeID,
		currentMetrics:    NodeMetrics{},
		metricsHistory:    make([]MetricsSnapshot, 0, 100),
		historyMaxSize:    100, // Store 100 snapshots (can be configured)
		lastNetworkIO:     make(map[string]net.IOCountersStat),
		messageRateWindow: make([]int64, 0, 10),
		customMetrics:     make(map[string]interface{}),
		stopCh:            make(chan struct{}),
	}

	return mm
}

// Start begins metrics collection
func (mm *MetricsManager) Start() {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.wg.Add(3) // Three collection goroutines

	// System metrics collector
	go mm.systemMetricsCollector()

	// Network metrics collector
	go mm.networkMetricsCollector()

	// Application metrics collector
	go mm.applicationMetricsCollector()
}

// Stop halts metrics collection
func (mm *MetricsManager) Stop() {
	close(mm.stopCh)
	mm.wg.Wait()
}

// systemMetricsCollector periodically collects system metrics
func (mm *MetricsManager) systemMetricsCollector() {
	defer mm.wg.Done()

	ticker := time.NewTicker(systemMetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.stopCh:
			return
		case <-ticker.C:
			mm.collectSystemMetrics()
		}
	}
}

// networkMetricsCollector periodically collects network metrics
func (mm *MetricsManager) networkMetricsCollector() {
	defer mm.wg.Done()

	ticker := time.NewTicker(networkMetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.stopCh:
			return
		case <-ticker.C:
			mm.collectNetworkMetrics()
		}
	}
}

// applicationMetricsCollector periodically collects application metrics
func (mm *MetricsManager) applicationMetricsCollector() {
	defer mm.wg.Done()

	ticker := time.NewTicker(applicationMetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mm.stopCh:
			return
		case <-ticker.C:
			mm.collectApplicationMetrics()
		}
	}
}

// collectSystemMetrics gathers system-level metrics
func (mm *MetricsManager) collectSystemMetrics() {
	// Collect and store metrics with proper error handling
	cpuUsage := mm.getCPUUsage()
	memoryUsage := mm.getMemoryUsage()
	diskUsage := mm.getDiskUsage()

	// Update current metrics under lock
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.currentMetrics.CPUUsage = cpuUsage
	mm.currentMetrics.MemoryUsage = memoryUsage
	mm.currentMetrics.DiskUsage = diskUsage
	mm.currentMetrics.LastSeen = time.Now().Unix()
	mm.currentMetrics.UpTime = time.Since(mm.lastMessageTime).Seconds()

	// Create a snapshot for the metrics history
	mm.storeMetricsSnapshot(MetricsTypeSystem, map[string]interface{}{
		"cpu_usage":    cpuUsage,
		"memory_usage": memoryUsage,
		"disk_usage":   diskUsage,
		"timestamp":    time.Now().Unix(),
	})

	mm.metricsCollected++
}

// collectNetworkMetrics gathers network-level metrics
func (mm *MetricsManager) collectNetworkMetrics() {
	// Get network IO stats
	networkIO, err := net.IOCounters(false) // false = all interfaces combined
	if err != nil {
		log.Printf("[Metrics] Error getting network stats: %v", err)
		return
	}

	now := time.Now()
	rxBytes := int64(0)
	txBytes := int64(0)

	// Only calculate rates if we have previous measurements
	if !mm.lastNetworkStatsTime.IsZero() && len(networkIO) > 0 {
		timeDiff := now.Sub(mm.lastNetworkStatsTime).Seconds()

		if timeDiff > 0 {
			// Calculate rates for all interfaces
			for i, stat := range networkIO {
				if prevStat, ok := mm.lastNetworkIO[stat.Name]; ok {
					rxDiff := stat.BytesRecv - prevStat.BytesRecv
					txDiff := stat.BytesSent - prevStat.BytesSent

					rxBytes += int64(float64(rxDiff) / timeDiff)
					txBytes += int64(float64(txDiff) / timeDiff)
				}

				// Store current stats for next comparison
				mm.lastNetworkIO[stat.Name] = networkIO[i]
			}
		}
	} else {
		// First run, just store the current stats
		for i, stat := range networkIO {
			mm.lastNetworkIO[stat.Name] = networkIO[i]
		}
	}

	mm.lastNetworkStatsTime = now

	// Update current metrics under lock
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.currentMetrics.NetworkRxBytes = rxBytes
	mm.currentMetrics.NetworkTxBytes = txBytes

	// Create a snapshot for the metrics history
	mm.storeMetricsSnapshot(MetricsTypeNetwork, map[string]interface{}{
		"rx_bytes":  rxBytes,
		"tx_bytes":  txBytes,
		"timestamp": now.Unix(),
	})
}

// collectApplicationMetrics gathers application-specific metrics
func (mm *MetricsManager) collectApplicationMetrics() {
	// Calculate message rate
	messageCount := mm.getMessageCount()
	messageRate := mm.calculateMessageRate(messageCount)

	// Update current metrics under lock
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.currentMetrics.MessageCount = messageCount
	mm.currentMetrics.MessageRate = messageRate

	// Add any custom metrics
	mm.customMetricsMu.RLock()
	appMetrics := make(map[string]interface{})
	for k, v := range mm.customMetrics {
		appMetrics[k] = v
	}
	mm.customMetricsMu.RUnlock()

	// Add standard application metrics
	appMetrics["message_count"] = messageCount
	appMetrics["message_rate"] = messageRate
	appMetrics["timestamp"] = time.Now().Unix()

	// Create a snapshot for the metrics history
	mm.storeMetricsSnapshot(MetricsTypeApplication, appMetrics)
}

// storeMetricsSnapshot adds a snapshot to the metrics history
func (mm *MetricsManager) storeMetricsSnapshot(metricsType MetricsType, metrics map[string]interface{}) {
	// Find existing snapshot for this timestamp or create new one
	var snapshot *MetricsSnapshot
	now := time.Now().Unix()

	// Look for a recent snapshot within 1 second
	for i := len(mm.metricsHistory) - 1; i >= 0; i-- {
		if mm.metricsHistory[i].Timestamp >= now-1 && mm.metricsHistory[i].Timestamp <= now+1 {
			snapshot = &mm.metricsHistory[i]
			break
		}
	}

	// Create new snapshot if none found
	if snapshot == nil {
		newSnapshot := MetricsSnapshot{
			NodeID:         mm.nodeID,
			Timestamp:      now,
			SystemMetrics:  make(map[string]interface{}),
			NodeMetrics:    make(map[string]interface{}),
			NetworkMetrics: make(map[string]interface{}),
			AppMetrics:     make(map[string]interface{}),
		}
		mm.metricsHistory = append(mm.metricsHistory, newSnapshot)

		// If we exceeded history size, remove oldest
		if len(mm.metricsHistory) > mm.historyMaxSize {
			mm.metricsHistory = mm.metricsHistory[1:]
		}

		snapshot = &mm.metricsHistory[len(mm.metricsHistory)-1]
	}

	// Update the appropriate metrics section
	switch metricsType {
	case MetricsTypeSystem:
		snapshot.SystemMetrics = metrics
	case MetricsTypeNode:
		snapshot.NodeMetrics = metrics
	case MetricsTypeNetwork:
		snapshot.NetworkMetrics = metrics
	case MetricsTypeApplication:
		snapshot.AppMetrics = metrics
	}
}

// GetCurrentMetrics returns the current node metrics
func (mm *MetricsManager) GetCurrentMetrics() NodeMetrics {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	// Return a copy to avoid race conditions
	return mm.currentMetrics
}

// GetMetricsSnapshot returns a specific metrics snapshot by index
func (mm *MetricsManager) GetMetricsSnapshot(index int) *MetricsSnapshot {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if index < 0 || index >= len(mm.metricsHistory) {
		return nil
	}

	// Return a copy to avoid race conditions
	snapshot := mm.metricsHistory[index]
	return &snapshot
}

// GetLatestMetricsSnapshot returns the most recent metrics snapshot
func (mm *MetricsManager) GetLatestMetricsSnapshot() *MetricsSnapshot {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	if len(mm.metricsHistory) == 0 {
		return nil
	}

	// Return a copy of the latest snapshot
	snapshot := mm.metricsHistory[len(mm.metricsHistory)-1]
	return &snapshot
}

// GetMetricsJSON returns the current metrics as JSON
func (mm *MetricsManager) GetMetricsJSON() ([]byte, error) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	return json.Marshal(mm.currentMetrics)
}

// AddMessageCount increments the message counter
func (mm *MetricsManager) AddMessageCount(count int) {
	mm.messageCountMu.Lock()
	defer mm.messageCountMu.Unlock()

	mm.messageCount += int64(count)

	// Update last message time for calculating rates
	now := time.Now()
	mm.messageRateWindow = append(mm.messageRateWindow, int64(count))

	// Keep window at most 10 entries
	if len(mm.messageRateWindow) > 10 {
		mm.messageRateWindow = mm.messageRateWindow[1:]
	}

	mm.lastMessageTime = now
}

// getMessageCount returns the current message count
func (mm *MetricsManager) getMessageCount() int64 {
	mm.messageCountMu.RLock()
	defer mm.messageCountMu.RUnlock()
	return mm.messageCount
}

// calculateMessageRate calculates the recent message rate
func (mm *MetricsManager) calculateMessageRate(currentCount int64) float64 {
	mm.messageCountMu.Lock()
	defer mm.messageCountMu.Unlock()

	if mm.lastMessageTime.IsZero() || len(mm.messageRateWindow) == 0 {
		return 0
	}

	// Calculate sum of recent counts
	var sum int64
	for _, count := range mm.messageRateWindow {
		sum += count
	}

	// Calculate rate over the window
	timeDiff := time.Since(mm.lastMessageTime.Add(-time.Duration(len(mm.messageRateWindow)) * time.Second))
	if timeDiff.Seconds() <= 0 {
		return 0
	}

	return float64(sum) / timeDiff.Seconds()
}

// AddCustomMetric adds or updates a custom application metric
func (mm *MetricsManager) AddCustomMetric(name string, value interface{}) {
	mm.customMetricsMu.Lock()
	defer mm.customMetricsMu.Unlock()
	mm.customMetrics[name] = value
}

// RemoveCustomMetric removes a custom application metric
func (mm *MetricsManager) RemoveCustomMetric(name string) {
	mm.customMetricsMu.Lock()
	defer mm.customMetricsMu.Unlock()
	delete(mm.customMetrics, name)
}

// getCPUUsage returns the current CPU usage percentage
func (mm *MetricsManager) getCPUUsage() float64 {
	percent, err := cpu.Percent(time.Second, false) // false = overall CPU percentage
	if err != nil {
		log.Printf("[Metrics] Error getting CPU usage: %v", err)
		return 0.0
	}
	if len(percent) == 0 {
		return 0.0
	}
	return percent[0] // Return the overall CPU usage percentage
}

// getMemoryUsage returns the current memory usage percentage
func (mm *MetricsManager) getMemoryUsage() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("[Metrics] Error getting memory usage: %v", err)
		return 0.0
	}
	return v.UsedPercent
}

// getDiskUsage returns the current disk usage percentage
func (mm *MetricsManager) getDiskUsage() float64 {
	parts, err := disk.Partitions(false)
	if err != nil {
		log.Printf("[Metrics] Error getting disk partitions: %v", err)
		return 0.0
	}

	if len(parts) == 0 {
		return 0.0
	}

	// Use the first partition as a sample
	usage, err := disk.Usage(parts[0].Mountpoint)
	if err != nil {
		log.Printf("[Metrics] Error getting disk usage: %v", err)
		return 0.0
	}

	return usage.UsedPercent
}

// GetMetricsCount returns the total number of metrics collections performed
func (mm *MetricsManager) GetMetricsCount() int64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.metricsCollected
}

// GetMetricsAsMap returns all current metrics as a map
func (mm *MetricsManager) GetMetricsAsMap() map[string]interface{} {
	mm.mu.RLock()
	currentMetrics := mm.currentMetrics
	mm.mu.RUnlock()

	// Use reflection or manual mapping to convert struct to map
	result := map[string]interface{}{
		"cpu_usage":          currentMetrics.CPUUsage,
		"memory_usage":       currentMetrics.MemoryUsage,
		"disk_usage":         currentMetrics.DiskUsage,
		"network_rx_bytes":   currentMetrics.NetworkRxBytes,
		"network_tx_bytes":   currentMetrics.NetworkTxBytes,
		"message_count":      currentMetrics.MessageCount,
		"message_rate":       currentMetrics.MessageRate,
		"active_topics":      currentMetrics.ActiveTopics,
		"active_subscribers": currentMetrics.ActiveSubscribers,
		"routing_table_size": currentMetrics.RoutingTableSize,
		"peer_count":         currentMetrics.PeerCount,
		"uptime":             currentMetrics.UpTime,
		"last_seen":          currentMetrics.LastSeen,
		"health_score":       currentMetrics.HealthScore,
	}

	// Add custom metrics
	mm.customMetricsMu.RLock()
	for k, v := range mm.customMetrics {
		result[k] = v
	}
	mm.customMetricsMu.RUnlock()

	return result
}
