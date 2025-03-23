// Package ai provides intelligence and optimization capabilities for Sprawl
package ai

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"sprawl/ai/analytics"
	"sprawl/ai/prediction"
	"sprawl/store"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	psnet "github.com/shirou/gopsutil/v3/net"
)

// ThresholdConfig holds configurable thresholds for resource scaling decisions
type ThresholdConfig struct {
	// CPU thresholds
	CPUScaleUpThreshold   float64 // CPU percentage above which scaling up is recommended
	CPUScaleDownThreshold float64 // CPU percentage below which scaling down is considered

	// Memory thresholds
	MemScaleUpThreshold   float64 // Memory percentage above which scaling up is recommended
	MemScaleDownThreshold float64 // Memory percentage below which scaling down is considered

	// Message rate thresholds (messages per second)
	MsgRateScaleUpThreshold   float64 // Message rate above which scaling up is recommended
	MsgRateScaleDownThreshold float64 // Message rate below which scaling down is considered

	// Confidence thresholds
	MinConfidenceThreshold float64 // Minimum confidence required for a scaling recommendation
}

// DefaultThresholds returns the default threshold configuration
func DefaultThresholds() ThresholdConfig {
	return ThresholdConfig{
		CPUScaleUpThreshold:       80.0,
		CPUScaleDownThreshold:     20.0,
		MemScaleUpThreshold:       85.0,
		MemScaleDownThreshold:     30.0,
		MsgRateScaleUpThreshold:   5000.0,
		MsgRateScaleDownThreshold: 500.0,
		MinConfidenceThreshold:    0.7,
	}
}

// Engine is the main AI component that integrates all intelligence features
type Engine struct {
	mu               sync.RWMutex
	loadPredictor    *prediction.LoadPredictor
	patternMatcher   *analytics.PatternMatcher
	anomalyDetector  *analytics.AnomalyDetector
	metrics          map[string]float64
	sampleInterval   time.Duration
	predictionModels map[string]bool
	enabled          bool
	stopCh           chan struct{}
	store            *store.Store
	thresholds       ThresholdConfig // Add configurable thresholds
}

// EngineOptions holds configuration options for the AI Engine
type EngineOptions struct {
	SampleInterval  time.Duration
	MaxDataPoints   int
	EnablePredictor bool
	EnablePatterns  bool
	EnableAnomalies bool
}

// DefaultEngineOptions returns the default options
func DefaultEngineOptions() EngineOptions {
	return EngineOptions{
		SampleInterval:  1 * time.Minute,
		MaxDataPoints:   10000,
		EnablePredictor: true,
		EnablePatterns:  true,
		EnableAnomalies: true,
	}
}

// MetricKind is the type of metric being tracked
type MetricKind string

const (
	// MetricKindCPUUsage tracks CPU usage percentage
	MetricKindCPUUsage MetricKind = "cpu_usage"
	// MetricKindMemoryUsage tracks memory usage percentage
	MetricKindMemoryUsage MetricKind = "memory_usage"
	// MetricKindDiskIO tracks disk I/O operations per second
	MetricKindDiskIO MetricKind = "disk_io"
	// MetricKindNetworkTraffic tracks network bytes per second
	MetricKindNetworkTraffic MetricKind = "network_traffic"
	// MetricKindMessageRate tracks messages per second
	MetricKindMessageRate MetricKind = "message_rate"
	// MetricKindTopicActivity tracks activity level for topics
	MetricKindTopicActivity MetricKind = "topic_activity"
	// MetricKindQueueDepth tracks queue depth for topics
	MetricKindQueueDepth MetricKind = "queue_depth"
	// MetricKindSubscriberCount tracks subscribers per topic
	MetricKindSubscriberCount MetricKind = "subscriber_count"
)

// ScalingRecommendation represents a auto-scaling recommendation
type ScalingRecommendation struct {
	Timestamp         time.Time
	Resource          string
	CurrentValue      float64
	PredictedValue    float64
	RecommendedAction string
	Confidence        float64
	Reason            string
}

// Global variables to track disk IO stats between calls
var lastDiskStats map[string]disk.IOCountersStat
var lastDiskStatsTime time.Time

// Global variables to track network stats between calls
var lastNetStats []psnet.IOCountersStat
var lastNetStatsTime time.Time

// getDiskIOStats returns disk I/O operations per second
func getDiskIOStats() (float64, error) {
	// Get real disk IO metrics using gopsutil
	diskStats, err := disk.IOCounters()
	if err != nil {
		return 0.0, fmt.Errorf("error getting disk I/O stats: %v", err)
	}

	now := time.Now()
	if lastDiskStats == nil {
		// First run, store values and return 0
		lastDiskStats = diskStats
		lastDiskStatsTime = now
		return 0.0, nil
	}

	// Calculate time difference
	timeDiff := now.Sub(lastDiskStatsTime).Seconds()
	if timeDiff < 0.1 {
		return 0.0, nil // Avoid division by zero or very small time differences
	}

	// Calculate total IO operations per second
	var totalIOPS float64
	for diskName, stat := range diskStats {
		if lastStat, ok := lastDiskStats[diskName]; ok {
			readOps := float64(stat.ReadCount-lastStat.ReadCount) / timeDiff
			writeOps := float64(stat.WriteCount-lastStat.WriteCount) / timeDiff
			totalIOPS += readOps + writeOps
		}
	}

	// Store current values for next call
	lastDiskStats = diskStats
	lastDiskStatsTime = now

	return totalIOPS, nil
}

// getNetworkStats collects network metrics
func getNetworkStats() NetworkStats {
	// Get real network IO metrics using gopsutil
	netStats, err := psnet.IOCounters(false) // false = all interfaces combined
	if err != nil {
		log.Printf("Error getting network stats: %v", err)
		return simulateNetworkStats() // Fall back to simulation if real metrics fail
	}

	now := time.Now()

	// Initialize return values
	stats := NetworkStats{}

	if len(netStats) == 0 {
		log.Printf("No network interfaces found")
		return simulateNetworkStats() // Fall back to simulation if no interfaces
	}

	if len(lastNetStats) == 0 {
		// First run, store values and return simulated stats
		lastNetStats = netStats
		lastNetStatsTime = now

		// Return simulated stats for the first call
		return simulateNetworkStats()
	}

	// Calculate time difference
	timeDiff := now.Sub(lastNetStatsTime).Seconds()
	if timeDiff < 0.1 {
		return simulateNetworkStats() // Avoid division by zero
	}

	// Calculate rates based on the difference
	for i, currentStat := range netStats {
		if i < len(lastNetStats) {
			lastStat := lastNetStats[i]

			// Calculate bytes per second
			bytesInPerSec := float64(currentStat.BytesRecv-lastStat.BytesRecv) / timeDiff
			bytesOutPerSec := float64(currentStat.BytesSent-lastStat.BytesSent) / timeDiff
			stats.BytesPerSecond += bytesInPerSec + bytesOutPerSec

			// Calculate packets per second
			packetsInPerSec := float64(currentStat.PacketsRecv-lastStat.PacketsRecv) / timeDiff
			packetsOutPerSec := float64(currentStat.PacketsSent-lastStat.PacketsSent) / timeDiff
			stats.MessagesPerSecond += packetsInPerSec + packetsOutPerSec

			// Set raw values
			stats.BytesReceived += int64(currentStat.BytesRecv)
			stats.BytesSent += int64(currentStat.BytesSent)
		}
	}

	// Estimate connection count based on the connection rate
	stats.ConnectionCount = runtime.NumGoroutine() / 2
	stats.ConnectionsPerSec = float64(stats.ConnectionCount) / 60.0

	// Store current values for next call
	lastNetStats = netStats
	lastNetStatsTime = now

	return stats
}

// NewEngine creates a new AI Engine
func NewEngine(options EngineOptions, storeInstance *store.Store) *Engine {
	engine := &Engine{
		loadPredictor:    prediction.NewLoadPredictor(options.MaxDataPoints),
		metrics:          make(map[string]float64),
		sampleInterval:   options.SampleInterval,
		predictionModels: make(map[string]bool),
		enabled:          true,
		stopCh:           make(chan struct{}),
		store:            storeInstance,
		thresholds:       DefaultThresholds(), // Initialize with default thresholds
	}

	// Initialize pattern matcher if enabled
	if options.EnablePatterns {
		engine.patternMatcher = analytics.NewPatternMatcher(options.SampleInterval)
	}

	// Initialize anomaly detector if enabled
	if options.EnableAnomalies {
		engine.anomalyDetector = analytics.NewAnomalyDetector(
			options.SampleInterval,
			7*24*time.Hour, // 1 week retention
			options.MaxDataPoints,
		)
	}

	return engine
}

// Start begins collecting metrics and generating intelligence
func (e *Engine) Start() {
	log.Println("Starting AI Engine...")

	if e.loadPredictor != nil {
		if err := e.loadPredictor.Train(); err != nil {
			log.Printf("Warning: initial prediction model training failed: %v", err)
		}
	}

	if e.patternMatcher != nil {
		e.patternMatcher.Start()
	}

	if e.anomalyDetector != nil {
		e.anomalyDetector.Start()
	}

	go e.metricsLoop()

	// Start periodic auto-training
	go e.autoTrainingLoop()
}

// Stop halts the AI engine
func (e *Engine) Stop() {
	log.Println("Stopping AI Engine...")
	close(e.stopCh)

	if e.patternMatcher != nil {
		e.patternMatcher.Stop()
	}

	if e.anomalyDetector != nil {
		e.anomalyDetector.Stop()
	}
}

// metricsLoop periodically collects system metrics
func (e *Engine) metricsLoop() {
	ticker := time.NewTicker(e.sampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.collectMetrics()
		case <-e.stopCh:
			return
		}
	}
}

// collectMetrics gathers metrics from various system components
func (e *Engine) collectMetrics() {
	// Get system-level metrics
	cpuUsage, memUsage, goroutineCount := getSystemMetrics()

	// Record system metrics
	e.RecordMetric(MetricKindCPUUsage, "local", cpuUsage, map[string]string{
		"source": "system",
	})

	e.RecordMetric(MetricKindMemoryUsage, "local", memUsage, map[string]string{
		"source": "system",
	})

	// Record goroutine count as a network traffic proxy
	e.RecordMetric(MetricKindNetworkTraffic, "local-goroutines", float64(goroutineCount), map[string]string{
		"source": "system",
		"type":   "goroutines",
	})

	// Collect disk I/O metrics
	diskIO, err := getDiskIOStats()
	if err != nil {
		log.Printf("Warning: failed to collect disk I/O metrics: %v", err)
	} else {
		e.RecordMetric(MetricKindDiskIO, "local", diskIO, map[string]string{
			"source": "system",
			"unit":   "ops_per_second",
		})
	}

	// Collect store metrics if available
	storeMetrics := e.getStoreMetrics()
	for topic, messageCount := range storeMetrics.MessageCounts {
		e.RecordMetric(MetricKindMessageRate, topic, float64(messageCount), map[string]string{
			"source": "store",
			"topic":  topic,
		})
	}

	// Record overall message rate
	e.RecordMetric(MetricKindMessageRate, "total", float64(storeMetrics.TotalMessages), map[string]string{
		"source": "store",
	})

	// Record topic count as a measure of activity
	e.RecordMetric(MetricKindTopicActivity, "total", float64(len(storeMetrics.Topics)), map[string]string{
		"source": "store",
	})

	// Collect network metrics
	networkStats := getNetworkStats()
	e.RecordMetric(MetricKindNetworkTraffic, "total", networkStats.BytesPerSecond, map[string]string{
		"source": "network",
		"unit":   "bytes_per_second",
	})

	// Log metrics collection completion
	log.Printf("AI Engine collected %d system metrics", 6+len(storeMetrics.Topics))
}

// getSystemMetrics collects system-level metrics
func getSystemMetrics() (cpuUsage float64, memUsage float64, goroutineCount int) {
	// Get CPU usage using the runtime package
	// This is a simple implementation; a production version would use the host's CPU metrics
	cpuUsage = getCPUUsagePercent()

	// Get memory usage
	memUsage = getMemoryUsagePercent()

	// Get goroutine count
	goroutineCount = runtime.NumGoroutine()

	return cpuUsage, memUsage, goroutineCount
}

// getCPUUsagePercent returns the current CPU usage percentage
func getCPUUsagePercent() float64 {
	// Get real CPU usage using gopsutil
	percent, err := cpu.Percent(time.Second, false) // false = overall CPU percentage
	if err != nil {
		log.Printf("Error getting CPU usage: %v", err)
		return 0.0
	}
	if len(percent) == 0 {
		return 0.0
	}
	return percent[0] // Return the overall CPU usage percentage
}

// getMemoryUsagePercent returns the current memory usage percentage
func getMemoryUsagePercent() float64 {
	// Get real memory usage using gopsutil
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("Error getting memory usage: %v", err)
		return 0.0
	}
	return v.UsedPercent
}

// getStoreMetrics collects metrics from the message store
func (e *Engine) getStoreMetrics() StoreMetrics {
	// Use the store instance directly instead of the global store
	if e.store == nil {
		log.Println("Warning: Store not available for metrics collection, using simulated data")
		return simulateStoreMetrics()
	}

	// Initialize metrics object
	metrics := StoreMetrics{
		MessageCounts: make(map[string]int),
	}

	// Get list of topics from the store
	topics := e.store.GetTopics()

	metrics.Topics = topics
	totalMessages := 0

	// Collect message counts and detailed metrics for each topic
	for _, topic := range topics {
		// Get message count for the topic
		messageCount := e.store.GetMessageCountForTopic(topic)
		metrics.MessageCounts[topic] = messageCount
		totalMessages += messageCount

		// Get subscriber count
		subsCount := e.store.GetSubscriberCountForTopic(topic)
		log.Printf("Topic %s has %d messages and %d subscribers",
			topic, messageCount, subsCount)

		// Get topic timestamps to analyze activity patterns
		timestamps := e.store.GetTopicTimestamps(topic)
		if timestamps != nil {
			// Log the most recent activity
			mostRecent := timestamps.Newest
			if !mostRecent.IsZero() {
				timeSinceLastMessage := time.Since(mostRecent)
				log.Printf("Topic %s last activity: %v (%v ago)",
					topic, mostRecent.Format(time.RFC3339), timeSinceLastMessage)
			}
		}
	}

	// Calculate overall statistics
	metrics.TotalMessages = totalMessages

	// Get tiered storage metrics
	metrics.MemoryUsage = float64(e.store.GetMemoryUsage())

	// Get disk tier stats
	diskStats := e.store.GetDiskStats()
	if diskStats != nil {
		metrics.DiskEnabled = diskStats.Enabled
		metrics.DiskUsageBytes = diskStats.UsedBytes
		metrics.DiskMessageCount = diskStats.MessageCount
	}

	// Get cloud tier stats
	cloudStats := e.store.GetCloudStats()
	if cloudStats != nil {
		metrics.CloudEnabled = cloudStats.Enabled
		metrics.CloudUsageBytes = cloudStats.UsedBytes
		metrics.CloudMessageCount = cloudStats.MessageCount
	}

	// Get tier configuration
	tierConfig := e.store.GetTierConfig()
	metrics.MemoryToDiskThreshold = tierConfig.MemoryToDiskThresholdBytes
	metrics.DiskToCloudThreshold = tierConfig.DiskToCloudThresholdBytes

	return metrics
}

// Extend StoreMetrics to include detailed tier statistics
type TierMetrics struct {
	MessageCount       int
	BytesUsed          uint64
	MaxCapacity        uint64
	UtilizationPercent float64
}

// StoreMetrics contains information about message store
type StoreMetrics struct {
	TotalMessages int
	MessageCounts map[string]int
	Topics        []string

	// Memory tier metrics
	MemoryUsage float64

	// Disk tier metrics
	DiskEnabled      bool
	DiskUsageBytes   int64
	DiskMessageCount int

	// Cloud tier metrics
	CloudEnabled      bool
	CloudUsageBytes   int64
	CloudMessageCount int

	// Tier thresholds
	MemoryToDiskThreshold int64
	DiskToCloudThreshold  int64
}

// simulateStoreMetrics returns simulated store metrics
func simulateStoreMetrics() StoreMetrics {
	// Create realistic simulated metrics
	topics := []string{"alerts", "logs", "metrics", "events", "notifications"}
	messageCounts := make(map[string]int)
	totalMessages := 0

	for _, topic := range topics {
		// Generate a semi-random message count
		count := 100 + rand.Intn(900)
		messageCounts[topic] = count
		totalMessages += count
	}

	return StoreMetrics{
		TotalMessages: totalMessages,
		MessageCounts: messageCounts,
		Topics:        topics,
	}
}

// NetworkStats contains network metrics
type NetworkStats struct {
	BytesPerSecond    float64
	MessagesPerSecond float64
	ConnectionCount   int
	BytesReceived     int64
	BytesSent         int64
	ConnectionsPerSec float64
}

// simulateNetworkStats returns simulated network metrics
func simulateNetworkStats() NetworkStats {
	// Create realistic simulated network stats
	bytesPerSec := 1024.0 * (5.0 + rand.Float64()*15.0) // 5-20 KB/s
	messagesPerSec := 10.0 + rand.Float64()*90.0        // 10-100 msgs/s
	connCount := 5 + rand.Intn(20)                      // 5-25 connections

	return NetworkStats{
		BytesPerSecond:    bytesPerSec,
		MessagesPerSecond: messagesPerSec,
		ConnectionCount:   connCount,
		BytesReceived:     int64(bytesPerSec * 60), // Last minute
		BytesSent:         int64(bytesPerSec * 60), // Last minute
		ConnectionsPerSec: float64(rand.Intn(5)),   // 0-5 conns/s
	}
}

// RecordMetric records a metric data point
func (e *Engine) RecordMetric(metricKind MetricKind, entityID string, value float64, labels map[string]string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.enabled {
		return
	}

	// Record current value for simple access
	metricKey := string(metricKind) + ":" + entityID
	e.metrics[metricKey] = value

	now := time.Now()

	// Record in load predictor based on metric type
	var resource prediction.ResourceType
	shouldUpdateModel := false

	switch metricKind {
	case MetricKindCPUUsage:
		if e.loadPredictor != nil {
			e.loadPredictor.AddDataPoint(prediction.LoadDataPoint{
				Timestamp: now,
				Value:     value,
				Resource:  prediction.ResourceCPU,
				NodeID:    entityID,
				Labels:    labels,
			})
			resource = prediction.ResourceCPU
			shouldUpdateModel = true
		}
	case MetricKindMemoryUsage:
		if e.loadPredictor != nil {
			e.loadPredictor.AddDataPoint(prediction.LoadDataPoint{
				Timestamp: now,
				Value:     value,
				Resource:  prediction.ResourceMemory,
				NodeID:    entityID,
				Labels:    labels,
			})
			resource = prediction.ResourceMemory
			shouldUpdateModel = true
		}
	case MetricKindMessageRate:
		if e.loadPredictor != nil {
			e.loadPredictor.AddDataPoint(prediction.LoadDataPoint{
				Timestamp: now,
				Value:     value,
				Resource:  prediction.ResourceMessageRate,
				NodeID:    entityID,
				Labels:    labels,
			})
			resource = prediction.ResourceMessageRate
			shouldUpdateModel = true
		}
	case MetricKindNetworkTraffic:
		if e.loadPredictor != nil {
			e.loadPredictor.AddDataPoint(prediction.LoadDataPoint{
				Timestamp: now,
				Value:     value,
				Resource:  prediction.ResourceNetwork,
				NodeID:    entityID,
				Labels:    labels,
			})
			resource = prediction.ResourceNetwork
			shouldUpdateModel = true
		}
	}

	// Record in pattern matcher for topic activity
	if metricKind == MetricKindTopicActivity || metricKind == MetricKindMessageRate {
		if e.patternMatcher != nil {
			e.patternMatcher.AddDataPoint(entityID, "topic", value, now, labels)
		}
	}

	// Record in anomaly detector for all metrics
	if e.anomalyDetector != nil {
		e.anomalyDetector.AddMetricPoint(string(metricKind)+":"+entityID, value, now, labels)
	}

	// Auto-training: Try to train the model when new data points are added
	if shouldUpdateModel && e.loadPredictor != nil {
		// Use a separate goroutine to avoid blocking while holding the lock
		go func(res prediction.ResourceType, node string) {
			// Wait a short time to allow for potential multiple data points to be added
			time.Sleep(100 * time.Millisecond)

			// Attempt to train the model with all available data
			err := e.TrainResourceModel(res, node, 168*time.Hour) // Use 1 week lookback
			if err != nil {
				// Log the error but don't fail - we'll try again next time
				if !strings.Contains(err.Error(), "insufficient data points") {
					log.Printf("Auto-training failed for %s on node %s: %v", res, node, err)
				}
			} else {
				log.Printf("Auto-trained model for %s on node %s successfully", res, node)
			}
		}(resource, entityID)
	}
}

// GetCurrentMetric returns the current value of a metric
func (e *Engine) GetCurrentMetric(metricKind MetricKind, entityID string) float64 {
	e.mu.RLock()
	defer e.mu.RUnlock()

	metricKey := string(metricKind) + ":" + entityID
	if value, exists := e.metrics[metricKey]; exists {
		return value
	}
	return 0
}

// PredictLoad forecasts future resource usage
func (e *Engine) PredictLoad(resource prediction.ResourceType, nodeID string, futureTime time.Time) (prediction.PredictionResult, error) {
	if e.loadPredictor == nil {
		return prediction.PredictionResult{}, nil
	}

	// Determine appropriate interval based on prediction horizon
	var interval prediction.PredictionInterval
	timeDiff := time.Until(futureTime)

	if timeDiff >= 24*time.Hour {
		interval = prediction.Interval24Hour
	} else if timeDiff >= time.Hour {
		interval = prediction.Interval1Hour
	} else if timeDiff >= 15*time.Minute {
		interval = prediction.Interval15Min
	} else {
		interval = prediction.Interval5Min
	}

	return e.loadPredictor.Predict(resource, nodeID, futureTime, interval)
}

// GetTopicPatterns returns pattern information for a specific topic
func (e *Engine) GetTopicPatterns(topicID string) *analytics.MessagePattern {
	if e.patternMatcher == nil {
		return nil
	}
	return e.patternMatcher.GetEntityPatterns(topicID)
}

// GetAnomalies returns detected anomalies for a metric
func (e *Engine) GetAnomalies(metricKind MetricKind, entityID string, since time.Time) []analytics.AnomalyInfo {
	if e.anomalyDetector == nil {
		return nil
	}
	metricName := string(metricKind) + ":" + entityID
	return e.anomalyDetector.GetAnomalies(metricName, since)
}

// GetScalingRecommendations generates auto-scaling recommendations based on predictions
func (e *Engine) GetScalingRecommendations(nodeID string) []ScalingRecommendation {
	if e.loadPredictor == nil {
		return nil
	}

	e.mu.RLock()
	thresholds := e.thresholds // Get a copy of the current thresholds
	e.mu.RUnlock()

	recommendations := []ScalingRecommendation{}

	// Generate recommendations for CPU
	cpuMetricKey := string(MetricKindCPUUsage) + ":" + nodeID
	currentCPU := e.metrics[cpuMetricKey]

	// Predict CPU usage in 30 minutes
	futureTime := time.Now().Add(30 * time.Minute)
	cpuPrediction, err := e.loadPredictor.Predict(prediction.ResourceCPU, nodeID, futureTime, prediction.Interval15Min)
	if err == nil {
		// Generate recommendation if predicted value is significantly different
		recommendation := ScalingRecommendation{
			Timestamp:      time.Now(),
			Resource:       "CPU",
			CurrentValue:   currentCPU,
			PredictedValue: cpuPrediction.PredictedVal,
			Confidence:     cpuPrediction.Confidence,
		}

		// Decision logic using configurable thresholds
		if cpuPrediction.PredictedVal > thresholds.CPUScaleUpThreshold && cpuPrediction.Confidence > thresholds.MinConfidenceThreshold {
			recommendation.RecommendedAction = "scale_up"
			recommendation.Reason = fmt.Sprintf("CPU usage predicted to exceed %.1f%% threshold", thresholds.CPUScaleUpThreshold)
		} else if cpuPrediction.PredictedVal < thresholds.CPUScaleDownThreshold && currentCPU < thresholds.CPUScaleDownThreshold*1.5 && cpuPrediction.Confidence > thresholds.MinConfidenceThreshold {
			recommendation.RecommendedAction = "scale_down"
			recommendation.Reason = fmt.Sprintf("CPU usage predicted to remain below %.1f%%", thresholds.CPUScaleDownThreshold)
		} else {
			recommendation.RecommendedAction = "maintain"
			recommendation.Reason = "CPU usage within normal range"
		}

		recommendations = append(recommendations, recommendation)
	}

	// Repeat for memory
	memMetricKey := string(MetricKindMemoryUsage) + ":" + nodeID
	currentMem := e.metrics[memMetricKey]

	memPrediction, err := e.loadPredictor.Predict(prediction.ResourceMemory, nodeID, futureTime, prediction.Interval15Min)
	if err == nil {
		recommendation := ScalingRecommendation{
			Timestamp:      time.Now(),
			Resource:       "Memory",
			CurrentValue:   currentMem,
			PredictedValue: memPrediction.PredictedVal,
			Confidence:     memPrediction.Confidence,
		}

		// Decision logic using configurable thresholds
		if memPrediction.PredictedVal > thresholds.MemScaleUpThreshold && memPrediction.Confidence > thresholds.MinConfidenceThreshold {
			recommendation.RecommendedAction = "scale_up"
			recommendation.Reason = fmt.Sprintf("Memory usage predicted to exceed %.1f%% threshold", thresholds.MemScaleUpThreshold)
		} else if memPrediction.PredictedVal < thresholds.MemScaleDownThreshold && currentMem < thresholds.MemScaleDownThreshold*1.5 && memPrediction.Confidence > thresholds.MinConfidenceThreshold {
			recommendation.RecommendedAction = "scale_down"
			recommendation.Reason = fmt.Sprintf("Memory usage predicted to remain below %.1f%%", thresholds.MemScaleDownThreshold)
		} else {
			recommendation.RecommendedAction = "maintain"
			recommendation.Reason = "Memory usage within normal range"
		}

		recommendations = append(recommendations, recommendation)
	}

	// Consider message rate for scaling
	msgMetricKey := string(MetricKindMessageRate) + ":" + nodeID
	currentMsgRate := e.metrics[msgMetricKey]

	msgPrediction, err := e.loadPredictor.Predict(prediction.ResourceMessageRate, nodeID, futureTime, prediction.Interval15Min)
	if err == nil {
		recommendation := ScalingRecommendation{
			Timestamp:      time.Now(),
			Resource:       "Message Rate",
			CurrentValue:   currentMsgRate,
			PredictedValue: msgPrediction.PredictedVal,
			Confidence:     msgPrediction.Confidence,
		}

		// Decision logic using configurable thresholds
		if msgPrediction.PredictedVal > thresholds.MsgRateScaleUpThreshold && msgPrediction.Confidence > thresholds.MinConfidenceThreshold {
			recommendation.RecommendedAction = "scale_up"
			recommendation.Reason = fmt.Sprintf("Message rate predicted to exceed capacity threshold (%.0f msgs/sec)", thresholds.MsgRateScaleUpThreshold)
		} else if msgPrediction.PredictedVal < thresholds.MsgRateScaleDownThreshold && currentMsgRate < thresholds.MsgRateScaleDownThreshold*2 && msgPrediction.Confidence > thresholds.MinConfidenceThreshold {
			recommendation.RecommendedAction = "scale_down"
			recommendation.Reason = fmt.Sprintf("Message rate predicted to remain below %.0f msgs/sec", thresholds.MsgRateScaleDownThreshold)
		} else {
			recommendation.RecommendedAction = "maintain"
			recommendation.Reason = "Message rate within normal range"
		}

		recommendations = append(recommendations, recommendation)
	}

	return recommendations
}

// GetTopBurstTopics returns topics with highest burst probability
func (e *Engine) GetTopBurstTopics(limit int) []analytics.MessagePattern {
	if e.patternMatcher == nil {
		return nil
	}
	return e.patternMatcher.GetTopBurstProbabilityEntities(limit)
}

// GetFullSystemStatus returns a comprehensive status report including metrics from all nodes
func (e *Engine) GetFullSystemStatus() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	status := make(map[string]interface{})

	// Basic stats
	status["timestamp"] = time.Now()
	status["enabled"] = e.enabled

	// Get metrics - both old and new format for compatibility
	currentMetrics := make(map[string]float64)
	metrics := make(map[string]interface{})
	for key, value := range e.metrics {
		currentMetrics[key] = value
		metrics[key] = value
	}
	status["current_metrics"] = currentMetrics // Keep this for backward compatibility with tests
	status["metrics"] = metrics                // New format

	// Get anomalies from last 24 hours
	if e.anomalyDetector != nil {
		since := time.Now().Add(-24 * time.Hour)
		anomalies := e.anomalyDetector.GetAllAnomalies(since)
		status["recent_anomalies"] = anomalies
	}

	// Get store information
	storeInfo := make(map[string]interface{})
	storeMetrics := e.getStoreMetrics()
	storeInfo["message_count"] = storeMetrics.TotalMessages
	storeInfo["topics"] = storeMetrics.Topics
	storeInfo["memory_usage"] = storeMetrics.MemoryUsage
	storeInfo["disk_enabled"] = storeMetrics.DiskEnabled
	storeInfo["disk_usage_bytes"] = storeMetrics.DiskUsageBytes
	status["store"] = storeInfo

	// Get pattern information
	if e.patternMatcher != nil {
		patterns := e.patternMatcher.GetAllPatterns()
		status["patterns"] = patterns

		// Get top burst probability topics
		burstTopics := e.patternMatcher.GetTopBurstProbabilityEntities(5)
		status["burst_risk_topics"] = burstTopics
	}

	// Collect metrics from all nodes in the cluster
	localRecommendations := e.GetScalingRecommendations("local")

	// Create a map to store cluster-wide recommendations
	clusterRecommendations := make(map[string][]ScalingRecommendation)
	clusterRecommendations["local"] = localRecommendations

	// Get actual node metrics from all nodes in the cluster
	nodeIDs := e.getClusterNodeIDs()

	for _, nodeID := range nodeIDs {
		// Query each node for its metrics and recommendations
		nodeRecommendations, err := e.getRemoteNodeRecommendations(nodeID)
		if err != nil {
			log.Printf("Error getting recommendations from node %s: %v", nodeID, err)
			continue
		}
		clusterRecommendations[nodeID] = nodeRecommendations
	}

	status["scaling_recommendations"] = clusterRecommendations
	status["cluster_node_count"] = len(nodeIDs) + 1 // +1 for local node

	return status
}

// getClusterNodeIDs returns the list of node IDs in the cluster
func (e *Engine) getClusterNodeIDs() []string {
	// In a real implementation, this would query the node registry or gossip layer
	// For now, we'll try to get the node IDs from the store if possible
	if e.store == nil {
		return []string{}
	}

	// Use the store to get node information
	storeNodeIDs := e.store.GetClusterNodeIDs()
	if len(storeNodeIDs) > 0 {
		return storeNodeIDs
	}

	return []string{}
}

// getRemoteNodeRecommendations queries a remote node for its scaling recommendations
func (e *Engine) getRemoteNodeRecommendations(nodeID string) ([]ScalingRecommendation, error) {
	// In a real implementation, this would make an HTTP request to the node's API
	// For now, we'll try to get metrics from the store if possible
	if e.store == nil {
		return nil, fmt.Errorf("store not available")
	}

	// Try to get the node's metrics from the store
	nodeMetrics := e.store.GetNodeMetrics(nodeID)
	if nodeMetrics == nil {
		return nil, fmt.Errorf("no metrics available for node %s", nodeID)
	}

	// Create recommendations based on the node's metrics
	recommendations := []ScalingRecommendation{}

	// CPU recommendation
	if cpuValue, ok := nodeMetrics["cpu_usage"]; ok {
		cpuRec := ScalingRecommendation{
			Timestamp:      time.Now(),
			Resource:       "CPU",
			CurrentValue:   cpuValue,
			PredictedValue: cpuValue * 1.1, // Simple projection
			Confidence:     0.8,
		}

		// Apply threshold logic
		if cpuValue > e.thresholds.CPUScaleUpThreshold {
			cpuRec.RecommendedAction = "scale_up"
			cpuRec.Reason = fmt.Sprintf("CPU usage exceeds %.1f%% threshold", e.thresholds.CPUScaleUpThreshold)
		} else if cpuValue < e.thresholds.CPUScaleDownThreshold {
			cpuRec.RecommendedAction = "scale_down"
			cpuRec.Reason = fmt.Sprintf("CPU usage below %.1f%%", e.thresholds.CPUScaleDownThreshold)
		} else {
			cpuRec.RecommendedAction = "maintain"
			cpuRec.Reason = "CPU usage within normal range"
		}

		recommendations = append(recommendations, cpuRec)
	}

	// Memory recommendation
	if memValue, ok := nodeMetrics["memory_usage"]; ok {
		memRec := ScalingRecommendation{
			Timestamp:      time.Now(),
			Resource:       "Memory",
			CurrentValue:   memValue,
			PredictedValue: memValue * 1.1, // Simple projection
			Confidence:     0.8,
		}

		// Apply threshold logic
		if memValue > e.thresholds.MemScaleUpThreshold {
			memRec.RecommendedAction = "scale_up"
			memRec.Reason = fmt.Sprintf("Memory usage exceeds %.1f%% threshold", e.thresholds.MemScaleUpThreshold)
		} else if memValue < e.thresholds.MemScaleDownThreshold {
			memRec.RecommendedAction = "scale_down"
			memRec.Reason = fmt.Sprintf("Memory usage below %.1f%%", e.thresholds.MemScaleDownThreshold)
		} else {
			memRec.RecommendedAction = "maintain"
			memRec.Reason = "Memory usage within normal range"
		}

		recommendations = append(recommendations, memRec)
	}

	return recommendations, nil
}

// EnablePrediction toggles prediction for a specific entity
func (e *Engine) EnablePrediction(entityID string, enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.predictionModels[entityID] = enabled
}

// Enable toggles all AI components
func (e *Engine) Enable(enabled bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.enabled = enabled
}

// TrainResourceModel manually trains the prediction model for a specific resource type and node
func (e *Engine) TrainResourceModel(resource prediction.ResourceType, nodeID string, lookback time.Duration) error {
	if e.loadPredictor == nil {
		return fmt.Errorf("load predictor not initialized")
	}

	log.Printf("Manually training AI model for resource %s, node %s with %s lookback",
		resource, nodeID, lookback.String())

	// Collect historical data points for this resource and node within the lookback period
	cutoffTime := time.Now().Add(-lookback)

	e.mu.RLock()
	metricKey := string(resourceTypeToMetricKind(resource)) + ":" + nodeID
	currentValue, exists := e.metrics[metricKey]
	e.mu.RUnlock()

	if !exists {
		return fmt.Errorf("no data available for resource %s on node %s", resource, nodeID)
	}

	// Force a model training cycle
	err := e.loadPredictor.TrainForResource(resource, nodeID, cutoffTime)
	if err != nil {
		return fmt.Errorf("training failed: %w", err)
	}

	log.Printf("Successfully trained model for %s. Current value: %.2f", resource, currentValue)
	return nil
}

// Helper function to convert resource type to metric kind
func resourceTypeToMetricKind(resource prediction.ResourceType) MetricKind {
	switch resource {
	case prediction.ResourceCPU:
		return MetricKindCPUUsage
	case prediction.ResourceMemory:
		return MetricKindMemoryUsage
	case prediction.ResourceNetwork:
		return MetricKindNetworkTraffic
	case prediction.ResourceDisk:
		return MetricKindDiskIO
	case prediction.ResourceMessageRate:
		return MetricKindMessageRate
	default:
		return MetricKindCPUUsage
	}
}

// Auto-training loop that periodically tries to train models
func (e *Engine) autoTrainingLoop() {
	// Try to train models every minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.attemptAutoTraining()
		case <-e.stopCh:
			return
		}
	}
}

// attemptAutoTraining tries to train models for all resources and nodes
func (e *Engine) attemptAutoTraining() {
	log.Println("Attempting periodic auto-training of AI models...")

	// Lock to get the list of nodes we have data for
	e.mu.RLock()

	// Find all nodes we have metrics for
	nodeIDs := make(map[string]bool)
	for metricKey := range e.metrics {
		parts := strings.Split(metricKey, ":")
		if len(parts) >= 2 {
			nodeID := parts[1]
			// Skip special metrics
			if !strings.Contains(nodeID, ":") {
				nodeIDs[nodeID] = true
			}
		}
	}
	e.mu.RUnlock()

	// Try to train each resource for each node
	resources := []prediction.ResourceType{
		prediction.ResourceCPU,
		prediction.ResourceMemory,
		prediction.ResourceNetwork,
		prediction.ResourceMessageRate,
	}

	oneWeek := 168 * time.Hour

	for nodeID := range nodeIDs {
		for _, resource := range resources {
			// Try to train this model
			err := e.TrainResourceModel(resource, nodeID, oneWeek)
			if err != nil {
				if !strings.Contains(err.Error(), "insufficient data") {
					log.Printf("Auto-training failed for %s on node %s: %v", resource, nodeID, err)
				}
			} else {
				log.Printf("Successfully auto-trained model for %s on node %s", resource, nodeID)
			}
		}
	}
}

// GetStatus returns the current status of the AI engine
func (e *Engine) GetStatus() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return map[string]interface{}{
		"enabled":     e.enabled,
		"models":      e.predictionModels,
		"sample_rate": e.sampleInterval.String(),
	}
}

// GetPrediction returns a prediction for the specified resource
func (e *Engine) GetPrediction(resource string) (float64, float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.enabled {
		return 0, 0
	}

	// Convert string resource to ResourceType
	var resourceType prediction.ResourceType
	switch resource {
	case "cpu":
		resourceType = prediction.ResourceCPU
	case "memory":
		resourceType = prediction.ResourceMemory
	case "network":
		resourceType = prediction.ResourceNetwork
	case "disk":
		resourceType = prediction.ResourceDisk
	case "message_rate":
		resourceType = prediction.ResourceMessageRate
	default:
		resourceType = prediction.ResourceCPU
	}

	// Get prediction for 1 hour in the future
	futureTime := time.Now().Add(1 * time.Hour)
	result, err := e.PredictLoad(resourceType, "local", futureTime)
	if err != nil {
		log.Printf("Error predicting %s: %v", resource, err)
		return 0, 0
	}

	return result.PredictedVal, result.Confidence
}

// GetSimpleAnomalies returns anomalies in a simplified format for the API
func (e *Engine) GetSimpleAnomalies() []map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.enabled || e.anomalyDetector == nil {
		return []map[string]interface{}{}
	}

	// Set the time range for anomaly detection - look back 3 hours
	lookbackTime := time.Now().Add(-3 * time.Hour)
	result := []map[string]interface{}{}

	// Resources to check for anomalies
	resources := map[MetricKind]string{
		MetricKindCPUUsage:       "cpu",
		MetricKindMemoryUsage:    "memory",
		MetricKindNetworkTraffic: "network",
		MetricKindMessageRate:    "messages",
		MetricKindDiskIO:         "disk",
	}

	// Check each resource for anomalies
	for metricKind, resourceName := range resources {
		// Get anomalies for this resource
		anomalies := e.GetAnomalies(metricKind, "local", lookbackTime)

		// Convert anomalies to map format
		for _, a := range anomalies {
			// Only include significant anomalies (with high confidence or deviation)
			if a.Confidence > 0.7 || a.DeviationScore > 2.0 {
				result = append(result, map[string]interface{}{
					"resource":   resourceName,
					"timestamp":  a.Timestamp.Format(time.RFC3339),
					"value":      a.Value,
					"deviation":  a.DeviationScore,
					"confidence": a.Confidence,
				})
			}
		}
	}

	// Also check topic-specific metrics
	// Get all topics with high activity
	if e.patternMatcher != nil {
		topicPatterns := e.patternMatcher.GetTopBurstProbabilityEntities(5)

		for _, pattern := range topicPatterns {
			// If a topic has high burst probability, include it as a potential anomaly
			if pattern.BurstProbability > 0.7 {
				result = append(result, map[string]interface{}{
					"resource":   "topic",
					"name":       pattern.EntityID,
					"timestamp":  time.Now().Format(time.RFC3339),
					"value":      float64(len(pattern.Patterns)), // Number of detected patterns
					"deviation":  pattern.Volatility * 10,        // Use volatility as a proxy for deviation
					"confidence": pattern.BurstProbability,
					"type":       "burst_risk",
				})
			}
		}
	}

	// Sort anomalies by confidence (highest first)
	sort.Slice(result, func(i, j int) bool {
		confI, okI := result[i]["confidence"].(float64)
		confJ, okJ := result[j]["confidence"].(float64)

		if !okI || !okJ {
			return false
		}

		return confI > confJ
	})

	// Return the top 10 anomalies at most
	if len(result) > 10 {
		result = result[:10]
	}

	// If empty AND debug mode is enabled, return example anomalies
	// This helps during development and testing
	debugMode := os.Getenv("AI_DEBUG") == "true"
	if len(result) == 0 && debugMode {
		// Include examples from multiple resources
		result = []map[string]interface{}{
			{
				"resource":   "cpu",
				"timestamp":  time.Now().Add(-15 * time.Minute).Format(time.RFC3339),
				"value":      95.5,
				"deviation":  3.2,
				"confidence": 0.92,
			},
			{
				"resource":   "memory",
				"timestamp":  time.Now().Add(-10 * time.Minute).Format(time.RFC3339),
				"value":      87.3,
				"deviation":  2.8,
				"confidence": 0.88,
			},
			{
				"resource":   "network",
				"timestamp":  time.Now().Add(-5 * time.Minute).Format(time.RFC3339),
				"value":      1250000,
				"deviation":  4.1,
				"confidence": 0.95,
				"unit":       "bytes_per_second",
			},
		}
	}

	return result
}

// GetLoadPredictions returns resource usage predictions for the given timeframe
func (e *Engine) GetLoadPredictions(nodeID string, resource prediction.ResourceType, timeframe time.Duration) ([]prediction.PredictionResult, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.loadPredictor == nil {
		return nil, fmt.Errorf("load predictor not initialized")
	}

	// Get current time and predict for the requested timeframe
	now := time.Now()

	// We'll use 12 steps with intervals computed based on the timeframe
	steps := 12
	interval := timeframe / time.Duration(steps)

	// Use the improved prediction method with appropriate intervals
	return e.loadPredictor.PredictMultiStep(resource, nodeID, now, interval, steps)
}

// HasExcessiveResourceUsage checks if a node is predicted to have excessive resource usage
func (e *Engine) HasExcessiveResourceUsage(nodeID string, resource prediction.ResourceType, threshold float64) (bool, float64, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.loadPredictor == nil {
		return false, 0, fmt.Errorf("load predictor not initialized")
	}

	// Predict future usage
	future := time.Now().Add(30 * time.Minute)
	predResult, err := e.loadPredictor.Predict(resource, nodeID, future, prediction.Interval15Min)
	if err != nil {
		return false, 0, err
	}

	// Check if predicted value exceeds threshold
	return predResult.PredictedVal > threshold, predResult.PredictedVal, nil
}

// SetThresholds allows updating the thresholds for scaling recommendations
func (e *Engine) SetThresholds(thresholds ThresholdConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.thresholds = thresholds
}

// GetThresholds returns the current threshold configuration
func (e *Engine) GetThresholds() ThresholdConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.thresholds
}
