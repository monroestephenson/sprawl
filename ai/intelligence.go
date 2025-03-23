// Package ai provides intelligence and optimization capabilities for Sprawl
package ai

import (
	"fmt"
	"log"
	"math"
	"runtime"
	"sprawl/store"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	psnet "github.com/shirou/gopsutil/v3/net"
)

// MetricType defines the type of metric being tracked
type MetricType string

const (
	// MetricMessageCount represents message volume metrics
	MetricMessageCount MetricType = "message_count"
	// MetricLatency represents message processing latency
	MetricLatency MetricType = "latency"
	// MetricResourceUsage represents resource utilization
	MetricResourceUsage MetricType = "resource_usage"
	// MetricNetworkTraffic represents network traffic metrics
	MetricNetworkTraffic MetricType = "network_traffic"
	// MetricCPU represents CPU usage metrics
	MetricCPU MetricType = "cpu"
	// MetricMemory represents memory usage metrics
	MetricMemory MetricType = "memory"
	// MetricSystem represents system metrics
	MetricSystem MetricType = "system"
	// MetricStorage represents storage usage metrics
	MetricStorage MetricType = "storage"
	// MetricNetwork represents network activity metrics
	MetricNetwork MetricType = "network"
)

// TimeSeriesDataPoint represents a single data point in a time series
type TimeSeriesDataPoint struct {
	Timestamp time.Time
	Value     float64
	Labels    map[string]string
}

// TimeSeriesData represents a collection of time-series data points
type TimeSeriesData struct {
	MetricType MetricType
	Points     []TimeSeriesDataPoint
}

// PredictionResult represents the output of a prediction model
type PredictionResult struct {
	MetricType   MetricType
	Timestamp    time.Time
	PredictedVal float64
	Confidence   float64
	Explanation  string
}

// Intelligence manages AI-powered features in Sprawl
type Intelligence struct {
	mu               sync.RWMutex
	timeSeriesData   map[MetricType]map[string]TimeSeriesData
	predictionModels map[MetricType]PredictionModel
	trafficAnalyzer  *TrafficAnalyzer
	anomalyDetector  *AnomalyDetector
	sampleInterval   time.Duration
	stopCh           chan struct{}
	dataLock         sync.RWMutex
}

// PredictionModel defines the interface for prediction models
type PredictionModel interface {
	// Train trains the model with historical data
	Train(data TimeSeriesData) error

	// Predict generates predictions for future values
	Predict(horizon time.Duration) ([]PredictionResult, error)

	// Update incrementally updates the model with new data
	Update(point TimeSeriesDataPoint) error
}

// TrafficAnalyzer analyzes message traffic patterns
type TrafficAnalyzer struct {
	topicPatterns map[string]TimeSeriesData
	nodePatterns  map[string]TimeSeriesData
}

// AnomalyDetector detects anomalies in metrics
type AnomalyDetector struct {
	thresholds    map[MetricType]float64
	detectionAlgo string
}

// Global variables to track network stats between calls
var lastNetIOStats []psnet.IOCountersStat
var lastNetIOStatsTime time.Time

// NewIntelligence creates a new Intelligence instance
func NewIntelligence(sampleInterval time.Duration) *Intelligence {
	return &Intelligence{
		timeSeriesData:   make(map[MetricType]map[string]TimeSeriesData),
		predictionModels: make(map[MetricType]PredictionModel),
		trafficAnalyzer: &TrafficAnalyzer{
			topicPatterns: make(map[string]TimeSeriesData),
			nodePatterns:  make(map[string]TimeSeriesData),
		},
		anomalyDetector: &AnomalyDetector{
			thresholds: map[MetricType]float64{
				MetricMessageCount:  100.0, // Default threshold for message count spikes
				MetricLatency:       200.0, // Default threshold for latency (ms)
				MetricResourceUsage: 80.0,  // Default threshold for resource usage (%)
			},
			detectionAlgo: "z-score", // Default detection algorithm
		},
		sampleInterval: sampleInterval,
		stopCh:         make(chan struct{}),
	}
}

// Start begins collecting and analyzing metrics
func (i *Intelligence) Start() {
	log.Println("Starting AI Intelligence system...")

	// Initialize metric maps
	i.timeSeriesData[MetricMessageCount] = make(map[string]TimeSeriesData)
	i.timeSeriesData[MetricLatency] = make(map[string]TimeSeriesData)
	i.timeSeriesData[MetricResourceUsage] = make(map[string]TimeSeriesData)
	i.timeSeriesData[MetricNetworkTraffic] = make(map[string]TimeSeriesData)

	// Start background collection
	go i.collectMetrics()
}

// Stop halts metric collection and analysis
func (i *Intelligence) Stop() {
	log.Println("Stopping AI Intelligence system...")
	close(i.stopCh)
}

// collectMetrics periodically collects system metrics
func (i *Intelligence) collectMetrics() {
	ticker := time.NewTicker(i.sampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			i.sampleMetrics()
		case <-i.stopCh:
			return
		}
	}
}

// sampleMetrics collects a single sample of metrics
func (i *Intelligence) sampleMetrics() {
	// Collect metrics from system components
	log.Println("Sampling system metrics for AI analysis...")

	// Get system metrics (CPU, memory, goroutines)
	cpuPercent, err := getCPUUsage()
	if err != nil {
		log.Printf("Error collecting CPU metrics: %v", err)
	} else {
		i.AddMetric(MetricCPU, "system", cpuPercent, map[string]string{
			"source": "system",
			"type":   "usage",
		})
	}

	memPercent := getMemoryUsage()
	i.AddMetric(MetricMemory, "system", memPercent, map[string]string{
		"source": "system",
		"type":   "usage",
	})

	goroutines := runtime.NumGoroutine()
	i.AddMetric(MetricSystem, "goroutines", float64(goroutines), map[string]string{
		"source": "runtime",
		"type":   "goroutines",
	})

	// Get storage metrics if available
	storageUsage := estimateStorageUsage()
	i.AddMetric(MetricStorage, "main", storageUsage, map[string]string{
		"source": "store",
		"type":   "usage",
	})

	// Sample network metrics
	networkActivity := estimateNetworkActivity()
	i.AddMetric(MetricNetwork, "traffic", networkActivity, map[string]string{
		"source": "network",
		"type":   "traffic",
	})
}

// getCPUUsage gets the current CPU usage
func getCPUUsage() (float64, error) {
	// Get real CPU usage using gopsutil
	percent, err := cpu.Percent(time.Second, false) // false = overall CPU percentage
	if err != nil {
		log.Printf("Error getting CPU usage: %v", err)

		// Fall back to synthetic estimation if real metrics fail
		numCPU := runtime.NumCPU()
		goroutines := runtime.NumGoroutine()
		estimate := float64(goroutines) / float64(numCPU) * 10

		// Ensure value is between 0-100
		if estimate > 100 {
			estimate = 100
		}

		return estimate, nil
	}

	if len(percent) == 0 {
		return 0.0, fmt.Errorf("no CPU percentage data received")
	}

	return percent[0], nil // Return the overall CPU usage percentage
}

// getMemoryUsage gets the current memory usage percentage
func getMemoryUsage() float64 {
	// Get real memory usage using gopsutil
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("Error getting memory usage: %v", err)

		// Fall back to synthetic estimation if real metrics fail
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		return float64(memStats.Alloc) / float64(memStats.Sys) * 100.0
	}

	return v.UsedPercent
}

// estimateStorageUsage estimates storage usage
func estimateStorageUsage() float64 {
	// Get a reference to the global store
	store := store.GetGlobalStore()
	if store == nil {
		// If store is not available, return a reasonable default
		log.Println("Warning: Store not available for storage usage estimation")
		return 50.0
	}

	// Calculate memory usage percentage
	var memoryUsedPercent float64

	// Get memory stats
	memStats := store.GetMemoryUsage()
	totalMemory := int64(0)

	// Use gopsutil to get total system memory
	v, err := mem.VirtualMemory()
	if err == nil {
		totalMemory = int64(v.Total)
	} else {
		// Fallback: use a reasonable default if we can't get system memory
		totalMemory = 8 * 1024 * 1024 * 1024 // Assume 8GB
	}

	// Calculate memory usage percentage
	if totalMemory > 0 {
		memoryUsedPercent = float64(memStats) / float64(totalMemory) * 100.0
	} else {
		memoryUsedPercent = 50.0 // Default if we can't calculate
	}

	// Get disk storage stats
	diskStats := store.GetDiskStats()
	var diskUsedPercent float64
	if diskStats != nil && diskStats.Enabled {
		// Get disk space info using gopsutil
		diskInfo, err := disk.Usage(store.GetTierConfig().DiskPath)
		if err == nil {
			diskUsedPercent = diskInfo.UsedPercent
		} else {
			// Fallback: estimate based on message count
			diskUsedPercent = float64(diskStats.MessageCount) / 1000000.0 * 100.0 // Assume 1M messages = 100%
			if diskUsedPercent > 100.0 {
				diskUsedPercent = 100.0
			}
		}
	} else {
		diskUsedPercent = 0.0 // Disk storage not enabled
	}

	// Get cloud storage stats
	cloudStats := store.GetCloudStats()
	var cloudUsedPercent float64
	if cloudStats != nil && cloudStats.Enabled {
		// Convert bytes to percentage (assume 10GB max for cloud)
		cloudMaxBytes := int64(10 * 1024 * 1024 * 1024)
		cloudUsedPercent = float64(cloudStats.UsedBytes) / float64(cloudMaxBytes) * 100.0
		if cloudUsedPercent > 100.0 {
			cloudUsedPercent = 100.0
		}
	} else {
		cloudUsedPercent = 0.0 // Cloud storage not enabled
	}

	// Calculate weighted average based on which tiers are enabled
	totalWeight := 1.0 // Memory always enabled
	weightedSum := memoryUsedPercent

	if diskStats != nil && diskStats.Enabled {
		totalWeight += 1.0
		weightedSum += diskUsedPercent
	}

	if cloudStats != nil && cloudStats.Enabled {
		totalWeight += 1.0
		weightedSum += cloudUsedPercent
	}

	return weightedSum / totalWeight
}

// estimateNetworkActivity estimates network activity
func estimateNetworkActivity() float64 {
	// Get real network IO metrics using gopsutil
	netStats, err := psnet.IOCounters(false) // false = all interfaces combined
	if err != nil {
		log.Printf("Error getting network stats: %v", err)

		// Fall back to synthetic estimation if real metrics fail
		goroutines := runtime.NumGoroutine()
		return float64(goroutines) * (0.5 + 0.5*float64(time.Now().UnixNano()%100)/100.0)
	}

	now := time.Now()

	if len(netStats) == 0 {
		log.Printf("No network interfaces found")

		// Fall back to synthetic estimation if no interfaces
		goroutines := runtime.NumGoroutine()
		return float64(goroutines) * (0.5 + 0.5*float64(time.Now().UnixNano()%100)/100.0)
	}

	if len(lastNetIOStats) == 0 {
		// First run, store values and return synthetic value
		lastNetIOStats = netStats
		lastNetIOStatsTime = now

		// Return synthetic value for the first call
		goroutines := runtime.NumGoroutine()
		return float64(goroutines) * (0.5 + 0.5*float64(now.UnixNano()%100)/100.0)
	}

	// Calculate time difference
	timeDiff := now.Sub(lastNetIOStatsTime).Seconds()
	if timeDiff < 0.1 {
		// Avoid division by zero or very small time differences
		goroutines := runtime.NumGoroutine()
		return float64(goroutines) * (0.5 + 0.5*float64(now.UnixNano()%100)/100.0)
	}

	// Calculate total network bytes per second across all interfaces
	var totalBytesPerSec float64
	for i, currentStat := range netStats {
		if i < len(lastNetIOStats) {
			lastStat := lastNetIOStats[i]

			// Calculate bytes per second
			bytesInPerSec := float64(currentStat.BytesRecv-lastStat.BytesRecv) / timeDiff
			bytesOutPerSec := float64(currentStat.BytesSent-lastStat.BytesSent) / timeDiff
			totalBytesPerSec += bytesInPerSec + bytesOutPerSec
		}
	}

	// Store current values for next call
	lastNetIOStats = netStats
	lastNetIOStatsTime = now

	return totalBytesPerSec
}

// AddMetric adds a new metric data point
func (i *Intelligence) AddMetric(metricType MetricType, identifier string, value float64, labels map[string]string) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Create data point
	dataPoint := TimeSeriesDataPoint{
		Timestamp: time.Now(),
		Value:     value,
		Labels:    labels,
	}

	// Initialize series if needed
	if _, exists := i.timeSeriesData[metricType]; !exists {
		i.timeSeriesData[metricType] = make(map[string]TimeSeriesData)
	}

	// Get existing series or create new one
	series, exists := i.timeSeriesData[metricType][identifier]
	if !exists {
		series = TimeSeriesData{
			MetricType: metricType,
			Points:     []TimeSeriesDataPoint{},
		}
	}

	// Add point to series
	series.Points = append(series.Points, dataPoint)

	// Store updated series
	i.timeSeriesData[metricType][identifier] = series

	// Update prediction models if available
	if model, exists := i.predictionModels[metricType]; exists {
		_ = model.Update(dataPoint) // Ignoring error for now
	}
}

// PredictFutureLoad predicts future load for a specific metric and identifier
func (i *Intelligence) PredictFutureLoad(metricType MetricType, identifier string, horizon time.Duration) ([]PredictionResult, error) {
	i.mu.RLock()
	model, exists := i.predictionModels[metricType]
	i.mu.RUnlock()

	if !exists {
		log.Printf("No prediction model available for metric type: %s", metricType)
		return nil, nil
	}

	return model.Predict(horizon)
}

// DetectAnomalies checks for anomalies in recent metrics
func (i *Intelligence) DetectAnomalies() map[string][]TimeSeriesDataPoint {
	// Real implementation of anomaly detection
	anomalies := make(map[string][]TimeSeriesDataPoint)

	// Lock data access
	i.dataLock.RLock()
	defer i.dataLock.RUnlock()

	// Check for anomalies in different metric types
	resourceMetricsToCheck := []MetricType{
		MetricResourceUsage,
		MetricLatency,
		MetricMessageCount,
		MetricNetworkTraffic,
	}

	// Time window for anomaly detection (last hour)
	lookbackPeriod := time.Hour
	now := time.Now()
	cutoff := now.Add(-lookbackPeriod)

	for _, metricType := range resourceMetricsToCheck {
		// Check each resource for this metric type
		if resourceData, exists := i.timeSeriesData[metricType]; exists {
			for resource, tsData := range resourceData {
				// Get only data points within the lookback period
				var recentPoints []TimeSeriesDataPoint
				for _, point := range tsData.Points {
					if point.Timestamp.After(cutoff) {
						recentPoints = append(recentPoints, point)
					}
				}

				// We need enough data points for meaningful anomaly detection
				if len(recentPoints) < 10 {
					continue
				}

				// Compute mean and standard deviation
				mean, stdDev := computeStats(recentPoints)

				// Check for anomalies (points outside 3 standard deviations)
				thresholdHigh := mean + 3*stdDev
				thresholdLow := mean - 3*stdDev

				// Find anomalous points
				var anomalousPoints []TimeSeriesDataPoint
				for _, point := range recentPoints {
					if point.Value > thresholdHigh || point.Value < thresholdLow {
						// This is an anomaly
						anomalousPoints = append(anomalousPoints, point)
					}
				}

				// If we found anomalies, add them to the result
				if len(anomalousPoints) > 0 {
					metricResource := fmt.Sprintf("%s:%s", metricType, resource)
					anomalies[metricResource] = anomalousPoints
				}
			}
		}
	}

	return anomalies
}

// computeStats calculates mean and standard deviation for a set of data points
func computeStats(points []TimeSeriesDataPoint) (float64, float64) {
	if len(points) == 0 {
		return 0, 0
	}

	// Calculate mean
	sum := 0.0
	for _, p := range points {
		sum += p.Value
	}
	mean := sum / float64(len(points))

	// Calculate standard deviation
	sumSquaredDiff := 0.0
	for _, p := range points {
		diff := p.Value - mean
		sumSquaredDiff += diff * diff
	}
	variance := sumSquaredDiff / float64(len(points))
	stdDev := math.Sqrt(variance)

	return mean, stdDev
}

// GetRecommendedActions returns AI-recommended actions based on current state
func (i *Intelligence) GetRecommendedActions() []string {
	// Real implementation for resource recommendations
	var recommendations []string

	// Lock data access
	i.dataLock.RLock()
	defer i.dataLock.RUnlock()

	// Check CPU usage metrics (if available)
	cpuRecommendation := i.getResourceRecommendation("cpu", MetricResourceUsage)
	if cpuRecommendation != "" {
		recommendations = append(recommendations, cpuRecommendation)
	}

	// Check memory usage metrics (if available)
	memoryRecommendation := i.getResourceRecommendation("memory", MetricResourceUsage)
	if memoryRecommendation != "" {
		recommendations = append(recommendations, memoryRecommendation)
	}

	// Check message rate metrics
	messageRateRecommendation := i.getResourceRecommendation("message_rate", MetricMessageCount)
	if messageRateRecommendation != "" {
		recommendations = append(recommendations, messageRateRecommendation)
	}

	// Check network traffic metrics
	networkRecommendation := i.getResourceRecommendation("network", MetricNetworkTraffic)
	if networkRecommendation != "" {
		recommendations = append(recommendations, networkRecommendation)
	}

	// Check latency metrics
	latencyRecommendation := i.getResourceRecommendation("processing", MetricLatency)
	if latencyRecommendation != "" {
		recommendations = append(recommendations, latencyRecommendation)
	}

	// Add general recommendations if none specific
	if len(recommendations) == 0 {
		recommendations = append(recommendations,
			"Monitor system for more data collection",
			"No urgent scaling actions required at this time")
	}

	return recommendations
}

// getResourceRecommendation returns a specific recommendation for a resource
func (i *Intelligence) getResourceRecommendation(resourceName string, metricType MetricType) string {
	// Get resource data if available
	resourceData, exists := i.timeSeriesData[metricType]
	if !exists {
		return ""
	}

	tsData, exists := resourceData[resourceName]
	if !exists || len(tsData.Points) < 10 {
		return ""
	}

	// Analyze last 20 points or all if fewer
	numPoints := min(20, len(tsData.Points))
	recentPoints := tsData.Points[len(tsData.Points)-numPoints:]

	// Calculate recent averages
	sum := 0.0
	for _, point := range recentPoints {
		sum += point.Value
	}
	average := sum / float64(len(recentPoints))

	// Get trend (simple linear trend)
	firstHalf := recentPoints[:len(recentPoints)/2]
	secondHalf := recentPoints[len(recentPoints)/2:]

	var firstSum, secondSum float64
	for _, p := range firstHalf {
		firstSum += p.Value
	}
	for _, p := range secondHalf {
		secondSum += p.Value
	}

	firstAvg := firstSum / float64(len(firstHalf))
	secondAvg := secondSum / float64(len(secondHalf))

	trend := secondAvg - firstAvg

	// Make recommendations based on resource type and metrics
	switch resourceName {
	case "cpu":
		if average > 80 && trend > 0 {
			return "CRITICAL: CPU usage consistently high (>80%) and increasing. Consider scaling up CPU resources immediately."
		} else if average > 70 && trend > 0 {
			return "WARNING: CPU usage approaching critical levels (>70%) and trending upward. Plan to add more CPU resources soon."
		} else if average > 60 {
			return "NOTICE: CPU usage moderately high (>60%). Monitor for continued increases."
		}

	case "memory":
		if average > 85 && trend > 0 {
			return "CRITICAL: Memory usage consistently high (>85%) and increasing. Consider scaling up memory immediately."
		} else if average > 75 && trend > 0 {
			return "WARNING: Memory usage approaching critical levels (>75%) and trending upward. Plan to add more memory soon."
		} else if average > 65 {
			return "NOTICE: Memory usage moderately high (>65%). Monitor for continued increases."
		}

	case "message_rate":
		// Messages per second thresholds depend on system capacity
		if average > 1000 && trend > 10 {
			return "WARNING: Message rate consistently high (>1000/sec) and increasing. Consider adding more nodes to the cluster."
		} else if average > 800 && trend > 0 {
			return "NOTICE: Message rate approaching high levels (>800/sec). Monitor for impacts on latency."
		}

	case "network":
		// Network traffic in KB/s
		if average > 10000 && trend > 0 {
			return "WARNING: Network traffic consistently high (>10MB/s) and increasing. Check for potential bottlenecks."
		} else if average > 5000 && trend > 100 {
			return "NOTICE: Network traffic increasing rapidly. Monitor for potential network saturation."
		}

	case "processing":
		// Processing latency in ms
		if average > 500 && trend > 0 {
			return "CRITICAL: Processing latency consistently high (>500ms) and increasing. Investigate performance issues immediately."
		} else if average > 200 && trend > 10 {
			return "WARNING: Processing latency increasing and approaching problematic levels (>200ms)."
		}
	}

	return ""
}

// min returns the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
