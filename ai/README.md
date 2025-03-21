# Sprawl AI Components

This package provides intelligent analytics, load prediction, and optimization capabilities for the Sprawl messaging system. The AI components help analyze message traffic patterns, detect anomalies, predict resource usage, and provide auto-scaling recommendations.

## Architecture

The AI system is composed of several key components:

### 1. AI Engine (`engine.go`)

The central coordinator that integrates all AI capabilities and provides a unified interface. It collects metrics, manages the lifecycle of AI components, and exposes high-level functions for the application.

### 2. Load Prediction (`prediction/load_predictor.go`)

Forecasts future resource usage (CPU, memory, network, etc.) using time series analysis and machine learning techniques. Features include:
- Multiple prediction models with ensemble learning
- Seasonality detection (daily, weekly patterns)
- Confidence scoring
- Multi-step forecasting

### 3. Anomaly Detection (`analytics/anomaly.go`)

Identifies unusual patterns in metrics that may indicate problems. Supports multiple detection algorithms:
- Z-score based detection
- Median Absolute Deviation (MAD)
- Interquartile Range (IQR)
- Exponential smoothing

### 4. Traffic Pattern Analysis (`analytics/traffic.go`, `analytics/patterns.go`)

Analyzes message traffic to discover patterns, trends, and behaviors. Capabilities include:
- Daily and weekly pattern recognition
- Traffic spike prediction
- Seasonal trend detection
- Pattern matching against known templates

### 5. Time Series Analytics (`analytics/timeseries.go`)

Base infrastructure for storing and analyzing time series data, including:
- Data point storage and management
- Various aggregation methods (sum, avg, min, max, percentiles)
- Bucketing and downsampling

## Integration

The AI components can be integrated into the Sprawl system at different levels:

1. **Node Level**: Each node can use its own AI Engine to make local decisions
2. **Cluster Level**: A dedicated AI coordinator can collect metrics from all nodes
3. **Hybrid Approach**: Local engines with shared models and coordination

## Usage Examples

### Initialize and Start the AI Engine

```go
// Create with default options
aiEngine := ai.NewEngine(ai.DefaultEngineOptions())

// Or customize options
options := ai.EngineOptions{
    SampleInterval:  10 * time.Second,
    MaxDataPoints:   20000,
    EnablePredictor: true,
    EnablePatterns:  true,
    EnableAnomalies: true,
}
aiEngine := ai.NewEngine(options)

// Start the engine
aiEngine.Start()
defer aiEngine.Stop()
```

### Record Metrics

```go
// Record CPU usage
aiEngine.RecordMetric(ai.MetricKindCPUUsage, "node1", 45.2, map[string]string{
    "zone": "us-west1",
})

// Record message rate
aiEngine.RecordMetric(ai.MetricKindMessageRate, "node1", 1240.5, nil)

// Record topic activity
aiEngine.RecordMetric(ai.MetricKindTopicActivity, "important-topic", 567.8, nil)
```

### Make Predictions

```go
// Predict CPU usage 30 minutes in the future
futureTime := time.Now().Add(30 * time.Minute)
result, err := aiEngine.PredictLoad(prediction.ResourceCPU, "node1", futureTime)
if err == nil {
    fmt.Printf("Predicted CPU: %.2f%% (confidence: %.2f)\n", 
        result.PredictedVal, 
        result.Confidence)
    fmt.Printf("Burst risk: %.2f%%\n", result.ProbBurstRisk * 100)
}
```

### Get Scaling Recommendations

```go
// Get auto-scaling recommendations
recommendations := aiEngine.GetScalingRecommendations("node1")
for _, rec := range recommendations {
    fmt.Printf("%s: %s (confidence: %.2f)\n", 
        rec.Resource, 
        rec.RecommendedAction,
        rec.Confidence)
    fmt.Printf("Reason: %s\n", rec.Reason)
}
```

### Check for Anomalies

```go
// Get anomalies from the last hour
since := time.Now().Add(-1 * time.Hour)
anomalies := aiEngine.GetAnomalies(ai.MetricKindCPUUsage, "node1", since)
for _, anomaly := range anomalies {
    fmt.Printf("%s anomaly detected at %s (score: %.2f)\n",
        anomaly.AnomalyType,
        anomaly.Timestamp.Format(time.RFC3339),
        anomaly.DeviationScore)
}
```

### Analyze Traffic Patterns

```go
// Get pattern information for a topic
patterns := aiEngine.GetTopicPatterns("important-topic")
if patterns != nil {
    fmt.Printf("Topic patterns: %+v\n", patterns)
    fmt.Printf("Burst probability: %.2f%%\n", patterns.BurstProbability * 100)
}

// Get topics with highest burst probability
burstTopics := aiEngine.GetTopBurstTopics(5)
for i, topic := range burstTopics {
    fmt.Printf("%d. %s (burst prob: %.2f%%)\n",
        i+1, 
        topic.EntityID, 
        topic.BurstProbability * 100)
}
```

## Demo Program

For a complete example, see the `examples/ai_demo.go` file, which demonstrates:
1. Setting up the AI engine
2. Generating simulated metrics with realistic patterns
3. Making predictions
4. Getting recommendations
5. Detecting anomalies

Run it with:

```bash
make run_demo
```

## Running the Demo

To run the AI demonstration:

1. Build the demo: `make ai_demo`
2. Run the demo: `./ai_demo`

The demo will:
1. Initialize the AI engine with all components
2. Generate simulated metrics with realistic patterns
3. After a minute of data collection, predict future resource usage
4. Display scaling recommendations
5. Show any detected anomalies
6. Output the full system status as JSON 