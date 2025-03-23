# Sprawl AI Analytics & Intelligence Features

## Overview

Sprawl incorporates sophisticated AI analytics capabilities to provide intelligent insights into network traffic, messaging patterns, and system resource utilization. These features enable predictive resource allocation, anomaly detection, and optimization opportunities based on real system metrics.

## Core Components

### Time Series Analysis

The time series analysis system provides fundamental signal processing capabilities:

- **Data Collection**: Automatic collection of metrics from store operations, network activity, and message traffic
- **Statistical Processing**: Computing of averages, percentiles, standard deviations, and other statistical measures
- **Signal Quality Assessment**: Evaluation of signal-to-noise ratio, stationarity, and seasonality
- **Outlier Detection**: Identification of anomalous values using robust statistical methods

Key implementations:
- `calculatePercentile`: Robust percentile calculation using sorted data
- `analyzeTimeSeriesQuality`: Multi-factor time series quality assessment
- `calculateAutocorrelation`: Time-delayed correlation analysis for pattern detection

### Pattern Detection

The pattern detection system identifies recurring patterns in message traffic and system usage:

- **Daily Patterns**: Detection of 24-hour cycle patterns (e.g., business hours vs. night)
- **Weekly Patterns**: Identification of weekly cycles (e.g., weekday vs. weekend differences)
- **Seasonal Analysis**: Detection of longer seasonal patterns when sufficient data is available
- **Confidence Scoring**: Multi-factor evaluation of pattern reliability

Confidence factors include:
- Data quantity and quality
- Signal-to-noise ratio
- Consistency (coefficient of variation)
- Stationarity (stability of statistical properties)
- Seasonality strength

### Traffic Analysis

The traffic analyzer provides insights into message patterns across topics:

- **Busiest Topics**: Identification of highest-volume messaging topics
- **Traffic Forecasting**: Prediction of future message volumes based on historical patterns
- **Peak Detection**: Identification of peak and trough times for capacity planning
- **Normalized Patterns**: Pattern comparison across different scales of traffic

### Network Intelligence

The network intelligence system monitors and optimizes network utilization:

- **Interface Monitoring**: Per-interface bandwidth utilization tracking
- **Activity Prediction**: Forecasting of network load based on historical patterns
- **Adaptive Behavior**: Informing routing decisions based on network conditions
- **Capacity Planning**: Insights for infrastructure scaling decisions

## Production Features

### Real Metrics Integration

All analytics features utilize real system metrics:

- Store metrics (memory usage, topic message counts)
- Network I/O statistics (per-interface bandwidth usage)
- CPU/Memory/Disk resource utilization

The system includes graceful fallback mechanisms using historical averages when real-time metrics are temporarily unavailable, ensuring continuity of analytics capabilities.

### Evening Pattern Detection

The evening pattern detector is specially designed to identify usage patterns that peak during evening hours:

```go
func detectEveningPattern(data []TimeSeriesPoint) float64 {
    // Categorize hours
    eveningHours := []int{18, 19, 20, 21, 22, 23}
    
    // Calculate average values for evening vs other times
    eveningSum, eveningCount := 0.0, 0
    otherSum, otherCount := 0.0, 0
    
    for _, point := range data {
        hour := point.Timestamp.Hour()
        isEvening := false
        for _, h := range eveningHours {
            if hour == h {
                isEvening = true
                break
            }
        }
        
        if isEvening {
            eveningSum += point.Value
            eveningCount++
        } else {
            otherSum += point.Value
            otherCount++
        }
    }
    
    // Calculate average values
    eveningAvg := 0.0
    if eveningCount > 0 {
        eveningAvg = eveningSum / float64(eveningCount)
    }
    
    otherAvg := 0.0
    if otherCount > 0 {
        otherAvg = otherSum / float64(otherCount)
    }
    
    // Calculate peak prominence with outlier robustness
    robustMax := calculateRobustMax(data)
    prominence := (eveningAvg - otherAvg) / robustMax
    
    // Calculate statistical significance (t-score equivalent)
    significance := calculateStatisticalSignificance(data, eveningHours)
    
    // Calculate effect size using Cohen's d
    cohensD := (eveningAvg - otherAvg) / calculateStdDev(data)
    
    // Calculate consistency (lower coefficient of variation is better)
    eveningConsistency := 1.0 - calculateCoeffOfVariation(data, eveningHours)
    
    // Calculate periodicity strength through autocorrelation
    periodicity := calculatePatternPeriodicity(data)
    
    // Combine factors with appropriate weights
    score := (
        0.30 * math.Max(0, math.Min(1, prominence*2)) +
        0.20 * math.Max(0, math.Min(1, significance/3.0)) +
        0.20 * math.Max(0, math.Min(1, cohensD/1.0)) +
        0.15 * eveningConsistency +
        0.15 * periodicity
    )
    
    // Apply sigmoid function for smoother scaling
    scaledScore := 1.0 / (1.0 + math.Exp(-5.0*(score-0.5)))
    
    return scaledScore
}
```

This combines multiple factors to produce a reliable score indicating the presence and strength of evening usage patterns.

## Implementation Details

### Error Handling and Resiliency

The analytics system includes robust error handling:

- **Historical Data Caching**: Maintains records of past metrics to use when current data is unavailable
- **Graceful Degradation**: Falls back to stored patterns when real-time analysis isn't possible
- **Data Quality Checks**: Skips analysis when data quality is insufficient for reliable results
- **Thread Safety**: All data structures are protected with appropriate locks for concurrent access

### Integration Points

The analytics system integrates with other Sprawl components through:

- `ai/intelligence.go`: Main interface for other components to request analytics
- `ai/predictor.go`: Load prediction interface for resource allocation
- `store/metrics.go`: Collection point for message and topic statistics
- `network/traffic.go`: Network utilization monitoring and optimization

## Usage Examples

### Pattern Detection

```go
analyzer := analytics.NewTrafficAnalyzer(15*time.Minute, 1000)
analyzer.Start()

// Add data points as they come in
analyzer.AddTopicDataPoint("news/sports", 42.0, time.Now(), nil)

// Later, get pattern information
pattern := analyzer.GetTopicPattern("news/sports", analytics.DailyPattern)
if pattern != nil && pattern.Confidence > 0.7 {
    fmt.Printf("Peak times: %v\n", pattern.PeakTimes)
    fmt.Printf("Average activity: %.2f\n", pattern.AverageValue)
}

// Predict future activity
expected := analyzer.PredictTopicActivity("news/sports", tomorrow)
```

### System Intelligence

```go
// Get intelligence about current system state
intel := ai.GetSystemIntelligence()

// Check if we're in a high-activity period
if intel.IsHighActivityPeriod() {
    // Adjust resource allocation
}

// Pre-allocate resources based on predicted load
expectedLoad := intel.PredictLoad(time.Now().Add(1*time.Hour))
if expectedLoad > 0.8 {
    // Scale up resources proactively
}
```

## Performance Considerations

- The analytics system operates in the background with minimal impact on core operations
- Heavy processing is done at configurable intervals (default 15 minutes)
- Memory usage scales linearly with the number of topics and configured data points
- CPU usage is minimal during normal operation, with brief spikes during analysis cycles

## Future Enhancements

Planned enhancements for the analytics system include:

- Machine learning models for more accurate prediction
- Anomaly detection with automatic alerting
- Cross-topic correlation analysis
- Geographic traffic pattern analysis
- More sophisticated network optimization strategies 