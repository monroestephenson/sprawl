// Package analytics provides data analysis capabilities for Sprawl
package analytics

import (
	"math"
	"sort"
	"time"
)

// TimeSeriesPoint represents a single data point in a time series
type TimeSeriesPoint struct {
	Timestamp time.Time
	Value     float64
	Labels    map[string]string
}

// Aggregation defines how time series data should be aggregated
type Aggregation string

const (
	// SumAggregation sums values in the time window
	SumAggregation Aggregation = "sum"
	// AvgAggregation calculates the average of values in the time window
	AvgAggregation Aggregation = "avg"
	// MinAggregation finds the minimum value in the time window
	MinAggregation Aggregation = "min"
	// MaxAggregation finds the maximum value in the time window
	MaxAggregation Aggregation = "max"
	// CountAggregation counts the number of values in the time window
	CountAggregation Aggregation = "count"
	// P95Aggregation calculates the 95th percentile of values in the time window
	P95Aggregation Aggregation = "p95"
	// P99Aggregation calculates the 99th percentile of values in the time window
	P99Aggregation Aggregation = "p99"
)

// TimeRange represents a time range for queries
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// MetricFilter represents a filter for metric queries
type MetricFilter struct {
	Key      string
	Operator string
	Value    string
}

// TimeSeriesQuery defines parameters for querying time series data
type TimeSeriesQuery struct {
	MetricName  string
	TimeRange   TimeRange
	Filters     []MetricFilter
	Aggregation Aggregation
	Interval    time.Duration
	Limit       int
}

// TimeSeriesResult represents the result of a time series query
type TimeSeriesResult struct {
	MetricName string
	Points     []TimeSeriesPoint
	Start      time.Time
	End        time.Time
	Resolution time.Duration
}

// AggregatePoints aggregates time series points based on the specified aggregation method
func AggregatePoints(points []TimeSeriesPoint, method Aggregation, interval time.Duration) []TimeSeriesPoint {
	if len(points) == 0 {
		return []TimeSeriesPoint{}
	}

	// Sort points by timestamp (we could use a library like sort.Slice here)
	// For simplicity, we'll assume points are already sorted by timestamp

	// Find the start and end times
	start := points[0].Timestamp
	end := points[len(points)-1].Timestamp

	// Calculate the number of buckets
	numBuckets := int(end.Sub(start)/interval) + 1
	if numBuckets <= 0 {
		numBuckets = 1
	}

	// Initialize buckets
	buckets := make([][]TimeSeriesPoint, numBuckets)
	for i := range buckets {
		buckets[i] = []TimeSeriesPoint{}
	}

	// Place points in buckets
	for _, point := range points {
		bucketIndex := int(point.Timestamp.Sub(start) / interval)
		if bucketIndex >= numBuckets {
			bucketIndex = numBuckets - 1
		}
		buckets[bucketIndex] = append(buckets[bucketIndex], point)
	}

	// Aggregate points in each bucket
	result := make([]TimeSeriesPoint, 0, numBuckets)
	for i, bucket := range buckets {
		if len(bucket) == 0 {
			continue
		}

		bucketTime := start.Add(time.Duration(i) * interval)
		var aggregatedValue float64

		switch method {
		case SumAggregation:
			for _, p := range bucket {
				aggregatedValue += p.Value
			}
		case AvgAggregation:
			sum := 0.0
			for _, p := range bucket {
				sum += p.Value
			}
			aggregatedValue = sum / float64(len(bucket))
		case MinAggregation:
			aggregatedValue = bucket[0].Value
			for _, p := range bucket {
				if p.Value < aggregatedValue {
					aggregatedValue = p.Value
				}
			}
		case MaxAggregation:
			aggregatedValue = bucket[0].Value
			for _, p := range bucket {
				if p.Value > aggregatedValue {
					aggregatedValue = p.Value
				}
			}
		case CountAggregation:
			aggregatedValue = float64(len(bucket))
		case P95Aggregation:
			values := make([]float64, len(bucket))
			for i, p := range bucket {
				values[i] = p.Value
			}

			// Calculate 95th percentile properly
			aggregatedValue = calculatePercentile(values, 95)
		case P99Aggregation:
			values := make([]float64, len(bucket))
			for i, p := range bucket {
				values[i] = p.Value
			}

			// Calculate 99th percentile properly
			aggregatedValue = calculatePercentile(values, 99)
		default:
			// Default to average
			sum := 0.0
			for _, p := range bucket {
				sum += p.Value
			}
			aggregatedValue = sum / float64(len(bucket))
		}

		// Create aggregated point
		aggregatedPoint := TimeSeriesPoint{
			Timestamp: bucketTime,
			Value:     aggregatedValue,
			Labels:    bucket[0].Labels, // Use labels from first point
		}
		result = append(result, aggregatedPoint)
	}

	return result
}

// calculatePercentile computes the requested percentile from a slice of values
func calculatePercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}

	// Sort the values
	sort.Float64s(values)

	// Calculate the index for the percentile
	// We use the Nearest Rank method: https://en.wikipedia.org/wiki/Percentile#The_Nearest-Rank_method
	index := int(math.Ceil((percentile/100)*float64(len(values)))) - 1

	// Boundary check
	if index < 0 {
		index = 0
	}
	if index >= len(values) {
		index = len(values) - 1
	}

	return values[index]
}
