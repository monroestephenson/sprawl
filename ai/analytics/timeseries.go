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

	// Sort points by timestamp
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp.Before(points[j].Timestamp)
	})

	// Find the start and end times
	start := points[0].Timestamp
	end := points[len(points)-1].Timestamp

	// Calculate the number of buckets
	numBuckets := int(end.Sub(start)/interval) + 1
	if numBuckets <= 0 {
		numBuckets = 1
	}

	// Initialize time-aligned buckets
	buckets := make(map[time.Time][]TimeSeriesPoint)
	for i := 0; i < numBuckets; i++ {
		bucketTime := start.Add(time.Duration(i) * interval)
		bucketTime = alignTimeToInterval(bucketTime, interval)
		buckets[bucketTime] = []TimeSeriesPoint{}
	}

	// Place points in buckets based on their timestamp
	for _, point := range points {
		// Find the bucket this point belongs to
		bucketTime := alignTimeToInterval(point.Timestamp, interval)
		buckets[bucketTime] = append(buckets[bucketTime], point)
	}

	// Aggregate points in each bucket
	result := make([]TimeSeriesPoint, 0, numBuckets)

	// Process buckets in chronological order
	bucketTimes := make([]time.Time, 0, len(buckets))
	for t := range buckets {
		bucketTimes = append(bucketTimes, t)
	}

	sort.Slice(bucketTimes, func(i, j int) bool {
		return bucketTimes[i].Before(bucketTimes[j])
	})

	for _, bucketTime := range bucketTimes {
		bucket := buckets[bucketTime]
		if len(bucket) == 0 {
			continue
		}

		var aggregatedValue float64
		var buckLabels map[string]string

		// Use first point's labels by default
		if len(bucket) > 0 {
			buckLabels = mergeLabels(bucket)
		}

		switch method {
		case SumAggregation:
			aggregatedValue = calculateSum(bucket)

		case AvgAggregation:
			aggregatedValue = calculateSeriesMean(extractValues(bucket))

		case MinAggregation:
			aggregatedValue = calculateMin(bucket)

		case MaxAggregation:
			aggregatedValue = calculateMax(bucket)

		case CountAggregation:
			aggregatedValue = float64(len(bucket))

		case P95Aggregation:
			aggregatedValue = calculatePercentile(extractValues(bucket), 95)

		case P99Aggregation:
			aggregatedValue = calculatePercentile(extractValues(bucket), 99)

		default:
			// Default to average
			aggregatedValue = calculateSeriesMean(extractValues(bucket))
		}

		// Create aggregated point
		aggregatedPoint := TimeSeriesPoint{
			Timestamp: bucketTime,
			Value:     aggregatedValue,
			Labels:    buckLabels,
		}
		result = append(result, aggregatedPoint)
	}

	return result
}

// alignTimeToInterval aligns a timestamp to the specified interval boundary
// This ensures consistent bucket boundaries
func alignTimeToInterval(t time.Time, interval time.Duration) time.Time {
	// Convert to Unix timestamp in nanoseconds
	unixNano := t.UnixNano()

	// Calculate the number of complete intervals
	intervalNano := interval.Nanoseconds()
	alignedNano := (unixNano / intervalNano) * intervalNano

	// Convert back to time.Time
	return time.Unix(0, alignedNano)
}

// mergeLabels combines labels from multiple points in a bucket
// When labels conflict, it uses the most common value
func mergeLabels(points []TimeSeriesPoint) map[string]string {
	if len(points) == 0 {
		return map[string]string{}
	}

	if len(points) == 1 {
		return points[0].Labels
	}

	// Count occurrences of each label value
	labelCounts := make(map[string]map[string]int)

	for _, point := range points {
		if point.Labels == nil {
			continue
		}

		for key, value := range point.Labels {
			if _, exists := labelCounts[key]; !exists {
				labelCounts[key] = make(map[string]int)
			}
			labelCounts[key][value]++
		}
	}

	// Select the most common value for each label
	result := make(map[string]string)
	for key, valueCounts := range labelCounts {
		maxCount := 0
		var mostCommonValue string

		for value, count := range valueCounts {
			if count > maxCount {
				maxCount = count
				mostCommonValue = value
			}
		}

		result[key] = mostCommonValue
	}

	return result
}

// Helper functions for aggregation calculations

// calculateSum computes the sum of all values in a bucket
func calculateSum(points []TimeSeriesPoint) float64 {
	sum := 0.0
	for _, p := range points {
		sum += p.Value
	}
	return sum
}

// calculateMin finds the minimum value in a bucket
func calculateMin(points []TimeSeriesPoint) float64 {
	if len(points) == 0 {
		return 0
	}

	min := points[0].Value
	for _, p := range points[1:] {
		if p.Value < min {
			min = p.Value
		}
	}
	return min
}

// calculateMax finds the maximum value in a bucket
func calculateMax(points []TimeSeriesPoint) float64 {
	if len(points) == 0 {
		return 0
	}

	max := points[0].Value
	for _, p := range points[1:] {
		if p.Value > max {
			max = p.Value
		}
	}
	return max
}

// extractValues extracts the values from a slice of TimeSeriesPoints
func extractValues(points []TimeSeriesPoint) []float64 {
	values := make([]float64, len(points))
	for i, p := range points {
		values[i] = p.Value
	}
	return values
}

// calculatePercentile computes the requested percentile from a slice of values
// This uses linear interpolation between closest ranks for precise percentiles
func calculatePercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}

	if len(values) == 1 {
		return values[0]
	}

	// Sort the values in ascending order
	sort.Float64s(values)

	// Calculate the rank of the percentile
	rank := (percentile / 100.0) * float64(len(values)-1)

	// If rank is an integer, return the value at that rank
	if rank == float64(int(rank)) {
		return values[int(rank)]
	}

	// Calculate the indices of the values to interpolate between
	lowerIndex := int(math.Floor(rank))
	upperIndex := int(math.Ceil(rank))

	// Calculate the fraction for interpolation
	fraction := rank - float64(lowerIndex)

	// Interpolate between the two values
	return values[lowerIndex] + fraction*(values[upperIndex]-values[lowerIndex])
}

// calculateSeriesMean computes the mean of a slice of values
func calculateSeriesMean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}

	return sum / float64(len(values))
}

// calculateSeriesStdDev computes the standard deviation of a slice of values
func calculateSeriesStdDev(values []float64, mean float64) float64 {
	if len(values) < 2 {
		return 0
	}

	sumSquaredDiff := 0.0
	for _, v := range values {
		diff := v - mean
		sumSquaredDiff += diff * diff
	}

	variance := sumSquaredDiff / float64(len(values)-1) // Use n-1 for sample standard deviation
	return math.Sqrt(variance)
}
