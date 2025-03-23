package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"time"

	"sprawl/ai"
	"sprawl/ai/prediction"
)

// Generates synthetic training data for the AI engine with realistic patterns

func main() {
	// Create an AI engine instance with default options
	options := ai.DefaultEngineOptions()
	options.SampleInterval = 10 * time.Second // Faster sampling for testing
	engine := ai.NewEngine(options, nil)
	fmt.Println("AI Data Generator started...")

	// Generate data for the past 7 days
	now := time.Now()
	startTime := now.Add(-7 * 24 * time.Hour)

	// Node IDs for simulation
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	// Generate data points every 5 minutes
	interval := 5 * time.Minute
	currentTime := startTime

	for currentTime.Before(now) {
		for _, nodeID := range nodeIDs {
			// Add CPU usage data with daily pattern and some weekly pattern
			// Morning peak (9-11 AM), afternoon plateau (1-4 PM), evening peak (7-9 PM)
			hour := currentTime.Hour()
			dayOfWeek := int(currentTime.Weekday())

			// Base CPU value
			cpuValue := 30.0 // Base CPU usage percentage

			// Daily pattern
			if hour >= 9 && hour <= 11 {
				cpuValue += 20.0 + (float64(hour-9) * 10.0) // Morning ramp up
			} else if hour >= 13 && hour <= 16 {
				cpuValue += 40.0 // Afternoon plateau
			} else if hour >= 19 && hour <= 21 {
				cpuValue += 25.0 + (float64(21-hour) * 5.0) // Evening peak
			} else if hour >= 0 && hour <= 5 {
				cpuValue -= 15.0 // Night valley
			}

			// Weekly pattern - weekends are lower
			if dayOfWeek == 0 || dayOfWeek == 6 { // Sunday or Saturday
				cpuValue *= 0.7 // 30% less usage on weekends
			}

			// Add some random noise (±5%)
			noise := (rand.Float64() * 10.0) - 5.0
			cpuValue += noise

			// Ensure value is between 5 and 100
			cpuValue = math.Max(5.0, math.Min(100.0, cpuValue))

			// Add the CPU data point
			engine.RecordMetric(ai.MetricKindCPUUsage, nodeID, cpuValue, map[string]string{"node_id": nodeID})

			// Generate memory usage with different pattern
			// Memory tends to grow throughout the day and reset after GC or restarts
			memoryValue := 40.0 // Base memory percentage

			// Memory grows through the day
			memoryValue += float64(hour) * 1.5

			// Weekly pattern
			if dayOfWeek == 0 || dayOfWeek == 6 {
				memoryValue *= 0.8
			}

			// Add some random noise (±3%)
			memNoise := (rand.Float64() * 6.0) - 3.0
			memoryValue += memNoise

			// Memory resets at night (simulating GC or restarts)
			if hour == 2 && rand.Float64() < 0.7 {
				memoryValue = 25.0 + (rand.Float64() * 10.0)
			}

			// Ensure value is between 10 and 95
			memoryValue = math.Max(10.0, math.Min(95.0, memoryValue))

			// Add the memory data point
			engine.RecordMetric(ai.MetricKindMemoryUsage, nodeID, memoryValue, map[string]string{"node_id": nodeID})

			// Generate network traffic with spikes and different patterns
			networkValue := 25.0 // Base network usage in MB/s

			// Business hours have higher network usage
			if hour >= 8 && hour <= 18 {
				networkValue += 15.0 + (float64(hour-8) * 1.2)
			}

			// Network spikes at certain hours
			if hour == 9 || hour == 13 || hour == 17 {
				networkValue += 20.0 + (rand.Float64() * 15.0)
			}

			// Weekend pattern
			if dayOfWeek == 0 || dayOfWeek == 6 {
				networkValue *= 0.6
			}

			// Add some random noise (±8%)
			netNoise := (rand.Float64() * 16.0) - 8.0
			networkValue += netNoise

			// Simulate occasional network spikes
			if rand.Float64() < 0.05 { // 5% chance of spike
				networkValue *= 2.0 + rand.Float64()
			}

			// Ensure value is between 5 and 200
			networkValue = math.Max(5.0, math.Min(200.0, networkValue))

			// Add the network data point
			engine.RecordMetric(ai.MetricKindNetworkTraffic, nodeID, networkValue, map[string]string{"node_id": nodeID})

			// Generate message rate data
			messageValue := 100.0 // Base message rate per second

			// Business hours pattern
			if hour >= 8 && hour <= 18 {
				messageValue += 50.0 + (float64(hour-8) * 10.0)
			}

			// Message spikes mid-morning and mid-afternoon
			if hour == 10 || hour == 15 {
				messageValue += 100.0 + (rand.Float64() * 50.0)
			}

			// Weekend pattern
			if dayOfWeek == 0 || dayOfWeek == 6 {
				messageValue *= 0.5
			}

			// Add some random noise (±10%)
			msgNoise := (rand.Float64() * 20.0) - 10.0
			messageValue += msgNoise

			// Simulate occasional message bursts
			if rand.Float64() < 0.03 { // 3% chance of burst
				messageValue *= 3.0 + (rand.Float64() * 2.0)
			}

			// Ensure value is between 10 and 2000
			messageValue = math.Max(10.0, math.Min(2000.0, messageValue))

			// Add the message data point
			engine.RecordMetric(ai.MetricKindMessageRate, nodeID, messageValue, map[string]string{"node_id": nodeID})
		}

		// Move to next time interval
		currentTime = currentTime.Add(interval)

		// Print progress every 6 hours of simulated time
		if currentTime.Hour()%6 == 0 && currentTime.Minute() == 0 {
			fmt.Printf("Generated data up to: %s\n", currentTime.Format("2006-01-02 15:04:05"))
		}
	}

	// Train the models with the generated data
	fmt.Println("Training models with generated data...")

	// Train models for each resource and node
	oneWeek := 7 * 24 * time.Hour
	for _, nodeID := range nodeIDs {
		err := engine.TrainResourceModel(prediction.ResourceCPU, nodeID, oneWeek)
		if err != nil {
			log.Printf("Failed to train CPU model for node %s: %v", nodeID, err)
		} else {
			fmt.Printf("Trained CPU model for node %s\n", nodeID)
		}

		err = engine.TrainResourceModel(prediction.ResourceMemory, nodeID, oneWeek)
		if err != nil {
			log.Printf("Failed to train Memory model for node %s: %v", nodeID, err)
		} else {
			fmt.Printf("Trained Memory model for node %s\n", nodeID)
		}

		err = engine.TrainResourceModel(prediction.ResourceNetwork, nodeID, oneWeek)
		if err != nil {
			log.Printf("Failed to train Network model for node %s: %v", nodeID, err)
		} else {
			fmt.Printf("Trained Network model for node %s\n", nodeID)
		}
	}

	fmt.Println("Data generation and training completed!")

	// Make some predictions to verify
	fmt.Println("\nSample predictions for the next 24 hours:")

	sampleNodeID := "node-1"
	predictionTimes := []time.Time{
		now.Add(1 * time.Hour),
		now.Add(6 * time.Hour),
		now.Add(12 * time.Hour),
		now.Add(24 * time.Hour),
	}

	for _, predTime := range predictionTimes {
		cpuPred, err := engine.PredictLoad(prediction.ResourceCPU, sampleNodeID, predTime)
		if err != nil {
			log.Printf("Error predicting CPU: %v", err)
			continue
		}

		memPred, err := engine.PredictLoad(prediction.ResourceMemory, sampleNodeID, predTime)
		if err != nil {
			log.Printf("Error predicting memory: %v", err)
			continue
		}

		netPred, err := engine.PredictLoad(prediction.ResourceNetwork, sampleNodeID, predTime)
		if err != nil {
			log.Printf("Error predicting network: %v", err)
			continue
		}

		fmt.Printf("\nPredictions for %s at %s:\n", sampleNodeID, predTime.Format("2006-01-02 15:04:05"))
		fmt.Printf("  CPU: %.2f%% (confidence: %.2f%%)\n", cpuPred.PredictedVal, cpuPred.Confidence*100)
		fmt.Printf("  Memory: %.2f%% (confidence: %.2f%%)\n", memPred.PredictedVal, memPred.Confidence*100)
		fmt.Printf("  Network: %.2f MB/s (confidence: %.2f%%)\n", netPred.PredictedVal, netPred.Confidence*100)
	}
}
