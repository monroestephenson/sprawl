// Package ai provides intelligence and optimization capabilities for Sprawl
package ai

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
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

	// Network thresholds
	NetworkScaleUpThreshold   float64       // Network throughput above which scaling up is recommended
	NetworkScaleDownThreshold float64       // Network throughput below which scaling down is considered
	NetworkFallbackRatio      float64       // Ratio of max capacity to use as fallback (0.0-1.0)
	NetworkCalibrationPeriod  time.Duration // How long to collect data for calibration

	// Confidence thresholds
	MinConfidenceThreshold float64 // Minimum confidence required for a scaling recommendation

	// Dynamic adjustment settings
	EnableDynamicAdjustment bool    // Whether to dynamically adjust thresholds based on system behavior
	AdjustmentFactor        float64 // Factor to adjust thresholds by (0.0-1.0, where 0.5 means 50% adjustment)
	LearningRate            float64 // Rate at which to learn from system behavior (0.0-1.0)

	// System capacity information
	MaxCPUCores  int     // Maximum CPU cores available to the system
	MaxMemoryGB  float64 // Maximum memory in GB available to the system
	MaxNetworkMB float64 // Maximum network throughput in MB/s

	// Hysteresis to prevent rapid oscillation
	HysteresisPercent float64 // Percentage buffer to prevent oscillation (usually 5-15%)

	// Feedback mechanism parameters
	BadThresholdDetectionEnabled bool    // Whether to detect and automatically adjust inappropriate thresholds
	BadThresholdAdjustmentFactor float64 // How much to adjust thresholds when they're deemed inappropriate (0.0-1.0)

	// Workload classification
	WorkloadProfiles map[string]WorkloadProfile // Map of workload types to their specific thresholds
	CurrentProfile   string                     // Current workload profile

	// Configuration source tracking
	LastConfigSource string    // Source of the configuration (file, env, api)
	LastUpdated      time.Time // When the configuration was last updated
}

// WorkloadProfile defines thresholds specific to a workload type
type WorkloadProfile struct {
	Name              string
	CPUScaleUp        float64
	CPUScaleDown      float64
	MemScaleUp        float64
	MemScaleDown      float64
	MsgRateScaleUp    float64
	MsgRateScaleDown  float64
	NetworkScaleUp    float64
	NetworkScaleDown  float64
	HysteresisPercent float64
	Confidence        float64
}

// DefaultThresholds returns the default threshold configuration
func DefaultThresholds() ThresholdConfig {
	// Get system capabilities
	maxCPU := runtime.NumCPU()
	maxMem := getSystemMemoryGB()
	maxNet := getSystemNetworkCapacity()

	// Create default workload profiles
	profiles := map[string]WorkloadProfile{
		"default": {
			Name:              "default",
			CPUScaleUp:        80.0,
			CPUScaleDown:      20.0,
			MemScaleUp:        85.0,
			MemScaleDown:      30.0,
			MsgRateScaleUp:    5000.0,
			MsgRateScaleDown:  500.0,
			NetworkScaleUp:    maxNet * 0.8, // 80% of max capacity
			NetworkScaleDown:  maxNet * 0.2, // 20% of max capacity
			HysteresisPercent: 10.0,
			Confidence:        0.7,
		},
		"high-throughput": {
			Name:              "high-throughput",
			CPUScaleUp:        90.0,
			CPUScaleDown:      30.0,
			MemScaleUp:        90.0,
			MemScaleDown:      40.0,
			MsgRateScaleUp:    10000.0,
			MsgRateScaleDown:  1000.0,
			NetworkScaleUp:    maxNet * 0.9,
			NetworkScaleDown:  maxNet * 0.3,
			HysteresisPercent: 15.0,
			Confidence:        0.8,
		},
		"low-latency": {
			Name:              "low-latency",
			CPUScaleUp:        70.0,
			CPUScaleDown:      15.0,
			MemScaleUp:        75.0,
			MemScaleDown:      25.0,
			MsgRateScaleUp:    3000.0,
			MsgRateScaleDown:  300.0,
			NetworkScaleUp:    maxNet * 0.7,
			NetworkScaleDown:  maxNet * 0.1,
			HysteresisPercent: 5.0,
			Confidence:        0.9,
		},
	}

	return ThresholdConfig{
		CPUScaleUpThreshold:          80.0,
		CPUScaleDownThreshold:        20.0,
		MemScaleUpThreshold:          85.0,
		MemScaleDownThreshold:        30.0,
		MsgRateScaleUpThreshold:      5000.0,
		MsgRateScaleDownThreshold:    500.0,
		NetworkScaleUpThreshold:      maxNet * 0.8,
		NetworkScaleDownThreshold:    maxNet * 0.2,
		NetworkFallbackRatio:         0.5,
		NetworkCalibrationPeriod:     5 * time.Minute,
		MinConfidenceThreshold:       0.7,
		EnableDynamicAdjustment:      true,
		AdjustmentFactor:             0.5,
		LearningRate:                 0.1,
		MaxCPUCores:                  maxCPU,
		MaxMemoryGB:                  maxMem,
		MaxNetworkMB:                 maxNet,
		HysteresisPercent:            10.0,
		BadThresholdDetectionEnabled: true,
		BadThresholdAdjustmentFactor: 0.3,
		WorkloadProfiles:             profiles,
		CurrentProfile:               "default",
		LastConfigSource:             "defaults",
		LastUpdated:                  time.Now(),
	}
}

// LoadThresholdsFromEnv loads threshold configuration from environment variables
func LoadThresholdsFromEnv() ThresholdConfig {
	// Start with default thresholds
	config := DefaultThresholds()

	// Load CPU thresholds
	if val := os.Getenv("SPRAWL_CPU_SCALE_UP_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 100 {
			config.CPUScaleUpThreshold = f
		}
	}

	if val := os.Getenv("SPRAWL_CPU_SCALE_DOWN_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 100 {
			config.CPUScaleDownThreshold = f
		}
	}

	// Load memory thresholds
	if val := os.Getenv("SPRAWL_MEM_SCALE_UP_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 100 {
			config.MemScaleUpThreshold = f
		}
	}

	if val := os.Getenv("SPRAWL_MEM_SCALE_DOWN_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 100 {
			config.MemScaleDownThreshold = f
		}
	}

	// Load message rate thresholds
	if val := os.Getenv("SPRAWL_MSG_RATE_SCALE_UP_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 {
			config.MsgRateScaleUpThreshold = f
		}
	}

	if val := os.Getenv("SPRAWL_MSG_RATE_SCALE_DOWN_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 {
			config.MsgRateScaleDownThreshold = f
		}
	}

	// Load confidence threshold
	if val := os.Getenv("SPRAWL_MIN_CONFIDENCE_THRESHOLD"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 1.0 {
			config.MinConfidenceThreshold = f
		}
	}

	// Load dynamic adjustment settings
	if val := os.Getenv("SPRAWL_ENABLE_DYNAMIC_ADJUSTMENT"); val != "" {
		config.EnableDynamicAdjustment = strings.ToLower(val) == "true"
	}

	if val := os.Getenv("SPRAWL_ADJUSTMENT_FACTOR"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 1.0 {
			config.AdjustmentFactor = f
		}
	}

	if val := os.Getenv("SPRAWL_LEARNING_RATE"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 1.0 {
			config.LearningRate = f
		}
	}

	// Load hysteresis percent
	if val := os.Getenv("SPRAWL_HYSTERESIS_PERCENT"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 50.0 {
			config.HysteresisPercent = f
		}
	}

	// Load feedback mechanism parameters
	if val := os.Getenv("SPRAWL_BAD_THRESHOLD_DETECTION"); val != "" {
		config.BadThresholdDetectionEnabled = strings.ToLower(val) == "true"
	}

	if val := os.Getenv("SPRAWL_BAD_THRESHOLD_ADJUSTMENT_FACTOR"); val != "" {
		if f, err := strconv.ParseFloat(val, 64); err == nil && f >= 0 && f <= 1.0 {
			config.BadThresholdAdjustmentFactor = f
		}
	}

	// Validate thresholds before returning
	if err := config.Validate(); err != nil {
		log.Printf("Warning: environment thresholds validation failed: %v. Using default values for invalid settings.", err)
	}

	config.LastConfigSource = "environment"
	config.LastUpdated = time.Now()

	return config
}

// Validate checks if the threshold configuration is valid
func (tc *ThresholdConfig) Validate() error {
	var validationErrors []string

	// Validate CPU thresholds
	if tc.CPUScaleUpThreshold < 0 || tc.CPUScaleUpThreshold > 100 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("CPU scale-up threshold must be between 0-100, got: %.1f", tc.CPUScaleUpThreshold))
	}
	if tc.CPUScaleDownThreshold < 0 || tc.CPUScaleDownThreshold > 100 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("CPU scale-down threshold must be between 0-100, got: %.1f", tc.CPUScaleDownThreshold))
	}
	if tc.CPUScaleUpThreshold <= tc.CPUScaleDownThreshold {
		validationErrors = append(validationErrors,
			fmt.Sprintf("CPU scale-up threshold (%.1f) must be greater than scale-down threshold (%.1f)",
				tc.CPUScaleUpThreshold, tc.CPUScaleDownThreshold))
	}

	// Validate memory thresholds
	if tc.MemScaleUpThreshold < 0 || tc.MemScaleUpThreshold > 100 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Memory scale-up threshold must be between 0-100, got: %.1f", tc.MemScaleUpThreshold))
	}
	if tc.MemScaleDownThreshold < 0 || tc.MemScaleDownThreshold > 100 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Memory scale-down threshold must be between 0-100, got: %.1f", tc.MemScaleDownThreshold))
	}
	if tc.MemScaleUpThreshold <= tc.MemScaleDownThreshold {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Memory scale-up threshold (%.1f) must be greater than scale-down threshold (%.1f)",
				tc.MemScaleUpThreshold, tc.MemScaleDownThreshold))
	}

	// Validate message rate thresholds
	if tc.MsgRateScaleUpThreshold < 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Message rate scale-up threshold cannot be negative, got: %.1f", tc.MsgRateScaleUpThreshold))
	}
	if tc.MsgRateScaleDownThreshold < 0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Message rate scale-down threshold cannot be negative, got: %.1f", tc.MsgRateScaleDownThreshold))
	}
	if tc.MsgRateScaleUpThreshold <= tc.MsgRateScaleDownThreshold {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Message rate scale-up threshold (%.1f) must be greater than scale-down threshold (%.1f)",
				tc.MsgRateScaleUpThreshold, tc.MsgRateScaleDownThreshold))
	}

	// Validate confidence threshold
	if tc.MinConfidenceThreshold < 0 || tc.MinConfidenceThreshold > 1.0 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Minimum confidence threshold must be between 0-1, got: %.2f", tc.MinConfidenceThreshold))
	}

	// Validate adjustment factors if dynamic adjustment is enabled
	if tc.EnableDynamicAdjustment {
		if tc.AdjustmentFactor < 0 || tc.AdjustmentFactor > 1.0 {
			validationErrors = append(validationErrors,
				fmt.Sprintf("Adjustment factor must be between 0-1, got: %.2f", tc.AdjustmentFactor))
		}
		if tc.LearningRate < 0 || tc.LearningRate > 1.0 {
			validationErrors = append(validationErrors,
				fmt.Sprintf("Learning rate must be between 0-1, got: %.2f", tc.LearningRate))
		}
	}

	// Validate bad threshold factors
	if tc.BadThresholdDetectionEnabled {
		if tc.BadThresholdAdjustmentFactor < 0 || tc.BadThresholdAdjustmentFactor > 1.0 {
			validationErrors = append(validationErrors,
				fmt.Sprintf("Bad threshold adjustment factor must be between 0-1, got: %.2f", tc.BadThresholdAdjustmentFactor))
		}
	}

	// Validate hysteresis
	if tc.HysteresisPercent < 0 || tc.HysteresisPercent > 50 {
		validationErrors = append(validationErrors,
			fmt.Sprintf("Hysteresis percent must be between 0-50, got: %.1f", tc.HysteresisPercent))
	}

	// If we have validation errors, collect them and return
	if len(validationErrors) > 0 {
		return fmt.Errorf("threshold validation failed with %d errors: %s",
			len(validationErrors), strings.Join(validationErrors, "; "))
	}

	return nil
}

// LoadThresholdsFromFile loads threshold configuration from a JSON file
func LoadThresholdsFromFile(filepath string) (ThresholdConfig, error) {
	// Start with default thresholds
	config := DefaultThresholds()

	// Read the file
	data, err := os.ReadFile(filepath)
	if err != nil {
		return config, fmt.Errorf("failed to read config file: %w", err)
	}

	// Parse the JSON
	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate the configuration
	if err := config.Validate(); err != nil {
		return config, fmt.Errorf("invalid config file: %w", err)
	}

	config.LastConfigSource = "file"
	config.LastUpdated = time.Now()

	return config, nil
}

// DetectInappropriateThresholds analyzes current metrics against thresholds
// and returns detailed feedback on any thresholds that appear inappropriate
func (tc *ThresholdConfig) DetectInappropriateThresholds(metrics map[string]float64, oscillationHistory []string) (bool, map[string]string) {
	if !tc.BadThresholdDetectionEnabled {
		return false, nil
	}

	// Track if any thresholds were inappropriate
	anyInappropriate := false

	// Map to store feedback messages for inappropriate thresholds
	feedbackMessages := make(map[string]string)

	// Check CPU thresholds
	if cpuUsage, ok := metrics["cpu_usage"]; ok {
		// Case 1: CPU usage consistently very high compared to threshold
		if cpuUsage > 95 && tc.CPUScaleUpThreshold < 80 {
			tc.CPUScaleUpThreshold = tc.CPUScaleUpThreshold * (1 + tc.BadThresholdAdjustmentFactor)
			if tc.CPUScaleUpThreshold > 95 {
				tc.CPUScaleUpThreshold = 95 // Cap at 95%
			}
			message := fmt.Sprintf("CPU usage (%0.1f%%) is critically high compared to the threshold (%0.1f%%). "+
				"Threshold has been automatically increased to %0.1f%%. Consider setting a higher CPU threshold or "+
				"investigating the high CPU usage.", cpuUsage, tc.CPUScaleUpThreshold/(1+tc.BadThresholdAdjustmentFactor), tc.CPUScaleUpThreshold)
			feedbackMessages["cpu_scale_up"] = message
			log.Printf("WARNING: %s", message)
			anyInappropriate = true
		} else if cpuUsage > (tc.CPUScaleUpThreshold + 20) {
			// Case 2: CPU usage moderately high compared to threshold
			tc.CPUScaleUpThreshold = tc.CPUScaleUpThreshold * (1 + tc.BadThresholdAdjustmentFactor/2)
			if tc.CPUScaleUpThreshold > 90 {
				tc.CPUScaleUpThreshold = 90
			}
			message := fmt.Sprintf("CPU usage (%0.1f%%) is significantly above your threshold (%0.1f%%). "+
				"Threshold has been automatically adjusted to %0.1f%%.",
				cpuUsage, tc.CPUScaleUpThreshold/(1+tc.BadThresholdAdjustmentFactor/2), tc.CPUScaleUpThreshold)
			feedbackMessages["cpu_scale_up"] = message
			log.Printf("INFO: %s", message)
			anyInappropriate = true
		}

		// Check for too-high CPU threshold
		if cpuUsage < (tc.CPUScaleUpThreshold-30) && tc.CPUScaleUpThreshold > 70 {
			tc.CPUScaleUpThreshold = tc.CPUScaleUpThreshold * (1 - tc.BadThresholdAdjustmentFactor/2)
			message := fmt.Sprintf("CPU usage (%0.1f%%) is far below your scale-up threshold (%0.1f%%). "+
				"Threshold has been automatically adjusted to %0.1f%% to reduce unnecessary alerts.",
				cpuUsage, tc.CPUScaleUpThreshold/(1-tc.BadThresholdAdjustmentFactor/2), tc.CPUScaleUpThreshold)
			feedbackMessages["cpu_scale_up"] = message
			log.Printf("INFO: %s", message)
			anyInappropriate = true
		}

		// Check for too-low CPU scale-down threshold
		if cpuUsage > (tc.CPUScaleDownThreshold+30) && tc.CPUScaleDownThreshold < 20 {
			tc.CPUScaleDownThreshold = tc.CPUScaleDownThreshold * (1 + tc.BadThresholdAdjustmentFactor)
			message := fmt.Sprintf("CPU usage (%0.1f%%) is much higher than your scale-down threshold (%0.1f%%). "+
				"Threshold has been automatically adjusted to %0.1f%% to prevent premature scale-down.",
				cpuUsage, tc.CPUScaleDownThreshold/(1+tc.BadThresholdAdjustmentFactor), tc.CPUScaleDownThreshold)
			feedbackMessages["cpu_scale_down"] = message
			log.Printf("INFO: %s", message)
			anyInappropriate = true
		}
	}

	// Check memory thresholds
	if memUsage, ok := metrics["memory_usage"]; ok {
		// Case 1: Memory usage consistently very high compared to threshold
		if memUsage > 95 && tc.MemScaleUpThreshold < 80 {
			tc.MemScaleUpThreshold = tc.MemScaleUpThreshold * (1 + tc.BadThresholdAdjustmentFactor)
			if tc.MemScaleUpThreshold > 95 {
				tc.MemScaleUpThreshold = 95 // Cap at 95%
			}
			message := fmt.Sprintf("Memory usage (%0.1f%%) is critically high compared to the threshold (%0.1f%%). "+
				"Threshold has been automatically increased to %0.1f%%. Consider setting a higher memory threshold or "+
				"adding more memory to your system.", memUsage, tc.MemScaleUpThreshold/(1+tc.BadThresholdAdjustmentFactor), tc.MemScaleUpThreshold)
			feedbackMessages["mem_scale_up"] = message
			log.Printf("WARNING: %s", message)
			anyInappropriate = true
		} else if memUsage > (tc.MemScaleUpThreshold + 15) {
			// Case 2: Memory usage moderately high compared to threshold
			tc.MemScaleUpThreshold = tc.MemScaleUpThreshold * (1 + tc.BadThresholdAdjustmentFactor/2)
			if tc.MemScaleUpThreshold > 90 {
				tc.MemScaleUpThreshold = 90
			}
			message := fmt.Sprintf("Memory usage (%0.1f%%) is significantly above your threshold (%0.1f%%). "+
				"Threshold has been automatically adjusted to %0.1f%%.",
				memUsage, tc.MemScaleUpThreshold/(1+tc.BadThresholdAdjustmentFactor/2), tc.MemScaleUpThreshold)
			feedbackMessages["mem_scale_up"] = message
			log.Printf("INFO: %s", message)
			anyInappropriate = true
		}
	}

	// Check for oscillation patterns in the history
	if len(oscillationHistory) >= 6 {
		// Look for different types of oscillation patterns
		rapidOscillationCount := 0
		gradualOscillationCount := 0
		lastAction := ""
		actionStreak := 0

		for i := 0; i < len(oscillationHistory)-1; i++ {
			currentAction := oscillationHistory[i]
			nextAction := oscillationHistory[i+1]

			// Check for rapid oscillation (immediate back-and-forth)
			if currentAction == "scale-up" && nextAction == "scale-down" {
				rapidOscillationCount++
			}

			// Check for gradual oscillation (alternating actions with gaps)
			if lastAction != "" && currentAction != lastAction {
				if actionStreak >= 2 {
					gradualOscillationCount++
				}
				actionStreak = 1
			} else {
				actionStreak++
			}

			lastAction = currentAction
		}

		// Calculate oscillation severity
		totalOscillationCount := rapidOscillationCount + gradualOscillationCount

		if totalOscillationCount >= 2 {
			// Determine the severity of oscillation
			severity := "moderate"
			if rapidOscillationCount > 0 {
				severity = "rapid"
			} else if gradualOscillationCount >= 3 {
				severity = "frequent"
			}

			// Calculate hysteresis adjustment based on severity
			var hysteresisAdjustment float64
			switch severity {
			case "rapid":
				hysteresisAdjustment = tc.BadThresholdAdjustmentFactor * 1.5
			case "frequent":
				hysteresisAdjustment = tc.BadThresholdAdjustmentFactor * 1.2
			default:
				hysteresisAdjustment = tc.BadThresholdAdjustmentFactor
			}

			// We have detected oscillation, increase hysteresis
			oldHysteresis := tc.HysteresisPercent
			tc.HysteresisPercent = tc.HysteresisPercent * (1 + hysteresisAdjustment)
			if tc.HysteresisPercent > 25 {
				tc.HysteresisPercent = 25 // Cap at 25%
			}

			// Create detailed feedback message
			message := fmt.Sprintf("Scaling oscillation detected (%s pattern). "+
				"Hysteresis has been automatically increased from %0.1f%% to %0.1f%% to reduce oscillation. "+
				"Pattern details: %d rapid oscillations, %d gradual oscillations. "+
				"Consider reviewing your scale-up/down thresholds and ensuring sufficient gap between them.",
				severity, oldHysteresis, tc.HysteresisPercent,
				rapidOscillationCount, gradualOscillationCount)

			feedbackMessages["oscillation"] = message
			log.Printf("WARNING: %s", message)
			anyInappropriate = true

			// If oscillation is severe, also adjust the thresholds
			if severity == "rapid" || severity == "frequent" {
				// Increase the gap between scale-up and scale-down thresholds
				scaleUpGap := tc.CPUScaleUpThreshold - tc.CPUScaleDownThreshold
				if scaleUpGap < 30 {
					tc.CPUScaleUpThreshold = tc.CPUScaleUpThreshold * (1 + tc.BadThresholdAdjustmentFactor/2)
					tc.CPUScaleDownThreshold = tc.CPUScaleDownThreshold * (1 - tc.BadThresholdAdjustmentFactor/2)

					message := fmt.Sprintf("Threshold gap increased to reduce oscillation. "+
						"CPU thresholds adjusted: scale-up %.1f%%, scale-down %.1f%%",
						tc.CPUScaleUpThreshold, tc.CPUScaleDownThreshold)
					feedbackMessages["threshold_gap"] = message
					log.Printf("INFO: %s", message)
				}
			}
		}
	}

	if anyInappropriate {
		// Update the configuration timestamp
		tc.LastUpdated = time.Now()
		tc.LastConfigSource = "automatic adjustment"
	}

	return anyInappropriate, feedbackMessages
}

// AdjustForSystemLoad dynamically adjusts thresholds based on current load
func (tc *ThresholdConfig) AdjustForSystemLoad(currentCPU, currentMem, messageRate float64) {
	if !tc.EnableDynamicAdjustment {
		return
	}

	// Calculate load factor (how close we are to max capacity)
	cpuLoadFactor := currentCPU / 100.0
	memLoadFactor := currentMem / 100.0

	// Use the highest load factor to determine adjustment
	loadFactor := cpuLoadFactor
	if memLoadFactor > loadFactor {
		loadFactor = memLoadFactor
	}

	// Calculate dynamic adjustment - higher load means more conservative thresholds
	adjustment := tc.AdjustmentFactor * loadFactor

	// Apply learning rate to smooth changes
	tc.CPUScaleUpThreshold = tc.CPUScaleUpThreshold*(1-tc.LearningRate) +
		(tc.CPUScaleUpThreshold-adjustment)*tc.LearningRate

	tc.MemScaleUpThreshold = tc.MemScaleUpThreshold*(1-tc.LearningRate) +
		(tc.MemScaleUpThreshold-adjustment)*tc.LearningRate

	// Apply hysteresis to prevent oscillation
	buffer := tc.HysteresisPercent / 100.0

	// Ensure down thresholds maintain sufficient gap from up thresholds
	minCPUDownThreshold := tc.CPUScaleUpThreshold * (1.0 - buffer)
	if tc.CPUScaleDownThreshold > minCPUDownThreshold {
		tc.CPUScaleDownThreshold = minCPUDownThreshold
	}

	minMemDownThreshold := tc.MemScaleUpThreshold * (1.0 - buffer)
	if tc.MemScaleDownThreshold > minMemDownThreshold {
		tc.MemScaleDownThreshold = minMemDownThreshold
	}
}

// getSystemMemoryGB returns the system's memory in GB
func getSystemMemoryGB() float64 {
	v, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("Error getting system memory: %v", err)
		return 8.0 // Default to 8GB if can't determine
	}
	return float64(v.Total) / (1024 * 1024 * 1024)
}

// getSystemNetworkCapacity returns the system's network capacity in MB/s
func getSystemNetworkCapacity() float64 {
	netIO, err := psnet.IOCounters(false)
	if err != nil {
		log.Printf("Error getting network capacity: %v", err)
		return 1000.0 // Default to 1 Gbps if can't determine
	}

	var maxSpeed float64
	for _, stat := range netIO {
		// Convert bytes to MB/s
		speed := float64(stat.BytesSent+stat.BytesRecv) / (1024 * 1024)
		if speed > maxSpeed {
			maxSpeed = speed
		}
	}

	// If we couldn't determine the speed, use a reasonable default
	if maxSpeed == 0 {
		return 1000.0 // Default to 1 Gbps
	}

	return maxSpeed
}

// MetricsCollector defines an interface for collecting and accessing system metrics
type MetricsCollector interface {
	// CollectNodeMetrics collects metrics from a specific node
	CollectNodeMetrics(nodeID string) (map[string]float64, error)

	// GetClusterNodes returns the list of node IDs in the cluster
	GetClusterNodes() []string

	// GetSystemMetrics returns current system CPU, memory, and resource utilization
	GetSystemMetrics() (cpu float64, memory float64, network float64, err error)
}

// defaultMetricsCollector provides a default implementation of MetricsCollector
type defaultMetricsCollector struct {
	store *store.Store
}

// NewDefaultMetricsCollector creates a new metrics collector using the store
func NewDefaultMetricsCollector(s *store.Store) MetricsCollector {
	return &defaultMetricsCollector{
		store: s,
	}
}

// CollectNodeMetrics implements MetricsCollector.CollectNodeMetrics
func (m *defaultMetricsCollector) CollectNodeMetrics(nodeID string) (map[string]float64, error) {
	if m.store == nil {
		return nil, fmt.Errorf("store not available")
	}

	metrics := m.store.GetNodeMetrics(nodeID)
	if metrics == nil {
		return nil, fmt.Errorf("no metrics available for node %s", nodeID)
	}

	return metrics, nil
}

// GetClusterNodes implements MetricsCollector.GetClusterNodes
func (m *defaultMetricsCollector) GetClusterNodes() []string {
	if m.store == nil {
		return []string{}
	}

	return m.store.GetClusterNodeIDs()
}

// GetSystemMetrics implements MetricsCollector.GetSystemMetrics
func (m *defaultMetricsCollector) GetSystemMetrics() (float64, float64, float64, error) {
	// Get real CPU metrics
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to get CPU metrics: %w", err)
	}

	cpuValue := 0.0
	if len(cpuPercent) > 0 {
		cpuValue = cpuPercent[0]
	}

	// Get real memory metrics
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		return cpuValue, 0, 0, fmt.Errorf("failed to get memory metrics: %w", err)
	}

	// Get network stats
	netStats := getNetworkStats()

	return cpuValue, memInfo.UsedPercent, netStats.BytesPerSecond, nil
}

// Engine is the main AI component that integrates all intelligence features
type Engine struct {
	mu                 sync.RWMutex
	loadPredictor      *prediction.LoadPredictor
	patternMatcher     *analytics.PatternMatcher
	anomalyDetector    *analytics.AnomalyDetector
	metrics            map[string]float64
	sampleInterval     time.Duration
	predictionModels   map[string]bool
	enabled            bool
	stopCh             chan struct{}
	store              *store.Store
	metricsCollector   MetricsCollector // Add metrics collector
	thresholds         ThresholdConfig
	configFile         string        // Path to config file for reloading
	reloadCh           chan struct{} // Channel to trigger config reload
	oscillationHistory []string      // Track scaling recommendation history
	lastReloadTime     time.Time     // Last time config was reloaded
	thresholdMetrics   *ThresholdAdjustmentMetrics
}

// EngineOptions holds configuration options for the AI Engine
type EngineOptions struct {
	SampleInterval   time.Duration
	MaxDataPoints    int
	EnablePredictor  bool
	EnablePatterns   bool
	EnableAnomalies  bool
	MetricsCollector MetricsCollector // Add metrics collector option
	ThresholdConfig  *ThresholdConfig // Configuration for thresholds
	ConfigFile       string           // Path to configuration file
	EnableAutoReload bool             // Whether to periodically reload config
	ReloadInterval   time.Duration    // How often to reload config
}

// DefaultEngineOptions returns the default options
func DefaultEngineOptions() EngineOptions {
	return EngineOptions{
		SampleInterval:   1 * time.Minute,
		MaxDataPoints:    10000,
		EnablePredictor:  true,
		EnablePatterns:   true,
		EnableAnomalies:  true,
		MetricsCollector: nil, // Will be initialized later
		ThresholdConfig:  nil, // Will use default thresholds
		ConfigFile:       "",  // No config file by default
		EnableAutoReload: false,
		ReloadInterval:   5 * time.Minute,
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

// Global network metrics tracker
var netTracker *NetworkMetricsTracker

// getNetworkStats collects network metrics
func getNetworkStats() NetworkStats {
	// Get network throughput using the thread-safe tracker
	bytesPerSec, err := netTracker.GetNetworkThroughput()
	if err != nil {
		log.Printf("Error getting network throughput: %v", err)
		return simulateNetworkStats() // Fall back to simulation if real metrics fail
	}

	// Get the latest stats directly for total values
	netIO, err := psnet.IOCounters(false)
	if err != nil {
		log.Printf("Error getting network stats: %v", err)
		return NetworkStats{BytesPerSecond: bytesPerSec}
	}

	if len(netIO) == 0 {
		return NetworkStats{BytesPerSecond: bytesPerSec}
	}

	stats := netIO[0]

	// Get connection count
	conns, err := psnet.Connections("all")
	if err != nil {
		log.Printf("Error getting connection stats: %v", err)
		return NetworkStats{
			BytesPerSecond: bytesPerSec,
			BytesReceived:  int64(stats.BytesRecv),
			BytesSent:      int64(stats.BytesSent),
		}
	}

	connCount := len(conns)

	// Estimate messages per second (assuming average message size of 1KB)
	messagesPerSec := bytesPerSec / 1024.0

	return NetworkStats{
		BytesPerSecond:    bytesPerSec,
		MessagesPerSecond: messagesPerSec,
		ConnectionCount:   connCount,
		BytesReceived:     int64(stats.BytesRecv),
		BytesSent:         int64(stats.BytesSent),
		ConnectionsPerSec: float64(connCount) / 60.0, // Rough estimate based on count
	}
}

// NewEngine creates a new AI Engine with the given options and store
func NewEngine(options EngineOptions, storeInstance *store.Store) *Engine {
	// Create a metrics collector if not provided
	metricsCollector := options.MetricsCollector
	if metricsCollector == nil && storeInstance != nil {
		metricsCollector = NewDefaultMetricsCollector(storeInstance)
	}

	// Initialize thresholds based on config options
	var thresholds ThresholdConfig

	// Try loading from config file first
	if options.ConfigFile != "" {
		var err error
		thresholds, err = LoadThresholdsFromFile(options.ConfigFile)
		if err != nil {
			log.Printf("Failed to load thresholds from file: %v, using environment variables", err)
			thresholds = LoadThresholdsFromEnv()
		}
	} else if options.ThresholdConfig != nil {
		// Use provided threshold config
		thresholds = *options.ThresholdConfig
		thresholds.LastConfigSource = "provided"
		thresholds.LastUpdated = time.Now()
	} else {
		// Try environment variables, fall back to defaults
		thresholds = LoadThresholdsFromEnv()
	}

	// Create engine with options
	engine := &Engine{
		metrics:            make(map[string]float64),
		sampleInterval:     options.SampleInterval,
		predictionModels:   make(map[string]bool),
		enabled:            true,
		stopCh:             make(chan struct{}),
		store:              storeInstance,
		metricsCollector:   metricsCollector,
		thresholds:         thresholds,
		configFile:         options.ConfigFile,
		reloadCh:           make(chan struct{}, 1),
		oscillationHistory: make([]string, 0, 10),
		lastReloadTime:     time.Now(),
	}

	// Initialize components based on options
	if options.EnablePredictor {
		engine.loadPredictor = prediction.NewLoadPredictor(options.MaxDataPoints)
	}

	if options.EnablePatterns {
		engine.patternMatcher = analytics.NewPatternMatcher(options.SampleInterval)
	}

	if options.EnableAnomalies {
		engine.anomalyDetector = analytics.NewAnomalyDetector(
			options.SampleInterval,
			7*24*time.Hour, // 1 week retention
			options.MaxDataPoints,
		)
	}

	// Start config reload goroutine if enabled
	if options.EnableAutoReload && options.ConfigFile != "" {
		go engine.configReloadLoop(options.ReloadInterval)
	}

	log.Printf("[AI Engine] Created new engine with sample interval %v and thresholds from %s",
		options.SampleInterval, thresholds.LastConfigSource)
	return engine
}

// configReloadLoop periodically reloads configuration from file
func (e *Engine) configReloadLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			e.ReloadConfig()
		case <-e.reloadCh:
			e.ReloadConfig()
		case <-e.stopCh:
			return
		}
	}
}

// ReloadConfig reloads thresholds configuration from the config file
func (e *Engine) ReloadConfig() {
	if e.configFile == "" {
		log.Println("No config file specified, using current thresholds")
		return
	}

	log.Printf("Reloading threshold configuration from %s", e.configFile)

	newConfig, err := LoadThresholdsFromFile(e.configFile)
	if err != nil {
		log.Printf("Failed to reload thresholds: %v, keeping current configuration", err)
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Check if configuration has actually changed
	oldConfig := e.thresholds
	if newConfig.CPUScaleUpThreshold == oldConfig.CPUScaleUpThreshold &&
		newConfig.CPUScaleDownThreshold == oldConfig.CPUScaleDownThreshold &&
		newConfig.MemScaleUpThreshold == oldConfig.MemScaleUpThreshold &&
		newConfig.MemScaleDownThreshold == oldConfig.MemScaleDownThreshold &&
		newConfig.MsgRateScaleUpThreshold == oldConfig.MsgRateScaleUpThreshold &&
		newConfig.MsgRateScaleDownThreshold == oldConfig.MsgRateScaleDownThreshold &&
		newConfig.MinConfidenceThreshold == oldConfig.MinConfidenceThreshold &&
		newConfig.EnableDynamicAdjustment == oldConfig.EnableDynamicAdjustment {
		log.Println("Threshold configuration unchanged, no update needed")
		return
	}

	// Update configuration
	e.thresholds = newConfig
	e.lastReloadTime = time.Now()

	log.Printf("Updated thresholds: CPU scale up=%.1f%%, down=%.1f%%, Memory scale up=%.1f%%, down=%.1f%%",
		newConfig.CPUScaleUpThreshold, newConfig.CPUScaleDownThreshold,
		newConfig.MemScaleUpThreshold, newConfig.MemScaleDownThreshold)
}

// TriggerConfigReload manually triggers a configuration reload
func (e *Engine) TriggerConfigReload() {
	// Only trigger if not too recent to avoid hammering the file system
	if time.Since(e.lastReloadTime) > 5*time.Second {
		select {
		case e.reloadCh <- struct{}{}:
			log.Println("Manual config reload triggered")
		default:
			log.Println("Config reload already pending")
		}
	} else {
		log.Println("Config reload requested too soon after previous reload")
	}
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

// collectMetrics gathers metrics from the system
func (e *Engine) collectMetrics() {
	if e.metricsCollector == nil {
		log.Println("Warning: metrics collector is nil, skipping metrics collection")
		return
	}

	// Get CPU, Memory, and Network metrics
	cpu, memory, network, err := e.metricsCollector.GetSystemMetrics()
	if err != nil {
		log.Printf("Error collecting system metrics: %v", err)
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.metrics["cpu_usage"] = cpu
	e.metrics["memory_usage"] = memory
	e.metrics["network_bandwidth"] = network
}

// getSystemMetrics retrieves CPU, memory, and goroutine metrics
// Kept for backwards compatibility with tests
//
//nolint:unused
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

// simulateStoreMetrics returns store metrics using real data when available
func simulateStoreMetrics() StoreMetrics {
	// In a production environment with multiple nodes, we would query all nodes
	// in the cluster to get a complete picture of storage usage

	// Check if we can get real metrics from the host
	diskUsage, err := disk.Usage("/")
	if err != nil {
		log.Printf("Failed to get disk usage: %v", err)
		return createFallbackStoreMetrics()
	}

	// Create base metrics with real disk data
	metrics := StoreMetrics{
		MessageCounts: make(map[string]int),
		Topics:        []string{"alerts", "logs", "metrics", "events", "notifications"},
		DiskEnabled:   true,
	}

	// Use real disk usage information
	metrics.DiskUsageBytes = int64(diskUsage.Used)

	// We don't have real message counts without a store instance,
	// so we'll provide realistic estimates
	metrics.TotalMessages = 0
	for _, topic := range metrics.Topics {
		// Generate a semi-random message count
		count := 100 + rand.Intn(900)
		metrics.MessageCounts[topic] = count
		metrics.TotalMessages += count
		metrics.DiskMessageCount += count
	}

	return metrics
}

// createFallbackStoreMetrics generates fallback store metrics when real data is unavailable
func createFallbackStoreMetrics() StoreMetrics {
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
		TotalMessages:  totalMessages,
		MessageCounts:  messageCounts,
		Topics:         topics,
		DiskEnabled:    true,
		DiskUsageBytes: int64(1024 * 1024 * (10 + rand.Intn(100))), // 10-110 MB
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

// NetworkMetricsTracker tracks network metrics over time to calculate rates
type NetworkMetricsTracker struct {
	mutex       sync.RWMutex
	lastStats   psnet.IOCountersStat
	lastTime    time.Time
	initialized bool
	calibration struct {
		startTime     time.Time
		maxThroughput float64
		samples       []float64
		complete      bool
		baseline      float64
		volatility    float64
		patterns      []struct {
			startTime time.Time
			endTime   time.Time
			avgRate   float64
			peakRate  float64
		}
	}
	thresholds *ThresholdConfig
	history    struct {
		throughput []float64
		timestamps []time.Time
		maxSize    int
	}
}

// NewNetworkMetricsTracker creates a new network metrics tracker
func NewNetworkMetricsTracker(thresholds *ThresholdConfig) *NetworkMetricsTracker {
	return &NetworkMetricsTracker{
		thresholds: thresholds,
		calibration: struct {
			startTime     time.Time
			maxThroughput float64
			samples       []float64
			complete      bool
			baseline      float64
			volatility    float64
			patterns      []struct {
				startTime time.Time
				endTime   time.Time
				avgRate   float64
				peakRate  float64
			}
		}{
			startTime: time.Now(),
			samples:   make([]float64, 0, 100),
		},
		history: struct {
			throughput []float64
			timestamps []time.Time
			maxSize    int
		}{
			throughput: make([]float64, 0, 1000),
			timestamps: make([]time.Time, 0, 1000),
			maxSize:    1000,
		},
	}
}

// GetNetworkThroughput calculates network throughput based on current and previous measurements
func (t *NetworkMetricsTracker) GetNetworkThroughput() (bytesPerSec float64, err error) {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	// Get current stats
	netIO, err := psnet.IOCounters(false) // false = get combined stats
	if err != nil {
		return t.getCalibratedFallback(), fmt.Errorf("failed to get network stats: %w", err)
	}

	// If we have no stats, return calibrated fallback
	if len(netIO) == 0 {
		return t.getCalibratedFallback(), fmt.Errorf("no network interfaces found")
	}

	// Get the combined stats
	currentStats := netIO[0]
	currentTime := time.Now()

	// If this is our first measurement, store it and return calibrated fallback
	if !t.initialized {
		t.lastStats = currentStats
		t.lastTime = currentTime
		t.initialized = true
		return t.getCalibratedFallback(), nil
	}

	// Calculate time difference in seconds
	timeDiff := currentTime.Sub(t.lastTime).Seconds()
	if timeDiff < 0.1 {
		// Return last known good value if measurements are too close
		return t.getCalibratedFallback(), fmt.Errorf("measurements too close together")
	}

	// Calculate bytes transmitted in the interval
	bytesSent := float64(currentStats.BytesSent - t.lastStats.BytesSent)
	bytesRecv := float64(currentStats.BytesRecv - t.lastStats.BytesRecv)
	totalBytes := bytesSent + bytesRecv

	// Calculate bytes per second
	bytesPerSec = totalBytes / timeDiff

	// Update history
	t.updateHistory(bytesPerSec, currentTime)

	// Update calibration data if needed
	if !t.calibration.complete && time.Since(t.calibration.startTime) < t.thresholds.NetworkCalibrationPeriod {
		t.calibration.samples = append(t.calibration.samples, bytesPerSec)
		if bytesPerSec > t.calibration.maxThroughput {
			t.calibration.maxThroughput = bytesPerSec
		}
	} else if !t.calibration.complete {
		// Calibration period complete, update thresholds
		t.calibration.complete = true
		if len(t.calibration.samples) > 0 {
			// Calculate baseline and volatility
			t.calibration.baseline = t.calculateBaseline()
			t.calibration.volatility = t.calculateVolatility()

			// Calculate 95th percentile of throughput
			sort.Float64s(t.calibration.samples)
			idx := int(float64(len(t.calibration.samples)) * 0.95)
			if idx >= len(t.calibration.samples) {
				idx = len(t.calibration.samples) - 1
			}
			maxThroughput := t.calibration.samples[idx]

			// Update network thresholds based on calibrated max throughput
			t.thresholds.MaxNetworkMB = maxThroughput / (1024 * 1024)
			t.thresholds.NetworkScaleUpThreshold = maxThroughput * 0.8
			t.thresholds.NetworkScaleDownThreshold = maxThroughput * 0.2

			// Detect patterns in the network usage
			t.detectPatterns()

			log.Printf("Network calibration complete. Max throughput: %.2f MB/s, Baseline: %.2f MB/s, Volatility: %.2f%%",
				t.thresholds.MaxNetworkMB, t.calibration.baseline/1024/1024, t.calibration.volatility*100)
		}
	}

	// If we somehow got 0 or negative bytes per second, use calibrated fallback
	if bytesPerSec <= 0 {
		bytesPerSec = t.getCalibratedFallback()
	}

	// Store current values for next calculation
	t.lastStats = currentStats
	t.lastTime = currentTime

	return bytesPerSec, nil
}

// getCalibratedFallback returns a system-aware fallback value
func (t *NetworkMetricsTracker) getCalibratedFallback() float64 {
	// If thresholds is nil, use safe default values
	if t.thresholds == nil {
		return 1024 * 1024 // 1 MB/s as safe default
	}

	if !t.calibration.complete {
		// During initial calibration, use system capacity-based fallback
		return t.thresholds.MaxNetworkMB * 1024 * 1024 * t.thresholds.NetworkFallbackRatio
	}

	// Use calibrated baseline with volatility adjustment
	volatilityFactor := 1.0 + (t.calibration.volatility * 0.5) // Adjust up to 50% based on volatility
	return t.calibration.baseline * volatilityFactor
}

// calculateBaseline calculates the baseline network throughput
func (t *NetworkMetricsTracker) calculateBaseline() float64 {
	if len(t.calibration.samples) == 0 {
		// If thresholds is nil, use safe default values
		if t.thresholds == nil {
			return 1024 * 1024 // 1 MB/s as safe default
		}
		return t.thresholds.MaxNetworkMB * 1024 * 1024 * t.thresholds.NetworkFallbackRatio
	}

	// Calculate median as baseline (more robust than mean)
	sort.Float64s(t.calibration.samples)
	median := t.calibration.samples[len(t.calibration.samples)/2]
	return median
}

// calculateVolatility calculates the volatility of network throughput
func (t *NetworkMetricsTracker) calculateVolatility() float64 {
	if len(t.calibration.samples) < 2 {
		return 0.0
	}

	// Calculate coefficient of variation (standard deviation / mean)
	mean := 0.0
	for _, sample := range t.calibration.samples {
		mean += sample
	}
	mean /= float64(len(t.calibration.samples))

	variance := 0.0
	for _, sample := range t.calibration.samples {
		diff := sample - mean
		variance += diff * diff
	}
	variance /= float64(len(t.calibration.samples))

	stdDev := math.Sqrt(variance)
	return stdDev / mean
}

// updateHistory maintains a rolling history of throughput measurements
func (t *NetworkMetricsTracker) updateHistory(throughput float64, timestamp time.Time) {
	t.history.throughput = append(t.history.throughput, throughput)
	t.history.timestamps = append(t.history.timestamps, timestamp)

	// Trim history if it exceeds max size
	if len(t.history.throughput) > t.history.maxSize {
		t.history.throughput = t.history.throughput[1:]
		t.history.timestamps = t.history.timestamps[1:]
	}
}

// detectPatterns analyzes network throughput patterns
func (t *NetworkMetricsTracker) detectPatterns() {
	if len(t.history.throughput) < 2 {
		return
	}

	// Look for patterns in the history
	var currentPattern struct {
		startTime time.Time
		endTime   time.Time
		avgRate   float64
		peakRate  float64
	}

	// Initialize first pattern
	currentPattern.startTime = t.history.timestamps[0]
	currentPattern.peakRate = t.history.throughput[0]
	sum := t.history.throughput[0]
	count := 1

	// Analyze throughput changes
	for i := 1; i < len(t.history.throughput); i++ {
		rate := t.history.throughput[i]

		// Check for significant changes (more than 50% difference)
		if math.Abs(rate-currentPattern.peakRate)/currentPattern.peakRate > 0.5 {
			// End current pattern if it's significant enough
			if count > 5 { // Require at least 5 samples for a pattern
				currentPattern.endTime = t.history.timestamps[i-1]
				currentPattern.avgRate = sum / float64(count)
				t.calibration.patterns = append(t.calibration.patterns, currentPattern)
			}

			// Start new pattern
			currentPattern = struct {
				startTime time.Time
				endTime   time.Time
				avgRate   float64
				peakRate  float64
			}{
				startTime: t.history.timestamps[i],
				peakRate:  rate,
			}
			sum = rate
			count = 1
		} else {
			// Update current pattern
			if rate > currentPattern.peakRate {
				currentPattern.peakRate = rate
			}
			sum += rate
			count++
		}
	}

	// Add final pattern if it exists
	if count > 5 {
		currentPattern.endTime = t.history.timestamps[len(t.history.timestamps)-1]
		currentPattern.avgRate = sum / float64(count)
		t.calibration.patterns = append(t.calibration.patterns, currentPattern)
	}

	// Log pattern information
	if len(t.calibration.patterns) > 0 {
		log.Printf("Detected %d network throughput patterns", len(t.calibration.patterns))
		for i, pattern := range t.calibration.patterns {
			log.Printf("Pattern %d: %.2f MB/s avg, %.2f MB/s peak, duration: %v",
				i+1, pattern.avgRate/1024/1024, pattern.peakRate/1024/1024,
				pattern.endTime.Sub(pattern.startTime))
		}
	}
}

// simulateNetworkStats returns network metrics using real-time measurements
func simulateNetworkStats() NetworkStats {
	// Create a default threshold config if netTracker is nil
	if netTracker == nil {
		defaultThresholds := DefaultThresholds()
		netTracker = NewNetworkMetricsTracker(&defaultThresholds)
	}

	// Get real network throughput
	bytesPerSec, err := netTracker.GetNetworkThroughput()
	if err != nil {
		log.Printf("Error calculating network throughput: %v", err)
		// Use system-specific fallback based on max capacity
		bytesPerSec = netTracker.thresholds.MaxNetworkMB * 1024 * 1024 * netTracker.thresholds.NetworkFallbackRatio
	}

	// Get the latest stats directly for total values
	netIO, err := psnet.IOCounters(false)
	if err != nil {
		log.Printf("Error getting network stats: %v", err)
		return NetworkStats{
			BytesPerSecond:    bytesPerSec,
			MessagesPerSecond: bytesPerSec / 1024.0,                                          // Assuming 1KB messages
			ConnectionCount:   10,                                                            // Reasonable fallback
			BytesReceived:     int64(netTracker.thresholds.MaxNetworkMB * 1024 * 1024 * 100), // 100MB received
			BytesSent:         int64(netTracker.thresholds.MaxNetworkMB * 1024 * 1024 * 50),  // 50MB sent
			ConnectionsPerSec: 0.5,                                                           // 0.5 connections per second
		}
	}

	if len(netIO) == 0 {
		return NetworkStats{
			BytesPerSecond:    bytesPerSec,
			MessagesPerSecond: bytesPerSec / 1024.0,
			ConnectionCount:   10,
			BytesReceived:     int64(netTracker.thresholds.MaxNetworkMB * 1024 * 1024 * 100),
			BytesSent:         int64(netTracker.thresholds.MaxNetworkMB * 1024 * 1024 * 50),
			ConnectionsPerSec: 0.5,
		}
	}

	stats := netIO[0]

	// Get connection count
	conns, err := psnet.Connections("all")
	if err != nil {
		log.Printf("Error getting connection stats: %v", err)
		return NetworkStats{
			BytesPerSecond:    bytesPerSec,
			MessagesPerSecond: bytesPerSec / 1024.0,
			ConnectionCount:   10,
			BytesReceived:     int64(stats.BytesRecv),
			BytesSent:         int64(stats.BytesSent),
			ConnectionsPerSec: 0.5,
		}
	}

	connCount := len(conns)

	// Estimate messages per second (assuming average message size of 1KB)
	messagesPerSec := bytesPerSec / 1024.0

	return NetworkStats{
		BytesPerSecond:    bytesPerSec,
		MessagesPerSecond: messagesPerSec,
		ConnectionCount:   connCount,
		BytesReceived:     int64(stats.BytesRecv),
		BytesSent:         int64(stats.BytesSent),
		ConnectionsPerSec: float64(connCount) / 60.0, // Rough estimate based on count
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

// GetScalingRecommendations returns scaling recommendations for all nodes or a specific node
func (e *Engine) GetScalingRecommendations(nodeID string) []ScalingRecommendation {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Check if engine is enabled
	if !e.enabled {
		return []ScalingRecommendation{}
	}

	// If nodeID specified, get recommendations for just that node
	if nodeID != "" {
		recommendations, err := e.getNodeRecommendations(nodeID)
		if err != nil {
			log.Printf("Error getting recommendations for node %s: %v", nodeID, err)
			return []ScalingRecommendation{}
		}

		// Apply minimum confidence threshold from config and track recommendation history
		var filteredRecs []ScalingRecommendation
		for _, rec := range recommendations {
			if rec.Confidence >= e.thresholds.MinConfidenceThreshold {
				filteredRecs = append(filteredRecs, rec)

				// Track action for oscillation detection
				if nodeID == "local" && (rec.Resource == "CPU" || rec.Resource == "Memory") {
					e.trackRecommendationAction(rec.RecommendedAction)
				}
			}
		}

		return filteredRecs
	}

	// Get recommendations for all nodes
	var allRecommendations []ScalingRecommendation
	nodeIDs := e.metricsCollector.GetClusterNodes()

	for _, id := range nodeIDs {
		recommendations, err := e.getNodeRecommendations(id)
		if err != nil {
			log.Printf("Error getting recommendations for node %s: %v", id, err)
			continue
		}

		// Apply minimum confidence threshold from config
		var filteredRecs []ScalingRecommendation
		for _, rec := range recommendations {
			if rec.Confidence >= e.thresholds.MinConfidenceThreshold {
				filteredRecs = append(filteredRecs, rec)

				// Track action history for local node to detect oscillations
				if id == "local" && (rec.Resource == "CPU" || rec.Resource == "Memory") {
					e.trackRecommendationAction(rec.RecommendedAction)
				}
			}
		}

		allRecommendations = append(allRecommendations, filteredRecs...)
	}

	// Sort by confidence (highest first)
	sort.Slice(allRecommendations, func(i, j int) bool {
		return allRecommendations[i].Confidence > allRecommendations[j].Confidence
	})

	// Check for oscillation patterns and adjust thresholds if needed
	e.detectAndAdjustThresholds()

	return allRecommendations
}

// trackRecommendationAction adds an action to the oscillation history
func (e *Engine) trackRecommendationAction(action string) {
	// Only track scale_up and scale_down actions
	if action != "scale_up" && action != "scale_down" {
		return
	}

	// Add to history (with mutex lock)
	e.mu.Lock()

	// Keep a limited history (only track the last 10 recommendations)
	if len(e.oscillationHistory) >= 10 {
		// Remove oldest entry
		e.oscillationHistory = e.oscillationHistory[1:]
	}

	// Add new entry
	e.oscillationHistory = append(e.oscillationHistory, action)

	e.mu.Unlock()
}

// detectAndAdjustThresholds checks and adjusts thresholds based on observed metrics
func (e *Engine) detectAndAdjustThresholds() {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Skip if feature is disabled or no metrics data is available
	if !e.thresholds.BadThresholdDetectionEnabled || len(e.metrics) == 0 {
		return
	}

	// Call threshold detection and get feedback messages
	adjusted, feedback := e.thresholds.DetectInappropriateThresholds(e.metrics, e.oscillationHistory)

	// Record threshold adjustment metrics for observability
	e.recordThresholdAdjustment(adjusted, feedback)

	if adjusted {
		log.Printf("[AI Engine] Threshold configuration has been automatically adjusted")

		// Log detailed feedback messages if available
		if len(feedback) > 0 {
			for threshold, message := range feedback {
				log.Printf("[AI Engine] Threshold Feedback (%s): %s", threshold, message)
			}
		}

		// If we're using a config file, consider persisting the changes
		if e.configFile != "" {
			thresholdData, err := json.MarshalIndent(e.thresholds, "", "  ")
			if err == nil {
				// Write updated thresholds back to file with a warning comment
				content := "// WARNING: This file was automatically updated due to detected threshold issues\n" +
					"// Timestamp: " + time.Now().Format(time.RFC3339) + "\n" +
					string(thresholdData)
				err = os.WriteFile(e.configFile+".adjusted", []byte(content), 0644)
				if err != nil {
					log.Printf("WARNING: Failed to persist adjusted thresholds: %v", err)
				} else {
					log.Printf("INFO: Adjusted thresholds persisted to %s.adjusted", e.configFile)
				}
			}
		}
	}
}

// getNodeRecommendations queries a node for its scaling recommendations using real metrics
func (e *Engine) getNodeRecommendations(nodeID string) ([]ScalingRecommendation, error) {
	// Check if metrics collector is available
	if e.metricsCollector == nil {
		log.Printf("Error: metrics collector is nil, cannot get recommendations for node %s", nodeID)
		return nil, fmt.Errorf("metrics collector not available")
	}

	// Use the metrics collector to get real node metrics
	nodeMetrics, err := e.metricsCollector.CollectNodeMetrics(nodeID)
	if err != nil {
		return nil, fmt.Errorf("failed to collect metrics from node %s: %w", nodeID, err)
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

// GetThresholds returns the current threshold configuration
func (e *Engine) GetThresholds() ThresholdConfig {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.thresholds
}

// SetThresholds updates the threshold configuration with error details
func (e *Engine) SetThresholds(config ThresholdConfig) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Validate the thresholds
	if err := config.Validate(); err != nil {
		return fmt.Errorf("invalid threshold configuration: %w", err)
	}

	// Store previous thresholds for logging
	prevCPUUp := e.thresholds.CPUScaleUpThreshold
	prevCPUDown := e.thresholds.CPUScaleDownThreshold
	prevMemUp := e.thresholds.MemScaleUpThreshold
	prevMemDown := e.thresholds.MemScaleDownThreshold
	prevMsgUp := e.thresholds.MsgRateScaleUpThreshold
	prevMsgDown := e.thresholds.MsgRateScaleDownThreshold

	// Update the timestamp and source
	config.LastUpdated = time.Now()
	if config.LastConfigSource == "" {
		config.LastConfigSource = "api"
	}

	// Apply the new thresholds
	e.thresholds = config

	// Log the changes for observability
	log.Printf("[AI Engine] Threshold configuration updated via %s", config.LastConfigSource)
	log.Printf("[AI Engine] CPU thresholds changed: up %.1f%% → %.1f%%, down %.1f%% → %.1f%%",
		prevCPUUp, config.CPUScaleUpThreshold, prevCPUDown, config.CPUScaleDownThreshold)
	log.Printf("[AI Engine] Memory thresholds changed: up %.1f%% → %.1f%%, down %.1f%% → %.1f%%",
		prevMemUp, config.MemScaleUpThreshold, prevMemDown, config.MemScaleDownThreshold)
	log.Printf("[AI Engine] Message rate thresholds changed: up %.1f → %.1f, down %.1f → %.1f",
		prevMsgUp, config.MsgRateScaleUpThreshold, prevMsgDown, config.MsgRateScaleDownThreshold)

	return nil
}

// Validate checks that all thresholds are within valid ranges with detailed errors
// GetFullSystemStatus returns a complete map of system status information
func (e *Engine) GetFullSystemStatus() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	statusMap := map[string]interface{}{
		"timestamp":                    time.Now().Format(time.RFC3339),
		"current_metrics":              e.metrics,
		"node_metrics":                 e.getNodesMetrics(),
		"prediction_state":             e.isPredictionEnabled(),
		"thresholds":                   e.thresholds,
		"threshold_adjustment_metrics": e.GetThresholdAdjustmentMetrics(),
	}

	// If store is available, add store metrics
	if e.store != nil {
		statusMap["store_metrics"] = e.getStoreMetrics()
	} else {
		statusMap["store_metrics"] = "Store not available"
	}

	// Add CPU and memory utilization
	cpu, mem, _ := getSystemMetrics()
	statusMap["cpu_utilization"] = cpu
	statusMap["memory_utilization"] = mem

	// Add network statistics if available
	netStats := getNetworkStats()
	statusMap["network_stats"] = netStats

	// If prediction is enabled, add predictions
	if e.loadPredictor != nil {
		predictions, err := e.getResourcePredictions("local", 60*time.Minute)
		if err == nil {
			statusMap["predictions"] = predictions
		} else {
			statusMap["predictions_error"] = err.Error()
		}
	}

	// Add anomaly information if available
	if e.anomalyDetector != nil {
		anomalies := e.GetAnomalies(MetricKindCPUUsage, "local", time.Now().Add(-24*time.Hour))
		if len(anomalies) > 0 {
			statusMap["recent_anomalies"] = anomalies
		}
	}

	// Add recommendations if possible
	if e.enabled {
		recommendations := e.GetScalingRecommendations("local")
		if len(recommendations) > 0 {
			statusMap["scaling_recommendations"] = recommendations
		}
	}

	// Include oscillation history information
	if len(e.oscillationHistory) > 0 {
		statusMap["scaling_history"] = e.oscillationHistory
	}

	// Include configuration metadata
	statusMap["config_metadata"] = map[string]interface{}{
		"last_config_reload":  e.lastReloadTime.Format(time.RFC3339),
		"config_file":         e.configFile,
		"auto_reload_enabled": (e.reloadCh != nil),
	}

	return statusMap
}

// RecordMetrics records multiple metrics for later analysis
func (e *Engine) RecordMetrics(metrics map[string]float64) error {
	if metrics == nil {
		return fmt.Errorf("metrics cannot be nil")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Record each metric
	for key, value := range metrics {
		e.metrics[key] = value
	}

	// Process metrics if needed
	return nil
}

// ThresholdAdjustmentMetrics tracks statistics about threshold adjustments
type ThresholdAdjustmentMetrics struct {
	// Total adjustment counts
	TotalAdjustments     int
	CPUThresholdAdjusted int
	MemThresholdAdjusted int
	MsgRateAdjusted      int
	HysteresisAdjusted   int

	// Oscillation tracking
	OscillationPatterns struct {
		RapidOscillations    int
		GradualOscillations  int
		LastOscillationTime  time.Time
		LastOscillationType  string
		OscillationSeverity  string
		ThresholdGapAdjusted bool
	}

	// Last adjustment timestamps
	LastAdjustmentTime time.Time

	// Sample metrics from last adjustment
	LastAdjustedCPUThreshold     float64
	LastAdjustedMemThreshold     float64
	LastAdjustedMsgRateThreshold float64

	// Feedback messages from most recent adjustments
	RecentFeedback []string
}

// recordThresholdAdjustment updates metrics for threshold adjustments
func (e *Engine) recordThresholdAdjustment(adjusted bool, feedback map[string]string) {
	if !adjusted {
		return
	}

	// Initialize metrics if needed
	if e.thresholdMetrics == nil {
		e.thresholdMetrics = &ThresholdAdjustmentMetrics{}
	}

	// Update counters
	e.thresholdMetrics.TotalAdjustments++
	e.thresholdMetrics.LastAdjustmentTime = time.Now()

	// Track specific threshold adjustments
	if _, ok := feedback["cpu_scale_up"]; ok {
		e.thresholdMetrics.CPUThresholdAdjusted++
		e.thresholdMetrics.LastAdjustedCPUThreshold = e.thresholds.CPUScaleUpThreshold
	}

	if _, ok := feedback["mem_scale_up"]; ok {
		e.thresholdMetrics.MemThresholdAdjusted++
		e.thresholdMetrics.LastAdjustedMemThreshold = e.thresholds.MemScaleUpThreshold
	}

	if _, ok := feedback["msg_rate_scale_up"]; ok {
		e.thresholdMetrics.MsgRateAdjusted++
		e.thresholdMetrics.LastAdjustedMsgRateThreshold = e.thresholds.MsgRateScaleUpThreshold
	}

	// Track oscillation patterns
	if _, ok := feedback["oscillation"]; ok {
		e.thresholdMetrics.HysteresisAdjusted++
		e.thresholdMetrics.OscillationPatterns.LastOscillationTime = time.Now()

		// Parse oscillation details from feedback message
		message := feedback["oscillation"]
		if strings.Contains(message, "rapid pattern") {
			e.thresholdMetrics.OscillationPatterns.RapidOscillations++
			e.thresholdMetrics.OscillationPatterns.LastOscillationType = "rapid"
			e.thresholdMetrics.OscillationPatterns.OscillationSeverity = "high"
		} else if strings.Contains(message, "frequent pattern") {
			e.thresholdMetrics.OscillationPatterns.GradualOscillations++
			e.thresholdMetrics.OscillationPatterns.LastOscillationType = "frequent"
			e.thresholdMetrics.OscillationPatterns.OscillationSeverity = "medium"
		} else {
			e.thresholdMetrics.OscillationPatterns.LastOscillationType = "moderate"
			e.thresholdMetrics.OscillationPatterns.OscillationSeverity = "low"
		}
	}

	// Track threshold gap adjustments
	if _, ok := feedback["threshold_gap"]; ok {
		e.thresholdMetrics.OscillationPatterns.ThresholdGapAdjusted = true
	}

	// Store recent feedback messages (keeping only the last 5)
	if len(feedback) > 0 {
		for _, message := range feedback {
			e.thresholdMetrics.RecentFeedback = append(e.thresholdMetrics.RecentFeedback, message)
		}

		// Trim to last 5 messages
		if len(e.thresholdMetrics.RecentFeedback) > 5 {
			e.thresholdMetrics.RecentFeedback = e.thresholdMetrics.RecentFeedback[len(e.thresholdMetrics.RecentFeedback)-5:]
		}
	}
}

// GetThresholdAdjustmentMetrics returns metrics about threshold adjustments
func (e *Engine) GetThresholdAdjustmentMetrics() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if e.thresholdMetrics == nil {
		return map[string]interface{}{
			"total_adjustments": 0,
			"enabled":           e.thresholds.BadThresholdDetectionEnabled,
		}
	}

	timeSinceLastAdjustment := time.Since(e.thresholdMetrics.LastAdjustmentTime).String()
	if e.thresholdMetrics.LastAdjustmentTime.IsZero() {
		timeSinceLastAdjustment = "never"
	}

	timeSinceLastOscillation := "never"
	if !e.thresholdMetrics.OscillationPatterns.LastOscillationTime.IsZero() {
		timeSinceLastOscillation = time.Since(e.thresholdMetrics.OscillationPatterns.LastOscillationTime).String()
	}

	return map[string]interface{}{
		"total_adjustments":          e.thresholdMetrics.TotalAdjustments,
		"cpu_adjustments":            e.thresholdMetrics.CPUThresholdAdjusted,
		"memory_adjustments":         e.thresholdMetrics.MemThresholdAdjusted,
		"message_rate_adjustments":   e.thresholdMetrics.MsgRateAdjusted,
		"hysteresis_adjustments":     e.thresholdMetrics.HysteresisAdjusted,
		"last_adjustment":            e.thresholdMetrics.LastAdjustmentTime.Format(time.RFC3339),
		"time_since_last_adjustment": timeSinceLastAdjustment,
		"current_cpu_threshold":      e.thresholds.CPUScaleUpThreshold,
		"current_memory_threshold":   e.thresholds.MemScaleUpThreshold,
		"recent_feedback":            e.thresholdMetrics.RecentFeedback,
		"enabled":                    e.thresholds.BadThresholdDetectionEnabled,
		"oscillation_patterns": map[string]interface{}{
			"rapid_oscillations":     e.thresholdMetrics.OscillationPatterns.RapidOscillations,
			"gradual_oscillations":   e.thresholdMetrics.OscillationPatterns.GradualOscillations,
			"last_oscillation_time":  e.thresholdMetrics.OscillationPatterns.LastOscillationTime.Format(time.RFC3339),
			"time_since_oscillation": timeSinceLastOscillation,
			"last_oscillation_type":  e.thresholdMetrics.OscillationPatterns.LastOscillationType,
			"oscillation_severity":   e.thresholdMetrics.OscillationPatterns.OscillationSeverity,
			"threshold_gap_adjusted": e.thresholdMetrics.OscillationPatterns.ThresholdGapAdjusted,
		},
	}
}

// getNodesMetrics collects metrics from all nodes in the cluster
func (e *Engine) getNodesMetrics() map[string]interface{} {
	result := make(map[string]interface{})

	// Skip if metrics collector is nil
	if e.metricsCollector == nil {
		return result
	}

	// Get all node IDs from the cluster
	nodeIDs := e.metricsCollector.GetClusterNodes()

	// Add local node metrics
	localMetrics := make(map[string]float64)
	for k, v := range e.metrics {
		if strings.HasPrefix(k, "local:") {
			// Strip the prefix for cleaner output
			metricName := strings.TrimPrefix(k, "local:")
			localMetrics[metricName] = v
		}
	}
	result["local"] = localMetrics

	// Add remote node metrics
	for _, nodeID := range nodeIDs {
		if nodeID == "local" {
			continue
		}

		// Try to get metrics for this node
		nodeMetrics, err := e.metricsCollector.CollectNodeMetrics(nodeID)
		if err != nil {
			log.Printf("Error collecting metrics from node %s: %v", nodeID, err)
			continue
		}

		result[nodeID] = nodeMetrics
	}

	return result
}

// isPredictionEnabled returns the state of prediction features
func (e *Engine) isPredictionEnabled() map[string]interface{} {
	result := map[string]interface{}{
		"engine_enabled": e.enabled,
	}

	// Add prediction models status
	if e.loadPredictor != nil {
		modelInfo := make(map[string]bool)
		for modelName, enabled := range e.predictionModels {
			modelInfo[modelName] = enabled
		}
		result["prediction_models"] = modelInfo
		result["predictor_available"] = true
	} else {
		result["predictor_available"] = false
	}

	// Add pattern detection status
	if e.patternMatcher != nil {
		result["pattern_detection_available"] = true
	} else {
		result["pattern_detection_available"] = false
	}

	// Add anomaly detection status
	if e.anomalyDetector != nil {
		result["anomaly_detection_available"] = true
	} else {
		result["anomaly_detection_available"] = false
	}

	return result
}

// getResourcePredictions returns predictions for the specified timeframe
func (e *Engine) getResourcePredictions(nodeID string, timeframe time.Duration) (map[string]interface{}, error) {
	if e.loadPredictor == nil {
		return nil, fmt.Errorf("predictor not enabled")
	}

	result := make(map[string]interface{})
	future := time.Now().Add(timeframe)

	// Get CPU prediction
	cpuPrediction, err := e.PredictLoad(prediction.ResourceCPU, nodeID, future)
	if err == nil {
		result["cpu"] = map[string]interface{}{
			"current":        e.GetCurrentMetric(MetricKindCPUUsage, nodeID),
			"predicted":      cpuPrediction.PredictedVal,
			"confidence":     cpuPrediction.Confidence,
			"prediction_for": future.Format(time.RFC3339),
		}
	} else {
		result["cpu_error"] = err.Error()
	}

	// Get memory prediction
	memPrediction, err := e.PredictLoad(prediction.ResourceMemory, nodeID, future)
	if err == nil {
		result["memory"] = map[string]interface{}{
			"current":        e.GetCurrentMetric(MetricKindMemoryUsage, nodeID),
			"predicted":      memPrediction.PredictedVal,
			"confidence":     memPrediction.Confidence,
			"prediction_for": future.Format(time.RFC3339),
		}
	} else {
		result["memory_error"] = err.Error()
	}

	// Get message rate prediction
	msgPrediction, err := e.PredictLoad(prediction.ResourceMessageRate, nodeID, future)
	if err == nil {
		result["message_rate"] = map[string]interface{}{
			"current":        e.GetCurrentMetric(MetricKindMessageRate, nodeID),
			"predicted":      msgPrediction.PredictedVal,
			"confidence":     msgPrediction.Confidence,
			"prediction_for": future.Format(time.RFC3339),
		}
	} else {
		result["message_rate_error"] = err.Error()
	}

	return result, nil
}

// reloadThresholds is a test helper that reloads thresholds from a file
func (e *Engine) reloadThresholds(filename string) error {
	newThresholds, err := LoadThresholdsFromFile(filename)
	if err != nil {
		return fmt.Errorf("failed to reload thresholds: %w", err)
	}

	// Set the source explicitly to "file" without the path
	newThresholds.LastConfigSource = "file"

	// Update the engine's thresholds
	return e.SetThresholds(newThresholds)
}

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
