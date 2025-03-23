package ai

import (
	"testing"
	"time"
)

// BenchmarkGetCPUUsagePercent benchmarks the CPU metrics collection function
func BenchmarkGetCPUUsagePercent(b *testing.B) {
	// Reset timer to exclude setup
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_ = getCPUUsagePercent()
	}
}

// BenchmarkGetMemoryUsagePercent benchmarks the memory metrics collection function
func BenchmarkGetMemoryUsagePercent(b *testing.B) {
	// Reset timer to exclude setup
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_ = getMemoryUsagePercent()
	}
}

// BenchmarkGetDiskIOStats benchmarks the disk I/O metrics collection function
func BenchmarkGetDiskIOStats(b *testing.B) {
	// Reset timer to exclude setup
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_, _ = getDiskIOStats()
	}
}

// BenchmarkGetNetworkStats benchmarks the network metrics collection function
func BenchmarkGetNetworkStats(b *testing.B) {
	// Reset timer to exclude setup
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_ = getNetworkStats()
	}
}

// BenchmarkCollectSystemMetrics benchmarks collecting all system metrics at once
func BenchmarkCollectSystemMetrics(b *testing.B) {
	// Create engine with normal collection interval
	options := EngineOptions{
		SampleInterval:  100 * time.Millisecond,
		MaxDataPoints:   100,
		EnablePredictor: false, // Disable predictor for pure metrics performance
	}

	engine := NewEngine(options, nil)
	engine.Start()

	// Let the engine initialize
	time.Sleep(200 * time.Millisecond)

	// Reset timer to exclude setup
	b.ResetTimer()

	// Run the benchmark - measure collection of system metrics
	for i := 0; i < b.N; i++ {
		engine.collectMetrics()
	}

	// Stop the timer before cleanup
	b.StopTimer()

	// Cleanup
	engine.Stop()
}

// BenchmarkCollectEngineMetrics benchmarks the entire metrics collection process
func BenchmarkCollectEngineMetrics(b *testing.B) {
	// Skip for short benchmarks
	if testing.Short() {
		b.Skip("Skipping engine metrics benchmark in short mode")
	}

	// Define frequency thresholds to test (in milliseconds)
	frequencies := []int{100, 50, 20}

	for _, freq := range frequencies {
		frequency := time.Duration(freq) * time.Millisecond

		b.Run(frequency.String(), func(b *testing.B) {
			// Create engine with specified collection interval
			options := EngineOptions{
				SampleInterval:  frequency,
				MaxDataPoints:   1000,
				EnablePredictor: false,
			}

			engine := NewEngine(options, nil)

			// Reset timer to exclude setup
			b.ResetTimer()

			// Start the engine
			engine.Start()

			// Let it run for 3 seconds at this frequency
			time.Sleep(3 * time.Second)

			// Stop the timer and engine
			b.StopTimer()
			engine.Stop()

			// Report metrics about the benchmark
			// We could examine engine.metricsData but it's private
			// Instead we'll just report the frequency we tested
			b.ReportMetric(float64(freq), "poll_interval_ms")
		})
	}
}

// BenchmarkMetricsWithSimulation benchmarks metrics collection against simulated loads
func BenchmarkMetricsWithSimulation(b *testing.B) {
	// Simulate different system loads
	scenarios := []struct {
		name string
		load func()
		cool func()
	}{
		{
			name: "Baseline",
			load: func() {},
			cool: func() {},
		},
		{
			name: "CPUIntensive",
			load: func() {
				// Start CPU-intensive goroutines
				done := make(chan bool)
				for i := 0; i < 4; i++ {
					go func() {
						for {
							select {
							case <-done:
								return
							default:
								// CPU-intensive operation
								for j := 0; j < 1000000; j++ {
									_ = j * j
								}
							}
						}
					}()
				}
				// Let CPU usage ramp up
				time.Sleep(500 * time.Millisecond)
				close(done)
			},
			cool: func() {
				time.Sleep(500 * time.Millisecond)
			},
		},
		{
			name: "MemoryIntensive",
			load: func() {
				// Allocate memory
				mem := make([][]byte, 10)
				for i := 0; i < 10; i++ {
					mem[i] = make([]byte, 10*1024*1024) // 10MB chunks
				}
				// Use the memory to prevent optimization
				for i := 0; i < 10; i++ {
					for j := 0; j < 1024; j++ {
						mem[i][j] = byte(j % 256)
					}
				}
				// Wait for a moment to let memory pressure register
				time.Sleep(200 * time.Millisecond)
			},
			cool: func() {
				// Force garbage collection
				time.Sleep(200 * time.Millisecond)
			},
		},
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			// Create engine
			options := EngineOptions{
				SampleInterval:  100 * time.Millisecond,
				MaxDataPoints:   100,
				EnablePredictor: false,
			}

			engine := NewEngine(options, nil)
			engine.Start()

			// Let the engine initialize
			time.Sleep(200 * time.Millisecond)

			// Run the benchmark multiple times
			for i := 0; i < b.N; i++ {
				// Apply load
				b.StopTimer()
				scenario.load()
				b.StartTimer()

				// Measure metrics collection under this load
				start := time.Now()
				// Measure just the system metrics collection for consistent comparison
				engine.collectMetrics()
				collectionTime := time.Since(start)

				// Report collection time
				b.ReportMetric(float64(collectionTime.Nanoseconds())/1e6, "ms/op")

				// Cool down
				b.StopTimer()
				scenario.cool()
				b.StartTimer()
			}

			// Cleanup
			b.StopTimer()
			engine.Stop()
		})
	}
}
