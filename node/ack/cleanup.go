package ack

import (
	"log"
	"sync"
	"time"
)

// CleanupWorker is responsible for cleaning up expired and orphaned ACKs
type CleanupWorker struct {
	tracker      *Tracker
	interval     time.Duration
	maxAckAge    time.Duration
	maxOrphanAge time.Duration
	batchSize    int
	lastCleanup  time.Time
	mu           sync.Mutex
	statsLock    sync.RWMutex
	cleanupStats CleanupStats
}

// CleanupStats tracks cleanup operation statistics
type CleanupStats struct {
	LastCleanupTime       time.Time
	ExpiredAcksRemoved    int64
	OrphanedAcksRemoved   int64
	IncompleteAcksRemoved int64
	TotalCleanupRuns      int64
	AvgCleanupDuration    time.Duration
	TotalCleanupDuration  time.Duration
}

// NewCleanupWorker creates a new cleanup worker
func NewCleanupWorker(tracker *Tracker, interval time.Duration) *CleanupWorker {
	return &CleanupWorker{
		tracker:      tracker,
		interval:     interval,
		maxAckAge:    24 * time.Hour, // Default max age for ACKs
		maxOrphanAge: 1 * time.Hour,  // Default max age for orphaned ACKs
		batchSize:    1000,           // Process in batches for efficiency
		lastCleanup:  time.Now(),
	}
}

// Run starts the cleanup worker's processing loop
func (c *CleanupWorker) Run(shutdownCh <-chan struct{}) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	log.Printf("[CleanupWorker] Started with interval %v", c.interval)

	for {
		select {
		case <-ticker.C:
			c.performCleanup()
		case <-shutdownCh:
			log.Println("[CleanupWorker] Shutting down")
			// Perform one final cleanup on shutdown
			c.performCleanup()
			return
		}
	}
}

// SetMaxAckAge sets the maximum age for ACKs before they're cleaned up
func (c *CleanupWorker) SetMaxAckAge(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxAckAge = duration
}

// SetMaxOrphanAge sets the maximum age for orphaned ACKs before they're cleaned up
func (c *CleanupWorker) SetMaxOrphanAge(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.maxOrphanAge = duration
}

// SetBatchSize sets the batch size for cleanup operations
func (c *CleanupWorker) SetBatchSize(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.batchSize = size
}

// GetStats returns the current cleanup statistics
func (c *CleanupWorker) GetStats() CleanupStats {
	c.statsLock.RLock()
	defer c.statsLock.RUnlock()
	return c.cleanupStats
}

// performCleanup executes the cleanup process
func (c *CleanupWorker) performCleanup() {
	c.mu.Lock()
	maxAckAge := c.maxAckAge
	maxOrphanAge := c.maxOrphanAge
	c.mu.Unlock()

	startTime := time.Now()
	log.Printf("[CleanupWorker] Starting cleanup process")

	// Track statistics
	var expiredCount, orphanedCount, incompleteCount int

	// Get a snapshot of current ACK states
	states := c.tracker.getAckStatesSnapshot()
	now := time.Now()

	// Divide into categories for cleanup
	var expired, orphaned, incomplete []string

	for msgID, state := range states {
		ackAge := now.Sub(state.CreatedAt)

		// Check for expired ACKs (too old)
		if ackAge > maxAckAge {
			expired = append(expired, msgID)
			expiredCount++
			continue
		}

		// Check for orphaned ACKs (no activity for a while)
		if ackAge > maxOrphanAge {
			lastActivity := now.Sub(state.LastAttempt)
			if lastActivity > maxOrphanAge {
				orphaned = append(orphaned, msgID)
				orphanedCount++
				continue
			}
		}

		// Check for incomplete ACKs (partial delivery that can't be completed)
		// This is a heuristic that might need tuning based on the system's behavior
		if ackAge > 5*time.Minute && state.RetryCount >= 3 {
			// If we've tried several times and it's been a while, mark as incomplete
			allFailed := true
			for _, status := range state.Destinations {
				if status != StatusFailed {
					allFailed = false
					break
				}
			}

			if allFailed {
				incomplete = append(incomplete, msgID)
				incompleteCount++
			}
		}
	}

	// Clean up in batches
	c.cleanupBatch(expired, "expired")
	c.cleanupBatch(orphaned, "orphaned")
	c.cleanupBatch(incomplete, "incomplete")

	// Update statistics
	duration := time.Since(startTime)
	c.updateStats(startTime, duration, int64(expiredCount), int64(orphanedCount), int64(incompleteCount))

	log.Printf("[CleanupWorker] Cleanup completed in %v. Removed: %d expired, %d orphaned, %d incomplete ACKs",
		duration, expiredCount, orphanedCount, incompleteCount)
}

// cleanupBatch removes a batch of ACKs
func (c *CleanupWorker) cleanupBatch(msgIDs []string, category string) {
	if len(msgIDs) == 0 {
		return
	}

	// Process in batches
	for i := 0; i < len(msgIDs); i += c.batchSize {
		end := i + c.batchSize
		if end > len(msgIDs) {
			end = len(msgIDs)
		}

		batch := msgIDs[i:end]
		// Remove from tracker
		for _, msgID := range batch {
			c.tracker.removeAck(msgID)
		}

		// Remove from persistence
		if err := c.tracker.persistence.DeleteBatch(batch); err != nil {
			log.Printf("[CleanupWorker] Error deleting %s ACKs batch: %v", category, err)
		}
	}
}

// updateStats updates the cleanup statistics
func (c *CleanupWorker) updateStats(startTime time.Time, duration time.Duration, expired, orphaned, incomplete int64) {
	c.statsLock.Lock()
	defer c.statsLock.Unlock()

	c.cleanupStats.LastCleanupTime = startTime
	c.cleanupStats.ExpiredAcksRemoved += expired
	c.cleanupStats.OrphanedAcksRemoved += orphaned
	c.cleanupStats.IncompleteAcksRemoved += incomplete
	c.cleanupStats.TotalCleanupRuns++
	c.cleanupStats.TotalCleanupDuration += duration

	// Calculate average duration
	c.cleanupStats.AvgCleanupDuration = time.Duration(
		c.cleanupStats.TotalCleanupDuration.Nanoseconds() / c.cleanupStats.TotalCleanupRuns,
	)
}
