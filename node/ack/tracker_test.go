package ack

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestTrackerBasicFunctionality(t *testing.T) {
	// Create a temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "ack-tracker-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a tracker
	config := DefaultConfig()
	tracker, err := NewTracker(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Track a message
	msgID := "test-message-1"
	destinations := []string{"node1", "node2"}

	handle, err := tracker.TrackMessage(ctx, msgID, destinations)
	if err != nil {
		t.Fatalf("Failed to track message: %v", err)
	}

	// Check that the message is not complete yet
	complete, err := tracker.IsComplete(msgID)
	if err != nil {
		t.Fatalf("Failed to check if message is complete: %v", err)
	}
	if complete {
		t.Errorf("Message should not be complete yet")
	}

	// Record an acknowledgment
	err = tracker.RecordAck(msgID, "node1")
	if err != nil {
		t.Fatalf("Failed to record ack: %v", err)
	}

	// Message should still be incomplete
	complete, err = tracker.IsComplete(msgID)
	if err != nil {
		t.Fatalf("Failed to check if message is complete: %v", err)
	}
	if complete {
		t.Errorf("Message should not be complete yet")
	}

	// Record another acknowledgment
	err = tracker.RecordAck(msgID, "node2")
	if err != nil {
		t.Fatalf("Failed to record ack: %v", err)
	}

	// Now message should be complete
	complete, err = tracker.IsComplete(msgID)
	if err != nil {
		t.Fatalf("Failed to check if message is complete: %v", err)
	}
	if !complete {
		t.Errorf("Message should be complete now")
	}

	// WaitForCompletion should return immediately
	err = handle.WaitForCompletion(ctx)
	if err != nil {
		t.Fatalf("WaitForCompletion failed: %v", err)
	}

	// Shutdown the tracker
	err = tracker.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown tracker: %v", err)
	}
}

func TestTrackerRetryFunctionality(t *testing.T) {
	// Create a temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "ack-tracker-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a tracker with shorter retry interval for testing
	config := DefaultConfig()
	config.RetryInterval = 100 * time.Millisecond
	tracker, err := NewTracker(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Track a message
	msgID := "test-message-2"
	destinations := []string{"node1", "node2"}

	_, err = tracker.TrackMessage(ctx, msgID, destinations)
	if err != nil {
		t.Fatalf("Failed to track message: %v", err)
	}

	// Record a failure
	err = tracker.RecordFailure(msgID, "node1")
	if err != nil {
		t.Fatalf("Failed to record failure: %v", err)
	}

	// Check metrics
	metrics := tracker.GetMetrics()
	retriesIssued := metrics["retries_issued"].(int64)
	if retriesIssued != 1 {
		t.Errorf("Expected 1 retry to be issued, got %d", retriesIssued)
	}

	// Shutdown the tracker
	err = tracker.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown tracker: %v", err)
	}
}

func TestTrackerCallbacks(t *testing.T) {
	// Create a temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "ack-tracker-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a tracker
	config := DefaultConfig()
	tracker, err := NewTracker(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Track a message
	msgID := "test-message-3"
	destinations := []string{"node1", "node2"}

	_, err = tracker.TrackMessage(ctx, msgID, destinations)
	if err != nil {
		t.Fatalf("Failed to track message: %v", err)
	}

	// Channel to receive callback notifications
	callbackCh := make(chan DeliveryStatus, 2)

	// Register a callback
	err = tracker.RegisterCallback(msgID, func(msgID string, nodeID string, status DeliveryStatus) {
		callbackCh <- status
	})
	if err != nil {
		t.Fatalf("Failed to register callback: %v", err)
	}

	// Record an acknowledgment
	err = tracker.RecordAck(msgID, "node1")
	if err != nil {
		t.Fatalf("Failed to record ack: %v", err)
	}

	// Wait for callback
	select {
	case status := <-callbackCh:
		if status != StatusDelivered {
			t.Errorf("Expected StatusDelivered, got %v", status)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Timed out waiting for callback")
	}

	// Shutdown the tracker
	err = tracker.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown tracker: %v", err)
	}
}

func TestTrackerPersistence(t *testing.T) {
	// Create a temporary directory for persistence
	tempDir, err := os.MkdirTemp("", "ack-tracker-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a tracker
	config := DefaultConfig()
	tracker, err := NewTracker(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Track a message
	msgID := "test-message-4"
	destinations := []string{"node1", "node2"}

	_, err = tracker.TrackMessage(ctx, msgID, destinations)
	if err != nil {
		t.Fatalf("Failed to track message: %v", err)
	}

	// Record one acknowledgment
	err = tracker.RecordAck(msgID, "node1")
	if err != nil {
		t.Fatalf("Failed to record ack: %v", err)
	}

	// Shutdown the tracker
	err = tracker.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown tracker: %v", err)
	}

	// Create a new tracker with the same persistence path
	tracker2, err := NewTracker(config, tempDir)
	if err != nil {
		t.Fatalf("Failed to create second tracker: %v", err)
	}

	// Check that the message was recovered
	complete, err := tracker2.IsComplete(msgID)
	if err != nil {
		t.Fatalf("Failed to check if message is complete: %v", err)
	}
	if complete {
		t.Errorf("Message should not be complete yet")
	}

	// Record the remaining acknowledgment
	err = tracker2.RecordAck(msgID, "node2")
	if err != nil {
		t.Fatalf("Failed to record ack: %v", err)
	}

	// Now message should be complete
	complete, err = tracker2.IsComplete(msgID)
	if err != nil {
		t.Fatalf("Failed to check if message is complete: %v", err)
	}
	if !complete {
		t.Errorf("Message should be complete now")
	}

	// Shutdown the second tracker
	err = tracker2.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown second tracker: %v", err)
	}
}
