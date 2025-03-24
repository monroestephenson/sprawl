package ack

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// This is an example integration test showing how the ACK Tracker
// could be integrated with the existing Router implementation.
// Note: This test doesn't actually use the real Router since it's
// meant to be an example only.
func TestRouterIntegration(t *testing.T) {
	// Skip in normal test runs as this is just an example
	t.Skip("This is an example integration test, not meant to be run automatically")

	// Setup the ACK Tracker
	config := DefaultConfig()
	tracker, err := NewTracker(config, t.TempDir())
	if err != nil {
		t.Fatalf("Failed to create tracker: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		if err := tracker.Shutdown(ctx); err != nil {
			t.Logf("Error shutting down tracker: %v", err)
		}
	}()

	// Mock Router implementation for the example
	type MockRouter struct {
		ackTracker *Tracker
	}

	// Create a new Mock Router with the ACK Tracker
	router := &MockRouter{
		ackTracker: tracker,
	}

	// Example message and destinations
	msgID := "example-message-123"
	destinations := []string{"node1", "node2", "node3"}

	// Example RouteMessage implementation that would use the ACK Tracker
	routeMessage := func(ctx context.Context, msgID string, destinations []string) error {
		// 1. Track the message with the ACK Tracker
		handle, err := router.ackTracker.TrackMessage(ctx, msgID, destinations)
		if err != nil {
			return fmt.Errorf("failed to track message: %w", err)
		}

		// 2. Register a callback for delivery notifications
		if err := router.ackTracker.RegisterCallback(msgID, func(msgID string, nodeID string, status DeliveryStatus) {
			fmt.Printf("Message %s delivery to node %s: %v\n", msgID, nodeID, status)
		}); err != nil {
			return fmt.Errorf("failed to register callback: %w", err)
		}

		// 3. Simulate message delivery to each destination
		// In a real implementation, this would be done asynchronously
		for _, nodeID := range destinations {
			// Simulate sending the message
			fmt.Printf("Sending message %s to node %s\n", msgID, nodeID)

			// Simulate a successful delivery for demonstration
			// In a real implementation, the actual delivery status would be received
			// from the destination node
			if err := router.ackTracker.RecordAck(msgID, nodeID); err != nil {
				return fmt.Errorf("failed to record ACK for node %s: %w", nodeID, err)
			}
		}

		// 4. Wait for all acknowledgments (example of synchronous waiting)
		// In a real implementation, this might be a goroutine or use channels
		err = handle.WaitForCompletion(ctx)
		if err != nil {
			return fmt.Errorf("error waiting for delivery completion: %w", err)
		}

		fmt.Printf("Message %s delivery complete to all destinations\n", msgID)
		return nil
	}

	// Call the route message function
	err = routeMessage(context.Background(), msgID, destinations)
	if err != nil {
		t.Errorf("Error routing message: %v", err)
	}

	// Verify the message was delivered (would be done with assertions in a real test)
	isComplete, err := tracker.IsComplete(msgID)
	if err != nil {
		t.Errorf("Error checking if message is complete: %v", err)
	}
	if !isComplete {
		t.Errorf("Expected message to be complete")
	}

	// Example of accessing metrics
	metrics := tracker.GetMetrics()
	fmt.Printf("ACK Tracker Metrics: %+v\n", metrics)
}
