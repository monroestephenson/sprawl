# ACK Tracker

The ACK Tracker is a production-ready component that ensures message delivery confirmation, persistence, retry management, and cleanup. This system is critical for guaranteeing message delivery in distributed scenarios and preventing message loss during node failures.

## Components

The ACK Tracker system consists of the following components:

1. **Tracker** (`tracker.go`): The core component that tracks message acknowledgments.
2. **Persistence** (`persistence.go`): Handles durably storing ACK states using LevelDB.
3. **Retry Manager** (`retry.go`): Manages message delivery retries with adaptive intervals and circuit breaking.
4. **Cleanup Worker** (`cleanup.go`): Handles expired ACKs and performs garbage collection.
5. **Metrics** (`metrics.go`): Tracks statistics for the ACK tracking system.

## Features

### Delivery Confirmation

- Unique ACK IDs tied to message IDs
- Callback registration system for delivery notifications
- Status tracking for partial/complete delivery confirmation
- Support for both sync/async confirmation patterns

### Persistence

- Durable storage of ACK state using LevelDB
- Batch write operations for efficiency
- ACK state recovery after restarts
- Periodic state snapshot functionality

### Retry Management

- Adaptive retry intervals with exponential backoff
- Per-node failure tracking to detect problematic nodes
- Circuit breaking for consistently failing destinations
- Configurable retry policies (count, timeout, etc.)
- Priority queuing for retries

### Cleanup Mechanism

- TTL-based state cleanup
- Periodic garbage collection for expired ACKs
- Monitoring for orphaned ACKs
- Safe concurrent cleanup operations
- Configurable cleanup policies

## Usage

### Basic Usage

```go
// Create a config
config := ack.DefaultConfig()

// Initialize the tracker
tracker, err := ack.NewTracker(config, "/path/to/persistence")
if err != nil {
    log.Fatalf("Failed to create ACK tracker: %v", err)
}

// Track a message that needs acknowledgment
msgID := "message-123"
destinations := []string{"node1", "node2", "node3"}
handle, err := tracker.TrackMessage(context.Background(), msgID, destinations)
if err != nil {
    log.Fatalf("Failed to track message: %v", err)
}

// Register a callback for when the message is fully acknowledged
tracker.RegisterCallback(msgID, func(msgID string, nodeID string, status ack.DeliveryStatus) {
    log.Printf("Message %s delivery status: %v", msgID, status)
})

// Wait for the message to be fully acknowledged
err = handle.WaitForCompletion(ctx)
if err != nil {
    log.Printf("Error waiting for completion: %v", err)
}

// Record an acknowledgment from a node
err = tracker.RecordAck(msgID, "node1")
if err != nil {
    log.Printf("Error recording ACK: %v", err)
}

// Shutdown the tracker
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
tracker.Shutdown(ctx)
```

### Integration with Router

To integrate with the message router:

1. Initialize the ACK tracker in the router's constructor.
2. Track messages when they're routed.
3. Record acknowledgments when delivery confirmations are received.
4. Check if messages are complete before removing them from the router's state.

## Performance Characteristics

- ACK processing time less than 1ms at P99
- Efficient memory footprint through batched persistence
- Support for at least 100,000 concurrent pending ACKs
- Batch processing of ACKs for efficiency
- Low latency impact on message routing path

## Monitoring

The ACK Tracker provides comprehensive metrics that can be used to monitor the system's health:

```go
// Get all metrics
metrics := tracker.GetMetrics()
fmt.Printf("Pending ACKs: %d\n", metrics["pending_acks"])
fmt.Printf("Completed ACKs: %d\n", metrics["completed_acks"])
fmt.Printf("Failed ACKs: %d\n", metrics["failed_acks"])
fmt.Printf("P99 ACK Latency: %v\n", metrics["p99_ack_latency"])
```

## Error Handling

The ACK Tracker provides robust error handling:

- Graceful degradation during overload
- Circuit breaking for problematic nodes
- Persistent storage for recovery after crashes
- Comprehensive error reporting

## Testing

The ACK Tracker includes comprehensive tests:

- Unit tests for all functionality
- Concurrency tests
- Persistence recovery tests
- Performance benchmarks

Run the tests with:

```
go test -v ./node/ack
```