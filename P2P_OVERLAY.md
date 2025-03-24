# P2P Overlay Implementation

This document describes the P2P Overlay implementation for the Sprawl messaging system, which includes the Gossip Protocol Manager, Failure Detection, Metrics Collection, and Membership Management.

## Overview

The P2P Overlay is a critical component that enables nodes to discover each other, share state information, and detect failures in a distributed manner. The implementation is built on the following key components:

1. **Gossip Protocol Manager** - Handles cluster membership and metadata gossip
2. **Failure Detector** - Monitors node health and detects failures
3. **Metrics Manager** - Collects and distributes system and application metrics
4. **Membership Manager** - Manages cluster membership state
5. **Health Handler** - Provides HTTP endpoints for health checks

## Architecture

The P2P Overlay is designed with the following architecture:

```
┌────────────────────────────────────────────────────────────┐
│                    Gossip Protocol Manager                  │
└─────────────────┬─────────────────┬──────────────┬─────────┘
                  │                 │              │
      ┌───────────┼─────────────┐   │   ┌─────────┼──────────┐
      │           │             │   │   │         │          │
┌─────▼───────┐ ┌─▼───────────┐ │ ┌─▼───▼─────┐ ┌─▼──────────┐
│ Memberlist  │ │ Failure     │ │ │ Membership │ │ Metrics    │
│ Integration │ │ Detector    │ │ │ Manager    │ │ Manager    │
└─────────────┘ └─────────────┘ │ └─────────────┘ └────────────┘
                                │
                   ┌────────────▼─────────────┐
                   │      Health Handler       │
                   └──────────────────────────┘
```

## Components

### Gossip Protocol Manager

The Gossip Protocol Manager (GossipManager) is the central component that manages the P2P overlay network. It is responsible for:

- Joining and leaving the cluster
- Broadcasting node metadata
- Detecting node failures
- Collecting and sharing metrics
- Managing leader election

The GossipManager uses the [Hashicorp Memberlist](https://github.com/hashicorp/memberlist) library for low-level gossip protocol implementation, which provides efficient and scalable cluster membership.

### Failure Detector

The Failure Detector monitors the health of nodes in the cluster and detects failures. It implements a SWIM-like failure detection algorithm with the following features:

- Heartbeat monitoring
- Suspicion mechanism
- Health probing
- Response time tracking
- Configurable thresholds

The failure detector classifies nodes into three states:
- `StateHealthy` - Node is operating normally
- `StateSuspect` - Node may be failing
- `StateFailed` - Node is considered failed

The failure detector meets the performance requirements of detecting node failures within 5 seconds and recovering from 33% node failure.

### Metrics Manager

The Metrics Manager collects and distributes system and application metrics. It provides the following features:

- CPU, memory, and disk usage monitoring
- Network traffic monitoring
- Message rate tracking
- Custom application metrics
- Historical metrics storage

The metrics data is used for health monitoring, load prediction, and route optimization.

### Membership Manager

The Membership Manager maintains the state of the cluster membership. It provides the following features:

- Member state tracking
- Member event history
- Leader election
- State change notifications
- Membership snapshots

### Health Handler

The Health Handler provides HTTP endpoints for health checking. It exposes the following endpoints:

- `/health` - Returns overall node health
- `/health/check` - Compatible with cloud provider health checks
- `/health/metrics` - Returns detailed metrics
- `/health/membership` - Returns membership information

## Usage

### Creating a Gossip Manager

```go
// Create a DHT instance
dhtInstance := dht.NewDHT("node-1")
dhtInstance.InitializeOwnNode("127.0.0.1", 7946, 8080)

// Create a gossip manager
gm, err := NewGossipManager("node-1", "127.0.0.1", 7946, dhtInstance, 8080)
if err != nil {
    log.Fatalf("Failed to create gossip manager: %v", err)
}
```

### Joining a Cluster

```go
// Join a cluster with seed nodes
err := gm.JoinCluster([]string{"192.168.1.100:7946", "192.168.1.101:7946"})
if err != nil {
    log.Fatalf("Failed to join cluster: %v", err)
}
```

### Getting Cluster Members

```go
// Get all members
members := gm.GetMembers()
for _, memberID := range members {
    log.Printf("Member: %s", memberID)
}

// Get detailed member info
info := gm.GetMemberInfo("node-2")
log.Printf("Member info: %+v", info)
```

### Tracking Message Counts

```go
// Add message count
gm.AddMessageCount(10)

// Get message count
count := gm.getMessageCount()
log.Printf("Message count: %d", count)
```

### Monitoring Node Health

```go
// Check if a node is healthy
state := gm.GetNodeState("node-2")
log.Printf("Node state: %s", state)

// Get failed nodes
failedNodes := gm.failureDetector.GetFailedNodes()
for _, nodeID := range failedNodes {
    log.Printf("Failed node: %s", nodeID)
}
```

### Getting Metrics

```go
// Get current metrics
metrics := gm.metricsManager.GetCurrentMetrics()
log.Printf("CPU usage: %.2f%%", metrics.CPUUsage)
log.Printf("Memory usage: %.2f%%", metrics.MemoryUsage)
log.Printf("Message rate: %.2f/s", metrics.MessageRate)
```

### Registering Health Endpoints

```go
// Create HTTP server
mux := http.NewServeMux()
RegisterHealthEndpoint(mux, gm, gm.metricsManager, "node-1", "1.0.0")
http.ListenAndServe(":8080", mux)
```

## Performance Considerations

The P2P overlay implementation is designed to meet the following performance requirements:

- Propagation time < 50ms for membership changes
- Recovery from 33% node failure within 5 seconds
- Low CPU and memory overhead
- Efficient network usage through compression

## Testing

The implementation includes comprehensive unit tests that verify:

- Basic functionality
- Cluster joining
- Message counting
- Failure detection
- Metrics collection
- Node probing
- Concurrent operations
- Leader election

Run the tests with:

```bash
go test -v ./node
```

## Future Improvements

Future improvements to the P2P overlay implementation could include:

1. **Advanced leader election** - Implement more sophisticated leader election algorithms based on node capabilities
2. **Secure gossip** - Add encryption and authentication to gossip messages
3. **Multi-datacenter awareness** - Add awareness of different datacenters for more efficient communication
4. **Adaptive failure detection** - Adjust failure detection parameters based on network conditions
5. **Performance optimizations** - Further reduce CPU, memory, and network usage

## Dependencies

- [Hashicorp Memberlist](https://github.com/hashicorp/memberlist) - Gossip protocol implementation
- [gopsutil](https://github.com/shirou/gopsutil) - System metrics collection 