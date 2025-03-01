# Sprawl

A distributed, scalable pub/sub messaging system with intelligent routing and DHT-based topic distribution.

## Features

### Implemented
- ✅ Distributed Hash Table (DHT) for topic management
- ✅ Gossip protocol for node discovery and state synchronization
- ✅ HTTP-based pub/sub endpoints
- ✅ In-memory message store
- ✅ Efficient message routing with TTL and ACK mechanisms
- ✅ Basic metrics collection and monitoring
- ✅ Cross-node message forwarding
- ✅ Route caching system
- ✅ Basic load-based topic balancing
- ✅ Testing CLI tool for integration and load testing

### Coming Soon
- 🔄 Tiered storage with RocksDB/LevelDB
- 🔄 Message replication and distributed subscriber registry
- 🔄 Advanced backpressure handling
- 🔄 AI-powered traffic analysis and load prediction
- 🔄 Security features (TLS, Authentication, E2E encryption)
- 🔄 Cloud-native deployment support

## Quick Start

### Prerequisites
- Go 1.21 or higher

### Running a Node
```bash
# Start the first node
go run cmd/sprawl/main.go --port 8080

# Start additional nodes
go run cmd/sprawl/main.go --port 8081
go run cmd/sprawl/main.go --port 8082
```

### Using the CLI Tool
```bash
# Subscribe to a topic
go run cmd/cli/main.go sub <topic>

# Publish to a topic
go run cmd/cli/main.go pub <topic> <message>

# View metrics
go run cmd/cli/main.go metrics
```

## Architecture

Sprawl uses a distributed hash table (DHT) for topic management and message routing. Each node in the cluster participates in:
- Topic distribution and management
- Message routing and forwarding
- State synchronization via gossip protocol
- Load balancing and metrics collection

## Current Status

The core infrastructure is complete and operational, including node discovery, DHT-based topic management, and basic message routing. The system successfully handles distributed pub/sub operations with proper message acknowledgment and retry mechanisms.

Development is ongoing for advanced features like tiered storage, AI-powered optimizations, and production-ready security features.

## Contributing

Contributions are welcome! Please check the TODO.md file for current development priorities and open tasks.

## License

[License details to be added]
