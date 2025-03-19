# Sprawl

A distributed, scalable pub/sub messaging system with intelligent routing and DHT-based topic distribution.

![Version](https://img.shields.io/badge/version-0.0.2-blue.svg)
![Go Version](https://img.shields.io/badge/go-%3E%3D1.21-blue)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## Features

### Core Features
- ✅ Distributed Hash Table (DHT) for topic management
- ✅ Gossip protocol for node discovery and state synchronization
- ✅ HTTP-based pub/sub endpoints
- ✅ Tiered storage (Memory, RocksDB, MinIO/S3)
- ✅ Raft consensus for cluster management
- ✅ Message replication and distributed subscriber registry
- ✅ Efficient message routing with TTL and ACK mechanisms
- ✅ Basic metrics collection and monitoring
- ✅ Cross-node message forwarding
- ✅ Route caching system
- ✅ Basic load-based topic balancing

### Coming Soon
- 🔄 Advanced backpressure handling
- 🔄 AI-powered traffic analysis and load prediction
- 🔄 Security features (TLS, Authentication, E2E encryption)
- 🔄 Kubernetes operator
- 🔄 WebSocket support
- 🔄 Stream processing capabilities

## Quick Start

### Prerequisites
- Go 1.21 or higher
- Docker (for MinIO/cloud storage)
- RocksDB (optional, for disk storage)

### Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/sprawl.git
cd sprawl

# Build the node and CLI tool
go build -o sprawl cmd/sprawl/main.go
go build -o sprawlctl cmd/sprawlctl/main.go
```

### Running a Node with Tiered Storage
```bash
# Create required directories
mkdir -p data/node1/disk

# Start a node with disk storage enabled
SPRAWL_STORAGE_DISK_ENABLED=true \
SPRAWL_STORAGE_DISK_PATH=./data/node1/disk \
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080

# Start MinIO (required for cloud storage)
docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e "MINIO_ROOT_USER=minioadmin" \
    -e "MINIO_ROOT_PASSWORD=minioadmin" \
    minio/minio server /data --console-address ":9001"

# Start a node with all storage tiers enabled
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin \
SPRAWL_STORAGE_DISK_ENABLED=true \
SPRAWL_STORAGE_DISK_PATH=./data/node1/disk \
SPRAWL_STORAGE_CLOUD_ENABLED=true \
SPRAWL_STORAGE_CLOUD_BUCKET=sprawl-messages \
SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=3600 \
SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=86400 \
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080
```

### Basic Usage
```bash
# Subscribe to a topic
./sprawlctl -n http://localhost:8080 subscribe -t my-topic

# Publish to a topic
./sprawlctl -n http://localhost:8080 publish -t my-topic -m "Hello, World!"

# Check storage tier status
curl http://localhost:8080/store | jq

# Run the verification script to test tiered storage
./scripts/verify-tiering.sh
```

## Tiered Storage Architecture

Sprawl implements a three-tiered storage system:

1. **Memory Tier**: Fast in-memory storage for recent messages
2. **Disk Tier**: RocksDB-based persistent storage for older messages
3. **Cloud Tier**: S3/MinIO storage for long-term archival

Messages automatically move between tiers based on:
- Age (configurable retention periods)
- Memory pressure (when memory usage exceeds threshold)
- Disk usage (when disk usage exceeds threshold)

### Configuration Options

Control tiered storage behavior with these environment variables:

```bash
# Enable/disable tiers
SPRAWL_STORAGE_DISK_ENABLED=true|false
SPRAWL_STORAGE_CLOUD_ENABLED=true|false

# Paths and endpoints
SPRAWL_STORAGE_DISK_PATH=/path/to/rocksdb
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=your-access-key
MINIO_SECRET_KEY=your-secret-key

# Thresholds and retention periods
SPRAWL_STORAGE_MEMORY_MAX_SIZE=104857600  # 100MB
SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=3600    # 1 hour
SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=86400    # 24 hours
```

## Architecture

Sprawl is built on several key components:

1. **DHT Layer**: Manages topic distribution and routing
2. **Storage Layer**: Tiered storage with memory, disk, and cloud options
3. **Consensus Layer**: Raft-based cluster management
4. **Router Layer**: Intelligent message routing and load balancing
5. **API Layer**: HTTP endpoints for pub/sub operations

For more details, see [architecture.md](architecture.md).

## Running a Cluster

```bash
# Start the first node (seed node)
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080

# Start additional nodes
./sprawl -bindAddr=127.0.0.1 -bindPort=7947 -httpPort=8081 -seeds=127.0.0.1:7946
```

## Configuration

Key configuration options:

```bash
# Node Configuration
-bindAddr string    # Gossip bind address (default "0.0.0.0")
-bindPort int      # Gossip bind port (default 7946)
-httpAddr string   # HTTP bind address (default "0.0.0.0")
-httpPort int      # HTTP server port (default 8080)
-seeds string      # Comma-separated list of seed nodes
```

## Production Deployment

For production deployment instructions, see [deployment.md](docs/deployment.md).

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/yourusername/sprawl/tags).

## Acknowledgments
- Built with Go's excellent standard library and community packages
