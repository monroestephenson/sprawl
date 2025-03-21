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
- ✅ AI-powered traffic analysis and load prediction
  - ✅ Automatic resource monitoring and prediction
  - ✅ Anomaly detection for network traffic
  - ✅ Self-training models requiring minimal data

### Coming Soon
- 🔄 Advanced backpressure handling
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

# Start a node with disk storage enabled and custom ports
SPRAWL_HEALTH_PORT=8091 \
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
SPRAWL_HEALTH_PORT=8091 \
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

# Check AI predictions for resource usage (CPU)
curl http://localhost:8080/ai/predictions?resource=cpu | jq

# Manually trigger AI model training (normally happens automatically)
curl -X POST http://localhost:8080/ai/train?resource=cpu,memory,message_rate | jq
```

## Port Configuration

Sprawl uses three distinct ports that can be configured:

1. **HTTP Server Port** (default: 8080)
   - Set with `-httpPort` flag
   - Used for pub/sub endpoints and API

2. **Health Server Port** (default: 8081)
   - Set with `SPRAWL_HEALTH_PORT` environment variable
   - Used for health checks and metrics

3. **Gossip/DHT Port** (default: 7946)
   - Set with `-bindPort` flag
   - Used for node discovery and DHT

Example with custom ports:
```bash
SPRAWL_HEALTH_PORT=8091 ./sprawl -bindAddr=127.0.0.1 -bindPort=7950 -httpPort=8087
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

## AI-Powered Features

Sprawl includes intelligent features for system optimization:

1. **Automatic Resource Monitoring**: Continuously tracks CPU, memory, message rate, and network metrics
2. **Self-Training ML Models**: Models automatically train with minimal data points (as few as 3)
3. **Predictive Capacity Planning**: Forecasts resource usage for the next hour
4. **Anomaly Detection**: Identifies unusual patterns in network traffic and system metrics
5. **Intelligent Routing**: Uses ML insights to optimize message paths

These features require no configuration and work automatically as the system runs. You can access predictions via the API endpoints:

```bash
# Get CPU usage predictions
curl http://localhost:8080/ai/predictions?resource=cpu | jq

# Get memory usage predictions
curl http://localhost:8080/ai/predictions?resource=memory | jq

# Get message rate predictions
curl http://localhost:8080/ai/predictions?resource=message_rate | jq

# Get network traffic predictions
curl http://localhost:8080/ai/predictions?resource=network | jq
```

## Running a Cluster

```bash
# Start the first node (seed node)
SPRAWL_HEALTH_PORT=8091 ./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080

# Start additional nodes with unique ports
SPRAWL_HEALTH_PORT=8092 ./sprawl -bindAddr=127.0.0.1 -bindPort=7947 -httpPort=8081 -seeds=127.0.0.1:7946
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

# Environment Variables
SPRAWL_HEALTH_PORT # Health server port (default 8081)
```

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/yourusername/sprawl/tags).

## Acknowledgments
- Built with Go's excellent standard library and community packages
