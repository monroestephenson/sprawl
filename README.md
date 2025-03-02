# Sprawl

A distributed, scalable pub/sub messaging system with intelligent routing and DHT-based topic distribution.

![Version](https://img.shields.io/badge/version-0.0.1-blue.svg)
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

# Build the CLI tool
go build -o sprawlctl cmd/sprawlctl/main.go
```

### Running a Cluster
```bash
# Start MinIO (required for cloud storage)
docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e "MINIO_ROOT_USER=minioadmin" \
    -e "MINIO_ROOT_PASSWORD=minioadmin" \
    minio/minio server /data --console-address ":9001"

# Start the first node (seed node)
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin \
go run main.go -bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080

# Start additional nodes
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin \
go run main.go -bindAddr=127.0.0.1 -bindPort=7947 -httpAddr=127.0.0.1 -httpPort=8081 -seeds=127.0.0.1:7946
```

### Basic Usage
```bash
# Subscribe to a topic
./sprawlctl -n http://localhost:8080 subscribe -t my-topic

# Publish to a topic
./sprawlctl -n http://localhost:8080 publish -t my-topic -m "Hello, World!"

# Run integration tests
./test.sh
```

## Architecture

Sprawl is built on several key components:

1. **DHT Layer**: Manages topic distribution and routing
2. **Storage Layer**: Tiered storage with memory, disk, and cloud options
3. **Consensus Layer**: Raft-based cluster management
4. **Router Layer**: Intelligent message routing and load balancing
5. **API Layer**: HTTP endpoints for pub/sub operations

For more details, see [architecture.md](architecture.md).

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
MINIO_ENDPOINT     # MinIO/S3 endpoint
MINIO_ACCESS_KEY   # MinIO/S3 access key
MINIO_SECRET_KEY   # MinIO/S3 secret key
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

- Thanks to all contributors who have helped shape Sprawl
- Built with Go's excellent standard library and community packages
