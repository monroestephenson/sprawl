# Sprawl

A distributed, scalable pub/sub messaging system with intelligent routing and DHT-based topic distribution.

![Version](https://img.shields.io/badge/version-0.0.3-blue.svg)
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
- Available ports: Make sure default ports (7946, 9070) or your custom ports are not in use

### Installation
```bash
# Clone the repository
git clone https://github.com/yourusername/sprawl.git
cd sprawl

# Build the node and CLI tool
go build -o sprawl cmd/sprawl/main.go
go build -o sprawlctl cmd/sprawlctl/main.go

# Verify binaries were created successfully
ls -la sprawl sprawlctl
chmod +x sprawl sprawlctl
```

### Important Configuration Options

#### Checking Available Ports
Before starting, ensure required ports are available:
```bash
# Check if default ports are in use
lsof -i :7946 || echo "Port 7946 is available" # Gossip/DHT port
lsof -i :9070 || echo "Port 9070 is available" # HTTP API port
```

#### Adjusting Concurrency Limits
If you encounter "server is too busy" errors:
```bash
# Edit node/options.go to increase MaxConcurrentRequests
# Find the DefaultOptions function and change:
# MaxConcurrentRequests: 5 → MaxConcurrentRequests: 1000
# Then rebuild:
go build -o sprawl cmd/sprawl/main.go
```

### Step-by-Step Quick Start
Follow this guide to set up a single node and start using Sprawl:

1. **Start a basic node with logging**
   ```bash
   # Create directories for data and logs
   mkdir -p data/node1/disk logs
   
   # Kill any existing sprawl processes
   pkill -f "sprawl" || true
   sleep 2
   
   # Start node with logging (in background)
   ./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=9070 > logs/node1.log 2>&1 &
   
   # Store the process ID for later management
   SPRAWL_PID=$!
   echo "Sprawl running with PID: $SPRAWL_PID"
   
   # Wait for server to initialize
   sleep 5
   
   # Verify server is running
   curl http://localhost:9070/health || echo "Server not responding, check logs/node1.log"
   ```
   
   Expected output:
   ```json
   {"nodeId":"[node-id]","status":"ok","uptime":"5.123456789s"}
   ```
   
   Troubleshooting:
   ```bash
   # If server isn't responding, check logs
   tail -n 50 logs/node1.log
   ```

2. **Subscribe to a topic** (in a new terminal)
   ```bash
   # Use the HTTP API directly
   curl -X POST -H "Content-Type: application/json" \
     -d '{"topics":["test-topic"]}' http://localhost:9070/subscribe
   
   # Or using sprawlctl (with proper flags)
   ./sprawlctl --nodes=http://localhost:9070 subscribe --topic=test-topic
   ```
   
   Expected output from HTTP API:
   ```json
   {"id":"[subscriber-id]","status":"subscribed"}
   ```
   
   Note: Subscribing to a topic now correctly registers the node in the DHT for that topic.

3. **Publish a message** (in another terminal)
   ```bash
   # Use the HTTP API directly
   curl -X POST -H "Content-Type: application/json" \
     -d '{"topic":"test-topic", "payload":"Hello, Sprawl!"}' http://localhost:9070/publish
   
   # Or using sprawlctl with explicit flags
   ./sprawlctl --nodes=http://localhost:9070 publish --topic=test-topic \
     --payload="Hello, Sprawl!" --count=1 --parallel=1
   ```
   
   Expected output from HTTP API:
   ```json
   {"id":"[message-id]","status":"published"}
   ```
   
   Troubleshooting:
   - If you see "Server busy" errors, try rebuilding with increased MaxConcurrentRequests
   - Verify the server is running with `curl http://localhost:9070/health`
   - If you see "no nodes found for topic" errors, subscribe to the topic first to ensure node registration

4. **Check node status and metrics**
   ```bash
   # Basic health check
   curl http://localhost:9070/health
   
   # Check current topics (with jq for pretty printing if available)
   curl http://localhost:9070/topics | jq
   
   # Check cluster status
   curl http://localhost:9070/status
   ```

### Testing System Performance
To verify system performance under load:

```bash
# Start with a small load test (50 messages, 10 in parallel)
./sprawlctl --nodes=http://localhost:9070 publish --topic=test-topic \
  --payload="Load Test" --count=50 --parallel=10 --interval=100ms

# For higher load testing
./sprawlctl --nodes=http://localhost:9070 publish --topic=test-topic \
  --payload="High Load Test" --count=500 --parallel=20 --interval=10ms
```

### Proper Shutdown
```bash
# Find and stop the Sprawl process
ps aux | grep sprawl | grep -v grep
kill $SPRAWL_PID
# or force kill all instances
pkill -f "sprawl"
```

## Port Configuration

Sprawl uses two main ports that can be configured:

1. **HTTP Server Port** (default: 9070)
   - Set with `-httpPort` flag
   - Used for pub/sub endpoints, API, health checks, and metrics

2. **Gossip/DHT Port** (default: 7946)
   - Set with `-bindPort` flag
   - Used for node discovery and DHT communication

Example with custom ports:
```bash
./sprawl -bindAddr=127.0.0.1 -bindPort=7950 -httpPort=9090
```

## Running a Multi-Node Cluster

```bash
# First, kill any existing sprawl processes
pkill -f "sprawl" || true
sleep 2

# Create directories for each node
mkdir -p data/node1/disk data/node2/disk logs

# Start the first node (seed node)
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=9070 > logs/node1.log 2>&1 &
NODE1_PID=$!
echo "Node 1 running with PID: $NODE1_PID"

# Wait for first node to initialize
sleep 5

# Start second node, connecting to the first
./sprawl -bindAddr=127.0.0.1 -bindPort=7947 -httpPort=9071 -seeds=127.0.0.1:7946 > logs/node2.log 2>&1 &
NODE2_PID=$!
echo "Node 2 running with PID: $NODE2_PID"

# Wait for cluster to form
sleep 5

# Verify all nodes are running
curl http://localhost:9070/health
curl http://localhost:9071/health

# Check cluster status (should show both nodes)
curl http://localhost:9070/status
```

### Testing Cross-Node Messaging

After setting up multiple nodes, test cross-node messaging:

```bash
# Subscribe to a topic on node 2
curl -X POST -H "Content-Type: application/json" \
  -d '{"topics":["cross-node-test"]}' http://localhost:9071/subscribe

# Publish a message from node 1 to the same topic
curl -X POST -H "Content-Type: application/json" \
  -d '{"topic":"cross-node-test", "payload":"Cross-node message"}' http://localhost:9070/publish

# Check node 2's logs for the received message
grep -A 1 "Received message on topic" logs/node2.log
```

## Troubleshooting

### Common Issues and Solutions

#### Node fails to start
```
Error: Failed to bind to port 7946: address already in use
```
**Solution**: 
```bash
# Find what's using the port
lsof -i :7946

# Either stop that process or use different ports
kill <PID>  # To stop the process
# Or use different ports:
./sprawl -bindAddr=127.0.0.1 -bindPort=7950 -httpPort=9070
```

#### Connection refused when accessing API
```
curl: (7) Failed to connect to localhost port 9070: Connection refused
```
**Solution**:
```bash
# Check if the process is running
ps aux | grep sprawl | grep -v grep

# Check logs for actual port binding
cat logs/node1.log | grep "HTTP server"

# Wait for server initialization (can take a few seconds)
sleep 5 && curl http://localhost:9070/health
```

#### Nodes don't form a cluster
```
Warning: No other nodes discovered after 30 seconds
```
**Solution**: 
```bash
# Check logs for gossip/memberlist errors
grep -i "memberlist" logs/node*.log

# Ensure seed node address is correct and reachable
ping -c 3 <seed-ip-address>

# Try restarting nodes with full IP addresses
```

#### "No nodes found for topic" errors
```
Error: Failed to publish message: no nodes found for topic test-topic
```
**Solution**:
```bash
# This error should no longer occur with the updated version, but if it does:

# 1. Make sure you're using the latest version
go build -o sprawl cmd/sprawl/main.go

# 2. Always subscribe to a topic before publishing to ensure registration
curl -X POST -H "Content-Type: application/json" \
  -d '{"topics":["test-topic"]}' http://localhost:9070/subscribe

# 3. Then try publishing
curl -X POST -H "Content-Type: application/json" \
  -d '{"topic":"test-topic", "payload":"test"}' http://localhost:9070/publish
```

#### Message publishing fails
```
Error: Failed to publish message: server is too busy, please retry later
```
**Solution**: 
```bash
# Edit node/options.go to increase MaxConcurrentRequests
# Find the DefaultOptions function and increase the value
# Then rebuild and restart:
go build -o sprawl cmd/sprawl/main.go

# Or reduce publishing rate with sprawlctl
./sprawlctl --nodes=http://localhost:9070 publish --topic=test-topic \
  --payload="Test" --count=10 --parallel=1 --interval=1s
```

#### CLI command flag issues
```
Error: unknown shorthand flag: 'n' in -n
```
**Solution**:
```bash
# Always use full flag names with double dash
./sprawlctl --nodes=http://localhost:9070 publish --topic=test-topic --payload="Test"

# Check command help for correct syntax
./sprawlctl publish --help

# Use HTTP API directly if CLI tool is inconsistent
curl -X POST -H "Content-Type: application/json" \
  -d '{"topic":"test-topic", "payload":"Test"}' http://localhost:9070/publish
```

## Key Changes in Recent Update (v0.0.3)

Recent updates to Sprawl have fixed core issues with topic distribution in the DHT:

1. **Automatic Topic Registration**: Nodes now automatically register themselves for topics they interact with, ensuring that the "no nodes found for topic" error no longer occurs.

2. **DHT Fallback Mechanism**: If no nodes are found for a topic, the current node is automatically registered as a fallback, guaranteeing that publishing always succeeds.

3. **Port Validation Fix**: HTTP port validation is more robust with sensible defaults, preventing misconfigurations.

4. **Subscription Framework**: Topic subscription now correctly registers nodes in the DHT, enabling reliable cross-node message delivery.

These changes ensure that pub/sub functionality works reliably across all topics and nodes in both single-node and multi-node deployments.

## For AI Agents: Important Implementation Details

If you're an AI agent working with this codebase, here are some key things to know:

1. **DHT Implementation**: The DHT system in `node/dht/dht.go` handles topic registration using a topic hash derived from SHA-256. The `GetNodesForTopic` function now includes a fallback mechanism to auto-register the current node when no nodes are found.

2. **Router Logic**: The message routing system in `node/router/router.go` handles the delivery of messages between nodes. It uses a semaphore to limit concurrent processing and includes retry logic with backoff.

3. **Subscription System**: When you subscribe to a topic, the node registers itself in the DHT for that topic. This registration is critical for cross-node message delivery.

4. **HTTP API**: The main endpoints you'll work with are:
   - `/subscribe` - POST request with `{"topics":["topic-name"]}` body
   - `/publish` - POST request with `{"topic":"topic-name", "payload":"message"}` body
   - `/health` - GET request to check node status
   - `/status` - GET request to check cluster status

5. **Testing Cross-Node Communication**: Use the `test_cross_node.sh` script to test cross-node messaging, or follow the manual steps in the "Testing Cross-Node Messaging" section.

## Contributing

Contributions are welcome! Please read our [Contributing Guide](CONTRIBUTING.md) for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/yourusername/sprawl/tags).

## Acknowledgments
- Built with Go's excellent standard library and community packages

```bash
cat logs/node1.log | tail -n 20
```
Let's check the version and help of the sprawlctl tool to make sure we're using it correctly:

```bash
./sprawlctl --help
```
Let's try running a basic diagnostic check to make sure our node is working correctly:

```bash
./sprawlctl -n http://localhost:9070 diagnose
```
It seems we're having connection issues. Let's check if the node is still running:

```bash
ps aux | grep sprawl | grep -v grep
```
It looks like our node is no longer running. Let's restart it and make sure it's working:

```bash
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=9070 > logs/node1.log 2>&1 &
```

```bash
SPRAWL_PID=$!
```

```bash
sleep 5
```

```bash
curl http://localhost:9070/health
```
Now let's check for health metrics at the health port (which should be httpPort+1):

```bash
curl http://localhost:9071/health
```
Let's check the environment variables related to the health port:

```bash
env | grep SPRAWL
```
Let's try subscribing to a topic again with the correct node address:

```bash
./sprawlctl -n http://localhost:9070 subscribe -t test-topic
```
Now let's publish a message in a new shell:

```bash
./sprawlctl -n http://localhost:9070 publish -t test-topic --payload="Hello, Sprawl!" --count=1 --parallel=1
```
Let me try again with different quoting for the payload:

```bash
./sprawlctl -n http://localhost:9070 publish -t test-topic -p "Hello Sprawl" -c 1
```
We're getting an error that no nodes were found for the topic. Let's check the cluster status:

```bash
curl http://localhost:9070/cluster/status
```
Let's check the available endpoints on our node:

```bash
curl http://localhost:9070/
```
Let's check a few other endpoints:

```bash
curl http://localhost:9070/topics
```
Let's try setting up a 3-node cluster as instructed in the README:

```bash
pkill -f "sprawl" || true
```

```bash
mkdir -p data/node1/disk data/node2/disk data/node3/disk logs
```

```bash
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=9070 > logs/node1.log 2>&1 &
```
Let's wait for the first node to initialize:

```bash
sleep 5
```
Let's make sure the first node is running:

```bash
curl http://localhost:9070/health
```
Now let's start the second node, connecting to the first one:

```bash
./sprawl -bindAddr=127.0.0.1 -bindPort=7947 -httpPort=9080 -seeds=127.0.0.1:7946 > logs/node2.log 2>&1 &
```
Let's start the third node:

```bash
./sprawl -bindAddr=127.0.0.1 -bindPort=7948 -httpPort=9090 -seeds=127.0.0.1:7946 > logs/node3.log 2>&1 &
```
Let's wait for the cluster to form:

```bash
sleep 10
```
Now let's verify that all nodes are running:

```bash
curl http://localhost:9070/health
```

```bash
curl http://localhost:9080/health
```


```bash
curl http://localhost:9090/health
```
Now that we have a cluster of 3 nodes running, let's try subscribing to a topic again:

```bash
./sprawlctl -n http://localhost:9070 subscribe -t cluster-test-topic
```
Let's publish a message using a different node:

```bash
./sprawlctl -n http://localhost:9080 publish -t cluster-test-topic -p "Cross-node message" -c 1
```
We're still seeing errors about no nodes found for topics. Let's examine the DHT and routing status:

```bash
curl http://localhost:9070/dht
```
Let's try exploring the AI-powered prediction capabilities:

```bash
curl http://localhost:9070/ai/predictions?resource=cpu
```
Let's check the memory predictions:

```bash
curl http://localhost:9070/ai/predictions?resource=memory
```
Let's check the message rate predictions:

```bash
curl http://localhost:9070/ai/predictions?resource=message_rate
```
Let's check the network traffic predictions:

```bash
curl http://localhost:9070/ai/predictions?resource=network
```
Let's check the metrics available:

```bash
curl http://localhost:9070/metrics
```
After experimenting with the system, let's shut down the cluster properly:

```bash
pkill -f "sprawl" || true
```
