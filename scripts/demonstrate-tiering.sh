#!/bin/bash
# Demonstrates tiered storage functionality in Sprawl

set -e

# Check if MinIO is running
check_minio() {
  echo "Checking if MinIO is running..."
  if nc -z localhost 9000 2>/dev/null; then
    echo "✅ MinIO is running"
    return 0
  else
    echo "❌ MinIO is not running"
    return 1
  fi
}

# Start a MinIO container if needed
start_minio() {
  echo "Starting MinIO container..."
  docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e "MINIO_ROOT_USER=minioadmin" \
    -e "MINIO_ROOT_PASSWORD=minioadmin" \
    minio/minio server /data --console-address ":9001"
  
  # Wait for MinIO to be ready
  echo "Waiting for MinIO to start..."
  for i in {1..30}; do
    if nc -z localhost 9000 2>/dev/null; then
      echo "✅ MinIO started successfully"
      return 0
    fi
    sleep 1
  done
  
  echo "❌ Failed to start MinIO"
  return 1
}

# Start Sprawl nodes with tiered storage enabled
start_sprawl_nodes() {
  echo "Starting Sprawl nodes with tiered storage..."
  
  # Create data directories
  mkdir -p data/node1/memory
  mkdir -p data/node1/disk
  mkdir -p data/node2/memory
  mkdir -p data/node2/disk
  
  # Start first node (seed)
  echo "Starting seed node on port 8080..."
  MINIO_ENDPOINT=http://localhost:9000 \
  MINIO_ACCESS_KEY=minioadmin \
  MINIO_SECRET_KEY=minioadmin \
  SPRAWL_STORAGE_MEMORY_MAX_SIZE=10485760 \
  SPRAWL_STORAGE_DISK_ENABLED=true \
  SPRAWL_STORAGE_DISK_PATH=./data/node1/disk \
  SPRAWL_STORAGE_CLOUD_ENABLED=true \
  SPRAWL_STORAGE_CLOUD_BUCKET=sprawl-messages \
  SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=60 \
  SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=120 \
  ./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080 > logs/node1.log 2>&1 &
  
  NODE1_PID=$!
  echo "Node 1 started with PID $NODE1_PID"
  
  # Wait a moment for the first node to initialize
  sleep 5
  
  # Start second node
  echo "Starting second node on port 8081..."
  MINIO_ENDPOINT=http://localhost:9000 \
  MINIO_ACCESS_KEY=minioadmin \
  MINIO_SECRET_KEY=minioadmin \
  SPRAWL_STORAGE_MEMORY_MAX_SIZE=10485760 \
  SPRAWL_STORAGE_DISK_ENABLED=true \
  SPRAWL_STORAGE_DISK_PATH=./data/node2/disk \
  SPRAWL_STORAGE_CLOUD_ENABLED=true \
  SPRAWL_STORAGE_CLOUD_BUCKET=sprawl-messages \
  SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=60 \
  SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=120 \
  ./sprawl -bindAddr=127.0.0.1 -bindPort=7947 -httpPort=8081 -seeds=127.0.0.1:7946 > logs/node2.log 2>&1 &
  
  NODE2_PID=$!
  echo "Node 2 started with PID $NODE2_PID"
  
  # Store PIDs for later cleanup
  echo "$NODE1_PID $NODE2_PID" > .node_pids
  
  # Wait for nodes to be ready
  echo "Waiting for nodes to be ready..."
  for i in {1..30}; do
    if curl -s http://localhost:8080/health > /dev/null && \
       curl -s http://localhost:8081/health > /dev/null; then
      echo "✅ Nodes started successfully"
      return 0
    fi
    sleep 1
  done
  
  echo "❌ Failed to start nodes"
  return 1
}

# Generate test data to trigger tiering
generate_data() {
  echo "Subscribing to test-tiering topic..."
  ./sprawlctl -n http://localhost:8080 subscribe -t test-tiering
  
  echo "Publishing messages to fill memory tier..."
  # Generate 1MB of data (100 messages, ~10KB each) to trigger memory -> disk transition
  for i in {1..100}; do
    # Generate a payload with random data (~10KB)
    PAYLOAD=$(dd if=/dev/urandom bs=10240 count=1 2>/dev/null | base64)
    ./sprawlctl -n http://localhost:8080 publish -t test-tiering -p "$PAYLOAD"
    echo -n "."
  done
  echo ""
  
  echo "Waiting for memory -> disk transition (60 seconds)..."
  sleep 60
  
  echo "Publishing more messages..."
  # Generate more data to trigger disk -> cloud transition
  for i in {1..50}; do
    PAYLOAD=$(dd if=/dev/urandom bs=10240 count=1 2>/dev/null | base64)
    ./sprawlctl -n http://localhost:8080 publish -t test-tiering -p "$PAYLOAD"
    echo -n "."
  done
  echo ""
  
  echo "Waiting for disk -> cloud transition (120 seconds)..."
  sleep 120
}

# Check storage tiers
check_tiers() {
  echo "Checking storage tiers..."
  
  echo "Memory tier stats:"
  curl -s http://localhost:8080/store | jq .memory
  
  echo "Disk tier stats:"
  curl -s http://localhost:8080/store | jq .disk
  
  echo "Cloud tier stats:"
  curl -s http://localhost:8080/store | jq .cloud
  
  # Check if messages moved to cloud tier
  if [[ $(curl -s http://localhost:8080/store | jq .cloud.message_count) -gt 0 ]]; then
    echo "✅ Storage tiering demonstration successful!"
    echo "Messages have moved through all tiers: memory -> disk -> cloud"
  else
    echo "❌ Storage tiering demonstration incomplete"
    echo "Messages may not have moved to all tiers yet"
  fi
}

# Cleanup
cleanup() {
  echo "Cleaning up..."
  
  # Kill Sprawl nodes
  if [ -f .node_pids ]; then
    read -r NODE1_PID NODE2_PID < .node_pids
    kill -9 $NODE1_PID $NODE2_PID 2>/dev/null || true
    rm .node_pids
  fi
  
  # Stop MinIO container
  docker stop minio || true
  docker rm minio || true
  
  echo "Cleanup complete"
}

# Main function
main() {
  # Create logs directory
  mkdir -p logs
  
  # Register cleanup on exit
  trap cleanup EXIT
  
  echo "=== Sprawl Tiered Storage Demonstration ==="
  echo ""
  
  # Check if MinIO is running, start if needed
  if ! check_minio; then
    start_minio
  fi
  
  # Start Sprawl nodes with tiered storage
  start_sprawl_nodes
  
  # Generate test data
  generate_data
  
  # Check storage tiers
  check_tiers
  
  echo ""
  echo "=== Demonstration Complete ==="
}

# Run the main function
main 