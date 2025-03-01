#!/bin/bash

# Kill any existing nodes
echo "Cleaning up any existing nodes..."
pkill -f "sprawl" || true
sleep 2

# Function to start a node in a new terminal
start_node() {
    local cmd="cd $(pwd) && go run main.go $1"
    echo "Starting node with command: $cmd"
    osascript -e "tell application \"Terminal\" to do script \"$cmd\"" > /dev/null 2>&1
}

# Function to wait for a node to be ready
wait_for_node() {
    local port=$1
    local retries=60
    local ready=false
    
    echo -n "Waiting for node on port $port "
    while [ $retries -gt 0 ] && [ "$ready" = false ]; do
        if curl -s "http://localhost:$port/status" > /dev/null 2>&1; then
            ready=true
            echo "✓"
        else
            echo -n "."
            sleep 1
            retries=$((retries-1))
        fi
    done
    
    if [ "$ready" = false ]; then
        echo "❌"
        echo "Node on port $port failed to start after 60 seconds!"
        return 1
    fi

    # Additional wait to ensure the node is fully initialized
    sleep 2
    return 0
}

# Build the CLI tool
echo "Building sprawlctl..."
go build -o sprawlctl cmd/sprawlctl/main.go || {
    echo "Failed to build sprawlctl"
    exit 1
}

echo "Starting node 1 (seed node)..."
start_node "-bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080"

echo "Waiting for seed node to start..."
sleep 5  # Give more time for initial compilation
wait_for_node 8080 || {
    echo "Failed to start seed node. Check the terminal window for errors."
    exit 1
}

echo "Starting node 2..."
start_node "-bindAddr=127.0.0.1 -bindPort=7947 -httpAddr=127.0.0.1 -httpPort=8081 -seeds=127.0.0.1:7946"

echo "Starting node 3..."
start_node "-bindAddr=127.0.0.1 -bindPort=7948 -httpAddr=127.0.0.1 -httpPort=8082 -seeds=127.0.0.1:7946"

# Wait for all nodes to be ready
echo "Waiting for nodes to be ready..."
wait_for_node 8081 || exit 1
wait_for_node 8082 || exit 1

# Additional wait for cluster formation
echo "Waiting for cluster to form..."
sleep 10

# Test cluster formation
echo -e "\nChecking cluster status..."
for port in 8080 8081 8082; do
    echo "Node on port $port status:"
    curl -s "http://localhost:$port/status" | jq '.' || echo "Failed to get status"
done

# Subscribe all nodes to test topics
echo -e "\nSubscribing nodes to test topics..."
./sprawlctl subscribe \
    --nodes="http://localhost:8080,http://localhost:8081,http://localhost:8082" \
    --topic="test"

./sprawlctl subscribe \
    --nodes="http://localhost:8080,http://localhost:8081,http://localhost:8082" \
    --topic="loadtest"

# Wait for subscriptions to propagate
echo "Waiting for subscriptions to propagate..."
sleep 10

# Run integration tests
echo -e "\nRunning integration tests..."
./sprawlctl test \
    --nodes="http://localhost:8080,http://localhost:8081,http://localhost:8082" \
    --count=100 \
    --parallel=10

# Run load test
echo -e "\nRunning load test..."
./sprawlctl publish \
    --nodes="http://localhost:8080,http://localhost:8081,http://localhost:8082" \
    --topic="loadtest" \
    --count=1000 \
    --parallel=20 \
    --interval=10ms

# Check final metrics
echo -e "\nFinal metrics:"
for port in 8080 8081 8082; do
    echo -e "\nNode on port $port metrics:"
    curl -s "http://localhost:$port/metrics" | jq '.' || echo "Failed to get metrics"
done

echo -e "\nTest complete. Check the terminal windows for node logs." 