#!/bin/bash

# Kill any existing nodes
echo "Cleaning up any existing nodes..."
pkill -f "sprawl" || true
sleep 2

# Function to start a node in a new terminal
start_node() {
    local cmd="cd $(pwd) && go run main.go $1"
    echo "Starting node with command: $cmd"
    osascript -e "tell app \"Terminal\" to do script \"$cmd\"" || {
        echo "Failed to start new terminal!"
        return 1
    }
}

# Function to wait for a node to be ready
wait_for_node() {
    local port=$1
    local retries=20
    local ready=false
    
    echo -n "Waiting for node on port $port "
    while [ $retries -gt 0 ] && [ "$ready" = false ]; do
        response=$(curl -s "http://localhost:$port/status" 2>&1)
        if [ $? -eq 0 ]; then
            ready=true
            echo "✓"
        else
            echo -n "."
            echo "Debug: curl response: $response" >&2
            sleep 2
            retries=$((retries-1))
        fi
    done
    
    if [ "$ready" = false ]; then
        echo "❌"
        echo "Node on port $port failed to start after 40 seconds!"
        echo "Try running: curl -v http://localhost:$port/status"
        return 1
    fi
    return 0
}

echo "Starting node 1 (seed node)..."
start_node "-bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080"

echo "Waiting for seed node to start..."
sleep 10  # Give more time for initial compilation
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

# Test cluster formation
echo -e "\nChecking cluster status..."
for port in 8080 8081 8082; do
    echo "Node on port $port status:"
    curl -s "http://localhost:$port/status" | jq '.' || echo "Failed to get status"
done

# Subscribe to topic on different nodes
echo -e "\nSubscribing to 'test' topic on node 2..."
curl -s -X POST "http://localhost:8081/subscribe" \
  -H "Content-Type: application/json" \
  -d '{"topic": "test"}' | jq '.' || echo "Failed to subscribe on node 2"

sleep 2

echo -e "\nSubscribing to 'test' topic on node 3..."
curl -s -X POST "http://localhost:8082/subscribe" \
  -H "Content-Type: application/json" \
  -d '{"topic": "test"}' | jq '.' || echo "Failed to subscribe on node 3"

sleep 2

# Publish messages through different nodes
echo -e "\nPublishing message through node 1..."
response=$(curl -s -X POST "http://localhost:8080/publish" \
  -H "Content-Type: application/json" \
  -d '{"topic": "test", "payload": "Message from node 1"}')
echo "$response" | jq '.' || echo "Error response: $response"

sleep 2

echo -e "\nPublishing message through node 2..."
response=$(curl -s -X POST "http://localhost:8081/publish" \
  -H "Content-Type: application/json" \
  -d '{"topic": "test", "payload": "Message from node 2"}')
echo "$response" | jq '.' || echo "Error response: $response"

sleep 2

# Check metrics
echo -e "\nChecking metrics after publishing..."
for port in 8080 8081 8082; do
    echo -e "\nNode on port $port metrics:"
    curl -s "http://localhost:$port/metrics" | jq '.' || echo "Failed to get metrics"
done

echo -e "\nTest complete. Check the terminal windows for node logs." 