#!/bin/bash

# Clean up any existing nodes
echo "Cleaning up any existing nodes..."
pkill -f "sprawl" || true
rm -f node*.log

# Build the CLI tool
echo "Building sprawlctl..."
go build -o sprawlctl cmd/sprawlctl/main.go

# Start node 1 (seed node)
echo "Starting node 1 (seed node)..."
echo "Starting node with command: cd $(pwd) && go run main.go -bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080"
go run main.go -bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080 > node1.log 2>&1 &

# Wait for seed node to start
echo "Waiting for seed node to start..."
echo -n "Waiting for node on port 8080 "
until curl -s http://localhost:8080/status > /dev/null; do
    echo -n "."
    sleep 1
done
echo "✓"

# Start node 2
echo "Starting node 2..."
echo "Starting node with command: cd $(pwd) && go run main.go -bindAddr=127.0.0.1 -bindPort=7947 -httpAddr=127.0.0.1 -httpPort=8081 -seeds=127.0.0.1:7946"
go run main.go -bindAddr=127.0.0.1 -bindPort=7947 -httpAddr=127.0.0.1 -httpPort=8081 -seeds=127.0.0.1:7946 > node2.log 2>&1 &

# Start node 3
echo "Starting node 3..."
echo "Starting node with command: cd $(pwd) && go run main.go -bindAddr=127.0.0.1 -bindPort=7948 -httpAddr=127.0.0.1 -httpPort=8082 -seeds=127.0.0.1:7946"
go run main.go -bindAddr=127.0.0.1 -bindPort=7948 -httpAddr=127.0.0.1 -httpPort=8082 -seeds=127.0.0.1:7946 > node3.log 2>&1 &

# Wait for nodes to be ready
echo "Waiting for nodes to be ready..."
echo -n "Waiting for node on port 8081 "
until curl -s http://localhost:8081/status > /dev/null; do
    echo -n "."
    sleep 1
done
echo "✓"

echo -n "Waiting for node on port 8082 "
until curl -s http://localhost:8082/status > /dev/null; do
    echo -n "."
    sleep 1
done
echo "✓"

# Wait for cluster to form
echo "Waiting for cluster to form..."
sleep 2

# Check cluster status
echo -e "\nChecking cluster status..."
echo "Node on port 8080 status:"
curl -s http://localhost:8080/status | jq .
echo "Node on port 8081 status:"
curl -s http://localhost:8081/status | jq .
echo "Node on port 8082 status:"
curl -s http://localhost:8082/status | jq .

# Subscribe nodes to test topics
echo -e "\nSubscribing nodes to test topics..."
./sprawlctl -n http://localhost:8080 subscribe -t test
./sprawlctl -n http://localhost:8081 subscribe -t test
./sprawlctl -n http://localhost:8082 subscribe -t test
./sprawlctl -n http://localhost:8080 subscribe -t loadtest
./sprawlctl -n http://localhost:8081 subscribe -t loadtest
./sprawlctl -n http://localhost:8082 subscribe -t loadtest
echo "Waiting for subscriptions to propagate..."
sleep 2

# Run integration tests
echo -e "\nRunning integration tests..."
./sprawlctl test

# Run load test
echo -e "\nRunning load test..."
./sprawlctl -n http://localhost:8080,http://localhost:8081,http://localhost:8082 test -c 100 -P 10

# Check for any errors in node logs
echo -e "\nChecking node logs for errors..."
echo "Node 1 errors:"
grep -i "error\|panic\|fatal" node1.log || true
echo -e "\nNode 2 errors:"
grep -i "error\|panic\|fatal" node2.log || true
echo -e "\nNode 3 errors:"
grep -i "error\|panic\|fatal" node3.log || true

# Clean up
pkill -f "sprawl" || true 