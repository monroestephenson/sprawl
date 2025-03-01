#!/bin/bash

# Clean up any existing nodes more thoroughly
echo "Cleaning up any existing nodes..."
pkill -f "sprawl" || true
sleep 2  # Give processes time to die
# Double check and force kill if needed
pkill -9 -f "sprawl" || true
# Check if ports are still in use
for port in 7946 7947 7948; do
    pid=$(lsof -ti :$port)
    if [ ! -z "$pid" ]; then
        echo "Force killing process using port $port"
        kill -9 $pid || true
    fi
done
rm -f node*.log

# Build the CLI tool
echo "Building sprawlctl..."
go build -o sprawlctl cmd/sprawlctl/main.go

# Start node 1 (seed node)
echo "Starting node 1 (seed node)..."
echo "Starting node with command: cd $(pwd) && go run main.go -bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080"
go run main.go -bindAddr=127.0.0.1 -bindPort=7946 -httpAddr=127.0.0.1 -httpPort=8080 > node1.log 2>&1 &

# Wait for seed node to start and be ready
echo "Waiting for seed node to start..."
echo -n "Waiting for node on port 8080 "
until curl -s http://localhost:8080/status > /dev/null; do
    echo -n "."
    sleep 1
done
echo "✓"

# Give seed node time to fully initialize
sleep 5

# Start node 2
echo "Starting node 2..."
echo "Starting node with command: cd $(pwd) && go run main.go -bindAddr=127.0.0.1 -bindPort=7947 -httpAddr=127.0.0.1 -httpPort=8081 -seeds=127.0.0.1:7946"
go run main.go -bindAddr=127.0.0.1 -bindPort=7947 -httpAddr=127.0.0.1 -httpPort=8081 -seeds=127.0.0.1:7946 > node2.log 2>&1 &

# Wait for node 2 to be ready
echo -n "Waiting for node on port 8081 "
until curl -s http://localhost:8081/status > /dev/null; do
    echo -n "."
    sleep 1
done
echo "✓"

# Give node 2 time to join the cluster
sleep 5

# Start node 3
echo "Starting node 3..."
echo "Starting node with command: cd $(pwd) && go run main.go -bindAddr=127.0.0.1 -bindPort=7948 -httpAddr=127.0.0.1 -httpPort=8082 -seeds=127.0.0.1:7946"
go run main.go -bindAddr=127.0.0.1 -bindPort=7948 -httpAddr=127.0.0.1 -httpPort=8082 -seeds=127.0.0.1:7946 > node3.log 2>&1 &

# Wait for node 3 to be ready
echo -n "Waiting for node on port 8082 "
until curl -s http://localhost:8082/status > /dev/null; do
    echo -n "."
    sleep 1
done
echo "✓"

# Wait for cluster to form and verify membership
echo "Waiting for cluster to form..."
sleep 10

# Function to check if a node sees all members
check_cluster_members() {
    local port=$1
    local members=$(curl -s http://localhost:$port/status | jq '.cluster_members | length')
    if [ "$members" -eq 3 ]; then
        return 0
    else
        return 1
    fi
}

# Wait for all nodes to see each other (up to 30 seconds)
echo "Verifying cluster membership..."
for i in {1..30}; do
    if check_cluster_members 8080 && check_cluster_members 8081 && check_cluster_members 8082; then
        echo "✓ All nodes see complete cluster"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "! Warning: Cluster formation incomplete after 30 seconds"
    fi
    sleep 1
done

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
sleep 5

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