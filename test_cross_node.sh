#!/bin/bash

# Test cross-node messaging in Sprawl
# This script starts multiple nodes and verifies that they can communicate with each other

set -e

echo "Building Sprawl..."
go build -o sprawl cmd/sprawl/main.go
go build -o sprawlctl cmd/sprawlctl/main.go

echo "Stopping any existing Sprawl nodes..."
pkill -f "sprawl" || true
sleep 2

# Ensure data directories exist
mkdir -p data/node1/disk
mkdir -p data/node2/disk
mkdir -p data/node3/disk

echo "Starting node 1..."
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=9070 > node1_log.txt 2>&1 &
NODE1_PID=$!
echo "Node 1 PID: $NODE1_PID"

sleep 2
echo "Checking node 1 health..."
curl -s http://127.0.0.1:9070/health

echo "Starting node 2..."
./sprawl -bindAddr=127.0.0.1 -bindPort=7947 -httpPort=9071 -seeds=127.0.0.1:7946 > node2_log.txt 2>&1 &
NODE2_PID=$!
echo "Node 2 PID: $NODE2_PID"

sleep 2
echo "Checking node 2 health..."
curl -s http://127.0.0.1:9071/health

echo "Starting node 3..."
./sprawl -bindAddr=127.0.0.1 -bindPort=7948 -httpPort=9072 -seeds=127.0.0.1:7946 > node3_log.txt 2>&1 &
NODE3_PID=$!
echo "Node 3 PID: $NODE3_PID"

sleep 2
echo "Checking node 3 health..."
curl -s http://127.0.0.1:9072/health

echo "Waiting for nodes to form cluster..."
sleep 5

echo "Checking cluster status on node 1..."
curl -s http://127.0.0.1:9070/status | jq

echo "Subscribing to test-topic on node 1..."
curl -s -X POST -H "Content-Type: application/json" http://127.0.0.1:9070/subscribe -d '{
  "id": "subscriber1",
  "topics": ["test-topic"]
}'

echo "Subscribing to test-topic on node 2..."
curl -s -X POST -H "Content-Type: application/json" http://127.0.0.1:9071/subscribe -d '{
  "id": "subscriber2",
  "topics": ["test-topic"]
}'

echo "Publishing message to test-topic from node 3..."
curl -s -X POST -H "Content-Type: application/json" http://127.0.0.1:9072/publish -d '{
  "topic": "test-topic",
  "payload": "Cross-node test message",
  "ttl": 3
}'

echo "Checking DHT state on node 1..."
curl -s http://127.0.0.1:9070/dht | jq

echo "Checking DHT state on node 2..."
curl -s http://127.0.0.1:9071/dht | jq

echo "Checking DHT state on node 3..."
curl -s http://127.0.0.1:9072/dht | jq

echo "Testing reverse direction - publishing from node 1 to node 3..."
curl -s -X POST -H "Content-Type: application/json" http://127.0.0.1:9070/publish -d '{
  "topic": "test-topic",
  "payload": "Reverse test message",
  "ttl": 3
}'

echo "Wait 5 seconds for messages to propagate..."
sleep 5

echo "Checking logs for received messages..."
echo "Node 1 logs:"
grep -A 1 "Received message on topic" node1_log.txt || echo "No messages found in node 1 logs"

echo "Node 2 logs:"
grep -A 1 "Received message on topic" node2_log.txt || echo "No messages found in node 2 logs"

echo "Node 3 logs:"
grep -A 1 "Received message on topic" node3_log.txt || echo "No messages found in node 3 logs"

echo "Cleaning up..."
kill $NODE1_PID $NODE2_PID $NODE3_PID || true
sleep 2

echo "Test completed." 