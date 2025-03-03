#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "Starting distributed registry tests..."

# Wait for service to be ready
wait_for_service() {
    local host=$1
    local port=$2
    local retries=30

    echo "Waiting for service at ${host}:${port}..."
    while ! curl -s "http://${host}:${port}/health" > /dev/null; do
        retries=$((retries - 1))
        if [ $retries -eq 0 ]; then
            echo -e "${RED}Service at ${host}:${port} failed to start${NC}"
            exit 1
        fi
        sleep 1
    done
    echo -e "${GREEN}Service at ${host}:${port} is ready${NC}"
}

# Wait for all nodes
wait_for_service localhost 18080
wait_for_service localhost 18081
wait_for_service localhost 18082

echo "Testing subscriber registration..."

# Register subscribers on different nodes
curl -X POST -H "Content-Type: application/json" -d '{
    "id": "sub1",
    "topics": ["test-topic"]
}' http://localhost:18080/subscribe

curl -X POST -H "Content-Type: application/json" -d '{
    "id": "sub2",
    "topics": ["test-topic"]
}' http://localhost:18081/subscribe

curl -X POST -H "Content-Type: application/json" -d '{
    "id": "sub3",
    "topics": ["test-topic"]
}' http://localhost:18082/subscribe

sleep 2

echo "Testing consumer group creation..."

# Create a consumer group
curl -X POST -H "Content-Type: application/json" -d '{
    "group_id": "group1",
    "topic": "test-topic",
    "members": ["sub1", "sub2"]
}' http://localhost:18080/consumer-groups

sleep 2

echo "Verifying consumer group state across nodes..."

# Check consumer group state on all nodes
for port in 18080 18081 18082; do
    response=$(curl -s "http://localhost:${port}/consumer-groups/group1")
    if ! echo "$response" | jq -e '.members | length == 2' > /dev/null; then
        echo -e "${RED}Consumer group state mismatch on port ${port}${NC}"
        echo "Response: $response"
        exit 1
    fi
done

echo -e "${GREEN}Consumer group state consistent across nodes${NC}"

echo "Testing consumer group rebalancing..."

# Add a new member to trigger rebalancing
curl -X PUT -H "Content-Type: application/json" -d '{
    "group_id": "group1",
    "topic": "test-topic",
    "members": ["sub1", "sub2", "sub3"]
}' http://localhost:18080/consumer-groups/group1

sleep 2

echo "Verifying rebalancing across nodes..."

# Check updated state on all nodes
for port in 18080 18081 18082; do
    response=$(curl -s "http://localhost:${port}/consumer-groups/group1")
    if ! echo "$response" | jq -e '.members | length == 3' > /dev/null; then
        echo -e "${RED}Rebalancing failed on port ${port}${NC}"
        echo "Response: $response"
        exit 1
    fi
done

echo -e "${GREEN}Rebalancing successful and consistent across nodes${NC}"

echo "Testing offset tracking..."

# Publish some messages and update offsets
curl -X POST -H "Content-Type: application/json" -d '{
    "topic": "test-topic",
    "group_id": "group1",
    "offset": 100
}' http://localhost:18080/offsets

sleep 2

echo "Verifying offset consistency..."

# Check offset state on all nodes
for port in 18080 18081 18082; do
    response=$(curl -s "http://localhost:${port}/offsets/test-topic/group1")
    if ! echo "$response" | jq -e '.current_offset == 100' > /dev/null; then
        echo -e "${RED}Offset state mismatch on port ${port}${NC}"
        echo "Response: $response"
        exit 1
    fi
done

echo -e "${GREEN}Offset state consistent across nodes${NC}"

echo -e "${GREEN}All distributed registry tests passed!${NC}" 