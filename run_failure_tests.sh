#!/bin/bash
# Set bash to stop on first error, but with trap for cleanup
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

TIMESTAMP=$(date +%s)
LOGS_DIR="simple_failure_test_logs_${TIMESTAMP}"
RECOVERY_BASE_PORT=9300  # Higher port range to avoid conflicts

# Create log directories
mkdir -p ${LOGS_DIR}

# Function to clean up
cleanup() {
    echo -e "\n${YELLOW}Cleaning up processes...${NC}"
    pkill -f "sprawl" > /dev/null 2>&1 || echo "No sprawl processes to clean up"
    echo -e "${YELLOW}Cleanup complete${NC}"
    exit 0
}

# Set up trap to ensure cleanup on exit
trap cleanup EXIT INT TERM

# Function to check if a node is healthy
check_node_health() {
    local port=$1
    local response=$(curl -s -m 3 http://localhost:${port}/health 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$response" ]; then
        echo -e "${GREEN}Node on port ${port} is healthy${NC}"
        return 0
    else
        echo -e "${RED}Node on port ${port} is not healthy${NC}"
        return 1
    fi
}

# Function to wait for a node to be ready
wait_for_node() {
    local port=$1
    local max_wait=${2:-30}
    
    echo -n "Waiting for node on port ${port} "
    for i in $(seq 1 $max_wait); do
        if curl -s -m 3 http://localhost:${port}/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e "${RED}✗${NC}"
    return 1
}

# Check for running sprawl processes first
echo -e "${YELLOW}Checking for running sprawl processes...${NC}"
ps aux | grep sprawl | grep -v grep || echo "No sprawl processes found"

# Clean up any existing nodes
echo -e "\n${YELLOW}Cleaning up any existing nodes...${NC}"
pkill -f "sprawl" > /dev/null 2>&1 || echo "No sprawl processes running"
sleep 2

# Check if ports are in use
echo -e "${YELLOW}Checking if ports are in use...${NC}"
for i in {0..2}; do
    PORT=$((RECOVERY_BASE_PORT+i))
    HTTP_PORT=$((RECOVERY_BASE_PORT+70+i))
    
    echo -n "Checking bind port $PORT: "
    if nc -z localhost $PORT 2>/dev/null; then
        echo -e "${RED}IN USE${NC}"
    else
        echo -e "${GREEN}Available${NC}"
    fi
    
    echo -n "Checking HTTP port $HTTP_PORT: "
    if nc -z localhost $HTTP_PORT 2>/dev/null; then
        echo -e "${RED}IN USE${NC}"
    else
        echo -e "${GREEN}Available${NC}"
    fi
done

# Build the binaries if needed
if [ ! -f "./sprawl" ] || [ ! -f "./sprawlctl" ]; then
    echo -e "${YELLOW}Building Sprawl binaries...${NC}"
    go build -o sprawl cmd/sprawl/main.go
    go build -o sprawlctl cmd/sprawlctl/main.go
    chmod +x sprawl sprawlctl
fi

# Start a 3-node cluster
echo -e "\n${BLUE}Starting a 3-node cluster...${NC}"

# Start seed node
echo "Starting seed node..."
./sprawl -bindAddr=127.0.0.1 -bindPort=${RECOVERY_BASE_PORT} -httpPort=$((RECOVERY_BASE_PORT+70)) > ${LOGS_DIR}/node0.log 2>&1 &
SEED_PID=$!
echo "Seed node started with PID: $SEED_PID"

if ! wait_for_node $((RECOVERY_BASE_PORT+70)) 30; then
    echo -e "${RED}Seed node failed to start. Aborting.${NC}"
    echo "Check the log file: ${LOGS_DIR}/node0.log"
    tail -50 ${LOGS_DIR}/node0.log
    exit 1
fi

# Start nodes 1 and 2
NODE_PIDS=()
for i in {1..2}; do
    echo "Starting node $i..."
    ./sprawl -bindAddr=127.0.0.1 -bindPort=$((RECOVERY_BASE_PORT+i)) -httpPort=$((RECOVERY_BASE_PORT+70+i)) -seeds=127.0.0.1:${RECOVERY_BASE_PORT} > ${LOGS_DIR}/node$i.log 2>&1 &
    pid=$!
    NODE_PIDS+=($pid)
    echo "Node $i started with PID: $pid"
    sleep 2
done

# Wait for nodes to be ready
echo -e "\n${BLUE}Waiting for all nodes to initialize...${NC}"
for i in {1..2}; do
    if ! wait_for_node $((RECOVERY_BASE_PORT+70+i)) 30; then
        echo -e "${RED}Node $i failed to start. Aborting.${NC}"
        echo "Check the log file: ${LOGS_DIR}/node$i.log"
        tail -50 ${LOGS_DIR}/node$i.log
        exit 1
    fi
done

echo -e "${GREEN}All 3 nodes started successfully${NC}"
echo "Waiting for cluster to form..."
sleep 10

# Check cluster status on all nodes
echo -e "\n${BLUE}Checking initial cluster status on all nodes...${NC}"
for i in {0..2}; do
    echo "Node $i status:"
    curl -s -m 5 http://localhost:$((RECOVERY_BASE_PORT+70+i))/status | grep cluster_members || echo "Failed to get cluster members"
done

# Create topics and publish messages
echo -e "\n${BLUE}Creating topics and publishing messages...${NC}"
for i in {1..2}; do
    echo "Creating topic recovery-topic-${i}..."
    curl -s -m 5 -X POST -H "Content-Type: application/json" \
         -d "{\"topics\":[\"recovery-topic-${i}\"]}" \
         http://localhost:$((RECOVERY_BASE_PORT+70))/subscribe
    
    echo -e "\nPublishing message to recovery-topic-${i}..."
    curl -s -m 5 -X POST -H "Content-Type: application/json" \
         -d "{\"topic\":\"recovery-topic-${i}\", \"payload\":\"Pre-failure message ${i}\"}" \
         http://localhost:$((RECOVERY_BASE_PORT+70))/publish
done

echo -e "\n${BLUE}Checking DHT topics on all nodes...${NC}"
for i in {0..2}; do
    echo "Node $i topics:"
    curl -s -m 5 http://localhost:$((RECOVERY_BASE_PORT+70+i))/topics | grep -o '"name":"[^"]*"' || echo "No topics found"
done

# Kill node 1
echo -e "\n${BLUE}Killing node 1...${NC}"
NODE1_PORT=$((RECOVERY_BASE_PORT+1))
NODE1_PID=$(ps aux | grep "sprawl.*-bindPort=${NODE1_PORT}" | grep -v grep | awk '{print $2}')

if [ -n "$NODE1_PID" ]; then
    echo "Found node 1 process (PID: $NODE1_PID), killing with SIGKILL..."
    kill -9 $NODE1_PID
    sleep 2
    if ! ps -p $NODE1_PID > /dev/null 2>&1; then
        echo -e "${GREEN}Node 1 process successfully terminated.${NC}"
    else
        echo -e "${RED}Warning: Failed to terminate node 1${NC}"
    fi
else
    echo -e "${RED}Warning: Could not find process for node 1${NC}"
    exit 1
fi

# Wait for cluster to detect the failure
echo -e "\n${BLUE}Waiting for cluster to detect node failure...${NC}"
echo "This may take 15-30 seconds for gossip protocol to propagate failure detection..."
sleep 15

# Check cluster status on remaining nodes
echo -e "\n${BLUE}Checking cluster status after node failure...${NC}"
for i in 0 2; do
    if check_node_health $((RECOVERY_BASE_PORT+70+i)); then
        echo "Node $i status: (expect to see only 2 members)"
        curl -s -m 5 http://localhost:$((RECOVERY_BASE_PORT+70+i))/status | grep cluster_members || echo "Failed to get cluster members"
        
        echo "Node $i topics: (these should not include topics only on the failed node)"
        curl -s -m 5 http://localhost:$((RECOVERY_BASE_PORT+70+i))/topics | grep -o '"name":"[^"]*"' || echo "No topics found"
    else
        echo -e "${RED}Node $i is not healthy!${NC}"
    fi
done

# Try to publish messages after node failure
echo -e "\n${BLUE}Publishing messages after node failure...${NC}"

SUCCESS=true
for i in {1..2}; do
    echo "Publishing to recovery-topic-${i}..."
    PUBLISH_RESULT=$(curl -s -m 5 -X POST -H "Content-Type: application/json" \
         -d "{\"topic\":\"recovery-topic-${i}\", \"payload\":\"After node failure message ${i}\"}" \
         http://localhost:$((RECOVERY_BASE_PORT+70))/publish)
    
    if ! echo "$PUBLISH_RESULT" | grep -q "\"status\":\"published\""; then
        echo -e "${RED}Failed to publish message to topic recovery-topic-${i}${NC}"
        SUCCESS=false
    else
        echo -e "${GREEN}Successfully published to recovery-topic-${i}${NC}"
    fi
done

echo -e "\n${GREEN}Test completed - see logs in ${LOGS_DIR} directory${NC}"

# Return proper exit code
if [ "$SUCCESS" = true ]; then
    echo -e "${GREEN}Single Node Failure Test: SUCCESS${NC}"
    exit 0
else
    echo -e "${RED}Single Node Failure Test: FAILED${NC}"
    exit 1
fi
# cleanup handled by trap 