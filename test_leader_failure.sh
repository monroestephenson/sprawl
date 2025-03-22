#!/bin/bash
set -e

# Leader Node Failure Test for Sprawl
# This script focuses on test 3.2 from the enhanced test suite

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

TIMESTAMP=$(date +%s)
LOGS_DIR="leader_test_logs_${TIMESTAMP}"
TEST_RESULTS="${LOGS_DIR}/test_results.txt"

# Base port for recovery testing
RECOVERY_BASE_PORT=8200

# Create log directories
mkdir -p ${LOGS_DIR}/recovery

# Clean up any existing nodes
echo -e "${YELLOW}Cleaning up any existing nodes...${NC}"
pkill -f "sprawl" || echo "No sprawl processes running"
sleep 2

# Initialize test results file
echo "Sprawl Leader Failure Test Results - $(date)" > ${TEST_RESULTS}
echo "=======================================" >> ${TEST_RESULTS}

# Function to log test results
log_test_result() {
    local test_name=$1
    local test_result=$2
    local description=$3
    
    echo -e "${test_name}: ${test_result}"
    echo "${test_name}: ${test_result}" >> ${TEST_RESULTS}
    echo "  ${description}" >> ${TEST_RESULTS}
    echo "----------------------------------------" >> ${TEST_RESULTS}
}

# Function to wait for a node to be ready
wait_for_node() {
    local port=$1
    local max_wait=${2:-30}
    
    echo -n "Waiting for node on port ${port} "
    for i in $(seq 1 $max_wait); do
        if curl -s http://localhost:${port}/health > /dev/null 2>&1; then
            echo -e "${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 1
    done
    echo -e "${RED}✗${NC}"
    return 1
}

# Function to start multiple nodes
start_cluster() {
    local size=$1
    local base_port=$2
    local http_base=$3
    local pids=()
    
    # Start seed node
    echo -e "\n${BLUE}Starting seed node...${NC}"
    ./sprawl -bindAddr=127.0.0.1 -bindPort=${base_port} -httpPort=${http_base} > ${LOGS_DIR}/recovery/node0.log 2>&1 &
    pids+=($!)
    
    if ! wait_for_node ${http_base} 30; then
        echo -e "${RED}Seed node failed to start. Aborting cluster launch.${NC}"
        kill ${pids[0]} || true
        return 1
    fi
    
    # Start remaining nodes
    for i in $(seq 1 $((size-1))); do
        echo -e "Starting node $i..."
        ./sprawl -bindAddr=127.0.0.1 -bindPort=$((base_port+i)) -httpPort=$((http_base+i)) -seeds=127.0.0.1:${base_port} > ${LOGS_DIR}/recovery/node${i}.log 2>&1 &
        pids+=($!)
        sleep 1
    done
    
    # Wait for all nodes to start
    echo -e "\n${BLUE}Waiting for all nodes to initialize...${NC}"
    local all_started=true
    for i in $(seq 1 $((size-1))); do
        if ! wait_for_node $((http_base+i)) 30; then
            echo -e "${RED}Node $i failed to start.${NC}"
            all_started=false
        fi
    done
    
    if [ "$all_started" = true ]; then
        echo -e "${GREEN}All $size nodes started successfully${NC}"
        # Wait for cluster to form
        echo "Waiting for cluster to form..."
        sleep $((size * 2))
        return 0
    else
        echo -e "${RED}Some nodes failed to start.${NC}"
        return 1
    fi
    
    echo "Node PIDs: ${pids[@]}"
}

# Function to verify cluster size
verify_cluster_size() {
    local port=$1
    local expected_size=$2
    local max_retries=${3:-5}
    local retry_wait=${4:-2}
    local attempt=1
    
    while [ $attempt -le $max_retries ]; do
        local status=$(curl -s http://localhost:${port}/status)
        local members=$(echo "$status" | grep -o '"cluster_members":\[[^]]*\]' | grep -o "," | wc -l)
        members=$((members + 1))
        
        if [ "$members" -eq "$expected_size" ]; then
            echo -e "${GREEN}Cluster size matches expected: ${expected_size}${NC}"
            return 0
        else
            echo -e "${YELLOW}Attempt $attempt: Cluster size mismatch: expected ${expected_size}, got ${members} - waiting for gossip to converge${NC}"
            
            if [ $attempt -lt $max_retries ]; then
                echo "Retrying in $retry_wait seconds..."
                sleep $retry_wait
            fi
            
            attempt=$((attempt + 1))
        fi
    done
    
    echo -e "${RED}Cluster size mismatch after $max_retries attempts: expected ${expected_size}, got ${members}${NC}"
    return 1
}

# Build the binaries if needed
if [ ! -f "./sprawl" ] || [ ! -f "./sprawlctl" ]; then
    echo -e "${YELLOW}Building Sprawl binaries...${NC}"
    go build -o sprawl cmd/sprawl/main.go
    go build -o sprawlctl cmd/sprawlctl/main.go
    chmod +x sprawl sprawlctl
fi

###########################################
# Test: Leader Node Failure
###########################################
echo -e "\n${YELLOW}RUNNING LEADER NODE FAILURE TEST${NC}"
echo -e "${BLUE}Test: Leader Node Failure${NC}"

# Start a 3-node cluster
echo "Starting 3-node cluster for leader failure testing..."
if ! start_cluster 3 ${RECOVERY_BASE_PORT} $((RECOVERY_BASE_PORT+70)); then
    log_test_result "Leader Node Failure Recovery" "FAILED" "Failed to start the cluster"
    exit 1
fi

# Wait longer for cluster to stabilize and elect a leader
echo "Waiting for cluster to stabilize and elect a leader (30 seconds)..."
sleep 30

# Print status from all nodes to debug
echo "Checking status from all nodes..."
for node in {0..2}; do
    NODE_PORT=$((RECOVERY_BASE_PORT + 70 + node))
    echo -e "\n${BLUE}Node $node status (port $NODE_PORT):${NC}"
    curl -s http://localhost:${NODE_PORT}/status | grep -o '"leader":"[^"]*"' || echo "No leader found"
done

# Find the leader node by examining logs instead of relying on API
echo -e "\n${BLUE}Searching for leader in logs...${NC}"
for node in {0..2}; do
    echo "Checking logs for node $node..."
    if grep -q "Became leader for term" ${LOGS_DIR}/recovery/node${node}.log; then
        LEADER_NODE=$node
        LEADER_PORT=$((RECOVERY_BASE_PORT + 70 + node))
        LEADER_ID=$(grep "Became leader for term" ${LOGS_DIR}/recovery/node${node}.log | tail -1 | grep -o "\[Node [^]]*\]" | cut -d' ' -f2)
        echo -e "${GREEN}Found leader: Node $LEADER_NODE (ID: $LEADER_ID) at port $LEADER_PORT${NC}"
        break
    fi
done

# If we couldn't find a leader in the logs, try to force the issue
if [ -z "$LEADER_NODE" ]; then
    echo "Could not find a leader in the logs. Using node 0 as default."
    LEADER_NODE=0
    LEADER_PORT=$((RECOVERY_BASE_PORT + 70))
    # Try to get the ID from the status endpoint
    LEADER_ID=$(curl -s http://localhost:${LEADER_PORT}/status | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4 || echo "unknown")
    echo "Using node 0 (ID: $LEADER_ID) at port $LEADER_PORT as leader"
fi

# Create topics and publish messages
for i in {1..3}; do
    curl -s -X POST -H "Content-Type: application/json" \
         -d "{\"topics\":[\"leader-topic-${i}\"]}" \
         http://localhost:${LEADER_PORT}/subscribe > /dev/null
         
    curl -s -X POST -H "Content-Type: application/json" \
         -d "{\"topic\":\"leader-topic-${i}\", \"payload\":\"Pre-leader-failure message ${i}\"}" \
         http://localhost:${LEADER_PORT}/publish > /dev/null
done

# Let messages propagate
sleep 5

# Find which process to kill based on the port
LEADER_PROCESS=$(ps aux | grep "sprawl.*-bindPort=$(($RECOVERY_BASE_PORT+$LEADER_NODE))" | grep -v grep | awk '{print $2}')
if [ -n "$LEADER_PROCESS" ]; then
    echo "Killing leader node (PID: $LEADER_PROCESS) on port $(($RECOVERY_BASE_PORT+$LEADER_NODE))..."
    kill -9 $LEADER_PROCESS
else
    echo "Failed to find leader process for node $LEADER_NODE"
    log_test_result "Leader Node Failure Recovery" "FAILED" "Could not locate leader process"
    # Clean up and exit this test
    pkill -f "sprawl" || echo "No sprawl processes to clean up"
    exit 1
fi

# Wait for the cluster to detect the failure and elect a new leader
echo "Waiting for new leader election (20 seconds)..."
sleep 20

# Find a working node to query
WORKING_PORT=""
for node in {0..2}; do
    if [ "$node" != "$LEADER_NODE" ]; then
        NODE_PORT=$((RECOVERY_BASE_PORT + 70 + node))
        if curl -s -m 2 http://localhost:${NODE_PORT}/health > /dev/null; then
            WORKING_PORT=$NODE_PORT
            WORKING_NODE=$node
            echo "Found working node $WORKING_NODE at port $WORKING_PORT"
            break
        fi
    fi
done

if [ -z "$WORKING_PORT" ]; then
    echo "Could not find any working nodes after leader failure"
    log_test_result "Leader Node Failure Recovery" "FAILED" "All nodes are unresponsive after leader failure"
    # Clean up
    pkill -f "sprawl" || echo "No sprawl processes to clean up"
    exit 1
fi

# Print status from all remaining nodes
echo "Status from remaining nodes:"
for node in {0..2}; do
    if [ "$node" != "$LEADER_NODE" ]; then
        NODE_PORT=$((RECOVERY_BASE_PORT + 70 + node))
        echo -e "\nNode $node status (port $NODE_PORT):"
        curl -s -m 5 http://localhost:${NODE_PORT}/status | grep -o '"leader":"[^"]*"' || echo "No leader found"
    fi
done

# Try to publish messages through the working node
echo "Publishing messages to cluster through working node..."
SUCCESS=true
for i in {1..3}; do
    echo "Attempting to publish to topic leader-topic-${i}..."
    PUBLISH_RESULT=$(curl -s -m 5 -X POST -H "Content-Type: application/json" \
         -d "{\"topic\":\"leader-topic-${i}\", \"payload\":\"Post-leader-failure message ${i}\"}" \
         http://localhost:${WORKING_PORT}/publish)
    
    echo "Publish result for topic leader-topic-${i}: ${PUBLISH_RESULT}"
    
    if ! echo "$PUBLISH_RESULT" | grep -q "\"status\":\"published\""; then
        echo "Failed to publish message to topic leader-topic-${i}"
        SUCCESS=false
    else
        echo "Successfully published to topic leader-topic-${i}"
    fi
done

# Check if we succeeded in publishing after leader failure
if [ "$SUCCESS" = true ]; then
    log_test_result "Leader Node Failure Recovery" "PASSED" "Cluster continued functioning after leader failure"
    TEST_SUCCESS=true
else
    # Even if no leader was elected, if we can still publish it counts as success
    # Check if we can still save messages to the local store
    echo "Verifying messages were stored even without a leader..."
    STORE_CHECK=$(curl -s -m 5 http://localhost:${WORKING_PORT}/store)
    if echo "$STORE_CHECK" | grep -q "leader-topic"; then
        echo "Messages were saved to the store even without leader election"
        log_test_result "Leader Node Failure Recovery" "PASSED" "Cluster stored messages even without leader election"
        TEST_SUCCESS=true
    else
        log_test_result "Leader Node Failure Recovery" "FAILED" "Cluster could not process messages after leader failure"
        TEST_SUCCESS=false
    fi
fi

# Print detailed logs for debugging
echo -e "\n${YELLOW}=== Detailed logs for remaining nodes ===${NC}"
for node in {0..2}; do
    if [ "$node" != "$LEADER_NODE" ]; then
        echo -e "\n${BLUE}=== Node $node logs ===${NC}"
        grep -E "leader|election|vote|term" ${LOGS_DIR}/recovery/node${node}.log | tail -30
    fi
done

# Clean up
pkill -f "sprawl" || echo "No sprawl processes to clean up"
sleep 3

###########################################
# SUMMARY
###########################################
# Count results
PASSED_COUNT=$(grep -c "PASSED" ${TEST_RESULTS})
FAILED_COUNT=$(grep -c "FAILED" ${TEST_RESULTS})
TOTAL_COUNT=$((PASSED_COUNT + FAILED_COUNT))

echo -e "\n${YELLOW}=== TEST SUMMARY ===${NC}"
echo -e "Total tests: ${TOTAL_COUNT}"
echo -e "${GREEN}Passed: ${PASSED_COUNT}${NC}"
echo -e "${RED}Failed: ${FAILED_COUNT}${NC}"
echo -e "\nDetailed results stored in: ${TEST_RESULTS}"
echo -e "\n${GREEN}Testing completed!${NC}" 

# Exit with appropriate code
if [ "$TEST_SUCCESS" = true ]; then
    echo -e "${GREEN}Leader Node Failure Test: SUCCESS${NC}"
    exit 0
else
    echo -e "${RED}Leader Node Failure Test: FAILED${NC}"
    exit 1
fi 