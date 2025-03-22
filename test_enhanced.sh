#!/bin/bash
set -e

# Enhanced testing script for Sprawl
# Focuses on storage configurations, high-scale deployments, and failure recovery

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

TIMESTAMP=$(date +%s)
LOGS_DIR="enhanced_test_logs_${TIMESTAMP}"
TEST_RESULTS="${LOGS_DIR}/test_results.txt"

# Base ports for different node ranges
STORAGE_BASE_PORT=8000
SCALE_BASE_PORT=8100
RECOVERY_BASE_PORT=8200

# Create log directories
mkdir -p ${LOGS_DIR}/{storage,scale,recovery}

# Clean up any existing nodes
echo -e "${YELLOW}Cleaning up any existing nodes...${NC}"
pkill -f "sprawl" || echo "No sprawl processes running"
sleep 2

# Build the binaries if needed
if [ ! -f "./sprawl" ] || [ ! -f "./sprawlctl" ]; then
    echo -e "${YELLOW}Building Sprawl binaries...${NC}"
    go build -o sprawl cmd/sprawl/main.go
    go build -o sprawlctl cmd/sprawlctl/main.go
    chmod +x sprawl sprawlctl
fi

# Initialize test results file
echo "Sprawl Enhanced Test Results - $(date)" > ${TEST_RESULTS}
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

###########################################
# PART 1: STORAGE CONFIGURATION TESTING
###########################################
echo -e "\n${YELLOW}PART 1: STORAGE CONFIGURATION TESTING${NC}"
echo -e "${BLUE}Testing different storage configurations and transitions${NC}"

# Function to verify storage type
verify_storage_type() {
    local port=$1
    local expected_type=$2
    local result
    
    sleep 5  # Give time for initialization
    result=$(curl -s http://localhost:${port}/metrics)
    storage_type=$(echo $result | grep -o '"storage_type":"[^"]*"' | cut -d'"' -f4)
    
    if [ "$storage_type" = "$expected_type" ]; then
        echo -e "${GREEN}Storage type matches expected: ${expected_type}${NC}"
        return 0
    else
        echo -e "${RED}Storage type mismatch: expected ${expected_type}, got ${storage_type}${NC}"
        return 1
    fi
}

# Test 1.1: Memory Storage (Default)
echo -e "\n${BLUE}Test 1.1: Memory Storage (Default)${NC}"
./sprawl -bindAddr=127.0.0.1 -bindPort=${STORAGE_BASE_PORT} -httpPort=$((STORAGE_BASE_PORT+70)) > ${LOGS_DIR}/storage/memory.log 2>&1 &
MEMORY_PID=$!

if wait_for_node $((STORAGE_BASE_PORT+70)); then
    if verify_storage_type $((STORAGE_BASE_PORT+70)) "memory"; then
        log_test_result "Memory Storage" "PASSED" "Default storage initialized correctly as memory"
    else
        log_test_result "Memory Storage" "FAILED" "Default storage did not initialize as memory"
    fi
else
    log_test_result "Memory Storage" "FAILED" "Node failed to start"
fi

# Clean up memory node
kill $MEMORY_PID || true
sleep 2

# Test 1.2: Disk Storage - Testing with environment variables
echo -e "\n${BLUE}Test 1.2: Disk Storage via Environment Variables${NC}"
mkdir -p data/disk_test

# Try different environment variable combinations to find which one works
export SPRAWL_STORAGE_TYPE=disk
export SPRAWL_STORAGE_PATH=./data/disk_test
export SPRAWL_DISK_STORAGE=true
export SPRAWL_STORAGE_DISK_PATH=./data/disk_test

./sprawl -bindAddr=127.0.0.1 -bindPort=${STORAGE_BASE_PORT} -httpPort=$((STORAGE_BASE_PORT+70)) > ${LOGS_DIR}/storage/disk.log 2>&1 &
DISK_PID=$!

if wait_for_node $((STORAGE_BASE_PORT+70)); then
    if verify_storage_type $((STORAGE_BASE_PORT+70)) "disk"; then
        log_test_result "Disk Storage (ENV)" "PASSED" "Disk storage initialized correctly via environment variables"
    else
        log_test_result "Disk Storage (ENV)" "FAILED" "Failed to initialize disk storage via environment variables"
        echo -e "${YELLOW}Checking logs for disk storage configuration issues...${NC}"
        grep -i "storage\|disk" ${LOGS_DIR}/storage/disk.log | tail -20
    fi
else
    log_test_result "Disk Storage (ENV)" "FAILED" "Node failed to start with disk storage environment variables"
fi

# Clean up disk node
kill $DISK_PID || true
sleep 2

# Test 1.3: Disk Storage - Testing with command line flags
echo -e "\n${BLUE}Test 1.3: Disk Storage via Command Line Flags${NC}"

# Try with our new flags
./sprawl -bindAddr=127.0.0.1 -bindPort=${STORAGE_BASE_PORT} -httpPort=$((STORAGE_BASE_PORT+70)) \
  -storageType=disk -diskStoragePath=./data/disk_test -memoryMaxSize=104857600 -memoryToDiskAge=60 \
  > ${LOGS_DIR}/storage/disk_flags.log 2>&1 &
DISK_FLAGS_PID=$!

if wait_for_node $((STORAGE_BASE_PORT+70)); then
    if verify_storage_type $((STORAGE_BASE_PORT+70)) "disk"; then
        log_test_result "Disk Storage (Flags)" "PASSED" "Disk storage initialized correctly via command line flags"
    else
        log_test_result "Disk Storage (Flags)" "FAILED" "Failed to initialize disk storage via command line flags"
        echo -e "${YELLOW}Checking logs for disk storage configuration issues...${NC}"
        grep -i "storage\|disk" ${LOGS_DIR}/storage/disk_flags.log | tail -20
    fi
else
    log_test_result "Disk Storage (Flags)" "FAILED" "Node failed to start with disk storage command line flags"
fi

# Clean up disk flags node
kill $DISK_FLAGS_PID || true
sleep 2

# Test 1.4: S3/MinIO Storage Configuration
echo -e "\n${BLUE}Test 1.4: S3/MinIO Storage Configuration${NC}"

# Only run this test if MinIO is running locally, otherwise skip
if nc -z localhost 9000 2>/dev/null; then
    ./sprawl -bindAddr=127.0.0.1 -bindPort=${STORAGE_BASE_PORT} -httpPort=$((STORAGE_BASE_PORT+70)) \
      -storageType=s3 -diskStoragePath=./data/s3_temp \
      -s3Bucket=sprawl-test -s3Endpoint=localhost:9000 \
      -s3AccessKey=minioadmin -s3SecretKey=minioadmin \
      -memoryMaxSize=104857600 -memoryToDiskAge=60 -diskToCloudAge=300 \
      > ${LOGS_DIR}/storage/s3.log 2>&1 &
    S3_PID=$!

    if wait_for_node $((STORAGE_BASE_PORT+70)); then
        if verify_storage_type $((STORAGE_BASE_PORT+70)) "s3"; then
            log_test_result "S3/MinIO Storage" "PASSED" "S3/MinIO storage initialized correctly"
        else
            log_test_result "S3/MinIO Storage" "FAILED" "Failed to initialize S3/MinIO storage"
            echo -e "${YELLOW}Checking logs for S3 storage configuration issues...${NC}"
            grep -i "storage\|s3\|minio" ${LOGS_DIR}/storage/s3.log | tail -20
        fi
    else
        log_test_result "S3/MinIO Storage" "FAILED" "Node failed to start with S3/MinIO storage configuration"
    fi

    # Clean up S3 node
    kill $S3_PID || true
    sleep 2
else
    echo -e "${YELLOW}Skipping S3/MinIO test as MinIO server is not running on port 9000${NC}"
    log_test_result "S3/MinIO Storage" "SKIPPED" "MinIO server not available on port 9000"
fi

###########################################
# PART 2: LARGE-SCALE DEPLOYMENT TESTING
###########################################
echo -e "\n${YELLOW}PART 2: LARGE-SCALE DEPLOYMENT TESTING${NC}"
echo -e "${BLUE}Testing larger cluster sizes and partition distribution${NC}"

# Function to start multiple nodes
start_cluster() {
    local size=$1
    local base_port=$2
    local http_base=$3
    local pids=()
    
    # Start seed node
    echo -e "\n${BLUE}Starting seed node...${NC}"
    ./sprawl -bindAddr=127.0.0.1 -bindPort=${base_port} -httpPort=${http_base} > ${LOGS_DIR}/scale/node0.log 2>&1 &
    pids+=($!)
    
    if ! wait_for_node ${http_base} 30; then
        echo -e "${RED}Seed node failed to start. Aborting cluster launch.${NC}"
        kill ${pids[0]} || true
        return 1
    fi
    
    # Start remaining nodes
    for i in $(seq 1 $((size-1))); do
        echo -e "Starting node $i..."
        ./sprawl -bindAddr=127.0.0.1 -bindPort=$((base_port+i)) -httpPort=$((http_base+i)) -seeds=127.0.0.1:${base_port} > ${LOGS_DIR}/scale/node${i}.log 2>&1 &
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

# Function to test topic distribution
test_topic_distribution() {
    local http_base=$1
    local size=$2
    local topics=$3
    
    # Subscribe to topics from different nodes
    echo "Creating $topics topics across the cluster..."
    for i in $(seq 1 $topics); do
        local node_idx=$((i % size))
        curl -s -X POST -H "Content-Type: application/json" \
             -d "{\"topics\":[\"scale-topic-${i}\"]}" \
             http://localhost:$((http_base+node_idx))/subscribe > /dev/null
    done
    
    # Give time for topic registrations to propagate
    sleep $((topics / 5 + 3))
    
    # Check topic distribution on all nodes
    local total_topics=0
    local node_topics=()
    
    for i in $(seq 0 $((size-1))); do
        local topics_data=$(curl -s http://localhost:$((http_base+i))/topics)
        local node_topic_count=$(echo "$topics_data" | grep -o '"name"' | wc -l)
        node_topics[i]=$node_topic_count
        total_topics=$((total_topics + node_topic_count))
        echo "Node $i has $(printf "%8d" ${node_topics[i]}) topics"
    done
    
    # Check if all topics are accounted for
    if [ "$total_topics" -ge "$topics" ]; then
        echo -e "${GREEN}All topics distributed across the cluster${NC}"
        
        # Calculate standard deviation to check distribution balance
        local sum=0
        local mean=$((total_topics / size))
        
        for i in $(seq 0 $((size-1))); do
            local diff=$((node_topics[i] - mean))
            sum=$((sum + diff*diff))
        done
        
        local variance=$((sum / size))
        echo "Topic distribution variance: $variance"
        
        if [ "$variance" -lt "$((topics / 2))" ]; then
            echo -e "${GREEN}Topics are relatively well-balanced across nodes${NC}"
            return 0
        else
            echo -e "${YELLOW}Topics are not well-balanced across nodes${NC}"
            return 1
        fi
    else
        echo -e "${RED}Some topics are missing: expected at least ${topics}, got ${total_topics}${NC}"
        return 1
    fi
}

# Test 2.1: Medium Cluster (5 nodes)
echo -e "\n${BLUE}Test 2.1: Medium Cluster (5 nodes)${NC}"
if start_cluster 5 ${SCALE_BASE_PORT} $((SCALE_BASE_PORT+70)); then
    if verify_cluster_size $((SCALE_BASE_PORT+70)) 5; then
        log_test_result "Medium Cluster Formation (5 nodes)" "PASSED" "All 5 nodes formed a cluster correctly"
        
        # Test topic distribution
        if test_topic_distribution $((SCALE_BASE_PORT+70)) 5 20; then
            log_test_result "Medium Cluster Topic Distribution" "PASSED" "Topics distributed correctly across 5 nodes"
        else
            log_test_result "Medium Cluster Topic Distribution" "FAILED" "Topics not distributed correctly across 5 nodes"
        fi
    else
        log_test_result "Medium Cluster Formation (5 nodes)" "FAILED" "5-node cluster did not form correctly"
    fi
else
    log_test_result "Medium Cluster Formation (5 nodes)" "FAILED" "Failed to start 5-node cluster"
fi

# Clean up
pkill -f "sprawl" || echo "No sprawl processes to clean up"
sleep 3

# Test 2.2: Large Cluster (10 nodes)
echo -e "\n${BLUE}Test 2.2: Large Cluster (10 nodes)${NC}"
if start_cluster 10 ${SCALE_BASE_PORT} $((SCALE_BASE_PORT+70)); then
    if verify_cluster_size $((SCALE_BASE_PORT+70)) 10; then
        log_test_result "Large Cluster Formation (10 nodes)" "PASSED" "All 10 nodes formed a cluster correctly"
        
        # Test topic distribution
        if test_topic_distribution $((SCALE_BASE_PORT+70)) 10 50; then
            log_test_result "Large Cluster Topic Distribution" "PASSED" "Topics distributed correctly across 10 nodes"
        else
            log_test_result "Large Cluster Topic Distribution" "FAILED" "Topics not distributed correctly across 10 nodes"
        fi
    else
        log_test_result "Large Cluster Formation (10 nodes)" "FAILED" "10-node cluster did not form correctly"
    fi
else
    log_test_result "Large Cluster Formation (10 nodes)" "FAILED" "Failed to start 10-node cluster"
fi

# Clean up
pkill -f "sprawl" || echo "No sprawl processes to clean up"
sleep 3

###########################################
# PART 3: FAILURE RECOVERY TESTING
###########################################
echo -e "\n${YELLOW}PART 3: FAILURE RECOVERY TESTING${NC}"
echo -e "${BLUE}Testing system behavior during node failures and network partitions${NC}"

# Clean up any existing nodes before starting failure tests
pkill -f "sprawl" || echo "No sprawl processes to clean up"
sleep 3

# Test 3.1: Single Node Failure and Recovery
echo -e "\n${BLUE}Test 3.1: Single Node Failure and Recovery${NC}"
echo "Running single node failure test using run_failure_tests.sh..."

# Make sure the script is executable
chmod +x run_failure_tests.sh

# Run the test script
./run_failure_tests.sh
NODE_FAILURE_EXIT_CODE=$?

# Check if the test was successful based on its exit code
if [ $NODE_FAILURE_EXIT_CODE -eq 0 ]; then
    log_test_result "Single Node Failure Recovery" "PASSED" "Node failure was properly detected and handled"
else
    log_test_result "Single Node Failure Recovery" "FAILED" "Issue with node failure recovery, exit code: $NODE_FAILURE_EXIT_CODE"
fi

# Clean up any remaining nodes
pkill -f "sprawl" || echo "No sprawl processes to clean up"
sleep 3

# Test 3.2: Leader Node Failure
echo -e "\n${BLUE}Test 3.2: Leader Node Failure${NC}"
echo "Running leader node failure test using test_leader_failure.sh..."

# Make sure the script is executable
chmod +x test_leader_failure.sh

# Run the test script
./test_leader_failure.sh
LEADER_FAILURE_EXIT_CODE=$?

# Check if the test was successful based on its exit code
if [ $LEADER_FAILURE_EXIT_CODE -eq 0 ]; then
    log_test_result "Leader Node Failure" "PASSED" "Leader failure was properly handled"
else
    log_test_result "Leader Node Failure" "FAILED" "Issue with leader failure recovery, exit code: $LEADER_FAILURE_EXIT_CODE"
fi

# Clean up any remaining nodes
pkill -f "sprawl" || echo "No sprawl processes to clean up"
sleep 3

###########################################
# SUMMARY
###########################################
# Count results
PASSED_COUNT=$(grep -c "PASSED" ${TEST_RESULTS})
FAILED_COUNT=$(grep -c "FAILED" ${TEST_RESULTS})
TOTAL_COUNT=$((PASSED_COUNT + FAILED_COUNT))

echo -e "\n${YELLOW}=== ENHANCED TEST SUMMARY ===${NC}"
echo -e "Total tests: ${TOTAL_COUNT}"
echo -e "${GREEN}Passed: ${PASSED_COUNT}${NC}"
echo -e "${RED}Failed: ${FAILED_COUNT}${NC}"
echo -e "\nDetailed results stored in: ${TEST_RESULTS}"

# Recommend enhancements based on test results
echo -e "\n${YELLOW}=== RECOMMENDATIONS ===${NC}"

if grep -q "Disk Storage.*FAILED" ${TEST_RESULTS}; then
    echo -e "1. ${RED}Implement proper CLI flags for storage configuration:${NC}"
    echo "   - Add -storageType flag (memory, disk, s3)"
    echo "   - Add -storagePath flag for path configuration"
    echo "   - Document all storage options in README.md"
fi

if grep -q "Topic Distribution.*FAILED" ${TEST_RESULTS}; then
    echo -e "2. ${RED}Improve topic distribution and partitioning:${NC}"
    echo "   - Enhance DHT algorithm for more balanced distribution"
    echo "   - Add explicit partition management endpoints"
    echo "   - Implement partition visibility in metrics"
fi

if grep -q "Failure Recovery.*FAILED" ${TEST_RESULTS}; then
    echo -e "3. ${RED}Enhance failure recovery mechanisms:${NC}"
    echo "   - Improve leader election algorithms"
    echo "   - Add automatic state reconciliation after rejoin"
    echo "   - Implement configurable failure detection timeouts"
fi

echo -e "\n${GREEN}Testing completed!${NC}" 