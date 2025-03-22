#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

# Variables
TEST_TIMEOUT=30 # timeout in seconds for individual tests
TIMESTAMP=$(date +%s)
LOGS_DIR="test_logs_${TIMESTAMP}"
COVERAGE_DIR="${LOGS_DIR}/coverage"
TEST_RESULTS="${LOGS_DIR}/test_results.txt"

# Base ports for different test phases
PHASE3_BASE_PORT=7950
PHASE4_BASE_PORT=7960
PHASE5_BASE_PORT=7970
PHASE6_BASE_PORT=7980
PHASE7_BASE_PORT=7990

# HTTP ports for different phases
PHASE3_HTTP_PORT=9085
PHASE4_HTTP_PORT=9090
PHASE5_HTTP_PORT=9100
PHASE6_HTTP_PORT=9110
PHASE7_HTTP_PORT=9120

# Health ports for different phases
PHASE3_HEALTH_PORT=9081
PHASE4_HEALTH_PORT=9082
PHASE5_HEALTH_PORT=9083
PHASE6_HEALTH_PORT=9084
PHASE7_HEALTH_PORT=9086

# Detect platform and set timeout command
TIMEOUT_CMD="timeout"
if [[ "$(uname)" == "Darwin" ]]; then
    if command -v gtimeout &> /dev/null; then
        TIMEOUT_CMD="gtimeout"
    else
        echo "gtimeout not found. On macOS, install it with 'brew install coreutils'"
        # Fall back to a simple implementation without timeout
        TIMEOUT_CMD="sh -c"
    fi
fi

# Clean up any existing nodes
echo -e "${YELLOW}Cleaning up any existing nodes...${NC}"
pkill -f "sprawl" || true
sleep 2
pkill -9 -f "sprawl" || true
mkdir -p ${LOGS_DIR}
mkdir -p ${COVERAGE_DIR}
mkdir -p data/test/disk

# Initialize test results file
echo "Sprawl Comprehensive Test Results - $(date)" > ${TEST_RESULTS}
echo "=======================================" >> ${TEST_RESULTS}

# Function to run a test and report its result
run_test() {
  local test_name=$1
  local test_cmd=$2
  local timeout_seconds=${3:-$TEST_TIMEOUT}
  
  echo -e "${YELLOW}Running test: ${test_name}${NC}"
  echo "Test: ${test_name}" >> ${TEST_RESULTS}
  echo "Command: ${test_cmd}" >> ${TEST_RESULTS}
  
  # Run the test with timeout
  local output_file="${LOGS_DIR}/${test_name// /_}.log"
  
  # Execute the test command, capturing output and exit status
  set +e
  if [[ "$TIMEOUT_CMD" == "sh -c" ]]; then
    # Simple execution without timeout for macOS without gtimeout
    sh -c "${test_cmd}" > ${output_file} 2>&1
    local exit_status=$?
  else
    # Use timeout command when available
    $TIMEOUT_CMD ${timeout_seconds}s bash -c "${test_cmd}" > ${output_file} 2>&1
    local exit_status=$?
  fi
  set -e
  
  if [ ${exit_status} -eq 0 ]; then
    echo -e "${GREEN}✓ ${test_name} - PASSED${NC}"
    echo "Status: PASSED" >> ${TEST_RESULTS}
  elif [ ${exit_status} -eq 124 ]; then
    echo -e "${RED}✗ ${test_name} - FAILED (TIMEOUT)${NC}"
    echo "Status: FAILED (TIMEOUT)" >> ${TEST_RESULTS}
  else
    echo -e "${RED}✗ ${test_name} - FAILED (code: ${exit_status})${NC}"
    echo "Status: FAILED (code: ${exit_status})" >> ${TEST_RESULTS}
  fi
  
  echo "Log: ${output_file}" >> ${TEST_RESULTS}
  echo "----------------------------------------" >> ${TEST_RESULTS}
}

# PHASE 1: Build all components
echo -e "${YELLOW}PHASE 1: Building all components${NC}"
run_test "Build sprawl" "go build -o sprawl ./cmd/sprawl/main.go" 60
run_test "Build sprawlctl" "go build -o sprawlctl ./cmd/sprawlctl/main.go" 60

# PHASE 2: Unit Tests
echo -e "${YELLOW}PHASE 2: Running unit tests with coverage${NC}"

# Run unit tests for each package with coverage
for pkg in ./store/... ./node/... ./ai/...; do
  pkg_name=$(echo $pkg | sed 's/\.\///g' | sed 's/\/\.\.\.//g')
  
  # Skip cloud storage tests that require AWS credentials
  if [ "$pkg_name" = "store" ]; then
    run_test "Unit tests for ${pkg_name}" "go test -timeout 60s -cover -coverprofile=${COVERAGE_DIR}/${pkg_name//\//_}.out ${pkg} -short" 120
  else
    run_test "Unit tests for ${pkg_name}" "go test -timeout 60s -cover -coverprofile=${COVERAGE_DIR}/${pkg_name//\//_}.out ${pkg}" 120
  fi
done

# Generate overall coverage report
run_test "Generate coverage report" "go tool cover -html=${COVERAGE_DIR}/store_....out -o ${COVERAGE_DIR}/coverage.html" 30

# PHASE 3: Single node functionality tests
echo -e "${YELLOW}PHASE 3: Single node functionality tests${NC}"

# Start a node for testing basic functionality
echo -e "${YELLOW}Starting node...${NC}"
export SPRAWL_HEALTH_PORT=${PHASE3_HEALTH_PORT}
./sprawl -bindAddr=127.0.0.1 -bindPort=${PHASE3_BASE_PORT} -httpPort=${PHASE3_HTTP_PORT} -advertiseAddr=127.0.0.1 -seeds= > ./phase3_test1.log 2>&1 &
NODE_PID=$!

# Wait for node to start
echo "Waiting for node to start..."
for i in {1..30}; do
  if curl -s http://localhost:${PHASE3_HEALTH_PORT}/health > /dev/null; then
    echo "Node started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start node${NC}"
    exit 1
  fi
  sleep 1
done

# Test store status
echo -e "${YELLOW}Testing store status...${NC}"
if curl -v -s http://localhost:${PHASE3_HTTP_PORT}/store | grep -q "memory"; then
  echo -e "${GREEN}Store status test passed${NC}"
else
  echo -e "${RED}Store status test failed${NC}"
  kill $NODE_PID
  exit 1
fi

# Test subscribe to topic
echo -e "${YELLOW}Testing subscribe to topic...${NC}"
if curl -v -s -X POST http://localhost:${PHASE3_HTTP_PORT}/subscribe -H 'Content-Type: application/json' -d '{"topics": ["test-topic"], "id": "test-client"}' | grep -q "subscribed"; then
  echo -e "${GREEN}Subscribe to topic test passed${NC}"
else
  echo -e "${RED}Subscribe to topic test failed${NC}"
  kill $NODE_PID
  exit 1
fi

# Test publish message
echo -e "${YELLOW}Testing publish message...${NC}"
if curl -v -s -X POST http://localhost:${PHASE3_HTTP_PORT}/publish -H 'Content-Type: application/json' -d '{"topic": "test-topic", "message": "Hello, World!"}'; then
  echo -e "${GREEN}Publish message test passed${NC}"
else
  echo -e "${RED}Publish message test failed${NC}"
  kill $NODE_PID
  exit 1
fi

# Test prediction
echo -e "${YELLOW}Testing prediction...${NC}"
if curl -v -s -X POST http://localhost:${PHASE3_HTTP_PORT}/predict -H 'Content-Type: application/json' -d '{"data":[1,2,3,4,5],"model":"linear"}'; then
  echo -e "${GREEN}Prediction test passed${NC}"
else
  echo -e "${RED}Prediction test failed${NC}"
  kill $NODE_PID
  exit 1
fi

# Test AI functionality
run_test "CPU predictions" "curl -s http://localhost:${PHASE3_HTTP_PORT}/ai/predictions?resource=cpu | grep -v 'Not Found'"
run_test "Memory predictions" "curl -s http://localhost:${PHASE3_HTTP_PORT}/ai/predictions?resource=memory | grep -v 'Not Found'"
run_test "Message rate predictions" "curl -s http://localhost:${PHASE3_HTTP_PORT}/ai/predictions?resource=message_rate | grep -v 'Not Found'"
run_test "Network predictions" "curl -s http://localhost:${PHASE3_HTTP_PORT}/ai/predictions?resource=network | grep -v 'Not Found'"
run_test "AI Status" "curl -s http://localhost:${PHASE3_HTTP_PORT}/ai/status | grep 'enabled'"
run_test "AI Anomalies" "curl -s http://localhost:${PHASE3_HTTP_PORT}/ai/anomalies"

# Clean up node
pkill -f sprawl || true
sleep 2

# PHASE 4: Multi-node cluster tests
echo -e "${YELLOW}PHASE 4: Multi-node cluster tests${NC}"

# Clean up existing nodes before starting multi-node test
pkill -f "sprawl" || true
sleep 2

# Start three nodes with unique ports
run_test "Start node 1 (seed)" "SPRAWL_HEALTH_PORT=${PHASE4_HEALTH_PORT} ./sprawl -bindAddr=127.0.0.1 -bindPort=${PHASE4_BASE_PORT} -httpPort=${PHASE4_HTTP_PORT} -seeds= > ${LOGS_DIR}/node1.log 2>&1 &" 10
sleep 5

# Wait for node 1 to start
echo "Waiting for node 1 to start..."
for i in {1..30}; do
  if curl -s http://localhost:${PHASE4_HEALTH_PORT}/health > /dev/null; then
    echo "Node 1 started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start node 1${NC}"
    exit 1
  fi
  sleep 1
done

run_test "Start node 2" "SPRAWL_HEALTH_PORT=$((${PHASE4_HEALTH_PORT}+1)) ./sprawl -bindAddr=127.0.0.1 -bindPort=$((${PHASE4_BASE_PORT}+1)) -httpPort=$((${PHASE4_HTTP_PORT}+1)) -seeds=127.0.0.1:${PHASE4_BASE_PORT} > ${LOGS_DIR}/node2.log 2>&1 &" 10
sleep 5

# Wait for node 2 to start
echo "Waiting for node 2 to start..."
for i in {1..30}; do
  if curl -s http://localhost:$((${PHASE4_HEALTH_PORT}+1))/health > /dev/null; then
    echo "Node 2 started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start node 2${NC}"
    exit 1
  fi
  sleep 1
done

run_test "Start node 3" "SPRAWL_HEALTH_PORT=$((${PHASE4_HEALTH_PORT}+2)) ./sprawl -bindAddr=127.0.0.1 -bindPort=$((${PHASE4_BASE_PORT}+2)) -httpPort=$((${PHASE4_HTTP_PORT}+2)) -seeds=127.0.0.1:${PHASE4_BASE_PORT} > ${LOGS_DIR}/node3.log 2>&1 &" 10
sleep 5

# Wait for cluster to form (15 seconds)
echo "Waiting for cluster to stabilize..."
sleep 15

# Test cross-node communication
run_test "Subscribe on node 1" "curl -X POST http://localhost:${PHASE4_HTTP_PORT}/subscribe -H 'Content-Type: application/json' -d '{\"topics\": [\"cross-node-test\"], \"id\": \"client1\"}' | grep 'subscribed'"
run_test "Publish from node 2" "curl -X POST http://localhost:$((${PHASE4_HTTP_PORT}+1))/publish -H 'Content-Type: application/json' -d '{\"topic\": \"cross-node-test\", \"message\": \"Cross-node message\"}'"

# Wait for message propagation across nodes
echo "Waiting for messages to propagate across the cluster..."
sleep 15

# Check store content on node 1
run_test "Check store on node 1" "curl -s http://localhost:${PHASE4_HTTP_PORT}/store"

# PHASE 5: Tiered storage tests
echo -e "${YELLOW}PHASE 5: Tiered storage tests${NC}"

# Clean up existing nodes
pkill -f sprawl || true
sleep 2

# Start a node with disk storage enabled
run_test "Start node with disk storage" "SPRAWL_HEALTH_PORT=${PHASE5_HEALTH_PORT} SPRAWL_STORAGE_DISK_ENABLED=true SPRAWL_STORAGE_DISK_PATH=./data/test/disk ./sprawl -bindAddr=127.0.0.1 -bindPort=${PHASE5_BASE_PORT} -httpPort=${PHASE5_HTTP_PORT} -seeds= > ${LOGS_DIR}/node_disk.log 2>&1 &" 10
sleep 5

# Wait for node with disk storage to start
echo "Waiting for node with disk storage to start..."
for i in {1..30}; do
  if curl -s http://localhost:${PHASE5_HEALTH_PORT}/health > /dev/null; then
    echo "Node started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start node with disk storage${NC}"
    exit 1
  fi
  sleep 1
done

# Test disk storage
run_test "Check disk storage enabled" "curl -s http://localhost:${PHASE5_HTTP_PORT}/store | grep -i 'disk.*enabled.*true'"
run_test "Subscribe to tiered topic" "curl -X POST http://localhost:${PHASE5_HTTP_PORT}/subscribe -H 'Content-Type: application/json' -d '{\"topics\": [\"tiered-topic\"], \"id\": \"tiered-client\"}' | grep 'subscribed'"

# Publish multiple messages to trigger tiering
echo "Publishing messages to trigger memory pressure..."
for i in {1..20}; do
  message="Tiered message ${i} with enough data to create larger payloads and trigger memory pressure mechanisms in the tiered storage system"
  curl -s -X POST http://localhost:${PHASE5_HTTP_PORT}/publish -H 'Content-Type: application/json' -d "{\"topic\": \"tiered-topic\", \"message\": \"${message}\"}" > /dev/null
done

# Wait for tiering to occur
sleep 10

# Check storage status after publishing
run_test "Check stored messages" "curl -s http://localhost:${PHASE5_HTTP_PORT}/store | grep 'message_count'"

# PHASE 6: Performance tests
echo -e "${YELLOW}PHASE 6: Performance tests${NC}"

# Clean up existing nodes
pkill -f sprawl || true
sleep 2

# Start a node for performance testing
run_test "Start performance test node" "SPRAWL_HEALTH_PORT=${PHASE6_HEALTH_PORT} ./sprawl -bindAddr=127.0.0.1 -bindPort=${PHASE6_BASE_PORT} -httpPort=${PHASE6_HTTP_PORT} -seeds= > ${LOGS_DIR}/perf_node.log 2>&1 &" 10
sleep 5

# Wait for performance test node to start
echo "Waiting for performance test node to start..."
for i in {1..30}; do
  if curl -s http://localhost:${PHASE6_HEALTH_PORT}/health > /dev/null; then
    echo "Performance test node started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start performance test node${NC}"
    exit 1
  fi
  sleep 1
done

# Subscribe to performance test topic
run_test "Subscribe to performance topic" "curl -X POST http://localhost:${PHASE6_HTTP_PORT}/subscribe -H 'Content-Type: application/json' -d '{\"topics\": [\"perf-topic\"], \"id\": \"perf-client\"}' | grep 'subscribed'"

# Run publish performance test (100 messages)
echo "Running publish performance test..."
start_time=$(date +%s.%N)

for i in {1..100}; do
  curl -s -X POST http://localhost:${PHASE6_HTTP_PORT}/publish -H 'Content-Type: application/json' -d "{\"topic\": \"perf-topic\", \"message\": \"Performance test message ${i}\"}" > /dev/null
done

end_time=$(date +%s.%N)
duration=$(echo "$end_time - $start_time" | bc)
messages_per_second=$(echo "100 / $duration" | bc -l | xargs printf "%.2f")

echo "Published 100 messages in $duration seconds ($messages_per_second messages/sec)"
echo "Publish performance: $messages_per_second messages/sec" >> ${TEST_RESULTS}

# Clean up all nodes
pkill -f sprawl || true
sleep 2

# PHASE 7: Node resilience tests 
echo -e "${YELLOW}PHASE 7: Node resilience tests${NC}"

# Clean up existing nodes
pkill -f sprawl || true
sleep 2

# Start two nodes for resilience testing
run_test "Start resilience node 1" "SPRAWL_HEALTH_PORT=${PHASE7_HEALTH_PORT} ./sprawl -bindAddr=127.0.0.1 -bindPort=${PHASE7_BASE_PORT} -httpPort=${PHASE7_HTTP_PORT} -seeds= > ${LOGS_DIR}/resilience_node1.log 2>&1 &" 10
sleep 5

# Wait for resilience node 1 to start
echo "Waiting for resilience node 1 to start..."
for i in {1..30}; do
  if curl -s http://localhost:${PHASE7_HEALTH_PORT}/health > /dev/null; then
    echo "Resilience node 1 started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start resilience node 1${NC}"
    exit 1
  fi
  sleep 1
done

run_test "Start resilience node 2" "SPRAWL_HEALTH_PORT=$((${PHASE7_HEALTH_PORT}+1)) ./sprawl -bindAddr=127.0.0.1 -bindPort=$((${PHASE7_BASE_PORT}+1)) -httpPort=$((${PHASE7_HTTP_PORT}+1)) -seeds=127.0.0.1:${PHASE7_BASE_PORT} > ${LOGS_DIR}/resilience_node2.log 2>&1 &" 10
sleep 5

# Wait for resilience node 2 to start
echo "Waiting for resilience node 2 to start..."
for i in {1..30}; do
  if curl -s http://localhost:$((${PHASE7_HEALTH_PORT}+1))/health > /dev/null; then
    echo "Resilience node 2 started successfully"
    # Add longer wait for HTTP server to fully initialize
    echo "Waiting additional time for HTTP server to initialize..."
    sleep 30
    break
  fi
  if [ $i -eq 30 ]; then
    echo -e "${RED}Failed to start resilience node 2${NC}"
    exit 1
  fi
  sleep 1
done

# Wait for cluster to form
sleep 15

# Subscribe to resilience test topic on both nodes
run_test "Subscribe to resilience topic node 1" "curl -X POST http://localhost:${PHASE7_HTTP_PORT}/subscribe -H 'Content-Type: application/json' -d '{\"topics\": [\"resilience-topic\"], \"id\": \"resilience-client1\"}' | grep 'subscribed'"
run_test "Subscribe to resilience topic node 2" "curl -X POST http://localhost:$((${PHASE7_HTTP_PORT}+1))/subscribe -H 'Content-Type: application/json' -d '{\"topics\": [\"resilience-topic\"], \"id\": \"resilience-client2\"}' | grep 'subscribed'"

# Publish to resilience topic
run_test "Publish to resilience topic" "curl -X POST http://localhost:${PHASE7_HTTP_PORT}/publish -H 'Content-Type: application/json' -d '{\"topic\": \"resilience-topic\", \"message\": \"Before node failure\"}'"

# Kill node 1
echo "Killing node 1 to test resilience..."
pkill -f "sprawl.*-bindPort=${PHASE7_BASE_PORT}" || true
sleep 5

# Publish another message to node 2
run_test "Publish to resilience topic after node failure" "curl -X POST http://localhost:$((${PHASE7_HTTP_PORT}+1))/publish -H 'Content-Type: application/json' -d '{\"topic\": \"resilience-topic\", \"message\": \"After node failure\"}'"

# Clean up all nodes
pkill -f sprawl || true
sleep 2

# PHASE 8: Test result summary
echo -e "${YELLOW}PHASE 8: Test results summary${NC}"

# Count passed and failed tests
passed_tests=$(grep -c "Status: PASSED" ${TEST_RESULTS})
failed_tests=$(grep -c "Status: FAILED" ${TEST_RESULTS})
total_tests=$((passed_tests + failed_tests))

# Display summary
echo -e "${YELLOW}=======================================${NC}"
echo -e "${YELLOW}           TEST SUMMARY               ${NC}"
echo -e "${YELLOW}=======================================${NC}"
echo -e "Total tests run: ${total_tests}"
echo -e "Tests passed:    ${GREEN}${passed_tests}${NC}"
echo -e "Tests failed:    ${RED}${failed_tests}${NC}"
echo -e "${YELLOW}=======================================${NC}"

if [ ${failed_tests} -eq 0 ]; then
  echo -e "${GREEN}All tests passed!${NC}"
else
  echo -e "${RED}Some tests failed. Check the logs for details.${NC}"
  
  # Show failed tests
  echo -e "${YELLOW}Failed tests:${NC}"
  grep -B 1 -A 1 "Status: FAILED" ${TEST_RESULTS}
fi

# Add summary to test results
echo -e "\n=======================================" >> ${TEST_RESULTS}
echo "TEST SUMMARY" >> ${TEST_RESULTS}
echo "=======================================" >> ${TEST_RESULTS}
echo "Total tests run: ${total_tests}" >> ${TEST_RESULTS}
echo "Tests passed:    ${passed_tests}" >> ${TEST_RESULTS}
echo "Tests failed:    ${failed_tests}" >> ${TEST_RESULTS}

echo -e "\nTest logs and results saved in: ${LOGS_DIR}"
echo -e "${YELLOW}Testing completed!${NC}" 