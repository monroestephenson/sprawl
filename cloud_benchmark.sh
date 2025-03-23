#!/bin/bash
set -e

# Cloud Storage Benchmark Utility
# This script runs the cloud storage benchmark against a local or remote S3-compatible storage

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default parameters
CONCURRENCY=5
DURATION=30
MESSAGE_SIZE=1024
TOPIC_COUNT=5
INCLUDE_READS=true
VERBOSE=true
DELAY=0

# S3 configuration
S3_ENDPOINT=${S3_ENDPOINT:-"localhost:9000"}
S3_BUCKET=${S3_BUCKET:-"sprawl-benchmark"}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-"minioadmin"}
S3_SECRET_KEY=${S3_SECRET_KEY:-"minioadmin"}
S3_REGION=${S3_REGION:-"us-east-1"}

# Display help
function show_help {
    echo -e "${BLUE}Cloud Storage Benchmark Utility${NC}"
    echo "This script benchmarks the Sprawl cloud storage against S3-compatible storage"
    echo ""
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -c, --concurrency N    Number of concurrent operations (default: $CONCURRENCY)"
    echo "  -d, --duration N       Duration in seconds (default: $DURATION)"
    echo "  -s, --size N           Message size in bytes (default: $MESSAGE_SIZE)"
    echo "  -t, --topics N         Number of topics to use (default: $TOPIC_COUNT)"
    echo "  -r, --no-reads         Disable read operations in benchmark"
    echo "  -q, --quiet            Disable verbose output"
    echo "  --delay N              Delay between operations in ms (default: $DELAY)"
    echo "  -e, --endpoint URL     S3 endpoint (default: $S3_ENDPOINT)"
    echo "  -b, --bucket NAME      S3 bucket name (default: $S3_BUCKET)"
    echo "  -a, --access-key KEY   S3 access key (default: $S3_ACCESS_KEY)"
    echo "  -k, --secret-key KEY   S3 secret key (default: $S3_SECRET_KEY)"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Environment variables:"
    echo "  S3_ENDPOINT            S3 endpoint URL"
    echo "  S3_BUCKET              S3 bucket name"
    echo "  S3_ACCESS_KEY          S3 access key"
    echo "  S3_SECRET_KEY          S3 secret key"
    echo "  S3_REGION              S3 region (default: us-east-1)"
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -c|--concurrency)
            CONCURRENCY="$2"
            shift 2
            ;;
        -d|--duration)
            DURATION="$2"
            shift 2
            ;;
        -s|--size)
            MESSAGE_SIZE="$2"
            shift 2
            ;;
        -t|--topics)
            TOPIC_COUNT="$2"
            shift 2
            ;;
        -r|--no-reads)
            INCLUDE_READS=false
            shift
            ;;
        -q|--quiet)
            VERBOSE=false
            shift
            ;;
        --delay)
            DELAY="$2"
            shift 2
            ;;
        -e|--endpoint)
            S3_ENDPOINT="$2"
            shift 2
            ;;
        -b|--bucket)
            S3_BUCKET="$2"
            shift 2
            ;;
        -a|--access-key)
            S3_ACCESS_KEY="$2"
            shift 2
            ;;
        -k|--secret-key)
            S3_SECRET_KEY="$2"
            shift 2
            ;;
        -h|--help)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            show_help
            ;;
    esac
done

# Verify the benchmark tool is built
if [ ! -f "./sprawlbench" ]; then
    echo -e "${YELLOW}Building benchmark tool...${NC}"
    go build -o sprawlbench cmd/benchmark/main.go
fi

# Check if MinIO is running
if [[ "$S3_ENDPOINT" == *"localhost"* ]] || [[ "$S3_ENDPOINT" == *"127.0.0.1"* ]]; then
    echo -e "${BLUE}Checking if MinIO is running at $S3_ENDPOINT...${NC}"
    if ! nc -z $(echo $S3_ENDPOINT | cut -d: -f1) $(echo $S3_ENDPOINT | cut -d: -f2) 2>/dev/null; then
        echo -e "${YELLOW}MinIO doesn't seem to be running. Starting a local instance...${NC}"
        docker run -d --name sprawl-minio \
            -p 9000:9000 -p 9001:9001 \
            -e "MINIO_ROOT_USER=$S3_ACCESS_KEY" \
            -e "MINIO_ROOT_PASSWORD=$S3_SECRET_KEY" \
            minio/minio server /data --console-address ":9001"
        
        echo -e "${GREEN}MinIO started on port 9000 (API) and 9001 (Console)${NC}"
        
        # Give it a moment to initialize
        sleep 3
        
        # Create the bucket
        echo -e "${BLUE}Creating bucket: $S3_BUCKET${NC}"
        AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$S3_SECRET_KEY \
        aws --endpoint-url http://$S3_ENDPOINT s3 mb s3://$S3_BUCKET || \
        echo -e "${YELLOW}Bucket creation failed. It might already exist or aws CLI is not installed.${NC}"
    fi
fi

# Run the benchmark
echo -e "${BLUE}Running cloud storage benchmark with the following parameters:${NC}"
echo "  Concurrency:      $CONCURRENCY"
echo "  Duration:         $DURATION seconds"
echo "  Message size:     $MESSAGE_SIZE bytes"
echo "  Topics:           $TOPIC_COUNT"
echo "  Include reads:    $INCLUDE_READS"
echo "  Endpoint:         $S3_ENDPOINT"
echo "  Bucket:           $S3_BUCKET"
echo ""

# Set delay parameter if specified
DELAY_PARAM=""
if [ "$DELAY" -gt 0 ]; then
    DELAY_PARAM="--delay $DELAY"
fi

# Run the actual benchmark using our benchmark tool
export S3_ENDPOINT=$S3_ENDPOINT
export S3_BUCKET=$S3_BUCKET
export S3_ACCESS_KEY=$S3_ACCESS_KEY
export S3_SECRET_KEY=$S3_SECRET_KEY
export S3_REGION=$S3_REGION

./sprawlbench cloud \
    --concurrency $CONCURRENCY \
    --duration $DURATION \
    --size $MESSAGE_SIZE \
    --topics $TOPIC_COUNT \
    $([[ "$INCLUDE_READS" == "true" ]] && echo "--include-reads") \
    $([[ "$VERBOSE" == "true" ]] && echo "--verbose") \
    $DELAY_PARAM

echo -e "${GREEN}Benchmark completed.${NC}" 