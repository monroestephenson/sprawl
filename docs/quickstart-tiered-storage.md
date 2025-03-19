# Quickstart Guide: Testing Tiered Storage

This guide will help you set up and test Sprawl's tiered storage functionality.

## Prerequisites

- Docker installed and running
- Go 1.21 or later
- 1GB of free disk space

## Step 1: Set Up MinIO (Cloud Storage Tier)

Start a MinIO container to act as the cloud storage tier:

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

Access the MinIO console at http://localhost:9001 (login with minioadmin/minioadmin).

## Step 2: Build Sprawl with Tiered Storage Support

Compile Sprawl with the necessary flags to enable all storage tiers:

```bash
go build -tags="rocksdb" -o sprawl ./cmd/sprawl
go build -o sprawlctl ./cmd/sprawlctl
```

## Step 3: Configure and Start Sprawl

Create a directory structure for the tiers:

```bash
mkdir -p data/memory data/disk
```

Start Sprawl with all tiers enabled:

```bash
MINIO_ENDPOINT=http://localhost:9000 \
MINIO_ACCESS_KEY=minioadmin \
MINIO_SECRET_KEY=minioadmin \
SPRAWL_STORAGE_MEMORY_MAX_SIZE=10485760 \
SPRAWL_STORAGE_DISK_ENABLED=true \
SPRAWL_STORAGE_DISK_PATH=./data/disk \
SPRAWL_STORAGE_CLOUD_ENABLED=true \
SPRAWL_STORAGE_CLOUD_BUCKET=sprawl-messages \
SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=60 \
SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=120 \
./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080
```

> **Note**: For demonstration purposes, we've set short transition times (60s for memory→disk, 120s for disk→cloud). In production, you'd use longer values like 3600s (1h) and 86400s (24h).

## Step 4: Test the API Endpoints

Verify the configuration is correct:

```bash
# Check storage tier configuration
curl http://localhost:8080/store/tiers | jq
```

The response should show three tiers (memory, disk, cloud) and their transition policies.

## Step 5: Generate Test Data

Subscribe to a test topic:

```bash
./sprawlctl -n http://localhost:8080 subscribe -t test-topic
```

Publish some messages:

```bash
# Generate 5MB of test data (50 messages of ~100KB each)
for i in {1..50}; do
  # Generate random payload (~100KB)
  PAYLOAD=$(dd if=/dev/urandom bs=102400 count=1 2>/dev/null | base64)
  ./sprawlctl -n http://localhost:8080 publish -t test-topic -p "$PAYLOAD"
  echo "Published message $i"
done
```

## Step 6: Monitor Tier Transitions

Check the storage statistics immediately:

```bash
curl http://localhost:8080/store | jq
```

The response should show messages in the memory tier.

Wait for at least 1 minute (memory→disk transition time), then check again:

```bash
curl http://localhost:8080/store | jq
```

You should see some messages moved to the disk tier.

Wait for at least 2 more minutes (disk→cloud transition time), then check again:

```bash
curl http://localhost:8080/store | jq
```

Now you should see some messages in the cloud tier.

## Step 7: Verify Cloud Storage

You can access the MinIO console at http://localhost:9001 to verify that the data has been stored in the cloud tier:

1. Login with minioadmin/minioadmin
2. Navigate to the Buckets section
3. Click on the "sprawl-messages" bucket
4. You should see objects corresponding to archived messages

## Step 8: Test Message Retrieval

To verify that messages are still accessible regardless of their storage tier:

```bash
# Publish a message to get its ID
MESSAGE_ID=$(./sprawlctl -n http://localhost:8080 publish -t test-retrieval -p "Test message" | grep -o "ID: [^ ]*" | cut -d " " -f 2)

# Wait for the message to transition through tiers (3+ minutes)
sleep 180

# Retrieve the message
curl -s "http://localhost:8080/messages/$MESSAGE_ID" | jq
```

The system should transparently retrieve the message from whichever tier it's stored in.

## Step 9: Trigger Manual Compaction

You can manually trigger compaction of the storage tiers:

```bash
curl -X POST http://localhost:8080/store/compact | jq
```

This is useful for testing and can be scheduled in production environments.

## Step 10: Clean Up

When finished testing, stop and remove the MinIO container:

```bash
docker stop minio
docker rm minio
```

Remove the data directories:

```bash
rm -rf data/
```

## Automated Testing Script

For your convenience, we've included a script that automates the testing process:

```bash
./scripts/demonstrate-tiering.sh
```

This script:
1. Sets up MinIO
2. Starts Sprawl with tiered storage
3. Generates test data
4. Monitors tier transitions
5. Cleans up resources when done

## Next Steps

- Read [store/README.md](../store/README.md) for more details on the tiered storage architecture
- Adjust the tier transition parameters for your production workload
- Set up monitoring for tier usage and transitions 