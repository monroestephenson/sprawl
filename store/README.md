# Sprawl Tiered Storage System

The Sprawl Tiered Storage System provides a hierarchical approach to message storage that optimizes for both performance and cost-effectiveness. This document outlines the architecture, configuration, and usage of the storage subsystem.

## Architecture Overview

The storage system consists of three tiers:

1. **Memory Tier** - In-memory storage optimized for high-speed read/write operations
2. **Disk Tier** - RocksDB-based persistent storage for medium-term retention
3. **Cloud Tier** - S3/MinIO-compatible object storage for long-term archival

Messages flow through these tiers based on configurable policies, allowing for optimal trade-offs between access speed and cost.

## Tier Transition Logic

Messages automatically transition between tiers based on the following criteria:

1. **Memory → Disk**
   - Age-based: Messages older than `SPRAWL_STORAGE_MEMORY_TO_DISK_AGE` seconds
   - Size-based: When memory usage exceeds `SPRAWL_STORAGE_MEMORY_MAX_SIZE` bytes
   
2. **Disk → Cloud**
   - Age-based: Messages older than `SPRAWL_STORAGE_DISK_TO_CLOUD_AGE` seconds
   - Size-based: When disk usage exceeds `SPRAWL_STORAGE_DISK_MAX_SIZE` bytes

Messages are automatically retrieved from deeper tiers when requested, providing transparent access regardless of which tier contains the data.

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SPRAWL_STORAGE_MEMORY_MAX_SIZE` | Maximum memory usage in bytes | 104857600 (100 MB) |
| `SPRAWL_STORAGE_MEMORY_TO_DISK_AGE` | Seconds before memory → disk transition | 3600 (1 hour) |
| `SPRAWL_STORAGE_DISK_ENABLED` | Enable disk tier | `false` |
| `SPRAWL_STORAGE_DISK_PATH` | Path to disk storage directory | `/data/rocksdb` |
| `SPRAWL_STORAGE_DISK_MAX_SIZE` | Maximum disk usage in bytes | 1073741824 (1 GB) |
| `SPRAWL_STORAGE_DISK_TO_CLOUD_AGE` | Seconds before disk → cloud transition | 86400 (24 hours) |
| `SPRAWL_STORAGE_CLOUD_ENABLED` | Enable cloud tier | `false` |
| `MINIO_ENDPOINT` | S3/MinIO endpoint URL | `` |
| `MINIO_ACCESS_KEY` | S3/MinIO access key | `` |
| `MINIO_SECRET_KEY` | S3/MinIO secret key | `` |
| `SPRAWL_STORAGE_CLOUD_BUCKET` | S3/MinIO bucket name | `sprawl-messages` |

### Example Configuration

```bash
# Memory tier with 50MB limit and 30-minute age threshold
SPRAWL_STORAGE_MEMORY_MAX_SIZE=52428800
SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=1800

# Enable disk tier with 500MB limit and 12-hour age threshold
SPRAWL_STORAGE_DISK_ENABLED=true
SPRAWL_STORAGE_DISK_PATH=/data/rocksdb
SPRAWL_STORAGE_DISK_MAX_SIZE=524288000
SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=43200

# Enable cloud tier with MinIO
SPRAWL_STORAGE_CLOUD_ENABLED=true
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
SPRAWL_STORAGE_CLOUD_BUCKET=sprawl-messages
```

## Implementation Details

### Memory Tier

The memory tier uses a custom ring buffer implementation optimized for:
- Fast append operations for new messages
- Efficient iteration over messages for a given topic
- Memory usage monitoring with configurable threshold triggers
- Automatic eviction policy for oldest messages when thresholds are reached

### Disk Tier (RocksDB)

The disk tier uses RocksDB, a high-performance embedded key-value store:
- Messages are indexed by topic and timestamp
- Efficient range queries for retrieving messages by topic
- Configurable compaction policies to manage disk usage
- Automatic backup/recovery mechanisms

### Cloud Tier (S3/MinIO)

The cloud tier uses S3-compatible object storage:
- Messages are batched by topic and time period
- Efficient retrieval with parallel downloads
- Transparent decompression for archived data
- Support for lifecycle policies (via S3 bucket policies)

## Monitoring and Management

The storage system exposes several endpoints for monitoring and management:

- `GET /store` - Shows statistics for all tiers
- `GET /store/tiers` - Shows tier configuration
- `POST /store/compact` - Triggers manual compaction

## Performance Considerations

- **Memory Tier**: Provides sub-millisecond latency for both reads and writes
- **Disk Tier**: Provides millisecond-level latency (typically 1-10ms)
- **Cloud Tier**: Provides higher latency (typically 100ms-1s)

The system is designed to keep hot data in faster tiers while automatically moving colder data to slower, more cost-effective tiers.

## Demonstration

A demonstration script is provided to showcase the tiered storage functionality:

```bash
./scripts/demonstrate-tiering.sh
```

This script:
1. Sets up a MinIO container for cloud storage
2. Starts Sprawl nodes with tiered storage enabled
3. Publishes enough messages to trigger tier transitions
4. Shows statistics for each tier after transitions

## API Examples

### Checking Storage Statistics

```bash
curl http://localhost:8080/store | jq
```

Response:
```json
{
  "memory": {
    "enabled": true,
    "capacity_bytes": 104857600,
    "used_bytes": 2048,
    "message_count": 42,
    "topics": ["test-topic"],
    "oldest_message": "2025-03-09T20:00:00Z",
    "newest_message": "2025-03-09T22:02:54Z"
  },
  "disk": {
    "enabled": true,
    "path": "/data/rocksdb",
    "used_bytes": 1048576,
    "message_count": 1024,
    "topics": ["test-topic"],
    "oldest_message": "2025-03-08T20:00:00Z",
    "newest_message": "2025-03-09T20:00:00Z"
  },
  "cloud": {
    "enabled": true,
    "endpoint": "http://minio:9000",
    "bucket": "sprawl-messages",
    "message_count": 10240,
    "oldest_message": "2025-03-01T20:00:00Z",
    "newest_message": "2025-03-08T20:00:00Z"
  }
}
```

### Viewing Tier Configuration

```bash
curl http://localhost:8080/store/tiers | jq
```

Response:
```json
{
  "tiers": ["memory", "disk", "cloud"],
  "policies": {
    "memory_to_disk_threshold_bytes": 104857600,
    "memory_to_disk_age_seconds": 3600,
    "disk_to_cloud_age_seconds": 86400,
    "disk_to_cloud_threshold_bytes": 1073741824
  }
}
```

### Triggering Compaction

```bash
curl -X POST http://localhost:8080/store/compact | jq
```

Response:
```json
{
  "status": "compaction_started",
  "tiers": ["disk"],
  "estimated_completion": "2025-03-09T22:10:00Z"
}
``` 