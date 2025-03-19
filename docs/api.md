# Sprawl API Documentation

## API Overview

Sprawl exposes a RESTful HTTP API for all publishing, subscription, and administrative operations. This document provides a comprehensive reference for all available endpoints.

## Core Endpoints

### Publish Messages

**Endpoint:** `POST /publish`

Publishes a message to a specified topic.

**Request:**
```json
{
  "topic": "my-topic",
  "payload": "Message content",
  "ttl": 3
}
```

**Response:**
```json
{
  "id": "736cf648-4584-47e1-b593-45cfa0ccd559",
  "status": "published"
}
```

**Status Codes:**
- `200 OK`: Message was successfully published
- `400 Bad Request`: Invalid request format
- `503 Service Unavailable`: Node is under heavy load

### Subscribe to Topics

**Endpoint:** `POST /subscribe`

Subscribes to one or more topics.

**Request:**
```json
{
  "topics": ["test-topic"]
}
```

**Response:**
```json
{
  "id": "510dabc5-b73f-4d1a-a256-cc67e0dbcfe3",
  "node_id": "7dc64994-8d23-44d1-a0b8-e5a794c01d55",
  "status": "subscribed",
  "topics": ["test-topic"]
}
```

**Status Codes:**
- `200 OK`: Successfully subscribed
- `400 Bad Request`: Invalid request format

### Health Check

**Endpoint:** `GET /health`

Provides the health status of the node.

**Response:**
```json
{
  "status": "ok",
  "uptime": "3h21m15s"
}
```

**Status Codes:**
- `200 OK`: Node is healthy
- `503 Service Unavailable`: Node is unhealthy

### Node Status

**Endpoint:** `GET /status`

Returns the current status and configuration of the node.

**Response:**
```json
{
  "address": "10.244.0.236",
  "cluster_members": [
    "07077c69-0c0a-4c36-9271-0aa70daf2212",
    "9d1bb80d-31ae-4a47-a0e6-eb05211f8a80",
    "7dc64994-8d23-44d1-a0b8-e5a794c01d55"
  ],
  "http_port": 8080,
  "node_id": "7dc64994-8d23-44d1-a0b8-e5a794c01d55"
}
```

**Status Codes:**
- `200 OK`: Status retrieved successfully

### Metrics

**Endpoint:** `GET /metrics`

Returns the current metrics for the node.

**Response:**
```json
{
  "cluster": ["node1-id", "node2-id", "node3-id"],
  "node_id": "node1-id",
  "router": {
    "avg_latency_ms": 0,
    "messages_routed": 1,
    "messages_sent": 0,
    "route_cache_hits": 0
  },
  "storage": {
    "bytes_stored": 0,
    "last_write_time": "2025-03-09T22:02:54.975184932Z",
    "messages_stored": 1,
    "storage_type": "memory",
    "topics": ["test-topic"]
  },
  "system": {
    "avg_latency_ms": 0,
    "cpu_usage": 0,
    "dht_lookups": 0,
    "goroutines": 22,
    "memory_usage": 0.28331025138341814,
    "messages_received": 1,
    "messages_sent": 0
  }
}
```

**Status Codes:**
- `200 OK`: Metrics retrieved successfully

## New Storage Management Endpoints

### Storage Statistics

**Endpoint:** `GET /store`

Returns detailed information about the storage subsystem.

**Response:**
```json
{
  "memory": {
    "capacity_bytes": 104857600,
    "used_bytes": 2048,
    "message_count": 42,
    "topics": ["topic1", "topic2"],
    "oldest_message": "2025-03-09T20:00:00Z",
    "newest_message": "2025-03-09T22:02:54Z"
  },
  "disk": {
    "enabled": true,
    "path": "/data/rocksdb",
    "used_bytes": 1048576,
    "message_count": 1024,
    "topics": ["topic1", "topic2", "topic3"],
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

**Status Codes:**
- `200 OK`: Storage stats retrieved successfully

### Storage Tier Management

**Endpoint:** `GET /store/tiers`

Returns information about storage tier configurations.

**Response:**
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

**Status Codes:**
- `200 OK`: Tier information retrieved successfully

### Storage Maintenance

**Endpoint:** `POST /store/compact`

Triggers a manual compaction of the storage layers.

**Response:**
```json
{
  "status": "compaction_started",
  "tiers": ["disk"],
  "estimated_completion": "2025-03-09T22:10:00Z"
}
```

**Status Codes:**
- `200 OK`: Compaction started successfully
- `409 Conflict`: Compaction already in progress

## DHT Management Endpoints

### DHT Status

**Endpoint:** `GET /dht`

Returns the current state of the Distributed Hash Table.

**Response:**
```json
{
  "node_count": 3,
  "topic_count": 2,
  "topics": {
    "test-topic": ["node1-id", "node2-id"],
    "another-topic": ["node3-id"]
  },
  "finger_table": {
    "0": ["node1-id"],
    "1": ["node2-id"],
    "2": ["node3-id"]
  }
}
```

**Status Codes:**
- `200 OK`: DHT status retrieved successfully

### Topic Management

**Endpoint:** `GET /topics`

Lists all topics in the system.

**Response:**
```json
{
  "topics": [
    {
      "name": "test-topic",
      "message_count": 42,
      "subscribers": 1,
      "nodes": ["node1-id", "node2-id"]
    },
    {
      "name": "another-topic",
      "message_count": 17,
      "subscribers": 0,
      "nodes": ["node3-id"]
    }
  ]
}
```

**Status Codes:**
- `200 OK`: Topics retrieved successfully

### Topic Details

**Endpoint:** `GET /topics/{topic}`

Returns detailed information about a specific topic.

**Response:**
```json
{
  "name": "test-topic",
  "message_count": 42,
  "subscribers": 1,
  "nodes": ["node1-id", "node2-id"],
  "oldest_message": "2025-03-09T20:00:00Z",
  "newest_message": "2025-03-09T22:02:54Z",
  "storage_tier": "memory"
}
```

**Status Codes:**
- `200 OK`: Topic information retrieved successfully
- `404 Not Found`: Topic does not exist

## Versioning

The current API version is v1. All endpoints should be considered part of this version.

Future versions will be accessed via `/v2/`, `/v3/`, etc. prefixes. 