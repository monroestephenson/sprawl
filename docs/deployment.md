# Deployment Guide

This guide covers deploying Sprawl in production environments.

## Docker Deployment

### Building the Docker Image

```dockerfile
FROM golang:1.21-alpine

# Install required system dependencies
RUN apk add --no-cache gcc musl-dev

# Set working directory
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN go build -o sprawl main.go

# Expose ports
EXPOSE 7946 8080

# Set environment variables
ENV MINIO_ENDPOINT=http://minio:9000 \
    BIND_ADDR=0.0.0.0 \
    HTTP_ADDR=0.0.0.0

# Run the application
CMD ["./sprawl"]
```

### Docker Compose Setup

```yaml
version: '3.8'

services:
  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    environment:
      - MINIO_ROOT_USER=minioadmin
      - MINIO_ROOT_PASSWORD=minioadmin
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio_data:/data

  node1:
    build: .
    ports:
      - "8080:8080"
      - "7946:7946"
    environment:
      - MINIO_ENDPOINT=http://minio:9000
      - MINIO_ACCESS_KEY=minioadmin
      - MINIO_SECRET_KEY=minioadmin
    depends_on:
      - minio

  node2:
    build: .
    ports:
      - "8081:8080"
      - "7947:7946"
    environment:
      - MINIO_ENDPOINT=http://minio:9000
      - MINIO_ACCESS_KEY=minioadmin
      - MINIO_SECRET_KEY=minioadmin
      - SEEDS=node1:7946
    depends_on:
      - node1

volumes:
  minio_data:
```

## Kubernetes Deployment

### Prerequisites

- Kubernetes cluster (v1.19+)
- kubectl configured
- Helm (optional)

### Basic Deployment

1. Create a namespace:
```bash
kubectl create namespace sprawl
```

2. Deploy MinIO:
```bash
helm repo add minio https://charts.min.io/
helm install minio minio/minio --namespace sprawl
```

3. Deploy Sprawl:
```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sprawl
  namespace: sprawl
spec:
  serviceName: sprawl
  replicas: 3
  selector:
    matchLabels:
      app: sprawl
  template:
    metadata:
      labels:
        app: sprawl
    spec:
      containers:
      - name: sprawl
        image: sprawl:latest
        ports:
        - containerPort: 7946
          name: gossip
        - containerPort: 8080
          name: http
        env:
        - name: MINIO_ENDPOINT
          value: "http://minio:9000"
        - name: BIND_ADDR
          value: "0.0.0.0"
        - name: HTTP_ADDR
          value: "0.0.0.0"
        - name: SEEDS
          value: "sprawl-0.sprawl:7946"
```

## Production Considerations

### Security

1. Enable TLS:
   - Generate certificates
   - Configure HTTPS endpoints
   - Enable mTLS for node communication

2. Authentication:
   - Implement API key authentication
   - Set up role-based access control
   - Use secure secrets management

### Monitoring

1. Metrics:
   - Configure Prometheus metrics
   - Set up Grafana dashboards
   - Monitor key metrics:
     - Message throughput
     - Latency
     - Error rates
     - Storage usage

2. Logging:
   - Configure structured logging
   - Set up log aggregation (ELK/EFK)
   - Implement log rotation

### High Availability

1. Multi-zone deployment:
   - Deploy across availability zones
   - Configure proper anti-affinity rules
   - Set up geo-replication

2. Backup and Recovery:
   - Regular state backups
   - Disaster recovery planning
   - Data retention policies

### Performance Tuning

1. Resource Allocation:
   - Configure proper CPU/memory limits
   - Optimize JVM settings
   - Tune network parameters

2. Storage:
   - Configure proper storage class
   - Set up volume claims
   - Monitor storage performance

## Maintenance

### Upgrades

1. Rolling updates:
```bash
kubectl set image statefulset/sprawl sprawl=sprawl:new-version
```

2. Rollback procedure:
```bash
kubectl rollout undo statefulset/sprawl
```

### Scaling

1. Horizontal scaling:
```bash
kubectl scale statefulset sprawl --replicas=5
```

2. Vertical scaling:
   - Adjust resource requests/limits
   - Monitor performance metrics

### Troubleshooting

1. Common issues:
   - Network connectivity
   - Storage problems
   - Memory pressure

2. Debug commands:
```bash
# Check logs
kubectl logs -f -l app=sprawl

# Get node status
curl http://localhost:8080/status

# Check metrics
curl http://localhost:8080/metrics
``` 