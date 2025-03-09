FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy and build the application
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o sprawl ./cmd/sprawl

# Final stage
FROM alpine:latest

WORKDIR /app

# Install Python and CA certificates
RUN apk add --no-cache ca-certificates python3

# Copy binary from builder stage
COPY --from=builder /app/sprawl .

# Copy health check script
COPY k8s/health-server.sh /app/health-server.sh
RUN chmod +x /app/health-server.sh

# Create data directory
RUN mkdir -p /data

# Set the health check server as the entry point
ENTRYPOINT ["/bin/sh", "-c", "/app/health-server.sh & /app/sprawl"] 