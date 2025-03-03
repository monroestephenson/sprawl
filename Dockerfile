FROM golang:1.21-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache gcc musl-dev

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the binary
RUN CGO_ENABLED=1 GOOS=linux go build -o sprawl ./cmd/sprawl

# Create final image
FROM alpine:latest

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates

# Copy binary from builder
COPY --from=builder /app/sprawl .

# Create data directory for RocksDB
RUN mkdir -p /data

# Expose ports
EXPOSE 8080 7946

# Set environment variables
ENV SPRAWL_DATA_DIR=/data
ENV SPRAWL_HTTP_PORT=8080
ENV SPRAWL_GOSSIP_PORT=7946

# Run the service
CMD ["./sprawl"] 