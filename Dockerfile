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

# Install required tools
RUN apk add --no-cache ca-certificates net-tools procps

# Copy binary from builder stage
COPY --from=builder /app/sprawl .

# Create data directory
RUN mkdir -p /data

# Set the application as the entry point
ENTRYPOINT ["/app/sprawl"] 