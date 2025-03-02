FROM golang:1.24-alpine

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