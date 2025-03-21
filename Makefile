# Sprawl Makefile
.PHONY: all build test clean run_node run_demo demo_tiering setup_minio run_full_node init_directories verify

# Default target
all: build

# Build all binaries
build: sprawl sprawlctl

# Build the main service
sprawl:
	go build -o sprawl ./cmd/sprawl/main.go

# Build the control CLI
sprawlctl:
	go build -o sprawlctl ./cmd/sprawlctl/main.go

# Run the demo
run_demo:
	./ai_demo

# Run a node with tiered storage enabled
run_node: sprawl
	SPRAWL_STORAGE_DISK_ENABLED=true \
	SPRAWL_STORAGE_DISK_PATH=./data/node1/disk \
	./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080

# Run tests
test:
	go test -v ./...

# Run tests with coverage
coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

# Run the tiered storage demonstration
demo_tiering: sprawl sprawlctl
	./scripts/demonstrate-tiering.sh

# Clean up
clean:
	rm -f sprawl
	rm -f sprawlctl
	rm -f coverage.out
	rm -f coverage.html
	rm -rf data/
	rm -rf minio_data/

# Set up MinIO for cloud storage tier
setup_minio:
	docker run -d --name minio \
		-p 9000:9000 -p 9001:9001 \
		-e "MINIO_ROOT_USER=minioadmin" \
		-e "MINIO_ROOT_PASSWORD=minioadmin" \
		minio/minio server /data --console-address ":9001"

# Run node with all tiers enabled
run_full_node: sprawl
	MINIO_ENDPOINT=http://localhost:9000 \
	MINIO_ACCESS_KEY=minioadmin \
	MINIO_SECRET_KEY=minioadmin \
	SPRAWL_STORAGE_DISK_ENABLED=true \
	SPRAWL_STORAGE_DISK_PATH=./data/node1/disk \
	SPRAWL_STORAGE_CLOUD_ENABLED=true \
	SPRAWL_STORAGE_CLOUD_BUCKET=sprawl-messages \
	SPRAWL_STORAGE_MEMORY_TO_DISK_AGE=3600 \
	SPRAWL_STORAGE_DISK_TO_CLOUD_AGE=86400 \
	./sprawl -bindAddr=127.0.0.1 -bindPort=7946 -httpPort=8080

# Initialize directories
init_directories:
	mkdir -p logs
	mkdir -p data/node1/memory
	mkdir -p data/node1/disk
	mkdir -p data/node2/memory
	mkdir -p data/node2/disk

# Verify system
verify:
	./scripts/verify-tiering.sh 