#!/bin/bash
set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "Building and starting Sprawl cluster..."

# Build and start the containers
docker-compose build
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 10

# Run the registry tests
./scripts/test-registry.sh

# Print logs if tests failed
if [ $? -ne 0 ]; then
    echo -e "${RED}Tests failed! Printing container logs...${NC}"
    docker-compose logs
    docker-compose down
    exit 1
fi

echo -e "${GREEN}Tests passed successfully!${NC}"

# Clean up
echo "Cleaning up..."
docker-compose down 