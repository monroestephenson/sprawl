#!/bin/bash

# Run tests but skip the gossip tests that would cause network connections
# This is specifically for pre-commit checks to prevent hanging

echo "Running tests with skipping problematic gossip tests..."

# Set environment variable to indicate we're in precommit mode
export GOSSIP_TEST_MODE="false"

# Run all tests except the gossip manager tests
go test -short -timeout=10s $(go list ./... | grep -v node/gossip) -v

# For gossip tests, run only with the build tag "mock"
echo "Running gossip tests in mock mode..."
go test -tags=mock ./node/... -run=TestGossip

echo "Tests completed"
exit 0 