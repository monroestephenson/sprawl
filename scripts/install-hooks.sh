#!/bin/bash

# Install Git hooks script for Sprawl
# This script installs all Git hooks into the local repository

# Colors for terminal output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

HOOKS_DIR="$(git rev-parse --git-dir)/hooks"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo -e "${YELLOW}Installing Git hooks for Sprawl...${NC}"

# Create hooks directory if it doesn't exist
mkdir -p "$HOOKS_DIR"

# Install pre-commit hook
echo -e "Installing pre-commit hook..."
cp "$PROJECT_ROOT/scripts/pre-commit" "$HOOKS_DIR/pre-commit"
chmod +x "$HOOKS_DIR/pre-commit"

# Check for required tools
echo -e "\nChecking for required tools:"
missing_tools=()

for cmd in go golangci-lint staticcheck; do
  if command -v $cmd &> /dev/null; then
    echo -e "${GREEN}✅ $cmd installed${NC}"
  else
    echo -e "${RED}❌ $cmd not found${NC}"
    missing_tools+=($cmd)
  fi
done

if [ ${#missing_tools[@]} -ne 0 ]; then
  echo -e "\n${YELLOW}Warning: Some required tools are missing: ${missing_tools[*]}${NC}"
  echo -e "Please install them to use the pre-commit hook effectively."
  
  # Print installation instructions
  echo -e "\nInstallation instructions:"
  
  if [[ " ${missing_tools[*]} " =~ " go " ]]; then
    echo -e "- Go: https://golang.org/doc/install"
  fi
  
  if [[ " ${missing_tools[*]} " =~ " golangci-lint " ]]; then
    echo -e "- golangci-lint: https://golangci-lint.run/usage/install/"
    echo -e "  Quick install: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"
  fi
  
  if [[ " ${missing_tools[*]} " =~ " staticcheck " ]]; then
    echo -e "- staticcheck: https://staticcheck.io/docs/getting-started/"
    echo -e "  Quick install: go install honnef.co/go/tools/cmd/staticcheck@latest"
  fi
else
  echo -e "\n${GREEN}All required tools are installed!${NC}"
fi

echo -e "\n${GREEN}Git hooks installed successfully!${NC}"
echo -e "Pre-commit hook will run: go fmt, go vet, golangci-lint, staticcheck, and tests"
