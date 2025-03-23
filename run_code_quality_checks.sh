#!/bin/bash

# Sprawl - Code Quality Check Script
# Runs the same checks as the pre-commit hook but on all Go files

# Colors and formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Emojis
CHECK="✅"
CROSS="❌"
INFO="ℹ️"
WARN="⚠️"

# Helper functions
print_header() {
  echo -e "\n${BOLD}${BLUE}=== $1 ===${NC}"
}

print_success() {
  echo -e "${GREEN}${CHECK} $1${NC}"
}

print_error() {
  echo -e "${RED}${CROSS} $1${NC}"
}

print_info() {
  echo -e "${BLUE}${INFO} $1${NC}"
}

print_warning() {
  echo -e "${YELLOW}${WARN} $1${NC}"
}

# Define paths to tools
GOLANGCI_LINT="$HOME/go/bin/golangci-lint"
STATICCHECK="$HOME/go/bin/staticcheck"

# Track overall status
PASS=0

# Main header
echo -e "${BOLD}${BLUE}Sprawl Code Quality Checks${NC}"
print_info "Running checks on all Go files in the repository..."

# Check if required tools are installed
check_tool() {
  if [[ ! -x "$1" ]]; then
    print_error "$2 is not installed or executable at $1"
    PASS=1
    return 1
  fi
  return 0
}

# Check for required tools
print_header "Checking required tools"
check_tool "$GOLANGCI_LINT" "golangci-lint"
check_tool "$STATICCHECK" "staticcheck"

# 1. Run go fmt
print_header "Running go fmt"
go_fmt_output=$(find . -name "*.go" -not -path "./vendor/*" | xargs gofmt -l 2>&1)
if [[ -z "$go_fmt_output" ]]; then
  print_success "go fmt passed"
else
  print_error "go fmt failed. The following files need formatting:"
  echo "$go_fmt_output"
  PASS=1
fi

# 2. Run go vet
print_header "Running go vet"
go_vet_output=$(go vet ./... 2>&1)
if [[ $? -eq 0 ]]; then
  print_success "go vet passed"
else
  print_error "go vet failed with the following issues:"
  echo "$go_vet_output"
  PASS=1
fi

# 3. Run golangci-lint
print_header "Running golangci-lint"
golangci_output=$("$GOLANGCI_LINT" run --timeout 3m ./... 2>&1)
if [[ $? -eq 0 ]]; then
  print_success "golangci-lint passed"
else
  print_error "golangci-lint failed with the following issues:"
  echo "$golangci_output"
  PASS=1
fi

# 4. Run staticcheck
print_header "Running staticcheck"
staticcheck_output=$("$STATICCHECK" ./... 2>&1)
if [[ $? -eq 0 ]]; then
  print_success "staticcheck passed"
else
  print_error "staticcheck failed with the following issues:"
  echo "$staticcheck_output"
  PASS=1
fi

# 5. Run tests
print_header "Running tests"
test_output=$(go test -short ./... 2>&1)
if [[ $? -eq 0 ]]; then
  print_success "All tests passed"
else
  print_error "Tests failed with the following issues:"
  echo "$test_output"
  PASS=1
fi

# Final summary
echo -e "\n${BOLD}${BLUE}=== Summary ===${NC}"
if [[ $PASS -eq 0 ]]; then
  print_success "All checks passed! Code quality is good."
else
  print_error "Some checks failed. Please review the issues above."
fi

exit $PASS 