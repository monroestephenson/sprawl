#!/bin/bash
# Simple script to verify tiered storage functionality

echo "=== Sprawl Tiered Storage Verification ==="
echo ""

# Check if our node is running
if ! curl -s http://localhost:8080/health > /dev/null; then
  echo "❌ Sprawl node is not running"
  exit 1
fi

echo "✅ Sprawl node is running"

# Check current storage stats
echo -e "\nInitial storage stats:"
curl -s http://localhost:8080/store | jq

# Subscribe to a test topic
echo -e "\nSubscribing to tiering-test topic..."
SUBSCRIPTION=$(curl -s -X POST -H "Content-Type: application/json" -d '{"topics":["tiering-test"]}' http://localhost:8080/subscribe)
echo "$SUBSCRIPTION"

# Generate some test data
echo -e "\nPublishing messages to fill memory tier..."
for i in {1..20}; do
  # Generate a payload with random data (~5KB)
  PAYLOAD=$(dd if=/dev/urandom bs=5120 count=1 2>/dev/null | base64)
  RESULT=$(curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"topic\":\"tiering-test\",\"payload\":\"$PAYLOAD\"}" \
    http://localhost:8080/publish)
  echo "Published message $i: $(echo $RESULT | jq -r '.id')"
done

# Check storage stats after publishing
echo -e "\nStorage stats after publishing:"
curl -s http://localhost:8080/store | jq

# Wait for memory -> disk transition
echo -e "\nWaiting 10 seconds for potential memory -> disk transition..."
sleep 10

# Check final storage stats
echo -e "\nFinal storage stats:"
curl -s http://localhost:8080/store | jq

echo -e "\n=== Verification Complete ===" 