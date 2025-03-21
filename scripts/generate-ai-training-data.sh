#!/bin/bash
# Script to generate varied training data for the Sprawl AI engine

echo "Starting AI training data generation..."

# Check if Sprawl is running
if ! curl -s http://localhost:8080/health > /dev/null; then
  echo "❌ Sprawl node is not running. Please start it first."
  exit 1
fi

echo "✅ Sprawl node is running"

# Generate data with various patterns for 30 minutes
END_TIME=$(( $(date +%s) + 1800 ))

# Create a few topics with different patterns
# - high_volume: consistent high traffic
# - burst_topic: occasional traffic spikes
# - cyclic_topic: regular pattern of activity
# - low_volume: minimal traffic
TOPICS=("high_volume" "burst_topic" "cyclic_topic" "low_volume")

counter=0
cycle_position=0

echo "Generating varied traffic patterns for 30 minutes..."
echo "Press Ctrl+C to stop earlier"

while [ $(date +%s) -lt $END_TIME ]; do
  # Increment counters
  counter=$((counter + 1))
  cycle_position=$((cycle_position + 1))
  if [ $cycle_position -gt 60 ]; then
    cycle_position=0
  fi
  
  # High volume topic - consistent traffic
  for i in {1..10}; do
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"topic\":\"high_volume\",\"payload\":\"message $counter-$i\"}" \
      http://localhost:8080/publish > /dev/null
  done
  
  # Burst topic - occasional spikes
  if [ $((counter % 20)) -eq 0 ]; then
    echo "Generating burst traffic at $(date +%H:%M:%S)..."
    # Create a traffic spike (30 messages at once)
    for i in {1..30}; do
      curl -s -X POST -H "Content-Type: application/json" \
        -d "{\"topic\":\"burst_topic\",\"payload\":\"burst message $counter-$i\"}" \
        http://localhost:8080/publish > /dev/null
    done
  elif [ $((counter % 5)) -eq 0 ]; then
    # Light traffic otherwise
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"topic\":\"burst_topic\",\"payload\":\"regular message $counter\"}" \
      http://localhost:8080/publish > /dev/null
  fi
  
  # Cyclic topic - follows a sine-wave like pattern
  if [ $cycle_position -lt 30 ]; then
    # Ascending part of the cycle (more messages)
    msg_count=$((cycle_position / 3))
  else
    # Descending part of the cycle (fewer messages)
    msg_count=$(((60 - cycle_position) / 3))
  fi
  
  for i in $(seq 1 $msg_count); do
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"topic\":\"cyclic_topic\",\"payload\":\"cyclic message $counter-$i\"}" \
      http://localhost:8080/publish > /dev/null
  done
  
  # Low volume topic - just occasional messages
  if [ $((counter % 15)) -eq 0 ]; then
    curl -s -X POST -H "Content-Type: application/json" \
      -d "{\"topic\":\"low_volume\",\"payload\":\"infrequent message $counter\"}" \
      http://localhost:8080/publish > /dev/null
  fi
  
  # Print metrics every 10 cycles
  if [ $((counter % 10)) -eq 0 ]; then
    echo "Cycle $counter - $(date +%H:%M:%S) - Checking metrics..."
    curl -s http://localhost:8080/ai | jq '.current_metrics'
  fi
  
  # Sleep for a bit to avoid overwhelming the system
  sleep 3
done

echo "Training data generation complete!"
echo "AI metrics summary:"
curl -s http://localhost:8080/ai | jq 