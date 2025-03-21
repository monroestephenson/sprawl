#!/bin/bash

# Script to build and run the AI data generator

echo "Building AI data generator..."
cd "$(dirname "$0")/.."
go build -o ai_data_generator ./tools/ai_data_generator.go

if [ $? -ne 0 ]; then
  echo "Failed to build the data generator. Please check the errors above."
  exit 1
fi

echo "Starting data generation process..."
./ai_data_generator

if [ $? -ne 0 ]; then
  echo "Data generation failed. See errors above."
  exit 1
fi

echo "Data generation completed successfully!"
echo "The AI models should now be trained with synthetic data."
echo "You can now restart the Sprawl node to use the trained models."

# Clean up the binary
rm ai_data_generator

echo "Done." 