#!/bin/bash

echo "Running benchmark comparison between streaming and batch processing..."

# Measure streaming processing time
echo "Measuring streaming processing time..."
start_time=$(date +%s)
python3 src/spark_streaming/spark_processor.py --benchmark
end_time=$(date +%s)
streaming_time=$((end_time - start_time))
echo "Streaming processing time: $streaming_time seconds"

# Measure batch processing time
echo "Measuring batch processing time..."
start_time=$(date +%s)
python3 src/batch_processing/batch_processor.py --benchmark
end_time=$(date +%s)
batch_time=$((end_time - start_time))
echo "Batch processing time: $batch_time seconds"

echo "Performance comparison:"
echo "Streaming time: $streaming_time seconds"
echo "Batch time: $batch_time seconds"
echo "Difference: $((streaming_time - batch_time)) seconds"
