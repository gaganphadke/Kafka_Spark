#!/bin/bash

# Create Kafka topics
echo "Creating Kafka topics..."
python src/kafka_setup/create_topics.py

# Run Kafka producer to load ecommerce data
echo "Starting Kafka producer to stream ecommerce dataset..."
python src/kafka_setup/kafka_producer.py

# Run Spark streaming job to process top-selling products and revenue
echo "Starting Spark streaming job..."
python src/spark_streaming/spark_processor.py &
SPARK_PID=$!

# Wait for Spark streaming to process data (adjust time as needed)
echo "Waiting for streaming processing to complete..."
sleep 30

# Kill Spark streaming job
kill $SPARK_PID

# Run batch processing job
echo "Starting batch processing job..."
python src/batch_processing/batch_processor.py

# Run benchmark to compare performance
echo "Running performance comparison..."
python src/utils/performance_metrics.py

echo "Pipeline execution completed!"