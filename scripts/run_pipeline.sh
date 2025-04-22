#!/bin/bash

# Create Kafka topics
echo "Creating Kafka topics..."
python3 src/kafka_setup/create_topics.py

# Run Kafka producer to load ecommerce data
echo "Starting Kafka producer to stream ecommerce dataset..."
python3 src/kafka_setup/kafka_producer.py

# Run Spark streaming job to process top-selling products and revenue
echo "Starting Spark streaming job..."
# python3 src/spark_streaming/spark_processor.py 
spark-submit   --packages     org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.7.5 src/spark_streaming/spark_processor.py > log.txt&
SPARK_PID=$!

# Wait for Spark streaming to process data (adjust time as needed)
echo "Waiting for streaming processing to complete..."
sleep 200

# Kill Spark streaming job
kill $SPARK_PID
echo "Kill Spark Streaming"

# Run batch processing job
echo "Starting batch processing job..."
python3 src/batch_processing/batch_processor.py

# Run benchmark to compare performance
echo "Running performance comparison..."
python3 src/utils/performance_metrics.py

echo "Pipeline execution completed!"