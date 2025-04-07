# Ecommerce Data Processing Pipeline

This project implements a data processing pipeline using Kafka, Spark Streaming, and PostgreSQL as specified in the database technologies course project requirements.

## Prerequisites

- Python 3.8 or higher
- Java 8 or higher(11) (required for Kafka and Spark)
- PostgreSQL database

## Directory Structure

```
ecommerce-data-pipeline/
├── README.md
├── requirements.txt
├── data/
│   └── ds.csv  # Your ecommerce dataset
├── src/
│   ├── kafka_setup/        # Kafka setup and producer
│   ├── spark_streaming/    # Spark streaming processor
│   ├── batch_processing/   # Batch processing
│   ├── database/           # Database handlers
│   └── utils/              # Utility functions
└── scripts/                # Setup and execution scripts
```

## Setup Instructions

### 1. Clone the Repository (not required)

```bash
git clone <repository-url>
cd <folder name>
```

### 2. Install Dependencies

Run the install script:

```bash
chmod +x scripts/install_dependencies.sh
./scripts/install_dependencies.sh
```

This will install:
- Required Python packages
- Java (if not already installed)
- Apache Kafka

### 3. Setup PostgreSQL

1. Install PostgreSQL if not already installed:
   ```bash
   sudo apt-get install postgresql postgresql-contrib
   ```

2. Start PostgreSQL service:
   ```bash
   sudo service postgresql start
   ```

3. Set a password for the postgres user:
   ```bash
   sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'postgres';"
   ```

4. Create the database and tables:
   ```bash
   python src/database/db_setup.py
   ```

### 4. Place Your Dataset

Place your dataset CSV file in the `data/` directory: (or just place ds.csv directly from folder)
```bash
mkdir -p data
cp /path/to/your/ds.csv data/
```

## Running the Pipeline

### 1. Start Kafka

Start Zookeeper and Kafka broker:
Mac:
brew services start postgres
brew services kafka
brew services start zookeeper

```bash
chmod +x scripts/start_kafka.sh
./scripts/start_kafka.sh
```

### 2. Run the Complete Pipeline

Execute the full pipeline:

```bash
chmod +x scripts/run_pipeline.sh
./scripts/run_pipeline.sh
```

This script will:
1. Create Kafka topics
2. Load your dataset into Kafka
3. Process the data with Spark Streaming
4. Run batch processing for comparison
5. Generate performance metrics

### 3. Run Performance Benchmark

To compare streaming versus batch processing performance:

```bash
chmod +x scripts/benchmark.sh
./scripts/benchmark.sh
```

## Individual Component Execution(not required)

You can also run individual components separately:

### 1. Create Kafka Topics

```bash
python src/kafka_setup/create_topics.py
```

### 2. Load Data to Kafka

```bash
python src/kafka_setup/kafka_producer.py
```

### 3. Run Spark Streaming

```bash
python src/spark_streaming/spark_processor.py
```

### 4. Run Batch Processing

```bash
python src/batch_processing/batch_processor.py
```

### 5. Generate Performance Metrics

```bash
python src/utils/performance_metrics.py
```
