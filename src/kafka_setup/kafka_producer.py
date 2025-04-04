# src/kafka_setup/kafka_producer.py
import os
import csv
import json
import time
import logging
from kafka import KafkaProducer
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SalesDataProducer:
    def __init__(self, bootstrap_servers='127.0.0.1:9092', topic_name='sales_topic'):
        """Initialize the Kafka producer for ecommerce sales data."""
        self.bootstrap_servers = bootstrap_servers
        self.topic_name = topic_name
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        
    def load_and_send_data(self, csv_path, batch_size=100, delay=0.1):
        """
        Load data from CSV and send to Kafka topic.
        
        Args:
            csv_path: Path to the CSV file containing sales data
            batch_size: Number of records to send in one batch
            delay: Delay in seconds between batches to simulate streaming
        """
        try:
            logger.info(f"Loading data from {csv_path}")
            df = pd.read_csv(csv_path)
            total_records = len(df)
            logger.info(f"Total {total_records} records found")
            
            records_sent = 0
            for i in range(0, total_records, batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    record_data = row.to_dict()
                    self.producer.send(self.topic_name, record_data)
                    records_sent += 1
                
                self.producer.flush()
                logger.info(f"Sent {records_sent}/{total_records} records to Kafka")
                time.sleep(delay)
                
            logger.info(f"All {records_sent} records sent to Kafka topic {self.topic_name}")
            return True
        except Exception as e:
            logger.error(f"Error sending data to Kafka: {e}")
            return False
        finally:
            self.producer.close()

if __name__ == "__main__":
    data_path = os.path.join('data', 'ds.csv')
    if not os.path.exists(data_path):
        logger.error(f"Data file not found: {data_path}")
        exit(1)
        
    producer = SalesDataProducer()
    producer.load_and_send_data(data_path)
