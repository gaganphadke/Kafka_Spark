# src/kafka_setup/create_topics.py
from kafka.admin import KafkaAdminClient, NewTopic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_kafka_topics():
    """Create required Kafka topics for ecommerce pipeline."""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='127.0.0.1:9092',
            client_id='admin-client'
        )
        
        # Define required topics
        topic_list = [
            NewTopic(name="ecommerce_raw_data", num_partitions=1, replication_factor=1),
            NewTopic(name="ecommerce_processed_data", num_partitions=1, replication_factor=1)
        ]
        
        # Filter out existing ones
        existing_topics = admin_client.list_topics()
        topics_to_create = [topic for topic in topic_list if topic.name not in existing_topics]
        
        if topics_to_create:
            admin_client.create_topics(new_topics=topics_to_create)
            logger.info(f"Created topics: {[t.name for t in topics_to_create]}")
        else:
            logger.info("All required topics already exist")
            
        admin_client.close()
        return True
    except Exception as e:
        logger.error(f"Error creating Kafka topics: {e}")
        return False

if __name__ == "__main__":
    create_kafka_topics()
