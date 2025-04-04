# src/database/db_setup.py
import psycopg2
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_database():
    """Create the PostgreSQL database and tables."""
    # Connection parameters for the default PostgreSQL database
    conn_params = {
        'dbname': 'postgres',
        'user': 'postgres',
        'password': 'postgres',  # Change this to your PostgreSQL password
        'host': 'localhost',
        'port': '5432'
    }
    
    conn = None
    try:
        # Connect to default PostgreSQL database
        conn = psycopg2.connect(**conn_params)
        conn.autocommit = True  # Need autocommit for creating database
        cursor = conn.cursor()
        
        # Create database if it doesn't exist
        cursor.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'twitter_data'")
        if not cursor.fetchone():
            cursor.execute("CREATE DATABASE twitter_data")
            logger.info("Database 'twitter_data' created successfully")
        else:
            logger.info("Database 'twitter_data' already exists")
            
        cursor.close()
        conn.close()
        
        # Connect to the newly created database
        conn_params['dbname'] = 'twitter_data'
        conn = psycopg2.connect(**conn_params)
        cursor = conn.cursor()
        
        # Create tables
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS hashtag_counts (
            id SERIAL PRIMARY KEY,
            hashtag VARCHAR(255) NOT NULL,
            count INTEGER NOT NULL,
            processing_type VARCHAR(50) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS performance_metrics (
            id SERIAL PRIMARY KEY,
            processing_type VARCHAR(50) NOT NULL,
            execution_time FLOAT NOT NULL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        conn.commit()
        logger.info("Tables created successfully")
        return True
    except Exception as e:
        logger.error(f"Error setting up database: {e}")
        return False
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    create_database()