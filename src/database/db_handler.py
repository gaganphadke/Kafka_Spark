import psycopg2
import pandas as pd
import logging
from psycopg2 import sql

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self, db_params=None):
        self.db_params = db_params or {
            'dbname': 'sales_data',
            'user': 'postgres',
            'password': 'postgres',  # Update this as needed
            'host': 'localhost',
            'port': '5432'
        }

    def get_connection(self):
        try:
            logger.info("Attempting to connect to the database...")
            conn = psycopg2.connect(**self.db_params)
            logger.info("Connection established successfully.")
            return conn
        except Exception as e:
            logger.error(f"Error connecting to the database: {e}")
            return None

    def ensure_table_exists(self, conn, table_name):
        cursor = conn.cursor()
        table_defs = {
            "streaming_top_qty": """
                CREATE TABLE IF NOT EXISTS streaming_top_qty (
                    sku TEXT PRIMARY KEY,
                    total_qty INTEGER
                );
            """,
            "streaming_top_revenue": """
                CREATE TABLE IF NOT EXISTS streaming_top_revenue (
                    sku TEXT PRIMARY KEY,
                    total_revenue DOUBLE PRECISION
                );
            """,
            "streaming_top_categories": """
                CREATE TABLE IF NOT EXISTS streaming_top_categories (
                    category TEXT PRIMARY KEY,
                    total_sales DOUBLE PRECISION
                );
            """,
            "batch_top_qty": """
                CREATE TABLE IF NOT EXISTS batch_top_qty (
                    sku TEXT PRIMARY KEY,
                    total_qty INTEGER
                );
            """,
            "batch_top_revenue": """
                CREATE TABLE IF NOT EXISTS batch_top_revenue (
                    sku TEXT PRIMARY KEY,
                    total_revenue DOUBLE PRECISION
                );
            """,
            "batch_top_categories": """
                CREATE TABLE IF NOT EXISTS batch_top_categories (
                    category TEXT PRIMARY KEY,
                    total_sales DOUBLE PRECISION
                );
            """,
            "performance_metrics": """
                CREATE TABLE IF NOT EXISTS performance_metrics (
                    id SERIAL PRIMARY KEY,
                    processing_type TEXT,
                    execution_time DOUBLE PRECISION
                );
            """
        }

        if table_name in table_defs:
            try:
                logger.info(f"Creating table: {table_name}")
                cursor.execute(table_defs[table_name])
                conn.commit()  # Ensure the table creation is committed to the database
                logger.info(f"Table '{table_name}' created successfully (or already exists).")
            except Exception as e:
                logger.error(f"Error creating table '{table_name}': {e}")
                conn.rollback()  # In case of any errors, rollback
        else:
            logger.warning(f"Table definition for '{table_name}' not found.")

    def save_streaming_results(self, df, table_name):
        conn = self.get_connection()
        if conn is None:    
            return False

        try:
            self.ensure_table_exists(conn, table_name)
            cursor = conn.cursor()

            for _, row in df.iterrows():
                if 'total_qty' in row:
                    cursor.execute(
                        """
                        INSERT INTO streaming_top_qty (sku, total_qty)
                        VALUES (%s, %s)
                        ON CONFLICT (sku) DO UPDATE SET total_qty = EXCLUDED.total_qty;
                        """,
                        (row['SKU'], int(row['total_qty']))
                    )
                elif 'total_revenue' in row:
                    cursor.execute(
                        """
                        INSERT INTO streaming_top_revenue (sku, total_revenue)
                        VALUES (%s, %s)
                        ON CONFLICT (sku) DO UPDATE SET total_revenue = EXCLUDED.total_revenue;
                        """,
                        (row['SKU'], float(row['total_revenue']))
                    )
                elif 'total_sales' in row and 'Category' in row:
                    cursor.execute(
                        """
                        INSERT INTO streaming_top_categories (category, total_sales)
                        VALUES (%s, %s)
                        ON CONFLICT (category) DO UPDATE SET total_sales = EXCLUDED.total_sales;
                        """,
                        (row['Category'], float(row['total_sales']))
                    )
            conn.commit()
            logger.info(f"Saved {len(df)} streaming results to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error saving streaming results: {e}")
            return False
        finally:
            conn.close()

    def save_batch_results(self, df, table_name):
        conn = self.get_connection()
        if conn is None:
            return False

        try:
            self.ensure_table_exists(conn, table_name)
            cursor = conn.cursor()

            for _, row in df.iterrows():
                if 'total_qty' in row:
                    cursor.execute(
                        """
                        INSERT INTO batch_top_qty (sku, total_qty)
                        VALUES (%s, %s)
                        ON CONFLICT (sku) DO UPDATE SET total_qty = EXCLUDED.total_qty;
                        """,
                        (row['SKU'], int(row['total_qty']))
                    )
                elif 'total_revenue' in row:
                    cursor.execute(
                        """
                        INSERT INTO batch_top_revenue (sku, total_revenue)
                        VALUES (%s, %s)
                        ON CONFLICT (sku) DO UPDATE SET total_revenue = EXCLUDED.total_revenue;
                        """,
                        (row['SKU'], float(row['total_revenue']))
                    )
                elif 'total_sales' in row and 'Category' in row:
                    cursor.execute(
                        """
                        INSERT INTO batch_top_categories (category, total_sales)
                        VALUES (%s, %s)
                        ON CONFLICT (category) DO UPDATE SET total_sales = EXCLUDED.total_sales;
                        """,
                        (row['Category'], float(row['total_sales']))
                    )
            conn.commit()
            logger.info(f"Saved {len(df)} batch results to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error saving batch results: {e}")
            return False
        finally:
            conn.close()

    def save_performance_metrics(self, processing_type, execution_time):
        conn = self.get_connection()
        if conn is None:
            return False

        try:
            self.ensure_table_exists(conn, "performance_metrics")
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO performance_metrics (processing_type, execution_time) VALUES (%s, %s);",
                (processing_type, execution_time)
            )
            conn.commit()
            logger.info(f"Saved performance metrics for {processing_type}: {execution_time:.2f} seconds")
            return True
        except Exception as e:
            logger.error(f"Error saving performance metrics: {e}")
            return False
        finally:
            conn.close()

    def compare_results(self):
        conn = self.get_connection()
        if conn is None:
            return None

        try:
            self.ensure_table_exists(conn, "performance_metrics")  # 🛠️ ensure the table exists

            streaming_df = pd.read_sql(
                "SELECT sku, total_revenue FROM streaming_top_revenue ORDER BY total_revenue DESC LIMIT 10;",
                conn
            )
            batch_df = pd.read_sql(
                "SELECT sku, total_revenue FROM batch_top_revenue ORDER BY total_revenue DESC LIMIT 10;",
                conn
            )
            perf_df = pd.read_sql(
                "SELECT processing_type, AVG(execution_time) as avg_time FROM performance_metrics GROUP BY processing_type;",
                conn
            )

            return {
                'streaming': streaming_df,
                'batch': batch_df,
                'performance': perf_df
            }
        except Exception as e:
            logger.error(f"Error comparing results: {e}")
            return None
        finally:
            conn.close()
