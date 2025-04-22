# src/batch_processing/batch_processor.py
import os
import sys
import argparse
import logging
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, desc, count

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_handler import DatabaseHandler

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BatchProcessor:
    def __init__(self):
        """Initialize the batch processor."""
        self.spark = SparkSession.builder \
            .appName("EcommerceSalesBatchProcessing") \
            .master("local[*]") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        self.db_handler = DatabaseHandler()
        
    def process_batch(self, csv_path=None, benchmark=False):
        """
        Process ecommerce sales data in batch mode.
        
        Args:
            csv_path: Path to the CSV file
            benchmark: Whether to run in benchmark mode
        """
        try:
            start_time = time.time()
            if csv_path is None:
                csv_path = os.path.join('data', 'ds.csv')
                
            logger.info(f"Starting batch processing of {csv_path}")
            
            # Load CSV
            df = self.spark.read.csv(csv_path, header=True, inferSchema=True)
            df = df.repartition(200)
            logger.info(f"Loaded {df.count()} records for batch processing")

            if {'Category', 'Amount', 'ship-state','SKU','Qty'}.issubset(df.columns):
                df = df.withColumn("Amount", col("Amount").cast("double"))
                df = df.withColumn("Qty", col("Qty").cast("int"))


                # 1. Top category by total sales (Amount)
                top_categories = df.groupBy("Category") \
                                   .agg(spark_sum("Amount").alias("total_sales")) \
                                   .orderBy(desc("total_sales"))

                # 2. Top prod by qty
                top_qty = df.groupBy("SKU") \
                    .agg(spark_sum("Qty").alias("total_qty")) \
                    .orderBy(desc("total_qty"))

                #3. Top revenue
                top_revenue = df.groupBy("SKU") \
                    .agg(spark_sum("Amount").alias("total_revenue")) \
                    .orderBy(desc("total_revenue"))


                # Save results
                self.db_handler.save_batch_results(top_categories.toPandas(), 'batch_top_categories')
                self.db_handler.save_batch_results(top_qty.toPandas(), 'batch_top_qty')
                self.db_handler.save_batch_results(top_revenue.toPandas(), 'batch_top_revenue')

                processing_time = time.time() - start_time
                logger.info(f"Batch processing completed in {processing_time:.2f} seconds")
                
                if benchmark:
                    self.db_handler.save_performance_metrics('batch', processing_time)
            else:
                logger.warning("Required columns ('Category', 'Amount', 'ship-state') not found in dataset")
                
            return True
        except Exception as e:
            logger.error(f"Error in batch processing: {e}")
            return False
        finally:
            self.spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Batch Data Processor")
    parser.add_argument("--csv", help="Path to the CSV file")
    parser.add_argument("--benchmark", action="store_true", help="Run in benchmark mode")
    args = parser.parse_args()
    
    processor = BatchProcessor()
    processor.process_batch(csv_path=args.csv, benchmark=args.benchmark)
