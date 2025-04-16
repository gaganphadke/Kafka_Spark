# src/spark_streaming/spark_processor.py
import os
import sys
import json
import time
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc, from_json, count, dense_rank
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from database.db_handler import DatabaseHandler
os.environ['JAVA_HOME'] = '/usr/local/opt/openjdk@11' 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SparkStreamProcessor:
    def __init__(self, kafka_bootstrap_servers='127.0.0.1:9092', kafka_topic='sales_topic'):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic

        self.spark = SparkSession.builder \
            .appName("E-Commerce Data Pipeline") \
            .master("local[*]") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.db_handler = DatabaseHandler()
        self.schema = self.infer_schema_from_csv()

    def infer_schema_from_csv(self):
        try:
            csv_path = os.path.join('data', 'ds.csv')
            df = pd.read_csv(csv_path, nrows=1)
            fields = [StructField(column, StringType(), True) for column in df.columns]
            return StructType(fields)
        except Exception as e:
            logger.error(f"Error inferring schema: {e}")
            return StructType([])

    def process_stream(self, processing_time="30 seconds", benchmark=False):
        try:
            df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_bootstrap_servers) \
                .option("subscribe", self.kafka_topic) \
                .option("startingOffsets", "earliest") \
                .load()

            parsed_df = df.selectExpr("CAST(value AS STRING) as json_data") \
                         .select(from_json(col("json_data"), self.schema).alias("data")) \
                         .select("data.*")

            # Cast types
            parsed_df = parsed_df.withColumn("Qty", col("Qty").cast("int"))
            parsed_df = parsed_df.withColumn("Amount", col("Amount").cast("double"))

            # 1. Top-selling products by Qty
            top_qty = parsed_df.groupBy("SKU") \
                        .agg(_sum("Qty").alias("total_qty")) \
                        .orderBy(desc("total_qty"))

            # 2. Top revenue-generating products by Amount
            top_revenue = parsed_df.groupBy("SKU") \
                            .agg(_sum("Amount").alias("total_revenue")) \
                            .orderBy(desc("total_revenue"))

            # 3. Total sales per category and highest value
            total_sales_category = parsed_df.groupBy("Category") \
                .agg(_sum("Amount").alias("total_sales")) \
                .orderBy(desc("total_sales"))

            # 4. Orders per shipping state, ranked
            order_count_state = parsed_df.groupBy("Shipping State") \
                .agg(count("*").alias("order_count")) \
                .orderBy(desc("order_count"))

            def foreach_batch_function(df, epoch_id, table_name):
                if not df.isEmpty():
                    self.db_handler.save_streaming_results(df.toPandas(), table_name)

            # Write all streams
            top_qty.writeStream \
                .outputMode("complete") \
                .foreachBatch(lambda df, eid: foreach_batch_function(df, eid, "streaming_top_qty")) \
                .trigger(processingTime=processing_time) \
                .start()

            top_revenue.writeStream \
                .outputMode("complete") \
                .foreachBatch(lambda df, eid: foreach_batch_function(df, eid, "streaming_top_revenue")) \
                .trigger(processingTime=processing_time) \
                .start()

            total_sales_category.writeStream \
                .outputMode("complete") \
                .foreachBatch(lambda df, eid: foreach_batch_function(df, eid, "streaming_top_category_sales")) \
                .trigger(processingTime=processing_time) \
                .start()

            order_count_state.writeStream \
                .outputMode("complete") \
                .foreachBatch(lambda df, eid: foreach_batch_function(df, eid, "streaming_order_count_by_state")) \
                .trigger(processingTime=processing_time) \
                .start() \
                .awaitTermination(120)

            return True
        except Exception as e:
            logger.error(f"Error processing Spark stream: {e}")
            return False
        finally:
            self.spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark Streaming Processor")
    parser.add_argument("--benchmark", action="store_true", help="Run in benchmark mode")
    args = parser.parse_args()

    processor = SparkStreamProcessor()
    processor.process_stream(benchmark=args.benchmark)
