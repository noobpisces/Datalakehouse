
import json
import platform
# import ast
# import os
import sys
import traceback
import logging
import ast
from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import functions as F
from pyspark.sql.functions import when,col, from_json,size, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType,IntegerType

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def init_spark_session():
    try:
        logging.info(f"Python version on driver: {platform.python_version()}")  # Log phiên bản Python trên driver

        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName('CleanCredits') \
            .master('spark://spark-master:7077') \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
            .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .getOrCreate()

        logging.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark session: {str(e)}")
        logging.error(traceback.format_exc())  # In ra toàn bộ chi tiết lỗi
        sys.exit(1)



def parse_json_safe(json_str):
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return []  # Return an empty list if JSON is invalid

def clean_credits(input_path: str, output_path: str):
    try:
        # Initialize Spark Session
        spark = init_spark_session()

        # Read data from Parquet
        logging.info(f"Reading data from {input_path}")
        df = spark.read.format("parquet").load(input_path)

        df_filtered = df.filter((col("cast") != "[]") & (col("crew") != "[]"))
        # Save cleaned data to Delta Lake
        logging.info(f"Saving cleaned data to {output_path}")
        df_filtered.write.format("delta").save(output_path)

        logging.info("Data cleaning and saving process completed successfully.")
    except Exception as e:
        logging.error(f"Error during cleaning process: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == '__main__':
    try:
        # Lấy đường dẫn đầu vào và đầu ra từ command line arguments
        if len(sys.argv) != 3:
            logging.error("Usage: spark-submit clean_credits.py <input_path> <output_path>")
            sys.exit(1)

        input_path = sys.argv[1]  # Nhận đường dẫn đầu vào từ Airflow
        output_path = sys.argv[2]  # Nhận đường dẫn đầu ra từ Airflow

        clean_credits(input_path, output_path)
    
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        logging.error(traceback.format_exc())  # In ra toàn bộ chi tiết lỗi
        sys.exit(1)




