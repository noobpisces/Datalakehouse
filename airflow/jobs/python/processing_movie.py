


import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr,when,to_date
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType
from pyspark.sql import functions as F
# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def init_spark_session():
    try:
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName('CleanMovies') \
            .master('spark://spark-master:7077') \
            .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
            .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
            .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
            .config("delta.enable-non-concurrent-writes", "true") \
            .config('spark.sql.warehouse.dir', "s3a://lakehouse/") \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4') \
            .getOrCreate()

        logging.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logging.error(f"Error initializing Spark session: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)

def clean_movies(input_path, output_path):
    try:
        # Initialize SparkSession
        spark = init_spark_session()

        # Define the schema with `release_date` as StringType
        movies_schema = StructType([
            StructField("adult", StringType(), nullable=True),
            StructField("belongs_to_collection", StringType(), nullable=True),
            StructField("budget", StringType(), nullable=True),
            StructField("genres", StringType(), nullable=True),
            StructField("homepage", StringType(), nullable=True),
            StructField("id", StringType(), nullable=True),
            StructField("imdb_id", StringType(), nullable=True),
            StructField("original_language", StringType(), nullable=True),
            StructField("original_title", StringType(), nullable=True),
            StructField("overview", StringType(), nullable=True),
            StructField("popularity", StringType(), nullable=True),
            StructField("poster_path", StringType(), nullable=True),
            StructField("production_companies", StringType(), nullable=True),
            StructField("production_countries", StringType(), nullable=True),
            StructField("release_date", StringType(), nullable=True),  # Initially StringType
            StructField("revenue", DoubleType(), nullable=True),
            StructField("runtime", DoubleType(), nullable=True),
            StructField("spoken_languages", StringType(), nullable=True),
            StructField("status", StringType(), nullable=True),
            StructField("tagline", StringType(), nullable=True),
            StructField("title", StringType(), nullable=True),
            StructField("video", BooleanType(), nullable=True),
            StructField("vote_average", DoubleType(), nullable=True),
            StructField("vote_count", DoubleType(), nullable=True)
        ])

        # Read data from Parquet
        logging.info(f"Reading data from {input_path}")
        df = spark.read.format("parquet").schema(movies_schema).load(input_path)

        # Cast `release_date` to `DateType`
        df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

        # Convert columns like `adult`, `budget`, `popularity` to appropriate types
        # df = df.withColumn("adult", col("adult").cast("string")).na.fill({"adult": "unknown"})
        # df = df.withColumn("budget", col("budget").cast("int").na.fill(0))
        df = df.withColumn("adult", col("adult").cast("string"))
        df = df.fillna({"adult": "unknown"})

        df = df.withColumn("budget", col("budget").cast("int"))
        df = df.fillna({"budget": 0})


        # Additional transformations
        df_cleaned = df.withColumn(
            "time",
            expr("concat(floor(runtime / 60), ' hours ', runtime % 60, ' minutes')")
        ).withColumn(
            "genres_convert",
            expr(
                "concat_ws(' ', transform(from_json(genres, 'array<struct<name:string>>'), x -> regexp_replace(x.name, ' ', '')))"
            )
        ).drop(
            'adult', 'belongs_to_collection', 'homepage', 'video',
            'spoken_languages', 'production_countries', 'production_companies', 'runtime'
        )

        # Reorder columns to match the final schema
        columns_order = [
            'budget', 'genres', 'genres_convert', 'id', 'imdb_id', 'original_language',
            'original_title', 'overview', 'popularity', 'poster_path', 'release_date', 
            'revenue', 'status', 'tagline', 'time', 'title', 'vote_average', 'vote_count'
        ]
        df_cleaned = df_cleaned.select(*columns_order)

        # Save cleaned data to Delta Lake on MinIO
        logging.info(f"Saving cleaned data to {output_path}")
        df_cleaned.write.format("delta").mode("overwrite").save(output_path)
        logging.info("Data cleaning and saving process completed successfully.")
        return df_cleaned

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
            logging.error("Usage: spark-submit clean_movies.py <input_path> <output_path>")
            sys.exit(1)

        input_path = sys.argv[1]  # Nhận đường dẫn đầu vào từ Airflow
        output_path = sys.argv[2]  # Nhận đường dẫn đầu ra từ Airflow

        clean_movies(input_path, output_path)
    
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)
