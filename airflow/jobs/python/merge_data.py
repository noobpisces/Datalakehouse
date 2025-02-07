from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType,ArrayType,LongType
import logging
import sys
import traceback
import ast

import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr,when,to_date ,udf
from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType
from pyspark.sql import functions as F


def init_spark_session():
    try:
        logging.info("Initializing Spark session...")
        spark = SparkSession.builder \
            .appName('MergeData') \
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
        logging.error(traceback.format_exc())
        sys.exit(1)

def parse_and_clean_keywords(keywords):
    try:
        keyword_list = ast.literal_eval(keywords)  # Convert string to list of dicts
        return ' '.join(item['name'].replace(" ", "") for item in keyword_list)
    except Exception as e:
        logging.error(f"Failed to parse keywords: {keywords}, error: {e}")
        return None

# Register the UDF
clean_keywords_udf = udf(parse_and_clean_keywords, StringType())

def clean_keywords(input_path):

    try:
        # Read data from parquet with schema
        logging.info(f"Reading data from {input_path}")
        df = spark.read.format("parquet").load(input_path)

        # Clean data by filtering non-null 'keywords' and removing duplicates
        logging.info("Cleaning data...")
        df_filtered = df.filter(col("keywords").isNotNull()).dropDuplicates()

        # Apply the UDF to transform keywords column
        df_cleaned = df_filtered.withColumn("keyword_convert", clean_keywords_udf(col("keywords")))

        logging.info("Data cleaned and transformed.")

        # Save cleaned data to Delta Lake on MinIO
        logging.info(f"Saving cleaned data to {output_path}")

        logging.info("Data cleaning and saving process completed successfully.")
        return df_cleaned
    except Exception as e:
        logging.error(f"Error during cleaning process: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)





def clean_credits(input_path: str):
    try:
        # Initialize Spark Session

        # Read data from Parquet
        logging.info(f"Reading data from {input_path}")
        df = spark.read.format("parquet").load(input_path)

        # Define schema
        credits_schema = StructType([
            StructField("cast", StringType(), nullable=True),
            StructField("crew", StringType(), nullable=True),
            StructField("id", StringType(), nullable=True)
        ])

        # Apply schema to the DataFrame
        df = spark.createDataFrame(df.rdd, schema=credits_schema)

        # Process and clean data using RDD
        rdd = df.rdd.map(lambda row: row.asDict()) \
            .map(lambda row: row.update({
                'director': ''.join(
                    [item['name'].replace(" ", "") for item in ast.literal_eval(row['crew']) if item.get('job') == 'Director']
                ) if row['crew'] else ''
            }) or row) \
            .map(lambda row: row.update({
                'cast_names': ' '.join(
                    [item['name'].replace(" ", "") for item in ast.literal_eval(row['cast'])[:3]]
                ) if row['cast'] else ''
            }) or row)

        # Convert RDD back to DataFrame
        df_final = spark.createDataFrame(rdd).select('id', 'cast_names', 'director')

        # Save cleaned data to Delta Lake
        return df_final
    except Exception as e:
        logging.error(f"Error during cleaning process: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)

def clean_movies(input_path):
    try:


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
        logging.info("Data cleaning and saving process completed successfully.")
        return df_cleaned

    except Exception as e:
        logging.error(f"Error during cleaning process: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)




def merge_data(output_path):
    try:


        # Đọc các DataFrame đã làm sạch từ Silver layer
        logging.info("Reading data from Silver layer...")
        keywords_df = clean_keywords(keywords_path)
        logging.info("keyyyyyyyyyyyyyy.")
        # logging.info(keywords_df.printSchema())
        
        movies_df=clean_movies(movies_path)
        # logging.info(movies_df.printSchema())


        logging.info("mvoieeeeeeeeeeee.")

        credits_df = clean_credits(credits_path)

        # logging.info(credits_df.printSchema())


        # Join và merge các DataFrame
        logging.info("Merging data...")
        final_df = movies_df \
            .join(keywords_df, on=['id'], how='inner') \
            .join(credits_df, on=['id'], how='inner') 
            #.withColumn('comb', concat_ws(" ", col('keyword_convert'), col('cast_names'), col('director'), col('genres_convert')))

        # Lưu dữ liệu đã merge vào Delta Lake
        logging.info(f"Saving merged data to {output_path}")
        final_df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').save(output_path)
        return final_df
        logging.info("Data merging and saving completed successfully.")
    except Exception as e:
        logging.error(f"Error during merge process: {str(e)}")
        logging.error(traceback.format_exc())
        sys.exit(1)



def gold_merge_movies(gold_movie,ouput_path_gold):
    spark_df = gold_movie.select('id', 'imdb_id', 'budget', 'genres_convert','keyword_convert', 'original_language', 
                                      'release_date', 'revenue', 'title', 'time', 
                                      'overview', 'vote_average', 'vote_count', 'cast_names', 'director')
    logging.info(f"Saving merged data to {output_path}")
    spark_df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').save(ouput_path_gold)

if __name__ == '__main__':

    spark = init_spark_session()

    keywords_path = sys.argv[1]
    movies_path = sys.argv[2]
    credits_path = sys.argv[3]
    output_path = sys.argv[4]
    ouput_path_gold =sys.argv[5]

    gold_data=merge_data(output_path)
    test_gold = gold_merge_movies(gold_data,ouput_path_gold)
    spark.stop()
