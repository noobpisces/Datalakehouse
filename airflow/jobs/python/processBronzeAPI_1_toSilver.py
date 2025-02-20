import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from pyspark.sql.functions import (
    col, from_json, explode, to_date, date_format,
    dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, when, unix_timestamp
)
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("MinIO with Delta Lake") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
    .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("delta.enable-non-concurrent-writes", "true") \
    .config('spark.sql.warehouse.dir', "s3a://lakehouse/") \
    .getOrCreate()

df = spark.read.format("delta").load("s3a://lakehouse/bronze/Bronze_API_1")
df = df.select(
    col("id").cast("integer"),
    col("budget").cast("integer"),
    col("popularity").cast("double").alias("popularity"),
    col("revenue").cast("double").alias("revenue"),
    col("vote_average").cast("double").alias("vote_average"),
    col("vote_count").cast("double").alias("vote_count"),
    date_format(col("release_date"), "yyyyMMdd").cast("integer").alias("date_id"),
    col("title"),                                                # Giữ nguyên kiểu string
    col("original_title"),                                       # Giữ nguyên kiểu string
    col("original_language").alias("language"),                  # Đổi tên trường: original_language -> language
    col("overview"),                                             # Giữ nguyên kiểu string
    col("runtime").cast("double").alias("runtime"),              # Ép về double
    col("tagline"),                                              # Giữ nguyên kiểu string
    col("status"),                                               # Giữ nguyên kiểu string
    col("homepage"),
    col("genres"),
    col("release_date")
)
df = df.filter(
                                (col("budget") != 0) &            # loại bỏ dòng có budget là "0" (kiểu string)
                                (col("id") != 0) &           # loại bỏ dòng có id là null
                                (col("revenue") != 0) &             # loại bỏ dòng có revenue bằng 0
                                (col("vote_average") != 0) &        # loại bỏ dòng có vote_average bằng 0
                                (col("vote_count") != 0) &          # loại bỏ dòng có vote_count bằng 0
                                (col("popularity") != 0) &        # loại bỏ dòng có popularity là "0" (kiểu string)
                                (col("date_id") != 0) & # loại bỏ dòng có release_date là null
                                (col("runtime") != 0)               # loại bỏ dòng có runtime bằng 0
                            )


df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/silver/Silver_API_1")
