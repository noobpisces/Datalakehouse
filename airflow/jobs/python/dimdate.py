import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType

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

df = spark.read.format("delta").load("s3a://lakehouse/silver/movies")
df = df.withColumn("parsed_date", F.to_date(F.col("release_date"), "yyyy-MM-dd"))

result = df.select(
    F.col("release_date"),
    F.dayofweek("parsed_date").alias("DayOfWeek"),
    F.date_format("parsed_date", "EEEE").alias("DayName"),
    F.dayofmonth("parsed_date").alias("DayOfMonth"),
    F.dayofyear("parsed_date").alias("DayOfYear"),
    F.weekofyear("parsed_date").alias("WeekOfYear"),
    F.date_format("parsed_date", "MMMM").alias("MonthName"),
    F.month("parsed_date").alias("MonthOfYear"),
    F.quarter("parsed_date").alias("Quarter"),
    F.year("parsed_date").alias("Year"),
    F.when((F.dayofweek("parsed_date") >= 2) & (F.dayofweek("parsed_date") <= 6), True).otherwise(False).alias("IsWeekDay")
)
result = result.withColumn(
    "DATE_ID",
    (F.col("Year") * 10000 + F.col("MonthOfYear") * 100 + F.col("DayOfMonth")).cast("long")
)
result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_date")