import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length,  lit, to_timestamp
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from pyspark.sql.functions import (
    col, from_json, explode, to_date, date_format,
    dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, when, unix_timestamp,current_timestamp, max
)
from delta.tables import DeltaTable
from airflow.models import Variable
from datetime import datetime, timedelta
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

df = spark.read.format("delta").load("s3a://lakehouse/silver/Silver_API_1")
# last_read_time = Variable.get("last_read_time_SG", default_var="1970-01-01T00:00:00")
# last_read_time_ts = to_timestamp(lit(last_read_time), "yyyy-MM-dd HH:mm:ss")
try:
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")
except:
    spark.sql("""
CREATE TABLE IF NOT EXISTS delta.`s3a://your-bucket/processing_state` (
    task_id STRING,
    last_read_time TIMESTAMP
) USING DELTA
""")
    spark.sql("""
    INSERT INTO delta.`s3a://lakehouse/ReadTime`
    VALUES ('BatchApi_Process', '1970-01-01 00:00:00')
    """)
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")

result = readTime.filter(f"task_id = 'BatchApi_Process'").select("last_read_time").collect()
last_read_time = result[0][0]

df = df.filter(f"read_time > '{last_read_time}'")
# --------------------------------------------------
# FACT TABLE: fact_movie
# --------------------------------------------------
fact_movie_df = df.select(
    col("id"),
    col("budget"),
    col("popularity"),
    col("revenue"),
    col("vote_average"),
    col("vote_count"),
    col("date_id")
).dropDuplicates(["id"])

try:

    fact_movie = DeltaTable.forPath(spark, "s3a://lakehouse/gold/fact_movies")

    fact_movie.alias("target").merge(
        fact_movie_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except :
    fact_movie_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/fact_movies")




# --------------------------------------------------
# DIMENSION TABLE: dim_movie
# Lưu ý: Đổi tên các cột để khớp với schema của Delta table hiện có:
# Schema mong đợi: id, title, original_title, language, overview, runtime, tagline, status, homepage
# --------------------------------------------------
dimmovie_df = df.select(
    col("id"),                        # Ép sang long
    col("title"),                                                # Giữ nguyên kiểu string
    col("original_title"),                                       # Giữ nguyên kiểu string
    col("language"),                  # Đổi tên trường: original_language -> language
    col("overview"),                                             # Giữ nguyên kiểu string
    col("runtime"),              # Ép về double
    col("tagline"),                                              # Giữ nguyên kiểu string
    col("status"),                                               # Giữ nguyên kiểu string
    col("homepage")                                              # Giữ nguyên kiểu string
).dropDuplicates(["id"])


try:
    dimmovie = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_movie")
    dimmovie.alias("target").merge(
        dimmovie_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    dimmovie_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/dim_movie")

# --------------------------------------------------
# DIMENSION TABLE: dim_date
# --------------------------------------------------
dimdate_df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
    .select(
        date_format(col("release_date"), "yyyy-MM-dd").alias("release_date"),
        dayofweek(col("release_date")).alias("DayOfWeek"),
        date_format(col("release_date"), "EEEE").alias("DayName"),
        dayofmonth(col("release_date")).alias("DayOfMonth"),
        dayofyear(col("release_date")).alias("DayOfYear"),
        weekofyear(col("release_date")).alias("WeekOfYear"),
        date_format(col("release_date"), "MMMM").alias("MonthName"),
        month(col("release_date")).alias("MonthOfYear"),
        quarter(col("release_date")).alias("Quarter"),
        year(col("release_date")).alias("Year"),
        when(dayofweek(col("release_date")).between(2, 6), True).otherwise(False).alias("IsWeekDay"),
        col("date_id")
    ).dropDuplicates(["date_id"])

try:
    dimdate = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_date")
    dimdate.alias("target").merge(
        dimdate_df.alias("source"),
        "target.date_id = source.date_id"
    ).whenNotMatchedInsertAll().execute()
except:
    dimdate_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/dim_date")


# --------------------------------------------------
# DIMENSION TABLE: dim_genre
# --------------------------------------------------
dim_genre_df = df.select(
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").cast("integer").alias("id"),
    col("genre.name").alias("name")
).dropDuplicates(["id"])

try:
    dim_genre = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_genre")
    dim_genre.alias("target").merge(
        dim_genre_df.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    dim_genre_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/dim_genre")


# --------------------------------------------------
# BRIDGE TABLE: movie_genres
# --------------------------------------------------
movie_genre_df = df.select(
    col("id").alias("movie_id"),
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").cast("integer").alias("genres_id"),
    col("movie_id").alias("id")
)

try:
    movie_genre = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_genre")
    movie_genre.alias("target").merge(
        movie_genre_df.alias("source"),
        "target.id = source.id AND target.genres_id = source.genres_id"
    ).whenNotMatchedInsertAll().execute()
except:
    movie_genre_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/gold/movie_genres")
    
max_read_time_row = df.agg(max("read_time")).collect()
max_read_time = max_read_time_row[0]["max(read_time)"] if max_read_time_row else None


readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")
    
    # Cập nhật hoặc chèn bản ghi mới
updated_df = readTime.filter(f"task_id != 'BatchApi_Process'").union(
    spark.createDataFrame([("BatchApi_Process", max_read_time)], ["task_id", "last_read_time"])
)

# Ghi đè Delta Table
updated_df.write.format("delta").mode("overwrite").save("s3a://lakehouse/ReadTime")