import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length, lit, to_timestamp,current_timestamp, max
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from pyspark.sql.functions import (
    col, from_json, explode, to_date, date_format,
    dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, when, unix_timestamp
)
from delta.tables import DeltaTable
from pyspark.sql import functions as F

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

df_Movies = spark.read.format("delta").load("s3a://lakehouse/bronze/Bronze_Movies_API")
df_Crews = spark.read.format("delta").load("s3a://lakehouse/bronze/Bronze_Crews_API")
df_Keywords = spark.read.format("delta").load("s3a://lakehouse/bronze/Bronze_Keywords_API")


df_Movies = df_Movies.select(
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
    col("release_date"),
    col("read_time")
)


try:
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")
except:
    spark.sql("""
CREATE TABLE IF NOT EXISTS delta.`s3a://lakehouse/ReadTime` (
    task_id STRING,
    last_read_time TIMESTAMP
) USING DELTA
""")
    spark.sql("""
    INSERT INTO delta.`s3a://lakehouse/ReadTime`
    VALUES 
      ('BatchApi_Process_Movies', '1970-01-01 00:00:00'),
      ('BatchApi_Process_Crews', '1970-01-01 00:00:00'),
      ('BatchApi_Process_Keywords', '1970-01-01 00:00:00')
""")
    readTime = spark.read.format("delta").load("s3a://lakehouse/ReadTime")

# result = readTime.filter(f"task_id = 'BatchApi_Process'").select("last_read_time").collect()
# last_read_time = result[0][0]
result_movie = readTime.filter(f"task_id = 'BatchApi_Process_Movies'").select("last_read_time").collect()
result_crew = readTime.filter(f"task_id = 'BatchApi_Process_Crews'").select("last_read_time").collect()
result_keyword = readTime.filter(f"task_id = 'BatchApi_Process_Keywords'").select("last_read_time").collect()

last_read_time_movie = result_movie[0][0]
last_read_time_crew = result_crew[0][0]
last_read_time_keyword = result_keyword[0][0]


df_Movies = df_Movies.filter(f"read_time > '{last_read_time_movie}'")
df_Crews = df_Crews.filter(f"read_time > '{last_read_time_crew}'")
df_Keywords = df_Keywords.filter(f"read_time > '{last_read_time_keyword}'")

# last_read_time = Variable.get("last_read_time", default_var="1970-01-01T00:00:00")
# last_read_time_ts = to_timestamp(lit(last_read_time), "yyyy-MM-dd HH:mm:ss")

# df = df.filter(
#                                 (col("budget") != 0) &            # loại bỏ dòng có budget là "0" (kiểu string)
#                                 (col("id") != 0) &           # loại bỏ dòng có id là null
#                                 (col("revenue") != 0) &             # loại bỏ dòng có revenue bằng 0
#                                 (col("vote_average") != 0) &        # loại bỏ dòng có vote_average bằng 0
#                                 (col("vote_count") != 0) &          # loại bỏ dòng có vote_count bằng 0
#                                 (col("popularity") != 0) &        # loại bỏ dòng có popularity là "0" (kiểu string)
#                                 (col("date_id") != 0) & # loại bỏ dòng có release_date là null
#                                 (col("runtime") != 0) &               # loại bỏ dòng có runtime bằng 0
#                                 (col("read_time") > last_read_time_ts)
#                             )
# df = df.filter(
#                                 (col("read_time") >= last_read_time_ts)
#                             )

# last_read_time = Variable.get("last_read_time", default_var="1970-01-01T00:00:00")

# # Chuyển đổi thành kiểu timestamp nếu cần
# filtered_df = df.filter(col("read_time") > lit(last_read_time).cast("timestamp"))



# df_Movies = df_Movies.filter(f"read_time > '{last_read_time}'")
# df_Crews = df_Crews.filter(f"read_time > '{last_read_time}'")
# df_Keywords = df_Keywords.filter(f"read_time > '{last_read_time}'")

try:
    tb_movie = DeltaTable.forPath(spark, "s3a://lakehouse/silver/Silver_Movies_API")
    tb_movie.alias("target").merge(
        df_Movies.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_Movies.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/silver/Silver_Movies_API")

try:
    tb_crew = DeltaTable.forPath(spark, "s3a://lakehouse/silver/Silver_Crews_API")
    tb_crew.alias("target").merge(
        df_Crews.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_Crews.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/silver/Silver_Crews_API")


try:
    tb_keyword = DeltaTable.forPath(spark, "s3a://lakehouse/silver/Silver_Keywords_API")
    tb_keyword.alias("target").merge(
        df_Keywords.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_Keywords.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/silver/Silver_Keywords_API")
