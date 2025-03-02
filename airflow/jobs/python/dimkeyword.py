import os
import sys
import traceback
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import  explode,col, expr,when,to_date, sum, from_json,size,length
from pyspark.sql.types import  ArrayType,StructType, StructField, BooleanType, StringType, IntegerType, DateType, FloatType,DoubleType, LongType
from delta.tables import DeltaTable
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

df = spark.read.format("delta").load("s3a://lakehouse/silver/keywords")
keyword_schema = ArrayType(
    StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
)
df_parsed = df.withColumn("keywords", from_json(col("keywords"), keyword_schema))

# Explode cột cast để có nhiều dòng
df_exploded = df_parsed.withColumn("keywords", explode(col("keywords")))

# Chọn các trường cần thiết
df_selected_Dim = df_exploded.select(
    col("keywords.name"),
    col("keywords.id")
)
df_selected_Bridge = df_exploded.select(
    col("keywords.name"),
    col("keywords.id").alias("keyword_id"),
    col("id")
                             
)

try:
    dim_keyword = DeltaTable.forPath(spark, "s3a://lakehouse/gold/dim_keyword")
    dim_keyword.alias("target").merge(
        df_selected_Dim.alias("source"),
        "target.id = source.id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/dim_keyword")
try:
    movie_keyword = DeltaTable.forPath(spark, "s3a://lakehouse/gold/movie_keyword")
    movie_keyword.alias("target").merge(
        df_selected_Bridge.alias("source"),
        "target.id = source.id AND target.keyword_id = source.keyword_id"
    ).whenNotMatchedInsertAll().execute()
except:
    df_selected_Bridge.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("s3a://lakehouse/gold/movie_keyword")



