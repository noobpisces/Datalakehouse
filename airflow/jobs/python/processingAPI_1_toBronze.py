from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, DateType
from pyspark.sql.functions import (
    col, from_json, explode, to_date, date_format,
    dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, when, unix_timestamp
)

# Tạo Spark Session
spark = SparkSession.builder \
    .appName("Kafka Streaming") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "conbo123") \
    .config("spark.hadoop.fs.s3a.secret.key", "123conbo") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .config("delta.enable-non-concurrent-writes", "true") \
    .config("spark.sql.warehouse.dir", "s3a://lakehouse/") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu JSON (runtime được định nghĩa là DoubleType)
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("original_title", StringType(), True),
    StructField("title", StringType(), True),
    StructField("original_language", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("runtime", DoubleType(), True),
    StructField("tagline", StringType(), True),
    StructField("status", StringType(), True),
    StructField("homepage", StringType(), True),
    StructField("budget", IntegerType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("revenue", IntegerType(), True),
    StructField("vote_average", DoubleType(), True),
    StructField("vote_count", IntegerType(), True),
    StructField("release_date", StringType(), True),
    StructField("genres", ArrayType(StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])), True)
])

# Đọc dữ liệu từ Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "tmdb_movies") \
    .option("startingOffsets", "latest") \
    .load()

# Chuyển đổi dữ liệu từ binary sang string và parse JSON
json_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = json_df.withColumn("data", from_json(col("value"), schema)).select("data.*")

# Ép release_date sang DateType
parsed_df = parsed_df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))

#--------------------------------------
# Broze DATA
#---------------------------------------
# bronze_df = parsed_df.select(
#     col("id").cast("integer"),
#     col("budget").cast("integer"),
#     col("popularity").cast("double").alias("popularity"),
#     col("revenue").cast("double").alias("revenue"),
#     col("vote_average").cast("double").alias("vote_average"),
#     col("vote_count").cast("double").alias("vote_count"),
#     unix_timestamp(col("release_date"), "yyyy-MM-dd").alias("date_id"),
#     col("title"),                                                # Giữ nguyên kiểu string
#     col("original_title"),                                       # Giữ nguyên kiểu string
#     col("original_language").alias("language"),                  # Đổi tên trường: original_language -> language
#     col("overview"),                                             # Giữ nguyên kiểu string
#     col("runtime").cast("double").alias("runtime"),              # Ép về double
#     col("tagline"),                                              # Giữ nguyên kiểu string
#     col("status"),                                               # Giữ nguyên kiểu string
#     col("homepage"),
#     col("genres"),
#     col("release_date")
# )
bronze_df = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/Bronze_API_1") \
    .option("path", "s3a://lakehouse/bronze/Bronze_API_1") \
    .start()


spark.streams.awaitAnyTermination()
