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

# --------------------------------------------------
# FACT TABLE: fact_movie
# --------------------------------------------------
fact_movie_df = parsed_df.select(
    col("id").cast("integer").alias("id"),
    col("budget").cast("integer").alias("budget"),
    col("popularity").cast("double").alias("popularity"),
    col("revenue").cast("double").alias("revenue"),
    col("vote_average").cast("double").alias("vote_average"),
    col("vote_count").cast("double").alias("vote_count"),
    unix_timestamp(col("release_date"), "yyyy-MM-dd").alias("date_id")
)

fact_movie_query = fact_movie_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/fact_movies") \
    .option("path", "s3a://lakehouse/gold/fact_movies") \
    .start()

# --------------------------------------------------
# DIMENSION TABLE: dim_movie
# Lưu ý: Đổi tên các cột để khớp với schema của Delta table hiện có:
# Schema mong đợi: id, title, original_title, language, overview, runtime, tagline, status, homepage
# --------------------------------------------------
dimmovie_df = parsed_df.select(
    col("id").cast("long").alias("id"),                          # Ép sang long
    col("title"),                                                # Giữ nguyên kiểu string
    col("original_title"),                                       # Giữ nguyên kiểu string
    col("original_language").alias("language"),                  # Đổi tên trường: original_language -> language
    col("overview"),                                             # Giữ nguyên kiểu string
    col("runtime").cast("double").alias("runtime"),              # Ép về double
    col("tagline"),                                              # Giữ nguyên kiểu string
    col("status"),                                               # Giữ nguyên kiểu string
    col("homepage")                                              # Giữ nguyên kiểu string
)

dimmovie_query = dimmovie_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/dim_movie") \
    .option("path", "s3a://lakehouse/gold/dim_movie") \
    .start()

# --------------------------------------------------
# DIMENSION TABLE: dim_date
# --------------------------------------------------
dimdate_df = parsed_df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd")) \
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
        unix_timestamp(col("release_date"), "yyyy-MM-dd").alias("DATE_ID")
    )

dim_date_query = dimdate_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/dim_date") \
    .option("path", "s3a://lakehouse/gold/dim_date") \
    .start()

# --------------------------------------------------
# DIMENSION TABLE: dim_genre
# --------------------------------------------------
dim_genre_df = parsed_df.select(
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").alias("id"),
    col("genre.name").alias("name")
).dropDuplicates(["id"])

dim_genre_query = dim_genre_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/dim_genre") \
    .option("path", "s3a://lakehouse/gold/dim_genre") \
    .start()

# --------------------------------------------------
# BRIDGE TABLE: movie_genres
# --------------------------------------------------
movie_genre_df = parsed_df.select(
    col("id").alias("movie_id"),
    explode(col("genres")).alias("genre")
).select(
    col("genre.id").alias("genres_id"),
    col("movie_id").alias("id")
)

movie_genres_query = movie_genre_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://lakehouse/check/movie_genres") \
    .option("path", "s3a://lakehouse/gold/movie_genres") \
    .start()

# Chờ cho đến khi có sự kiện dừng của streaming query
spark.streams.awaitAnyTermination()
