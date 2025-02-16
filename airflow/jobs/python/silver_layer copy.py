from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("MinIO with Delta Lake") \
        .config("spark.jars", "opt/bitnami/spark/jars/hadoop-aws-3.3.4.jar,opt/bitnami/spark/jars/kafka-clients-3.3.2.jar,opt/bitnami/spark/jars/aws-java-sdk-bundle-1.12.262.jar,opt/bitnami/spark/jars/delta-core_2.12-2.2.0.jar,opt/bitnami/spark/jars/delta-storage-2.2.0.jar") \
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

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "tmdb_movies") \
        .option("startingOffsets", "earliest") \
        .load()

    # Chuyển đổi dữ liệu từ binary sang string
    json_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Ghi dữ liệu ra console để kiểm tra
    query = json_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination()

if __name__ == "__main__":
    main()
