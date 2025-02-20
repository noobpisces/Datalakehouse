from pyspark.sql.functions import col

# Giả sử bạn đã lưu lại thời điểm (ISO format) của lần xử lý trước đó
last_modified_ts = "2025-02-16T15:00:00.000Z"

# Đọc dữ liệu incremental từ bảng silver_dim_date kể từ last_modified_ts
cdf_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingTimestamp", last_modified_ts) \
    .load("s3a://lakehouse/silver/silver_dim_date")

# Nếu bạn chỉ muốn xử lý các bản ghi mới được chèn, có thể lọc:
new_inserts_df = cdf_df.filter(col("_change_type") == "insert")

# Sau đó, thực hiện các xử lý (ví dụ ghi vào bảng Gold)
new_inserts_df.write.format("delta") \
    .mode("append") \
    .save("s3a://lakehouse/gold/dim_date")ơ-