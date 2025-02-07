import logging
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Thiết lập logger
log = logging.getLogger(__name__)

# Khai báo các tham số mặc định cho DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 28),
    'retries': 1,
    'on_failure_callback': lambda context: log.error(f"Task {context['task_instance_key_str']} failed."),
}

# Khai báo DAG
with DAG(
    dag_id='Gold_Layer',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    merge_data = SparkSubmitOperator(
        task_id='Gold_Data',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='2',
        driver_memory='1g',
        application="/opt/airflow/jobs/python/merge_data.py",  # Đường dẫn đến file Python Spark
        packages="org.apache.hadoop:hadoop-aws:3.3.4",  # Thêm Delta Lake vào packages
        application_args=[
            "s3a://lakehouse/bronze/keywords.parquet",
            "s3a://lakehouse/bronze/movies.parquet",
            "s3a://lakehouse/bronze/credits.parquet",
            "s3a://lakehouse/merge_data-movies/merged_data",
            "s3a://lakehouse/gold"

        ],
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",  # Kết nối Spark
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )

    # Thiết lập các phụ thuộc giữa các task
    merge_data


