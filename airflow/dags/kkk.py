
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
with DAG(
    dag_id='test',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Bước 1: Xử lý làm sạch từ khóa bằng SparkSubmitOperator
    clean_keywords = SparkSubmitOperator(
        task_id='keyword',
        total_executor_cores='2',
        executor_cores='2',
        executor_memory='2g',
        num_executors='2',
        driver_memory='1g',
        application="/opt/airflow/jobs/python/processing_keyword.py",  # Đường dẫn đến file Python Spark
        application_args=["s3a://lakehouse/bronze/keywords.parquet", "s3a://lakehouse/silver/keywords"],  # Truyền input và output từ Airflow
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,",  # Đường dẫn JARs cần thiết
        conn_id="spark-conn",  # Kết nối Spark
        conf={
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        }
    )

    clean_keywords