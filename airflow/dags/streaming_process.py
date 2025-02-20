
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import logging
from datetime import datetime
from airflow import DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 28),
    'retries': 1
}
with DAG(
    dag_id='test_streaming',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    spark_task = SparkSubmitOperator(
        task_id='streaming_process',
        application='/opt/airflow/jobs/python/processingAPI_1_toBronze.py',  # Lưu ý: cần tách hàm okok() thành file riêng
        jars = "/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,"
             "/opt/airflow/jars/kafka-clients-3.3.2.jar,"
             "/opt/airflow/jars/commons-pool2-2.11.1.jar,"
             "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,"
             "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar",
        conn_id="spark-conn"
    )

    spark_task