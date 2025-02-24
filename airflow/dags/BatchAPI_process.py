
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
    dag_id='BatchAPI_Processing',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    Bronze_Silver = SparkSubmitOperator(
        task_id='Bronze_Silver',
        application='/opt/airflow/jobs/python/processBronzeAPI_1_toSilver.py',
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,"
             "/opt/airflow/jars/kafka-clients-3.3.2.jar,"
             "/opt/airflow/jars/commons-pool2-2.11.1.jar,"
             "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,"
             "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar",
        conn_id="spark-conn",
        conf={
            "spark.executor.memory": "1G",  # Giới hạn RAM
            "spark.executor.cores": "1",  # Dùng 1 core còn lại
            "spark.dynamicAllocation.enabled": "false",  # Tắt tự động mở rộng executor
            "spark.sql.shuffle.partitions": "10"  # Giảm số lượng partition để tối ưu hiệu suất
        }
    )

    Silver_Gold = SparkSubmitOperator(
        task_id='Silver_Gold',
        application='/opt/airflow/jobs/python/processSilverAPI_1_toGold.py',  # Chạy từ Silver → Gold
        jars="/opt/airflow/jars/hadoop-aws-3.3.4.jar,"
             "/opt/airflow/jars/s3-2.18.41.jar,"
             "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,"
             "/opt/airflow/jars/aws-java-sdk-1.12.367.jar,"
             "/opt/airflow/jars/delta-core_2.12-2.2.0.jar,"
             "/opt/airflow/jars/delta-storage-2.2.0.jar,"
             "/opt/airflow/jars/kafka-clients-3.3.2.jar,"
             "/opt/airflow/jars/commons-pool2-2.11.1.jar,"
             "/opt/airflow/jars/spark-token-provider-kafka-0-10_2.12-3.3.2.jar,"
             "/opt/airflow/jars/spark-sql-kafka-0-10_2.12-3.2.1.jar",
        conn_id="spark-conn",
        conf={
            "spark.executor.memory": "1G",
            "spark.executor.cores": "1",
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.shuffle.partitions": "10"
        }
    )

    Bronze_Silver >> Silver_Gold  # Chạy tuần tự từ Bronze -> Silver -> Gold
