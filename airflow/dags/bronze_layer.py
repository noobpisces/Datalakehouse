import os
import sys
import traceback
import logging
import pandas as pd
import boto3
from io import BytesIO
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Hàm kết nối MinIO
def connect_minio():
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id="conbo123",
            aws_secret_access_key="123conbo",
            endpoint_url='http://minio:9000' 
        )
        return s3_client
    except Exception as e:
        logging.error(f"Error connecting to MinIO: {str(e)}")
        raise e

# Hàm lấy dữ liệu từ MinIO
def get_data_from_raw(name):
    try:
        client = connect_minio()
        response = client.get_object(Bucket="lakehouse", Key=f'raw/{name}.csv')
        df = pd.read_csv(response.get("Body"), low_memory=False)
        return df
    except Exception as e:
        logging.error(f"Error getting data from MinIO: {str(e)}")
        raise e

# Hàm ghi dữ liệu vào MinIO
def save_data_to_bronze(df, name):
    try:
        client = connect_minio()
        # Sử dụng BytesIO để lưu trữ dữ liệu dưới dạng Parquet
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)  # Reset buffer position
        client.put_object(Bucket="lakehouse", Key=f'bronze/{name}.parquet', Body=parquet_buffer.getvalue())
        logging.info(f"Data saved to bronze/{name}.parquet successfully.")
    except Exception as e:
        logging.error(f"Error saving data to MinIO: {str(e)}")
        raise e

# Các task sẽ chạy dưới dạng Python Operator
def bronze_keywords(**kwargs):
    try:
        df = get_data_from_raw('keywords')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'keywords')
    except Exception as e:
        logging.error(f"Error in bronze_keywords task: {str(e)}")
        raise e

def bronze_movies(**kwargs):
    try:
        df = get_data_from_raw('movies_metadata')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'movies')
    except Exception as e:
        logging.error(f"Error in bronze_movies task: {str(e)}")
        raise e

def bronze_credits(**kwargs):
    try:
        df = get_data_from_raw('credits')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'credits')
    except Exception as e:
        logging.error(f"Error in bronze_credits task: {str(e)}")
        raise e

def bronze_ratings(**kwargs):
    try:
        df = get_data_from_raw('ratings')
        df = df(10000)
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'ratings')
    except Exception as e:
        logging.error(f"Error in bronze_ratings task: {str(e)}")
        raise e

def bronze_links(**kwargs):
    try:
        df = get_data_from_raw('links')
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'links')
    except Exception as e:
        logging.error(f"Error in bronze_links task: {str(e)}")
        raise e

def bronze_review(**kwargs):
    try:
        df = get_data_from_raw("IMDB_Dataset")
        logging.info(f"Table extracted with shape: {df.shape}")
        save_data_to_bronze(df, 'review')
    except Exception as e:
        logging.error(f"Error in bronze_review task: {str(e)}")
        raise e

# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'Bronze_Layer',
    default_args=default_args,
    description='A simple DAG to process raw data to bronze layer',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Định nghĩa các task
    task_bronze_keywords = PythonOperator(
        task_id='bronze_keywords',
        python_callable=bronze_keywords,
        provide_context=True
    )

    task_bronze_movies = PythonOperator(
        task_id='bronze_movies',
        python_callable=bronze_movies,
        provide_context=True
    )

    task_bronze_credits = PythonOperator(
        task_id='bronze_credits',
        python_callable=bronze_credits,
        provide_context=True
    )

    task_bronze_ratings = PythonOperator(
        task_id='bronze_ratings',
        python_callable=bronze_ratings,
        provide_context=True
    )

    task_bronze_links = PythonOperator(
        task_id='bronze_links',
        python_callable=bronze_links,
        provide_context=True
    )

    task_bronze_review = PythonOperator(
        task_id='bronze_review',
        python_callable=bronze_review,
        provide_context=True
    )

    # Thiết lập thứ tự chạy các task
    task_bronze_keywords >> task_bronze_movies >> task_bronze_credits >> task_bronze_ratings >> task_bronze_links >> task_bronze_review
