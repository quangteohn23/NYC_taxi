import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator # có thể dùng Dummy nhưng mà hơi cũ

from scripts_pipeline.extract import extract_load
from scripts_pipeline.transform_data import transform_data
from scripts_pipeline.load_delta import load_to_delta

default_args = {
    "owner": "thienquang",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

with DAG(
    "etl_pipeline", 
    start_date=datetime(2024, 1, 1), 
    schedule=None, 
    default_args=default_args, 
    catchup=False
) as dag:
    
    start_pipeline = EmptyOperator(task_id="start_pipeline")
    
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_load,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY
        }
    )
    

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY
        }
    )
    
    load_delta_task = PythonOperator(
        task_id="load_delta",
        python_callable=load_to_delta,
        op_kwargs={
            'endpoint_url': MINIO_ENDPOINT, 
            'access_key': MINIO_ACCESS_KEY, 
            'secret_key': MINIO_SECRET_KEY
        }
    )
    
    end_pipeline = EmptyOperator(task_id="end_pipeline")
    
    start_pipeline >> extract_task >> transform_task >> load_delta_task >> end_pipeline