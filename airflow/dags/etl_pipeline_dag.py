import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator
from airflow.operators.empty import EmptyOperator  # (có thể dùng Dummy nhưng mà nó hơi cũ)

default_args = {
    "owner": "thienquang",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

DAG_FOLDER = os.path.dirname(os.path.abspath(__file__))
PATH_TO_PYTHON_DATA = os.path.join(DAG_FOLDER, '../../venv_data/bin/python')

MINIO_ENDPOINT = os.environ.get('MINIO_ENDPOINT')
MINIO_ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

def extract_wrapper(endpoint_url, access_key, secret_key):
    from scripts_pipeline.extract import extract
    return extract(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)

def transform_wrapper(endpoint_url, access_key, secret_key):
    from scripts_pipeline.transform_data import transform_data
    return transform_data(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)

def load_delta_wrapper(endpoint_url, access_key, secret_key):
    from scripts_pipeline.load_delta import load_delta
    return load_delta(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)


with DAG("etl_pipeline", start_date=datetime(2026, 3, 22), schedule=None, default_args=default_args) as dag:
    
    start_pipeline = EmptyOperator(task_id="start_pipeline")
    
    extract_task = ExternalPythonOperator(
        task_id="extract",
        python=PATH_TO_PYTHON_DATA,
        python_callable=extract_wrapper,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )
    
    transform_task = ExternalPythonOperator(
        task_id="transform_data",
        python=PATH_TO_PYTHON_DATA,
        python_callable=transform_wrapper,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )
    
    load_delta_task = ExternalPythonOperator(
        task_id="load_delta",
        python=PATH_TO_PYTHON_DATA,
        python_callable=load_delta_wrapper,
        op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY}
    )
    
    end_pipeline = EmptyOperator(task_id="end_pipeline")
    
    start_pipeline >> extract_task >> transform_task >> load_delta_task >> end_pipeline