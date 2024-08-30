from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
from pathlib import Path

sys.path.append('/opt/airflow/src')

from extract import extract_data
from load import load_data
from transform import transform_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'elt_pipeline',
    default_args=default_args,
    description='Pipeline ELT que extrae, carga y transforma los datos',
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

extract_task >> load_task >> transform_task
