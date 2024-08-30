from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

# Define the default_args for your DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 30),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'elt_pipeline_dag',
    default_args=default_args,
    description='A simple ELT pipeline DAG',
    schedule_interval='@daily',
)

# Define the tasks
def extract():
    os.system('python src/extract.py')

def load():
    os.system('python src/load.py')

def transform():
    os.system('pytest tests/test_transform.py')

# Create the tasks using PythonOperator
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# Define the task dependencies
extract_task >> load_task >> transform_task
