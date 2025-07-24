from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def test_func():
    print("✅ Airflow is working!")

default_args = {
    'start_date': datetime(2025, 7, 16),
    'catchup': False
}

with DAG(
    dag_id='test_kafka_dag',
    default_args=default_args,
    schedule_interval=None,
    description='Test DAG for Airflow setup',
) as dag:
    
    task = PythonOperator(
        task_id='print_hello',
        python_callable=test_func
    )
