# dags/fintech_hourly_batch_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'fintech-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'fintech_hourly_processing',
    default_args=default_args,
    description='Hourly processing for fintech dashboard',
    schedule_interval='5 * * * *',  # Every hour at 5 minutes past (e.g., 10:05, 11:05)
    catchup=False,
    max_active_runs=1,
    tags=['fintech', 'hourly', 'dashboard']
) as dag:

    run_hourly_processor = BashOperator(
    task_id='run_hourly_processor',
    bash_command="""
    docker exec spark spark-submit \
      --master local[*] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      /home/jovyan/work/hourly_processor.py
    """
)