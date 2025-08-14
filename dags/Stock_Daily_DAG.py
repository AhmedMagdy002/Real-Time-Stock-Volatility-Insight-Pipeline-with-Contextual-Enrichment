# dags/fintech_daily_batch_dag.py
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'fintech-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    'fintech_daily_processing',
    default_args=default_args,
    description='Daily processing for fintech historical analysis',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['fintech', 'daily', 'historical']
) as dag:

    run_daily_processor = BashOperator(
    task_id='run_daily_processor',
    bash_command="""
    docker exec spark spark-submit \
      --master local[*] \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      /home/jovyan/work/daily_processor.py
    """,
    dag=dag
)