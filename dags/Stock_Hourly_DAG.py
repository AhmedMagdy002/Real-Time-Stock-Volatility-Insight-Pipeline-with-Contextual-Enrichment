# dags/fintech_hourly_processing_v2.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'fintech-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['alerts@example.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'fintech_hourly_processing_v2',
    default_args=default_args,
    description='Hourly with schema + business quality checks',
    schedule_interval='5 * * * *',  # every hour at 5 min past
    catchup=False,
    max_active_runs=1
) as dag:

    run_hourly_processor = BashOperator(
        task_id='run_hourly_processor',
        bash_command="""
        docker exec spark spark-submit \
          --master local[*] \
          --packages io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /home/jovyan/work/hourly_processor.py
        """
    )

    validate_hourly = BashOperator(
        task_id="validate_hourly",
        bash_command="""
        docker exec spark spark-submit \
          --master local[*] \
          --packages io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /home/jovyan/work/validate_hourly.py
        """
    )

    hourly_quality_checks = BashOperator(
        task_id="hourly_quality_checks",
        bash_command="""
        docker exec spark spark-submit \
          --master local[*] \
          --packages io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /home/jovyan/work/hourly_quality_checks.py
        """
    )

    run_hourly_processor >> validate_hourly >> hourly_quality_checks