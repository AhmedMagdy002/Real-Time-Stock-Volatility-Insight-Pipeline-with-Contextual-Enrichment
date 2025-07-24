from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 7, 20),
}

with DAG('kafka_spark_pipeline',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    run_producer = BashOperator(
        task_id='run_kafka_producer',
        bash_command='python3 /opt/airflow/work/simple_producer.py'
    )

    run_spark_consumer = BashOperator(
        task_id='run_spark_consumer',
        bash_command="""
        docker exec spark spark-submit \
          --master local[*] \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,\
io.delta:delta-core_2.12:2.4.0,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
          /home/jovyan/work/simple_spark_consumer.py
        """
    )

    run_producer >> run_spark_consumer
