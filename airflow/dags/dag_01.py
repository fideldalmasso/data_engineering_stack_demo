from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG('dag_01') as dag:
        
    extract = SparkSubmitOperator(
        task_id='extract',
        application='include/extract.py',
        conn_id='my_spark_conn',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='512m',
        num_executors='1',
        driver_memory='2g',
        verbose=True
    )

    extract