# from https://www.youtube.com/watch?v=ZerBdBHPusA
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

with DAG('dag_example_2', start_date=datetime(2022,1,1)) as dag:
    
    submit_job = SparkSubmitOperator(
        task_id='submit_job',
        application='include/dag_example_2/pyspark_script.py',
        conn_id='my_spark_conn',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='2g',
        num_executors='1',
        driver_memory='2g',
        verbose=False
    )

    submit_job