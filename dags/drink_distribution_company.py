from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import runpy
import airflow
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
with DAG('drink_distribution_company') as dag:
    
    @task.bash(task_id='task00_convert_notebooks')
    def convert_notebooks():
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
        INPUT_PATH = f'{AIRFLOW_HOME}/include/drink_distribution_company'
        OUTPUT_PATH = f'{AIRFLOW_HOME}/include/drink_distribution_company/converted'

        nb_files = os.listdir(INPUT_PATH)
        cmds = []
        for file in nb_files:
            if file.endswith('.ipynb'):
                cmds.append(f'jupyter nbconvert --to python {INPUT_PATH}/{file} --output {OUTPUT_PATH}/{file.replace('.ipynb','.py')}')
        res = ' & '.join(cmds)
        return res

    @task(task_id='task01_download')
    def download():
        print(airflow.__version__)
        runpy.run_path('include/drink_distribution_company/converted/task01-download.py', run_name='__main__')

    @task(task_id='task02_unzip')
    def unzip():
        print(airflow.__version__)
        runpy.run_path('include/drink_distribution_company/converted/task02-unzip.py', run_name='__main__')

    # @task(task_id='task03_transform')
    # def transform():
    #     print(airflow.__version__)
    #     runpy.run_path('include/drink_distribution_company/converted/task03-transform.py', run_name='__main__')

    transform = SparkSubmitOperator(
        task_id='task03_transform',
        application='include/drink_distribution_company/converted/task03-transform.py',
        conn_id='my_spark_conn',
        total_executor_cores='1',
        executor_cores='1',
        executor_memory='512m',
        num_executors='1',
        driver_memory='2g',
        verbose=True
    )

    @task(task_id='task04_write-to-db')
    def write_to_db():
        print(airflow.__version__)
        runpy.run_path('include/drink_distribution_company/converted/task04-write-to-db.py', run_name='__main__')

    convert_notebooks() >> download() >> unzip() >> transform >> write_to_db()

