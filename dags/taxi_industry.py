from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
import runpy
import airflow
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
with DAG('taxi_industry') as dag:
    
    @task.bash(task_id='task00_convert_notebooks')
    def convert_notebooks():
        AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME")
        INPUT_PATH = f'{AIRFLOW_HOME}/include/taxi_industry'
        OUTPUT_PATH = f'{AIRFLOW_HOME}/include/taxi_industry/converted'

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
        runpy.run_path('include/taxi_industry/converted/task01-download.py', run_name='__main__')

    @task(task_id='task03_transform')
    def transform():
        print(airflow.__version__)
        runpy.run_path('include/taxi_industry/converted/task03-transform.py', run_name='__main__')


    @task(task_id='task04_write-to-db')
    def write_to_db():
        print(airflow.__version__)
        runpy.run_path('include/taxi_industry/converted/task04-write-to-db.py', run_name='__main__')

    convert_notebooks() >> download() >>  transform() >> write_to_db()

