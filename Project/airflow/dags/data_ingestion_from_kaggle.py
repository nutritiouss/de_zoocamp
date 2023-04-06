import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from helper_kaggle import download_kaggle_dataset

CWD = os.getcwd()
PATH_DATA = os.path.abspath(os.path.join(CWD,'data'))

args = {
        "owner": "airflow",
        "start_date": days_ago(1),
        "depends_on_past": False,
        "retries": 1,
}

dag = DAG(
    dag_id='1_download_kaggle_dataset',
    default_args=args,
    description='download dataset from kaggle with API key',
    schedule_interval="@yearly",
)

download_kaggle_dataset = PythonOperator(
    task_id='download_kaggle_dataset',
    python_callable=download_kaggle_dataset,
    dag=dag,
)
unzip_dataset_task = BashOperator(
    task_id="unzip_dataset_task",
    dag = dag,
    bash_command=f"unzip -o {PATH_DATA}/dutch-energy.zip -d {PATH_DATA}&&rm {PATH_DATA}/dutch-energy.zip",
)

download_kaggle_dataset>>unzip_dataset_task