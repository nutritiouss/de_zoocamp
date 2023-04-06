from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from helper import transformed_to_parquet, upload_to_bucket

args = {
        "owner": "airflow",
        "start_date": datetime(2009, 1, 1),
        "end_date": datetime(2020, 1, 1),
        "depends_on_past": True,
        "retries": 1,
}

dag = DAG(
    dag_id='2_transform_and_upload_GCS',
    default_args=args,
    description='transform data from CSC to parquet and upload to GCS yearly',
    schedule_interval="@yearly",
)

transformed_to_parquet = PythonOperator(
    task_id='transformed_to_parquet',
    python_callable=transformed_to_parquet,
    dag=dag,
    op_kwargs={
            "year": f"{{{{ execution_date.strftime(\'%Y\') }}}}",
        },
)

upload_to_bucket = PythonOperator(
    task_id='upload_to_bucket_S3',
    python_callable=upload_to_bucket,
    dag=dag,
    op_kwargs={
            "year": f"{{{{ execution_date.strftime(\'%Y\') }}}}",
        },
)
transformed_to_parquet>>upload_to_bucket