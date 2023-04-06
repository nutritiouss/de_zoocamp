import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator
)


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'energy_data')
BIGQUERY_DATASET_PROD = os.environ.get("BIGQUERY_DATASET_PROD", 'energy_prod')

#varibles for load data
DATASET = "bq_energy_dataset"
PARTITION_COL = 'year'
CLUSTER_COL = 'city'
INPUT_PART = "raw"
INPUT_FILETYPE = "parquet"


#tables for prod
TABLE_CITIES = "whole_cities"
TABLE_ANNUAL_CONN = "annual_connections"


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 3,
}

folders  = []
years = ['2009', '2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020']
[folders.append(f"gs://{BUCKET}/{year}/*.parquet")for year in years]


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="3_gcs_to_bq",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['energy'],
) as dag:

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATASET}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": folders,
            },
        },
    )

    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATASET} \
        PARTITION BY DATE({PARTITION_COL}) \
        CLUSTER BY {CLUSTER_COL} AS \
        SELECT * FROM {BIGQUERY_DATASET}.{DATASET}_external_table;"
    )

    # Create a partitioned and clustered table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{DATASET}_optimized_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    CREATE_BQ_TBL_CITIES = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET_PROD}.{TABLE_CITIES} as\
        select city,sum(annual_consume) as whole \
        FROM {BIGQUERY_DATASET}.{DATASET} \
        group by city \
        order by 2 desc \
        limit 10;"
    )
    # Create a partitioned and clustered table from external table
    bq_create_table_cities = BigQueryInsertJobOperator(
        task_id=f"bq_create_{TABLE_CITIES}_table",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_CITIES,
                "useLegacySql": False,
            }
        }
    )

    CREATE_BQ_TBL_NUM_CONNECTIONS = (
        f"""CREATE OR REPLACE TABLE {BIGQUERY_DATASET_PROD}.{TABLE_ANNUAL_CONN} AS
         select year,
             max(case when  net_manager = 'Enexis B.V.' then delivery_perc else null end)  as EnexisBV ,
             max(case when  net_manager = 'Liander N.V.' then delivery_perc else null end) as LianderNV
         from (
              SELECT year,net_manager,avg(num_connections) as delivery_perc
              FROM {BIGQUERY_DATASET}.{DATASET}
              where net_manager in ('Enexis B.V.', 'Liander N.V.')
              group by year,net_manager
        )
        group by year
        order by 1;"""
    )
    # Create a partitioned and clustered table from external table
    bq_create_tbl_annual_connections = BigQueryInsertJobOperator(
        task_id=f"bq_create_{TABLE_ANNUAL_CONN}_table",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_NUM_CONNECTIONS,
                "useLegacySql": False,
            }
        }
    )

    bigquery_external_table_task >> bq_create_partitioned_table_job>>bq_create_table_cities>>bq_create_tbl_annual_connections
