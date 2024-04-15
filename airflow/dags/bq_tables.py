from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)

"""  
Creates 2 datasets and 2 tables on BigQuery:
1. Stage dataset with an external table linked to GCS parquet file (tranformed with Spark)
2. Main table partitioned by every month and clustered by the method the service request was received.

Tasks run every week on Sunday at 4 am till June, 30 2024
"""

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="create_tables_01",
    default_args=default_args,
    start_date=datetime(2024, 4, 12),
    # every Sunday at 04:00 AM GMT
    end_date=datetime(2024, 6, 30),
    schedule='0 4 * * SUN',
    catchup=False
) as dag:
    # EXTERNAL TABLE
    create_stage_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_stage_dataset_task",
        dataset_id="apd_stage",
        location="us",
    )
    create_stage_table_task = BigQueryInsertJobOperator(
        task_id="create_stage_table_task",
        configuration={
            "query": {
                "query": "create_external_table.sql",
                "useLegacySql": False,
            }
        },
    )
    # MAIN TABLE
    create_main_dataset_task = BigQueryCreateEmptyDatasetOperator(
        task_id="create_main_dataset_task",
        dataset_id="apd311",
        location="us",
    )
    create_main_table_task = BigQueryInsertJobOperator(
        task_id="create_main_table_task",
        configuration={
            "query": {
                "query": "create_partitioned_table.sql",
                "useLegacySql": False,
            }
        },
    )

create_stage_dataset_task >> \
    create_stage_table_task >> \
        create_main_dataset_task >> \
            create_main_table_task