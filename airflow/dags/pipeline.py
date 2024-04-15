from datetime import timedelta, datetime

# airflow imports
from airflow import DAG
#from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# # google cloud imports
# from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

import functions as f


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    'retry_delay': timedelta(minutes=5)
}
# scheduler cron notation
# Minute Hour Day(month) Month Day(week)

"""  
Creates  DAGs.
1. dag1 -> scheduled to perform only once, loads a spark file into a Google Cloud Storage bucket
2. dag2 -> the main data DAG, scheduled to run weeakly each Sunday at midnight til June, 30 2024:
            * loads data from API and saves into GCS
            * creates a Dataproc cluster
            * submits a spark job for data transformations
            * deletes a Dataproc cluster
"""

with DAG(
    dag_id='upload_spark_file',
    default_args=default_args,
    start_date=datetime(2024, 4, 12),
    schedule='@once',
    catchup=False
) as dag1:
    spark_job_file_task = PythonOperator(
        task_id = 'spark_job_file_task', 
        python_callable = f.spark_job_file
    )

with DAG(
    dag_id='pipeline_v01',
    default_args=default_args,
    start_date=datetime(2024, 4, 12),
    # every Sunday at 00:00
    end_date=datetime(2024, 6, 30),
    schedule='0 0 * * SUN'
) as dag2:
    # load data to GCS
    save_data_task = PythonOperator(
        task_id = 'save_data_task',
        python_callable = f.save_data,
        # (offset=1, limit = 10_000)
        op_kwargs = {
            "offset": 1,
            "limit": 100_000
        }
    )

    # create a Dataproc cluster
    create_cluster_task = PythonOperator(
        task_id = 'create_cluster_task',
        python_callable = f.create_dataproc_cluster
    )

    # submit spark job to Dataproc
    submit_spark_job_task = BashOperator(
        task_id = 'submit_spark_job_task',
        bash_command = f"""
        gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS &&\
        gcloud dataproc jobs submit pyspark \
            --project={f.GCP_PROJECT_ID}\
            --cluster={f.cluster_name} \
            --region={f.region} \
            gs://{f.GCP_GCS_BUCKET}/code/{f.spark_filename} 
        """
    )

    # delete Dataproc cluster
    delete_cluster_task = PythonOperator(
        task_id = 'delete_cluster_task',
        python_callable = f.delete_dataproc_cluster
    )

    save_data_task >> \
        create_cluster_task >> \
            submit_spark_job_task >> \
                delete_cluster_task