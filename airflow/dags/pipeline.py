from datetime import timedelta, datetime


# airflow imports
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# # google cloud imports
# from google.cloud import storage
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
import functions as f



default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}
# scheduler cron notation
# Minute Hour Day(month) Month Day(week)

with DAG(
    dag_id='pipeline_v0',
    default_args=default_args,
    start_date=datetime(2024, 4, 10),
    # every Sunday at 00:00
    end_date=datetime(2024, 6, 30),
    schedule='0 0 * * SUN'
) as dag:

    # saves data from Austin Public Data API as parquet files on GCS
    save_data_task = PythonOperator(
        task_id = 'save_data_task',
        python_callable = f.save_data,
        # (offset=1, limit = 10_000)
        op_kwargs = {
            "offset": 1,
            "limit": 100_000
        }
    )

    # loads spark file for data transformation into GCS
    spark_job_file_task = PythonOperator(
        task_id = 'spark_job_file_task', # change to dag -> only once
        python_callable = f.spark_job_file
    )

    # creates DataProc cluster on GCS
    create_cluster_task = PythonOperator(
        task_id = 'create_cluster_task',
        python_callable = f.create_dataproc_cluster
    )

    # submits spark job on Dataproc
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

    # deletes a cluster
    delete_cluster_task = PythonOperator(
        task_id = 'delete_cluster_task',
        python_callable = f.delete_dataproc_cluster
    )

    save_data_task >> \
        spark_job_file_task >> \
            create_cluster_task >> \
                submit_spark_job_task >> \
                    delete_cluster_task