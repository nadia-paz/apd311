import os
import logging

import requests
import json

from datetime import timedelta, datetime

#import pyarrow.csv as pv

import pyarrow as pa
import pyarrow.parquet as pq


# airflow imports
from airflow import DAG
from airflow.utils.dates import days_ago
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# google cloud imports
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

#### VARS ###

# project_id = os.environ.get("GCP_PROJECT_ID")
# bucket_name = os.environ.get("GCP_GCS_BUCKET")

#cr = "~/.gc/apd311.json"
#  os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "~/.gc/apd311.json"
cr = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
# bucket_name = os.environ['GCP_GCS_BUCKET']
# project_id = os.environ['GCP_PROJECT_ID']
project_id = 'apd311'
bucket_name = 'apd311'
pq_file = 'apd311_row.parquet'
#table_name = "row"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def get_json(ti, offset=1, limit = 100_000):
    while True:
        url = 'https://data.austintexas.gov/resource/xwdj-i9he.json'
        
        params = {
            "$limit": limit,
            "$offset": offset
        }

        # Make the GET request to the API
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an HTTPError for bad responses
        data_json = response.json()
        print(f'start {offset} with {len(data_json)} records')

        # if the page has no records, stop iterating
        if data_json:
            yield data_json
            if len(data_json) < limit:
                print('Exit from len(data) with offset', offset)
                ti.xcom_push(key='offset', value=offset)
                break
            else:
                offset += limit
                #ti.xcom_push(key='offset', value=offset)
        else:
            # No more data, break the loop
            break

def preprocess_data(data_json):
    """ 
    Parameters:
        data_json: list of dictionaries from API request
    """
    # create a table
    # schema with data types throws errors
    # data types changed throgh casting below

    py_table = pa.Table.from_pylist(data_json)
    # drop not needed columns
    py_table = py_table.drop_columns([
            'sr_location',
            # 'sr_location_council_district',
            'sr_location_lat_long', 
            'sr_location_map_tile', 
            'sr_location_map_page',
            # 'sr_location_street_number',
            'sr_location_street_name'])
    if 'sr_location_street_number' in py_table.column_names:
        py_table = py_table.drop_columns('sr_location_street_number')
    if 'sr_location_council_district' in py_table.column_names:
        py_table = py_table.drop_columns('sr_location_council_district')
    
        
    # rename columns, remove sr_ prefix
    cols = py_table.column_names
    cols = [col[3:] if col!='sr_number' else 'request_id' for col in cols]
    py_table = py_table.rename_columns(cols)

    # sort columns by type and info
    request_info = ['request_id', 'status_desc', 'type_desc', 'method_received_desc']
    date_info = ['created_date', 'status_date', 'updated_date']
    address_info = ['location_county', 'location_city', 'location_zip_code']
    gis_info = ['location_x', 'location_y', 'location_lat', 'location_long']

    # cast date 
    for col in date_info:
        arr = py_table[col].cast(pa.timestamp('ms'))
        py_table = py_table.drop_columns(col)
        py_table = py_table.append_column(col, arr)
    
    # cast gis info to float
    for col in gis_info:
        arr = py_table[col].cast(pa.float64())
        py_table = py_table.drop_columns(col)
        py_table = py_table.append_column(col, arr)
    
    # new column order
    new_order = request_info + date_info + address_info + gis_info
    return py_table.select(new_order)

def upload_to_gcs(bucket_name, object_name, pq_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    # f"raw/{pq_file}"
    blob = bucket.blob(object_name)
    blob.upload_from_filename(pq_file)

def save_data(ti, offset=1, limit = 100_000):
    # extract
    n = 1
    for data in get_json(ti, offset, limit):
        
        #data = [*data, *j]
        # preprocess
        table = preprocess_data(data)
        #table = pa.Table.from_pylist(data)
        # write to local file
        filename = f'data_{n:02d}.parquet'
        object_name = f"raw/{filename}"
        local_file = f"{path_to_local_home}/{filename}"
        # save table into a file
        pq.write_table(table, local_file)
        # upload to gcs

        upload_to_gcs(bucket_name, object_name, local_file)
        os.remove(local_file)
        n += 1

default_args = {
    'owner': 'airflow',
    'retries': 0
    # 'retry_delay': timedelta(minutes=1)
}
# scheduler cron notation
# Minute Hour Day(month) Month Day(week)

with DAG(
    dag_id='ETL_v07',
    default_args=default_args,
    start_date=datetime(2024, 4, 10),
    # every Sunday at 00:00
    end_date=datetime(2024, 6, 30),
    schedule='0 0 * * SUN'
) as dag:
    save_data_task = PythonOperator(
        task_id = 'save_data_task',
        python_callable = save_data,
        # (offset=1, limit = 10_000)
        op_kwargs = {
            "offset": 1,
            "limit": 100_000
        }
    )