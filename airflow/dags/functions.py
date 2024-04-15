import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

# import logging

import requests
import json

from datetime import timedelta, datetime
from airflow import DAG


import pyarrow as pa
import pyarrow.parquet as pq

# google cloud imports
from google.cloud import storage
from google.cloud import dataproc_v1 as dataproc
from google.oauth2 import service_account


#### VARS #####
GOOGLE_APPLICATION_CREDENTIALS = os.environ['GOOGLE_APPLICATION_CREDENTIALS']
credentials = service_account.Credentials.from_service_account_file(
                    GOOGLE_APPLICATION_CREDENTIALS)
GCP_GCS_BUCKET = os.environ['GCP_GCS_BUCKET']
GCP_PROJECT_ID = os.environ['GCP_PROJECT_ID']
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
spark_filename = 'spark_job.py'
pyspark_uri = f"{GCP_GCS_BUCKET}/code/{spark_filename}"
region = 'us-west1'
cluster_name = 'apd-cluster'


#######

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
        local_file = f"{AIRFLOW_HOME}/{filename}"
        # save table into a file
        pq.write_table(table, local_file)
        # upload to gcs

        upload_to_gcs(GCP_GCS_BUCKET, object_name, local_file)
        os.remove(local_file)
        n += 1
    # ti.xcom_push(f"gs://{bucket_name}/raw/*")

def spark_job_file():
    object_name=f"code/{spark_filename}"
    local_file = f"{AIRFLOW_HOME}/dags/{spark_filename}"
    upload_to_gcs(GCP_GCS_BUCKET, object_name, local_file)
    print('Spark file uploaded')

def create_dataproc_cluster():
    # Create a Dataproc client
    client = dataproc.ClusterControllerClient(credentials = credentials,\
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'})

    # Define the cluster config
    cluster = {
        "project_id": GCP_PROJECT_ID,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n1-standard-2",
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n1-standard-2",
            },
            "config_bucket": "",
        },
    }

    # Create the cluster
    operation = client.create_cluster(
        request={"project_id": GCP_PROJECT_ID, "region": region, "cluster": cluster}
    )
    response = operation.result()

    print("Cluster created successfully.")

def delete_dataproc_cluster():
    # Create a Dataproc client
    client = dataproc.ClusterControllerClient(credentials = credentials,\
        client_options={'api_endpoint': f'{region}-dataproc.googleapis.com:443'})

    # Delete the cluster
    operation = client.delete_cluster(
        request={"project_id": GCP_PROJECT_ID, "region": region, "cluster_name": cluster_name}
    )
    response = operation.result()

    print("Cluster deleted successfully.")