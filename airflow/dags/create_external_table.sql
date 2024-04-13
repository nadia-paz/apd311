CREATE OR REPLACE EXTERNAL TABLE `apd311.apd_stage.apd_external` (
      request_id STRING,
      status_desc STRING,
      type_desc STRING,
      method_received STRING,
      method_received_desc STRING,
      created_date TIMESTAMP,
      month_created STRING, --safe cast to int?
      month STRING,
      year INTEGER,
      status_date TIMESTAMP,
      case_duration_days INTEGER,
      location_county STRING,
      location_city STRING,
      location_zip_code STRING,
      location_lat FLOAT64,
      location_long FLOAT64,
) 
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://apd311/stage/part*']);