CREATE OR REPLACE TABLE `apd311.apd311.main_table` 
  PARTITION BY DATETIME_TRUNC(created_date, MONTH)
  -- RANGE BETWEEN TIMESTAMP_SUB(DATE(month_created + '-01'), INTERVAL 1 MONTH) AND TIMESTAMP_ADD(DATE(month_created + '-01'), INTERVAL 1 MONTH)
  CLUSTER BY method_received AS (
    SELECT * FROM `apd311.apd_stage.apd_external`
  );