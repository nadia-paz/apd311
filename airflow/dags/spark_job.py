import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from google.cloud import storage

project_id = 'apd311'
bucket_name = 'apd311'
input_path = f"gs://{bucket_name}/raw/*"
output_gs = f"gs://{bucket_name}/stage"
output_bq = 'apd311.stage'

# to run on Dataproc we don't create a master!
spark = SparkSession.builder \
    .appName('apd311') \
    .getOrCreate()

# load data from GCS
df = spark.read.parquet(input_path)
print('DF loaded')

# the amount of null values is small, we can safely drop them
df = df.dropna()
print('Dropped nulls')

# create columns month(December), year (YYYY), month created(YYYYMM) based on created date
df = df.withColumn(
    'month', F.date_format(df.created_date, 'MMMM')
    ).withColumn(
    'monthx', F.month('created_date')
    ).withColumn(
    'year', F.year('created_date')
    ).withColumn('month_created', F.concat('year', 'monthx')).drop('monthx')

print('Created date columns')
# standardize location_city
replace_dict = {
        'Travis County':'Austin',
        'Dripping Sprin':'Dripping Springs',
        'Austin 3-1-1':'Austin',
        'West Lake Hill':'West Lake Hills',
        'A':'Austin',
        'Austin 5 Etj':'Austin',
        'Village Of The Hills':'The Hills',
        'Village Of San Leanna':'San Leanna',
        'Village Of Webberville':'Webberville',
        'Village Of Creedmoor':'Creedmoor',
        'Village Of Mustang Ridge':'Mustang Ridge',
        'Village Of Point Venture':'Point Venture',
        'Village Of Briarcliff':'Briarcliff',
        'Village Of Volente':'Volente',
        'Aust':'Austin',
        'Ausitn':'Austin',
        'Austibn':'Austin',
        'Austn':'Austin',
        'Austi':'Austin',
        'Aus':'Austin',
        'Atx':'Austin',
        'Au':'Austin',
        'Austtin':'Austin',
        'Austin `':'Austin',
        'Austin.':'Austin',
        'Austun':'Austin',
        'Austin, Tx':'Austin'
}

df = df.withColumn(
    'location_city', F.initcap('location_city')
).withColumn(
    'location_city', F.col('location_city')
).replace(replace_dict)

# standardize status description
replace_status = {
        'Work In Progress':'Open',
        'Duplicate (closed)':'Duplicate',
        'Duplicate (open)':'Duplicate',
        'TO BE DELETED':'Closed',
        'Resolved':'Closed',
        'Closed -Incomplete Information':'Closed -Incomplete',
        'New':'Open',
        'CancelledTesting':'Closed'
}
df = df.replace(replace_status, subset='status_desc')

# create a new column method received, transform detailed description into generalized one
df = df.withColumn(
    'method_received', 
    F.when((F.col('method_received_desc') == 'Mobile Created'), 'app')\
    .when((F.col('method_received_desc') == 'Spot311 Interface'), 'app')\
    .when((F.col('method_received_desc') == 'CSR - Follow On SR'), 'app')\
    .when((F.col('method_received_desc') == 'PremierOne CSR Mob'), 'app')\
    .when((F.col('method_received_desc') == 'Mobile Device'), 'app')\
    .when((F.col('method_received_desc') =='Open311'), 'phone')\
    .when((F.col('method_received_desc') =='Phone'), 'phone')\
    .when((F.col('method_received_desc') =='E-Mail'), 'e-mail')\
    .when((F.col('method_received_desc') =='Web'), 'web')\
    .when((F.col('method_received_desc') =='External Interface'), 'web')\
    .otherwise('other')
)
print('Standartized values')
# new column how many days case was open: status date - created date
df = df.withColumn(
    'case_duration_days', 
    F.datediff(
        F.to_date(F.col('status_date')),
        F.to_date(F.col('created_date'))
        )
)
print('Created case duration days')
# if the status is not closed yet, replace with null
df = df.withColumn(
    'case_duration_days', 
    F.when(F.col('status_desc') == 'Closed', F.col('case_duration_days'))\
    .otherwise(None)
)

# reorder columns

# drop 'updated_date' - > not needed for analysis
# drop 'location_x', 'location_y' -> state plane coordinates

new_order = ['request_id', 'status_desc', 'type_desc', 'method_received', 'method_received_desc',
       'created_date', 'month_created', 'month', 'year', 'status_date', 'case_duration_days',
        'location_county', 'location_city', 'location_zip_code',
       'location_lat', 'location_long', 
       ]

df = df.select(new_order)
print('Reordered')

df.coalesce(1).write.parquet(output_gs, mode='overwrite')
print('Saved to GCS')