import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext


#### VARS #####
cr = '/Users/nadina/.gc/apd311.json'
project_id = 'apd311'
bucket_name = 'apd311'

from pyspark.sql import types
from pyspark.sql import functions as F

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('apd311') \
    .set("spark.jars", "./gcs-connector-hadoop3-2.2.5.jar") \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", cr)


sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()
# the code below tells spark that when there is a location 
# "gs://..." it needs to connect to GCS
hadoop_conf.set("fs.AbstractFileSystem.gs.impl",  "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", cr)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()

df = spark.read.parquet(f"gs://{bucket_name}/raw/*")

# the amount of null values is small, we can safely drop them
df = df.dropna()

# create columns month and year based on created date
df = df.withColumn(
    'month_created', F.month('created_date')
    ).withColumn(
    'year_created', F.year('created_date')
    )

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


