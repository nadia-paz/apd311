# Austin Service Explorer

<h2 style="color:#777;">1. Project's description</h2>

This project is a part of the Data Engineering Zoomcamp. The main goal of the project is to seamlessly transfer data from the source into a data analytics dashboard using data engineering tools. The project uses the City of Austin Open Data portal to collect over 1.8 million service requests originating from the city and its metropolitan area. These requests date back to 01/03/2014 and are updated daily at 4:00 am. The dataset contains various service requests made by residents, covering issues ranging from park maintenance to broken traffic lights and beyond.

The project attempts to collect the city's service requests data, clean, validate and prepare it for data analysis. Airflow is used as a main orchestration tool to coordinate a workflow: data extraction, preprocessing, storage, and transformation.


Extracting:
* The data was retrieved through API requests.

Preprocessing: 
* The correct data types were assined to the collected data, performed formatting, dropped non-meaningful columns and redundant information.

Storage: 
* The preprocessed data is stored on the Google Cloud Storage bucket as a collection multiple _parquet_ files.

Transformation: 
* With the help of PySpark, the data was cleaned and standardized to ensure its quality. For example, values like _Austin, AUSTIN, AUS, and austin_ were replaced with only one standard _Austin_, and all errors in city names and locations were removed. In the data transformation step, I performed as well feature engineering and created new columns, for example: generalizing types of service requests, bringing them down to phone, email, web, app, and other; created the column that calculates how long the case was opened, extracted the values like month and year.

Load:
* The transformed data has been saved on the GCS bucket and then loaded into BigQuery as an external table. With the help of SQL query I created as well a table that is partitioned by date, using monthly intervals, and clustered by the method, the service request was received. These transformations help to optimize the SQL queries' performance.


<h2 style="color:#777;">2. Technology stack of the project</h2>

<img src="./images/tech_stack.png" alt="Tasks">
<br>
<br>

* Infrastracture as Code: Terraform
* Data Lake: Google Cloud Storage
* Data Warehouse: BigQuery
* Orchestration: Airflow
* Data Transformations: Spark
* Serverless Cluster Service: Dataproc
* Containerization: Docker
* Data Visualisation: Looker


<h2 style="color:#777;">3. Prerequisites</h2>
<details><summary><i>Expand</i></summary>
To reproduce this project you need to have a Google Cloud Account (additional cost may apply), have Docker and Docker Compose installed, have at least 5GB of free disk space to load docker images and start the project. 
</details>

<h2 style="color:#777;">4. Installations</h2>

Please ensure that you have all the necessary installations and install any missing ones by following the instructions provided below.

### Docker
<details><summary><i>Expand</i></summary>

```bash
sudo apt-get update
sudo apr-get install docker.io
```
</details>

### docker-compose
<details><summary><i>Expand</i></summary>

1. Check if you have `~/bin/` directory. If not, create on with the command 
`cd && mkdir bin`
2. Move to the `bin/` with the command `cd bin`.
3. Download the binary file from [Docker's GihHub repository](https://github.com/docker/compose/releases).
```bash
wget https://github.com/docker/compose/releases/download/v2.24.1/docker-compose-linux-x86_64
```
4. Rename the file to `docker-compose`
5. Make it executable `chmod +x docker-compose`
6. Open `.bashrc` file by running command `nano ~/.bashrc` and add the following line of code in the end of the file:
```bash
export PATH="${HOME}/bin:${PATH}"
``` 
7. Run `source ~/.bashrc`
8. Check if docker compose works:
    * `which docker-compose` should return the path to `~/bin/docker-compose`
    * `docker-compose --version` should return the version you installed.

</details>

### Terraform
<details><summary><i>Expand</i></summary>

1. Download a binary file from [Terraform site](https://developer.hashicorp.com/terraform/install)
2. Unzip it into the `~/bin` directory.
3. Check the terraform version by running a command `terraform --version`

</details>

### Google Cloud SDK _- Optional_
<details><summary><i>Expand</i></summary>
Google Cloud SDK will be installed automatically on project's Docker Image. If you'd like to install it locally as well, follow the official installation guide from [Google Cloud](https://cloud.google.com/sdk/docs/install)

</details>

<h2 style="color:#777;">5. Google Cloud Credentials</h2>

To use Google Cloud SDK and Terraform, you need to grant access to your Google Cloud Account. Follow the steps below to prepare your GCP account for the project.

<details><summary><i>Expand</i></summary>

1. On GCP create a project.
2. Enable API's for your project:
    * [Identity and Access Management (IAM) API](https://console.cloud.google.com/apis/library/iam.googleapis.com)
    * [IAM Service Account Credentials API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)
    * Compute Engine API
    * Cloud Dataproc API
    * BigQuery API
    * Bigquery Storage API

3. Navigate to __IAM & Admin__ -> Service Accounts.
4. Create a new service account for the project. Make sure to add the following roles into it:
    * BigQuery Admin
    * Compute Admine
    * Storage Admin
    * Storage Object Admin
    * Dataproc Administrator
    * Owner

5. Click on the name of the service account and move to keys. Create a `*.json` file with a key, download it, and rename as `apd311.json`. In the root dicrectory create a folder `.gc` and move the key file into that folder:

```bash
mkdir ~/.gc
mv apd311.json ~/.gc/apd311.json
```
Alternativaly, you can store it in your preffered location and replace the path to the file in the `docker-compose` and `.env` files.

_The next 2 steps are valid only for those, who choose to install Google SDK locally:_
6. To add the environment variable with your credentials run in the terminal:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="~/.gc/apd311.json"
```
If you don't want to manually add credentials path each session, you can add the code above into `~/.bashrc` file.
7. Authentificate Google SDK.
```bash
gcloud auth application-default login
```
</details>

<h2 style="color:#777;">6. Reproduce the project </h2>

1. Clone the project from GitHub
```bash
https://github.com/nadia-paz/apd311.git
```
or download it by clicking on Code -> Download ZIP

2. In the terminal move into the project's folder `apd311`.

**Note!!!** GCP buckets and project names are globally unique. You won't be able to create and/or use the same variables that I do. For reproducing the code you'll need to replace     `GCP_PROJECT_ID` and `GCP_GCS_BUCKET` values with your own. Put your caustom values in `docker-compose.yaml` file located in `airflow` directory (section `x-airflow-common:` -> `environment`). If you stored and named the secret key other than `~/.gc/apd311.json`, you need to add custom value in `volumes` section as well (In the code lines: 11,12,14,15,30). You can move to the next step only after you updated the information.

3. Move to `terraform` directory. Run the commands `terraform init` and `terraform apply`. 

4. Move back to the airflow directoy `cd .. && cd airflow`

5. In addition to `dags` directory, you'll need to create directories `plugins` and`logs`. 
```bash
mkdir plugins logs
```
**Note for Linux users!!!** You might need to do some workaround before starting Docker. Run in terminal 
```bash
echo $(id -u)
```
In the `.env`file replace the default numeric value of `AIRFLOW_UID` with the one you've got in the terminal.

6. Now we are ready to start our Docker. First, we need to rebuild the official airflow image to make it fit our needs. Then we are ready to start **Airflow**. In the `airflow` directory step by step run the commands:
```bash
sudo docker build . -t airflow-plus:latest
docker-compose up airflow-init
docker-compose up -d
```

With the command `docker ps` check if all containers started. You should see:
* airflow-webserver
* airflow-scheduler
* airflow-init
* postgres


7. If everything runs OK, we can login into Airflow Web. In the browser go to `http://localhost:8080`, enter the *login*: `airflow`, and *password*: `airflow`. On the home page you will see 3 DAGs (Directed Acyclic Graphs), that organize tasks together. Activate all DAGs by clicking on toggle button next to them.

### DAGs and Tasks
All DAGs are scheduled to run. Alternatively, you can manually trigger them in the order: 
* `upload_spark_file` >> `pipeline` >> `create_tables`

#### `upload_spark_file`
Runs -> once on April, 11 2024, not scheduled.
Contains one task `spark_job_file_task` that uploads the file `spark_job.py` to Google Cloud Storage.

<img src="./images/upload_spark_file.png"  width="300" height="100">

#### `pipeline`
* Runs -> scheduled to run every week on Sunday at midnight GMT. 
* Start date: April, 12 2024. 
* End date: June 30, 2024
* Tasks:
    * `save_data_task` -> extracts data from [City of Austin Open Data Portal](https://data.austintexas.gov/Utilities-and-City-Services/Austin-311-Public-Data) and loads it onto GCS bucket.
    * `create_cluster_task` -> creates Dataproc cluster on Google Cloud Platform
    * `submit_spark_job_task` -> submits Spark Job into Dataproc cluster.
    * `delete_cluster_task` -> deletes Dataproc cluster

<img src="./images/pipeline.png" alt="Tasks">

#### `create_tables`
* Runs -> scheduled to run every week on Sunday at 4:00 AM GMT. 
* Start date: April, 12 2024. 
* End date: June 30, 2024
* Tasks:
    * _for external table:_
        * `create_stage_dataset_task` 
        * `create_stage_table_task`
    * _for partitioned table:_
        * `create_main_dataset_task`
        * `create_main_table_task`

<img src="./images/create_tables.png" alt="Tasks">

After the tasks finish their run, you can move to BigQuery to work with data. Look for dataset `apd311`, and table `main_table`.

<h2 style="color:#777;">7. Analyze data with Looker </h2>

To create your own report with [Looker Studio](https://lookerstudio.google.com/), click __Create__ , pick **Data Source** -> BigQuery -> Project Name -> Dataset `apd311` -> Table `main_table`, press **Create report**. Make sure that you *change all location fields to Geo data type*. Press the `ABC` icon next to `location_sity`, `location_county`, `location_zip` fields, and the `123` icon next to `location_lat` and `location_long`. Pick `GEO`. Create a dashboard.

[My Analytical Dashboard](https://lookerstudio.google.com/s/rHWz0UUdnNo)

<img src="./images/dashboard.png" alt="Tasks">


<h2 style="color:#777;">8. Clean resources </h2>

1. In the terminal move to the `terraform` directory and run the command `terraform destroy`
2. Go to [Google Cloud Storge](https://console.cloud.google.com/storage) and manually delete Dataproc clusters.
3. Move to `airflow` directory and disconnect `Docker` by runnining command
```bash
docker-compose down --rmi "all"  --volumes
```
`--volume` and `-rmi` are optional flags to remove all volumes (`--volume`) and images (`-rmi`) from your computer.

4. Delete the project's directory.