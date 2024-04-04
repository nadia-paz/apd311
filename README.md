# dez-project

<h2 style="color:#777;">1. Project's description</h2>
<details><summary><i>Expand</i></summary>

</details>


<h2 style="color:#777;">2. Data Source and Aquisition</h2>
<details><summary><i>Expand</i></summary>

</details>

<h2 style="color:#777;">3. Prerequisites</h2>
<details><summary><i>Expand</i></summary>
The project created on Ubuntu 20.04 with Python 3.11.
To reproduce this project you need to have a Google Cloud Account (additional cost may apply). 
You need to install Docker and Docker Compose.
</details>

<h2 style="color:#777;">4. Installations</h2>

Please, make sure that you have all needed installations and install the missing ones following the instructions provided below.

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

### Google Cloud SDK
<details><summary><i>Expand</i></summary>

Please, follow the official installation guide from [Google Cloud](https://cloud.google.com/sdk/docs/install)

</details>




<h2 style="color:#777;">5. Google Cloud Credentials</h2>

To use Google Cloud SDK, Terraform you need to provide the access to your Google Cloud Account. Follow the steps below to create a service account and create an API key.

<details><summary><i>Expand</i></summary>

1. On GCP create a project.
2. Enable API's for your project:
    * [Identity and Access Management (IAM) API](https://console.cloud.google.com/apis/library/iam.googleapis.com)
    * [IAM Service Account Credentials API](https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com)
    Compute Engine API
* Cloud Dataproc API
* Cloud Dataproc Control API
* BigQuery API
* Bigquery Storage API
3. Navigate to __IAM & Admin__ -> Service Accounts.
4. Create a new service account for the project. Make sure to add the following roles into it:
    * BigQuery Admin
    * Compute Admine
    * Storage Admin
    * Storage Object Admin

    ---
    * Artifact Registry Reader
    * Artifact Registry Writer
    * Cloud Run Developer
    * Cloud SQL Admin
    * Service Account Token Creator
    * Owner

5. Download the `*.json` file with the service account key and store it on your machine. 
6. To add the environment variable with your credentials run in the terminal:
```bash
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
```
If you don't want to manually add credentials path each session, you can add the code above into `~/.bashrc` file.
7. Authentificate Google SDK.
```bash
gcloud auth application-default login
```

</details>





