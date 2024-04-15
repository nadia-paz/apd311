FROM apache/airflow:2.8.2

ENV AIRFLOW_HOME=/opt/airflow

USER root

RUN apt-get update -qq && apt-get install vim -qqq

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# USER 0

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --additional-components alpha beta kubectl \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && rm -rf "${GCLOUD_HOME}/.install/.backup/" \
    && gcloud --version

WORKDIR $AIRFLOW_HOME


USER $AIRFLOW_UID

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt