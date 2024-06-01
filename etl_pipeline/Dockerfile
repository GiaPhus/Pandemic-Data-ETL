#FROM python:3.9.16-slim
FROM python:3.11-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
USER root

ARG openjdk_version="17"

RUN apt-get update --yes && \
    apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


WORKDIR /opt/dagster/app

COPY requirements.txt /opt/dagster/app
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . /opt/dagster/app/etl_pipeline
CMD ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "etl_pipeline"]