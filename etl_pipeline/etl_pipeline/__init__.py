from dagster import Definitions
from .assets.bronze_layer import country_vaccinations,covid19_country_wise,covid19_recovered,covid19_confirmed,hospital_icu_covid19,covid19_death
from .assets.silver_layer import silver_country_vaccination,silver_covid19_isu_patient_country,silver_covid19_infos
from .assets.gold_layer import gold_global_infos,gold_total_infos_covid19
from .assets.warehouse_layer import warehouse_total_infos_covid19,warehouse_global_infos
from .resources.MYSQLIOManager import MySQLIOManager
from .resources.MinioIOManager import MinioIOManager
from .resources.SparkIOManager import SparkIOManager
from .resources.PostgreIOManager import PostgreIOManager

import socket
import os

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "user": os.getenv("MYSQL_USER")
}

MINIO_CONFIG = {
    "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
    "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket_name": os.getenv("MINIO_BUCKET_NAME")
}

SPARK_CONFIG = {
    "spark_master": f"spark://{socket.gethostbyname('spark-master')}:7077",
    "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
    "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
    "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    "bucket_name": os.getenv("MINIO_BUCKET_NAME")
}
PSQL_CONFIG={
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


defs = Definitions(
    assets=[
    covid19_confirmed,
    covid19_recovered,
    hospital_icu_covid19,
    country_vaccinations,
    covid19_country_wise,
    covid19_death,
    silver_country_vaccination,
    silver_covid19_infos,
    silver_covid19_isu_patient_country,
    gold_global_infos,
    gold_total_infos_covid19,
    warehouse_global_infos,
    warehouse_total_infos_covid19
],
    resources ={
        'MYSQLIOManager': MySQLIOManager(MYSQL_CONFIG),
        'MinioIOManager': MinioIOManager(MINIO_CONFIG),
        'SparkIOManager': SparkIOManager(SPARK_CONFIG),
        'PostgreIOManager': PostgreIOManager(PSQL_CONFIG)
    }
)


