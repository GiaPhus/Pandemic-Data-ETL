import inspect
from dagster import asset,Output,AssetIn
import pandas as pd
import os
from ..resources.SparkIOManager import get_spark_session
from datetime import datetime


@asset(
    ins={
        "silver_covid19_isu_patient_country": AssetIn(key_prefix=["silver", "covy"]),
        "silver_country_vaccination": AssetIn(key_prefix=["silver", "covy"]),
        "silver_covid19_infos": AssetIn(key_prefix=["silver", "covy"]),
    },

    description="global day-by-day covy WHO",
    compute_kind="plotly",
    key_prefix=["gold", "covy"],
    io_manager_key="SparkIOManager",
    group_name="gold"
)
def gold_global_infos(silver_covid19_isu_patient_country:pd.DataFrame,
                   silver_country_vaccination:pd.DataFrame,
                   silver_covid19_infos:pd.DataFrame)-> Output[pd.DataFrame]:
    config = {
        "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
        "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    current_function_name = inspect.currentframe().f_code.co_name

    # Sử dụng context manager để tạo phiên Spark
    with get_spark_session(config, current_function_name) as spark:
        spark_covid19_isu = spark.createDataFrame(silver_covid19_isu_patient_country)
        spark_country_vaccination= spark.createDataFrame(silver_country_vaccination)
        spark_covid19_info = spark.createDataFrame(silver_covid19_infos)
        spark_covid19_isu.createOrReplaceTempView("icu_patient")
        spark_country_vaccination.createOrReplaceTempView("vaccination")
        spark_covid19_info.createOrReplaceTempView("covid_info")

        query = '''
        SELECT
            c.Date,
            c.region,
            v.total_vaccinations,
            v.people_vaccinated,
            v.people_fully_vaccinated,
            c.Cofirmed,
            c.Recovered,
            c.Death,
            i.icu_patients,
            i.hosp_patient,
            c.WHO_Region,
            c.Lat,
            c.Long
        FROM
            covid_info c 
         LEFT JOIN
             vaccination v ON v.date = c.Date AND v.Country = c.region
         LEFT JOIN
            icu_patient i ON v.date = i.date AND c.WHO_Region = i.WHO_Region AND c.Lat = i.Lat AND c.Long = i.Long
        '''
        merged_df = spark.sql(query)
        merged_df = merged_df.fillna(0,subset=["icu_patients","hosp_patient"])
        pd_df=merged_df.toPandas()
        return Output(
            pd_df,
            metadata={
                "table": "gold_global_infos",
                "record count": len(pd_df),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )

@asset(
    ins={
        "silver_covid19_infos": AssetIn(key_prefix=["silver", "covy"]),
    },
    description="Total infos covid19",
    compute_kind="PySpark",
    key_prefix=["gold", "covy"],
    io_manager_key="SparkIOManager",
    group_name="gold"
)
def gold_total_infos_covid19(
                   silver_covid19_infos:pd.DataFrame)-> Output[pd.DataFrame]:
    config = {
        "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
        "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    #
    current_function_name = inspect.currentframe().f_code.co_name

    with get_spark_session(config, current_function_name) as spark:
        spark_covid19_info = spark.createDataFrame(silver_covid19_infos)
        spark_covid19_info.createOrReplaceTempView("covid_info")

        query = '''
        SELECT
        region,
            MAX(Cofirmed) as total_Confirmed,
            MAX(Recovered) as total_Recovered,
            MAX(Death) as total_Death,
            WHO_Region
        FROM
            covid_info
        GROUP BY region, WHO_Region

        '''
        merged_df = spark.sql(query)
        pd_df=merged_df.toPandas()
        return Output(
            pd_df,
            metadata={
                "table": "total_global_infos",
                "record count": len(pd_df),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )
