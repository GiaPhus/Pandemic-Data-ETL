import inspect
from dagster import asset,Output,AssetIn
import pandas as pd
import os
from ..resources.SparkIOManager import get_spark_session
from datetime import datetime
@asset(
    ins={
        "hospital_icu_covid19" : AssetIn(key_prefix=["bronze","covy"]),
        "covid19_country_wise" : AssetIn(key_prefix=["bronze","covy"])
    },
    description="Country isu patient",
    compute_kind="PySpark",
    key_prefix=["silver","covy"],
    io_manager_key="SparkIOManager",
    group_name="silver"
)
def silver_covid19_isu_patient_country(context,
                                      hospital_icu_covid19: pd.DataFrame,
                                      covid19_country_wise: pd.DataFrame) -> Output[pd.DataFrame]:
    config = {
        "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
        "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    current_function_name = inspect.currentframe().f_code.co_name
    with get_spark_session(config,current_function_name) as spark :

        spark_icu_covid19 = spark.createDataFrame(hospital_icu_covid19)
        spark_country_wise = spark.createDataFrame(covid19_country_wise)
        spark_icu_covid19 = spark_icu_covid19.filter(
            (spark_icu_covid19["icu_patients"] != 0) & (spark_icu_covid19["hosp_patient"] != 0))
        spark_icu_covid19.fillna(0)
        spark_df = spark_icu_covid19.join(spark_country_wise, spark_country_wise["region"]==spark_icu_covid19["location"],"inner") \
                   .select(spark_country_wise["State"],
                           spark_icu_covid19["location"],
                           spark_icu_covid19["date"],
                           spark_icu_covid19["icu_patients"],
                           spark_icu_covid19["hosp_patient"],
                           spark_country_wise["Lat"],
                           spark_country_wise["Long"],
                           spark_country_wise["WHO_Region"])
        pd_df = spark_df.toPandas()
        sc=spark.sparkContext
        sc.setLogLevel("DEBUG")
        return Output(
            pd_df,
            metadata={
                "table": "covid19_isu_patient_country",
                "record count": len(pd_df),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
             }
            )



@asset(
    ins={
        "country_vaccinations": AssetIn(key_prefix=["bronze", "covy"]),
        "covid19_country_wise": AssetIn(key_prefix=["bronze", "covy"])
    },
    description="Country Vaccination",
    compute_kind="PySpark",
    key_prefix=["silver", "covy"],
    io_manager_key="SparkIOManager",
    group_name="silver"
)
def silver_country_vaccination(context,
                               country_vaccinations: pd.DataFrame,
                               covid19_country_wise: pd.DataFrame) -> Output[pd.DataFrame]:
    config = {
        "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
        "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    current_function_name = inspect.currentframe().f_code.co_name

    # Sử dụng context manager để tạo phiên Spark
    with get_spark_session(config,current_function_name) as spark:
        # Tạo DataFrame Spark từ DataFrame Pandas

        spark_country_vaccinations = spark.createDataFrame(country_vaccinations)
        spark_country_vaccinations.cache()
        spark_covid19_country_wise = spark.createDataFrame(covid19_country_wise)

        spark_country_vaccinations = spark_country_vaccinations.filter(
            (spark_country_vaccinations["total_vaccinations"] != 0) & (spark_country_vaccinations["people_vaccinated"] != 0))

        spark_country_vaccinations.fillna(0)
        # Tạo DataFrame cho WHO_Region
        who_region_df = spark_covid19_country_wise.select("region", "WHO_Region").distinct()

        result_df = spark_country_vaccinations.join(
            who_region_df,
            spark_country_vaccinations["country"] == who_region_df["region"],
            "inner"
        ).select(
            spark_country_vaccinations["date"],
            spark_country_vaccinations["country"],
            spark_country_vaccinations["total_vaccinations"],
            spark_country_vaccinations["people_vaccinated"],
            spark_country_vaccinations["people_fully_vaccinated"],
            who_region_df["WHO_Region"]
        ).toPandas()

        sc = spark.sparkContext
        sc.setLogLevel("DEBUG")
        # Trả về kết quả với Output và metadata
        return Output(
            result_df,
            metadata={
                "table": "covid19_vaccination_WHO",
                "record count": len(result_df),
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        )



@asset(
        ins={"covid19_country_wise":AssetIn(key_prefix=["bronze","covy"]),
            "covid19_confirmed": AssetIn(key_prefix=["bronze","covy"]),
            "covid19_recovered": AssetIn(key_prefix=["bronze","covy"]),
            "covid19_death" : AssetIn(key_prefix=["bronze","covy"])
             },
        description="Covid19 infos",
        key_prefix=["silver","covy"],
        group_name="silver",
        io_manager_key="SparkIOManager",
        compute_kind="PySpark"
)
def silver_covid19_infos(context,
                          covid19_country_wise: pd.DataFrame,
                          covid19_confirmed: pd.DataFrame,
                          covid19_recovered: pd.DataFrame,
                          covid19_death: pd.DataFrame) -> Output[pd.DataFrame]:
    config = {
        "minio_url": os.getenv("MINIO_ENDPOINT_URL"),
        "aws_access_key": os.getenv("MINIO_ACCESS_KEY"),
        "aws_secret_key": os.getenv("MINIO_SECRET_KEY"),
    }
    current_function_name = inspect.currentframe().f_code.co_name

    # Sử dụng context manager để tạo phiên Spark
    with get_spark_session(config,current_function_name) as spark:
        spark_country_wise = spark.createDataFrame(covid19_country_wise)
        spark_confirmed = spark.createDataFrame(covid19_confirmed)
        spark_recovered = spark.createDataFrame(covid19_recovered)
        spark_death = spark.createDataFrame(covid19_death)

        spark_df = spark_confirmed.join(spark_recovered,
                                        (spark_confirmed["Lat"] == spark_recovered["Lat"]) &
                                        (spark_confirmed["Long"] == spark_recovered["Long"]) &
                                        (spark_confirmed["Date"] == spark_recovered["Date"]), "inner") \
            .join(spark_death,
                  (spark_confirmed["Lat"] == spark_death["Lat"]) &
                  (spark_confirmed["Long"] == spark_death["Long"]) &
                  (spark_confirmed["Date"] == spark_death["Date"]), "inner") \
            .join(spark_country_wise,
                  (spark_confirmed["Lat"] == spark_country_wise["Lat"]) &
                  (spark_confirmed["Long"] == spark_country_wise["Long"]), "inner") \
             \
            .select(spark_confirmed["Lat"],
                    spark_confirmed["Long"],
                    spark_confirmed["Date"],
                    spark_confirmed["region"],
                    spark_confirmed["Cofirmed"],
                    spark_recovered["Recovered"],
                    spark_death["Death"],
                    spark_country_wise["WHO_Region"])
        spark_df = spark_df.filter(
            (spark_confirmed["Cofirmed"] != 0) |
            (spark_death["Death"] != 0) |
            (spark_recovered["Recovered"] != 0)
        )
        pd_df = spark_df.toPandas()
        sc = spark.sparkContext
        sc.setLogLevel("DEBUG")
        return Output(
        pd_df,
        metadata={
            "table": "covid19_infos_WHO",
            "record count": len(pd_df),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        )



