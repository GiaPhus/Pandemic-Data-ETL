from dagster import asset, Output
import pandas as pd


# bronze layer vaccine
@asset(
    compute_kind="MySQL",
    required_resource_keys={"MYSQLIOManager"},
    io_manager_key="MinioIOManager",
    description="Raw vaccine Covid19",
    key_prefix=["bronze", "covy"],
    group_name="bronze"
)
def country_vaccinations(context) -> Output[pd.DataFrame]:
    # Select values table
    sql_stm = "SELECT * FROM country_vaccinations;"
    df = context.resources.MYSQLIOManager.conn_data(sql_stm)
    return Output(
        value=df,
        metadata={
            "table": "vaccinations",
            "record count": len(df),
        },
    )


# bronze layer country_wise
@asset(
    compute_kind="MySQL",
    required_resource_keys={"MYSQLIOManager"},
    io_manager_key="MinioIOManager",
    description="Raw country wise Lat/Long Covid19",
    key_prefix=["bronze", "covy"],
    group_name="bronze"
)
def covid19_country_wise(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_country_wise;"
    df = context.resources.MYSQLIOManager.conn_data(sql_stm)
    return Output(
        value=df,
        metadata={
            "table": "country_wise",
            "record count": len(df),
        },
    )


# bronze layer confimed case

@asset(
    compute_kind="MySQL",
    required_resource_keys={"MYSQLIOManager"},
    io_manager_key="MinioIOManager",
    description="Raw cofirmed case Covid19",
    key_prefix=["bronze", "covy"],
    group_name="bronze"
)
def covid19_confirmed(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_confirmed;"
    df = context.resources.MYSQLIOManager.conn_data(sql_stm)
    return Output(
        value=df,
        metadata={
            "table": "covid19_confirm",
            "record count": len(df),
        },
    )


# bronze layer recovered case

@asset(
    compute_kind="MySQL",
    required_resource_keys={"MYSQLIOManager"},
    io_manager_key="MinioIOManager",
    description="Raw recovered case Covid19",
    key_prefix=["bronze", "covy"],
    group_name="bronze"
)
def covid19_recovered(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_recovered;"
    df = context.resources.MYSQLIOManager.conn_data(sql_stm)
    return Output(
        value=df,
        metadata={
            "table": "covid19_recovered",
            "record count": len(df),
        },
    )


# bronze layer death  case

@asset(
    compute_kind="MySQL",
    required_resource_keys={"MYSQLIOManager"},
    io_manager_key="MinioIOManager",
    description="Raw death case Covid19",
    key_prefix=["bronze", "covy"],
    group_name="bronze"
)
def covid19_death(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_death;"
    df = context.resources.MYSQLIOManager.conn_data(sql_stm)
    return Output(
        value=df,
        metadata={
            "table": "covid19_death",
            "record count": len(df),
        },
    )


# bronze layer hospital icu patients case

@asset(
    compute_kind="MySQL",
    required_resource_keys={"MYSQLIOManager"},
    io_manager_key="MinioIOManager",
    description="Raw hospital icu patients Covid19",
    key_prefix=["bronze", "covy"],
    group_name="bronze"
)
def hospital_icu_covid19(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM hospital_icu_covid19;"
    df = context.resources.MYSQLIOManager.conn_data(sql_stm)
    return Output(
        value=df,
        metadata={
            "table": "hospital_icu_patients",
            "record count": len(df),
        },
    )
