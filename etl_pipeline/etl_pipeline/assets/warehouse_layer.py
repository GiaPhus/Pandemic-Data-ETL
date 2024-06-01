from dagster import multi_asset,Output,AssetIn,AssetOut
import pandas as pd

@multi_asset(
    ins={
        "gold_global_infos": AssetIn(key_prefix=["gold", "covy"],)
    },
    outs={
        "warehouse_global_infos": AssetOut(
            io_manager_key="PostgreIOManager",
            key_prefix=["warehouse","public"],
            group_name="warehouse",
        )
    },
    description="global day-by-day covy WHO",
    compute_kind="PostgreSQL"
    )
def warehouse_global_infos(gold_global_infos) -> Output[pd.DataFrame]:
    return Output(
        gold_global_infos,
        metadata={
            "schema": "public",
            "table": "warehouse_global_infos",
            "records counts": len(gold_global_infos),
            "database": "covid_19_warehouse",
        },
    )

@multi_asset(
    ins={
        "gold_total_infos_covid19": AssetIn(key_prefix=["gold", "covy"])
    },
    outs={
        "warehouse_total_infos_covid19": AssetOut(
            io_manager_key="PostgreIOManager",
            key_prefix=["warehouse", "public"],

        )
    },
    group_name="warehouse",
    description="total infos covid19",
    compute_kind="PostgreSQL"
    )
def warehouse_total_infos_covid19(gold_total_infos_covid19) -> Output[pd.DataFrame]:
    return Output(
        gold_total_infos_covid19,
        metadata={
            "schema": "public",
            "table": "warehouse_total_infos_covid19",
            "database": "covid_19_warehouse",
            "records counts": len(gold_total_infos_covid19),
        },
    )