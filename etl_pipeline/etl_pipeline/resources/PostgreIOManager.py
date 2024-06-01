
from dagster import InputContext,OutputContext,IOManager
from sqlalchemy import create_engine
from contextlib import contextmanager
import pandas as pd

@contextmanager
def conn_psql(config):
    conn=(
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn)
    try:
        yield db_conn
    except Exception as e:
        print(f"Error while connecting to PostgreSQL: {e}")
        raise e

class PostgreIOManager(IOManager):
    def __init__(self,config):
        self._config = config
    def load_input(self, context: "InputContext") -> pd.DataFrame:
        pass
    def handle_output(self, context: "OutputContext", obj: pd.DataFrame):
        table_name = context.asset_key.path[-1]
        schema = context.asset_key.path[-2]
        with conn_psql(self._config) as conn:
            #try to connect psql
            try:
                ls_columns = (context.metadata or {}).get("columns", obj.columns.tolist())
                obj[ls_columns].to_sql(name=table_name, schema=schema, con=conn, if_exists="replace", index=False)
                context.log.info(f"Data saved to {schema}.{table_name}")
            except Exception as e:
                context.log.error(f"Error while saving data to {schema}.{table_name}: {e}")
                raise e

