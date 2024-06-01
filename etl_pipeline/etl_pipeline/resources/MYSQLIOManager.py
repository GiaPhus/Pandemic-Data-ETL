from dagster import InputContext,OutputContext,IOManager
from sqlalchemy import create_engine
from contextlib import contextmanager
import pandas as pd


@contextmanager
def connect_mysql(config):
    conn = (
        f"mysql+pymysql://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn)
    try :
        yield db_conn
    except Exception:
        raise None


class MySQLIOManager(IOManager):
    def __init__(self,config):
        self._config = config

    def handle_output(self, context: "OutputContext", obj: pd.DataFrame):
        pass

    def load_input(self, context: "InputContext") -> pd.DataFrame:
        pass
    #connect mysql
    def conn_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self._config) as conn:
            pd_data = pd.read_sql_query(sql, conn)
            return pd_data



