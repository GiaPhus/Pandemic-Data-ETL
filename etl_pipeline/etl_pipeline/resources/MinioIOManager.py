from minio import Minio
import os
import pandas as pd
from dagster import IOManager,OutputContext,InputContext
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from typing import Union
from contextlib import contextmanager

@contextmanager
def connect_minio(config):
    client = Minio(
        endpoint=config.get("minio_url"),
        access_key=config.get("aws_access_key"),
        secret_key=config.get("aws_secret_key"),
        secure=False,
    )
    try:
        yield client
    except Exception as ex:
        raise ex


def make_bucket(client:Minio,bucket_name):
    found = client.bucket_exists(bucket_name)
    if found:
        print(f"{bucket_name} was exists")
    else:
        client.make_bucket(bucket_name)


class MinioIOManager (IOManager):
       def __init__(self,config):
           self._config = config

       def _getpath(self,context: Union[InputContext,OutputContext]):
           # tach ra asset_key
           # Ex: [bronze,covid19,covid_confirmed]
            layer,schema,table = context.asset_key.path
            key = "/".join([layer,schema,table.replace(f"{layer}_","")])
            tmp_file ="/tmp/file_{}_{}.parquet".format(
                "_".join(context.asset_key.path),datetime.today().strftime("%Y%m%d%H%M%S")
            )#Partition
            if context.has_partition_key:
                partition_str = {table} + context.asset_partition_key
                return os.path.join(key,f"{partition_str}.parquet"),tmp_file
            else:
                return f"{key}.parquet",tmp_file

       def handle_output(self, context: "OutputContext", obj: pd.DataFrame):
           key_name, tmp_file= self._getpath(context)
           table = pa.Table.from_pandas(obj)
           pq.write_table(table,tmp_file)

           try:
               bucket_name = self._config.get("bucket_name")
               with connect_minio(self._config) as client:
                   make_bucket(client,bucket_name)
                   client.fput_object(bucket_name,key_name,tmp_file)
                   context.log.info(
                       f"{obj.shape}"

                   )
                   context.add_output_metadata({"path": key_name, "tmp": tmp_file})

                # Clean up tmp file
                   os.remove(tmp_file)
           except Exception as e:
               raise e

       def load_input(self, context: "InputContext")  -> pd.DataFrame:
           bucket_name = self._config.get("bucket_name")
           key_name, tmp_file_path = self._getpath(context)
           try:
               with connect_minio(self._config) as client:
                   # Make bucket if not exist
                   make_bucket(client=client,bucket_name=bucket_name)

                   context.log.info(f"(MinIO load_input) from key_name: {key_name}")
                   client.fget_object(bucket_name, key_name, tmp_file_path)
                   df_data = pd.read_parquet(tmp_file_path)
                   context.log.info(
                       f"(MinIO load_input) Got pandas dataframe with shape: {df_data.shape}"
                   )

                   return df_data
           except Exception:
               raise None







