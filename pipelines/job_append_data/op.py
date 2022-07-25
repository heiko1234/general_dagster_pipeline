

import io
import os
from dagster import op
from dagster import In, Out
import pandas as pd
from pathlib import Path

from pipelines.general.resources import (
    azurite_resource
)




@op(ins={"container_name": In(dagster_type=str),
    "sub_container": In(dagster_type=str), 
    "file_name": In(dagster_type=str)},
    required_resource_keys={"azure_blob"}
)
def load_file_from_blobstorage(context, container_name, sub_container, file_name):
    """Dagster Op to download file from blob storage

    Args:
        context (str): _description_
        cointainer_name (str): a string of containername
        sub_container (str): a string of subcontainername
        file_name (str): a string of the file
    """
    context.log.info(f"containername: {container_name}")
    context.log.info(f"subcontainer: {sub_container}")
    context.log.info(f"filename: {file_name}")

    strblobpath = f"{sub_container}/{file_name}"
    context.log.info(f"filepath on blob: {strblobpath}")

    client = context.resources.azure_blob.blob_client.get_blob_client(
        container=container_name, blob=strblobpath
    )
    bytes = client.download_blob().readall()
    pq_file = io.BytesIO(bytes)

    df = pd.read_parquet(pq_file)



@op(ins={"container_name": In(dagster_type=str),
    "sub_container": In(dagster_type=str), 
    "file_name": In(dagster_type=str)},
    required_resource_keys={"azure_blob"}
)
def append_blob_data(context, df, container_name, sub_container, file_name):
    context.log.info(f"containername: {container_name}")
    context.log.info(f"subcontainer: {sub_container}")
    context.log.info(f"filename: {file_name}")
    context.log.info(f"df: {df.head()}")

    strblobpath = f"{sub_container}/{file_name}"
    context.log.info(f"filepath on blob: {strblobpath}")

    try:

        client = context.resources.azure_blob.blob_client.get_blob_client(
            container=container_name, blob=strblobpath
        )
        bytes = client.download_blob().readall()
        pq_file = io.BytesIO(bytes)

        df_exist = pd.read_parquet(pq_file)
        context.log.info(f"Successful load of blobdata: {df.head()}")
    
    except BaseException:
        context.log.info("No data available")
        df_exist = None

    df_list = [df_exist, df]
    new_df = pd.concat(df_list)

    new_df = new_df.drop_duplicates(keep="first")

    new_df = new_df.reset_index(drop=True)

    relative_file_name = f"./data/{sub_container}/{file_name}"
    file = Path(relative_file_name)
    dir_to_create = file.parent
    os.makedirs(dir_to_create, exist_ok=True)

    new_df.to_parquet(relative_file_name)

    client = context.resources.azure_blob.blob_client.get_blob_client(
        container=container_name, blob = f"{sub_container}/{file_name}"
    )
    with file.open("rb") as data:
        client.upload_blob(data, overwrite=True)
    context.log.info(f"successfully wrote data to blob")

    Path(relative_file_name).unlink()
    




