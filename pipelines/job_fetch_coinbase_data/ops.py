

from dagster import op
from pathlib import Path
import os
import pandas as pd


from pipelines.job_fetch_coinbase_data.coinbase_functions import (
    public_candles
)
from pipelines.general.resources import (
    BlobStorageConnector,
    upload_data_to_blob
)


@op()
def load_coinbase_data(context):

    product_id = context.op_config["product_id"]
    granularity = context.op_config["granularity"]
    context.log.info(f"product_id: {product_id}")
    context.log.info(f"granularity: {granularity}")

    # data = public_candles(product_id="ETH-EUR", start=None, end= None, granularity=None, localtime=True)

    data = public_candles(product_id=product_id, start=None, end= None, granularity=granularity, localtime=True)

    # data=pd.DataFrame(data=[["2022-05-20", 2, 10, 5, 8, 1000]], columns=["time", "low", "high", "open", "close", "volume"])

    context.log.info(f"data: {data.head()}")
    context.log.info(f"data: {data.tail()}")

    return data


@op()
def upload_data_to_blob_context(context, df):

    containername = context.op_config["blob_container"]
    context.log.info(f"containername: {containername}")

    subcontainername = context.op_config["subblob_container"]
    context.log.info(f"subcontainername: {subcontainername}")

    filename = context.op_config["filename"]
    context.log.info(f"filename: {filename}")

    file_name = filename.split(".")[0]
    file_type = filename.split(".")[1]

    context.log.info(f"file_name: {file_name}")
    context.log.info(f"file_type: {file_type}")

    upload_data_to_blob(df=df, 
        container_name=containername, 
        subcontainer_name=subcontainername, 
        filename=file_name, 
        filetype=file_type)






