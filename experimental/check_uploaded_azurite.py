

import pandas as pd


from pipelines.general.resources import (
    BlobStorageConnector,
    read_parquet_file,
    upload_data_to_blob
)


data=pd.DataFrame(data=[["2022-05-20", 2, 10, 5, 8, 1000], ["2022-05-21", 3, 11, 6, 9, 1007]], columns=["time", "low", "high", "open", "close", "volume"])




upload_data_to_blob(df=data, 
    container_name="coinbasedata", 
    subcontainer_name="datadownload", 
    filename="rawdata", 
    filetype="parquet")


df = read_parquet_file(container_name="coinbasedata", 
    blob="datadownload", file="rawdata.parquet")


df.head()

