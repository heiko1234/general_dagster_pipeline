

import io
import os
from dagster import op
from dagster import In, Out
import pandas as pd

from pipelines.general.resources import (
    azurite_resource
)


client_resource = azurite_resource()
client_resource.blob_client.get_blob_client(container="")


client = client_resource.blob_client.get_blob_client(
        container="sklearn", blob="data/ChemicalManufacturingProcess.parquet"
    )
bytes = client.download_blob().readall()
pq_file = io.BytesIO(bytes)

df = pd.read_parquet(pq_file)
df
