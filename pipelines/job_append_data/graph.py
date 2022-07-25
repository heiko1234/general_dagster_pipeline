

from dagster import graph

from pipelines.job_append_data.op import (
    load_file_from_blobstorage,
    append_blob_data
)



@graph()
def aggregate_data_graph():

    df = load_file_from_blobstorage()

    append_blob_data(df=df)


