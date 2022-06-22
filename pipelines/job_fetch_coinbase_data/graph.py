


from dagster import graph


from pipelines.job_fetch_coinbase_data.ops import (
    load_coinbase_data,
    upload_data_to_blob_context
)


@graph
def coinbase_data_execution_graph():

    df = load_coinbase_data()

    upload_data_to_blob_context(df=df)








