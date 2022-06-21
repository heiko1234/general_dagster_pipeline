



from pipelines.job_fetch_coinbase_data.graph import (
    coinbase_data_execution_graph
)


job_fetch_coinbase_data = coinbase_data_execution_graph.to_job(
    name="coinbase_data_fetch",
    resource_defs={}
)





