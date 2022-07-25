


from pipelines.general.resources import azurite_resource

from dagster import job, resource

from pipelines.job_append_data.graph import (
    aggregate_data_graph
)



job_data_aggregation = aggregate_data_graph.to_job(
    name="load_and_append_coinbase_data",
    resource_defs={
        "azure_blob": azurite_resource
    }
)






