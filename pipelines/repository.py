



from dagster import repository

from pipelines.job_fetch_coinbase_data.job import (
    job_fetch_coinbase_data
)
from pipelines.job_fetch_coinbase_data.trigger import (
    trigger_fetch_coinbase_data_execution
)
from pipelines.job_append_data.job import (
    job_data_aggregation
)
from pipelines.job_append_data.trigger import (
    sensor_data_aggregation
)


@repository
def data_pipeline():
    """Dagster repository to run pipelines for project"""


    return [
        job_fetch_coinbase_data,
        trigger_fetch_coinbase_data_execution,
        job_data_aggregation,
        sensor_data_aggregation
    ]


