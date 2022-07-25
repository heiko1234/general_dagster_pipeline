

import datetime
from re import sub

from dagster import RunRequest, SkipReason, sensor

from pipelines.job_append_data.job import (
    job_data_aggregation
)

from pipelines.general.resources import (
    BlobStorageConnector,
    get_list_files_in_subcontainer
)
from pipelines.general.utility import (
    get_missing_elements,
    read_configuration
)



@sensor(job=job_data_aggregation, minimum_interval_seconds=60)
def sensor_data_aggregation(context):

    config = read_configuration(configuration_file_path="./pipelines/job_append_data/aggregation.yaml")

    blob_container = config["blob_container"]
    source_subblob_container = config["source_subcontainer"]
    target_subblob_container = config["target_subcontainer"]
    file_name = config["file_name"]

    DataFetcher = BlobStorageConnector(container_name=blob_container)

    try: 
        target_file = DataFetcher.get_parquet_file(
            subcontainer=target_subblob_container, file=file_name
        )
    except BaseException:
        target_file = None

    















