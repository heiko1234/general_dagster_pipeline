

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
    source_file_name= config["source_file_name"]
    target_subblob_container = config["target_subcontainer"]
    target_file_name = config["target_file_name"]

    DataFetcher = BlobStorageConnector(container_name=blob_container)

    try: 
        target_file = DataFetcher.get_parquet_file(
            subcontainer=target_subblob_container, file=target_file_name
        )
    except BaseException:
        target_file = None


    source_file = DataFetcher.get_parquet_file(
        subcontainer=source_subblob_container, file=source_file_name
    )

    actual_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

    last_source_date=list(source_file.loc[:, "time"])[-1]

    try: 
        last_target_date = list(target_file.loc[:,"time"])[-1]
    except BaseException:
        last_target_date = None

    if last_source_date == last_target_date:
        yield SkipReason(skip_message= "data are up-to-date")
    
    else:

        context.update_cursor(str(actual_time))

        yield RunRequest(
            run_key=f"updated_timestamp_{actual_time}",
            run_config= {
                "ops": {
                    "load_file_from_blobstorage":{
                        "inputs":{
                            "container_name": blob_container,
                            "sub_container": source_subblob_container,
                            "file_name": source_file_name
                        }
                    },
                    "append_blob_data":{
                        "inputs":{
                            "container_name": blob_container,
                            "sub_container": target_subblob_container,
                            "file_name": target_file_name
                        }
                    }
                }
            }
        )






