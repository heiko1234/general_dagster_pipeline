



# import datetime as dt
# from dotenv import load_dotenv
# from dagster import RunRequest, sensor, SkipReason


from dagster import ScheduleEvaluationContext, schedule

from pipelines.job_fetch_coinbase_data.job import (
    job_fetch_coinbase_data
)




@schedule(
    job=job_fetch_coinbase_data,
    cron_schedule="*/2 * * * *",  # every 5 min
    pipeline_name="schedule_coinbase_data"
)
def trigger_fetch_coinbase_data_execution(context: ScheduleEvaluationContext):
    output = {
        "ops":
            {
                "load_coinbase_data":
                    {"config": 
                        {
                            "product_id": "ETH-EUR",
                            "granularity": 15   #Min
                        }
                    },
                "upload_data_to_blob_context":
                    {"config":
                        {
                            "blob_container": "coinbasedata",
                            "subblob_container": "datadownload",
                            "filename": "rawdata.parquet"
                        }
                    }
            }
    }
    return output





