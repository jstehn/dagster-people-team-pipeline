import os
from pathlib import Path

from dagster import AssetExecutionContext, EnvVar
from dagster_dlt import DagsterDltResource, dlt_assets
from dlt import pipeline
from dlt.destinations import duckdb

from .bamboo_api_pipeline import bamboohr_source
from .paycom_pipeline import paycom_source
from .position_control_pipeline import position_control_source


# Dynamically determine destination based on environment
def get_destination():
    env = EnvVar("DAGSTER_ENV").get_value("production").lower()
    if env in ["dev", "development", "local"]:
        return duckdb(EnvVar("DUCKDB_PATH").get_value())
    else:
        return "bigquery"


DEST = get_destination()
# Set DLT config directory to the moved .dlt folder
DLT_CONFIG_DIR = Path(__file__).parent / ".dlt"
os.environ["DLT_CONFIG_DIR"] = str(DLT_CONFIG_DIR)


@dlt_assets(
    dlt_source=bamboohr_source(),
    dlt_pipeline=pipeline(
        pipeline_name="bamboohr_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_bamboohr",
    # group_name="raw_people_data",
)
def dagster_bamboohr_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    yield from dlt.run(context=context, write_disposition="merge")


@dlt_assets(
    dlt_source=paycom_source(),
    dlt_pipeline=pipeline(
        pipeline_name="paycom_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_paycom",
    # group_name="raw_people_data",
)
def dagster_paycom_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    yield from dlt.run(context=context, write_disposition="merge")


@dlt_assets(
    dlt_source=position_control_source(),
    dlt_pipeline=pipeline(
        pipeline_name="position_control_pipeline",
        dataset_name="staff",
        destination=DEST,
        progress="log",
    ),
    name="raw_position_control",
    # group_name="raw_people_data",
)
def dagster_position_control_assets(
    context: AssetExecutionContext, dlt: DagsterDltResource
):
    yield from dlt.run(context=context, write_disposition="merge")
