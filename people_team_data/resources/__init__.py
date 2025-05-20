import os

from dagster import EnvVar, InitResourceContext, resource  # noqa: F401
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_gcp import BigQueryResource, GCSResource
from sqlalchemy import create_engine

from ..assets.dbt.project import dbt_project

# Construct the expected path to keyfile.json using dbt_project properties
# This assumes project.py (which defines dbt_project) has run and dbt_project is initialized.
keyfile_path = dbt_project.profiles_dir.joinpath("keyfile.json").resolve()
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(keyfile_path)
print(
    f"resources/__init__.py: Setting GOOGLE_APPLICATION_CREDENTIALS to: {str(keyfile_path)}"
)

# Define resources directly
dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)
dlt_resource = DagsterDltResource()
gcp_project_val = EnvVar("GCP_PROJECT").get_value()
if not gcp_project_val:
    raise ValueError(
        "Environment variable GCP_PROJECT is not set. This is required for GCS and BigQuery resources."
    )

lake_resource = GCSResource(
    bucket="pipeline_data_raw", project=gcp_project_val
)  # project directly for GCS
warehouse_resource = BigQueryResource(project=gcp_project_val, dataset="prod")


# Collection of all resources for easy import in definitions.py
all_resources = {
    "dbt": dbt_resource,
    "dlt": dlt_resource,
    "lake": lake_resource,
    "warehouse": warehouse_resource,
}
