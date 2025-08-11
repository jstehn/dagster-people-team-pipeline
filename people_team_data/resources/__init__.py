import logging  # Added logging
from pathlib import Path  # Added Path

from dagster import EnvVar, InitResourceContext, resource  # noqa: F401
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_gcp import BigQueryResource, GCSResource

from ..assets.dbt.project import dbt_project

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s (resources/__init__.py)",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# -- Configure paths --
BIGQUERY_KEYFILE_NAME = "gcp_bigquery.json"
bigquery_keyfile_path = (
    Path(__file__).parent.parent / ".secrets" / BIGQUERY_KEYFILE_NAME
).resolve()

# -- Configure resources --
dbt_resource = DbtCliResource(
    project_dir=dbt_project,
    profiles_dir=dbt_project.profiles_dir,
)
dlt_resource = DagsterDltResource()
gcp_project_val = EnvVar("GCP_BASE_PROJECT").get_value()
if not gcp_project_val:
    raise ValueError(
        "Environment variable GCP_BASE_PROJECT is not set. This is required for GCS and BigQuery resources."
    )
lake_resource = GCSResource(project=gcp_project_val)
warehouse_resource = BigQueryResource(project=gcp_project_val)


# Collection of all resources for easy import in definitions.py
all_resources = {
    "dbt": dbt_resource,
    "dlt": dlt_resource,
    "lake": lake_resource,
    "warehouse": warehouse_resource,
}
