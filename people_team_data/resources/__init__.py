from dagster import (
    Definitions,  # noqa: F401
    EnvVar,
)
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_gcp import BigQueryResource, GCSResource

from .google_service_account_resource import *  # noqa: F403
from .postgres_db_resource import *  # noqa: F403


class EnvironmentConfig:
    def __init__(self, env: str):
        self.env = env
        self.dbt_target = f"{env}"
        self.dlt_dataset = f"raw_{env}"


def get_environment_resources(env: str):
    base_resources = {
        "dlt": {
            "pipeline_name": f"{env}_pipeline",
            "dataset_name": f"raw_{env}",
        },
        "dbt": DbtCliResource(
            project_dir=EnvVar("DBT_PROJECT_DIR").get_value(),
            profiles_dir=EnvVar("DBT_PROFILES_DIR").get_value(),
            target=env,
        ),
        "config": EnvironmentConfig(env),
    }

    if env == "dev":
        return {
            **base_resources,
            "lake": ...,
            "warehouse": ...,
            "dlt": DagsterDltResource(
                **base_resources["dlt"], destination="postgres"
            ),
        }
    elif env == "stage":
        return {
            **base_resources,
            "lake": GCSResource(
                project=EnvVar("GCP_PROJECT_STAGE"),
                bucket="pipeline_data_raw",
                gcp_credentials=EnvVar("GCP_CREDS_STAGE"),
            ),
            "warehouse": BigQueryResource(
                project=EnvVar("GCP_PROJECT_STAGE"),
                dataset=base_resources["dlt"].dataset_name,
                gcp_credentials=EnvVar("GCP_CREDS_STAGE"),
            ),
            "dlt": DagsterDltResource(
                **base_resources["dlt"], destination="bigquery"
            ),
        }
    elif env == "prod":
        return {
            **base_resources,
            "lake": GCSResource(
                project=EnvVar("GCP_PROJECT"),
                bucket="pipeline_data_raw",
                gcp_credentials=EnvVar("GCP_CREDS"),
            ),
            "warehouse": BigQueryResource(
                project=EnvVar("PROD_GCP_PROJECT"),
                dataset=base_resources["dlt"].dataset_name,
                gcp_credentials=EnvVar("GCP_CREDS"),
            ),
            "dlt": DagsterDltResource(
                **base_resources["dlt"], destination="bigquery"
            ),
        }
    else:
        raise ValueError(f"Unknown environment: {env}")
