from dagster import (
    Definitions,  # noqa: F401
    EnvVar,
)
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_gcp import BigQueryResource

from .google_service_account_resource import *  # noqa: F403
from .postgres_db_resource import *  # noqa: F403


class EnvironmentConfig:
    def __init__(self, env: str):
        self.env = env
        self.dbt_target = f"{env}"
        self.dlt_dataset = f"raw_{env}"


def get_environment_resources(env: str):
    base_resources = {
        "dlt": DagsterDltResource(
            pipeline_name=f"{env}_pipeline", dataset_name=f"raw_{env}"
        ),
        "dbt": DbtCliResource(
            project_dir=EnvVar("DBT_PROJECT_DIR"),
            profiles_dir=EnvVar("DBT_PROFILES_DIR"),
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
                **base_resources["dlt"]._asdict(), destination="postgres"
            ),
        }
    elif env == "stage":
        return {
            **base_resources,
            "lake": ...,
            "warehouse": BigQueryResource(
                project=EnvVar("stage_GCP_PROJECT"),
                dataset=base_resources["dlt"].dataset_name,
                gcp_credentials=EnvVar("GCP_CREDENTIALS"),
            ),
            "dlt": DagsterDltResource(
                **base_resources["dlt"]._asdict(), destination="bigquery"
            ),
        }
    elif env == "prod":
        return {
            **base_resources,
            "lake": ...,
            "warehouse": BigQueryResource(
                project=EnvVar("PROD_GCP_PROJECT"),
                dataset=base_resources["dlt"].dataset_name,
                gcp_credentials=EnvVar("GCP_CREDENTIALS"),
            ),
            "dlt": DagsterDltResource(
                **base_resources["dlt"]._asdict(), destination="bigquery"
            ),
        }
    else:
        raise ValueError(f"Unknown environment: {env}")
