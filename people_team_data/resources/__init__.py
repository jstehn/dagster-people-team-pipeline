import os

from dagster import EnvVar, InitResourceContext, resource  # noqa: F401
from dagster_dbt import DbtCliResource
from dagster_dlt import DagsterDltResource
from dagster_gcp import BigQueryResource, GCSResource
from sqlalchemy import create_engine


@resource
def postgres_db(context: InitResourceContext):
    """A resource that provides a PostgreSQL database connection using SQLAlchemy."""
    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@localhost:5432/calibrate"
    )
    return engine


def get_environment_resources(env: str):
    base_resources = {
        "dbt": DbtCliResource(
            project_dir=EnvVar("DBT_PROJECT_DIR").get_value(),
            target=env,
        ),
        "dlt": DagsterDltResource(),
    }

    if env == "dev":
        return {**base_resources, "lake": None, "warehouse": postgres_db}
    elif env == "stage":
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = EnvVar(
            "GCP_CREDS_STAGE"
        ).get_value()
        gcs_config = {"project": EnvVar("GCP_PROJECT_STAGE"), "dataset": "prod"}
        return {
            **base_resources,
            "lake": GCSResource(bucket="pipeline_data_raw", **gcs_config),
            "warehouse": BigQueryResource(**gcs_config),
        }
    elif env == "prod":
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = EnvVar(
            "GCP_CREDS"
        ).get_value()
        gcs_config = {"project": EnvVar("GCP_PROJECT"), "dataset": "prod"}
        return {
            **base_resources,
            "lake": GCSResource(bucket="pipeline_data_raw", **gcs_config),
            "warehouse": BigQueryResource(**gcs_config),
        }
    else:
        raise ValueError(f"Unknown environment: {env}")
