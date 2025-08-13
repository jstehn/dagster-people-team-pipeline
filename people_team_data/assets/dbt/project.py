"""
This module defines the configuration for the DBT project used in the Dagster pipeline.

It includes:
- Paths to the DBT project and profiles directories.
- Logic to determine the target environment based on the `DAGSTER_ENV` environment variable.
- Initialization of the `DbtProject` object for use in the pipeline.
"""

from pathlib import Path
from typing import Literal

from dagster import EnvVar
from dagster_dbt import DbtProject

DBT_PROJECT_ROOT_DIR = Path(__file__).joinpath("..").resolve()
DBT_PROFILES_DIR = Path(__file__).joinpath("..", ".dbt").resolve()


def get_target() -> Literal["staff", "duckdb_dev"]:
    """
    Determine the target environment based on the `DAGSTER_ENV` environment variable.

    Returns:
        Literal["staff", "duckdb_dev"]: The target environment name.
    """
    environment = EnvVar("DAGSTER_ENV").get_value("production")
    if environment in ("dev", "development"):
        target = "duckdb_dev"
    else:
        target = "staff"
    return target


TARGET = get_target()

# Initialize the DBT project configuration
dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    target=TARGET,
)
dbt_project.prepare_if_dev()
