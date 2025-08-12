from pathlib import Path
from typing import Literal

from dagster import EnvVar
from dagster_dbt import DbtProject

DBT_PROJECT_ROOT_DIR = Path(__file__).joinpath("..").resolve()
DBT_PROFILES_DIR = Path(__file__).joinpath("..", ".dbt").resolve()


def get_target() -> Literal["staff", "duckdb_dev"]:
    """Determine the target environment based on the environment variable."""
    environment = EnvVar("DAGSTER_ENV").get_value("production")
    if environment in ("dev", "development"):
        target = "duckdb_dev"
    else:
        target = "staff"
    return target


TARGET = get_target()

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    target=TARGET,
)
dbt_project.prepare_if_dev()
