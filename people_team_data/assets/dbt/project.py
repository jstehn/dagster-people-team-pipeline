from pathlib import Path

from dagster import EnvVar
from dagster_dbt import DbtProject

DBT_PROJECT_DIR = Path(
    EnvVar("DBT_PROJECT_DIR").get_value("people_team_data/assets/dbt")
).resolve()
DBT_PROFILES_DIR = Path(
    EnvVar("DBT_PROFILES_DIR").get_value("people_team_data/assets/dbt/.dbt")
).resolve()

dbt_models_project = DbtProject(
    project_dir=DBT_PROJECT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    packaged_project_dir=DBT_PROJECT_DIR.joinpath("dbt-project/"),
)
dbt_models_project.prepare_if_dev()
