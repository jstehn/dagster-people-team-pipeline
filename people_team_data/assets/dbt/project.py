from pathlib import Path

from dagster_dbt import DbtProject

DBT_PROJECT_ROOT_DIR = Path(__file__).joinpath("..").resolve()
DBT_PROFILES_DIR = Path(__file__).joinpath("..", ".dbt").resolve()

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR, profiles_dir=DBT_PROFILES_DIR
)
dbt_project.prepare_if_dev()
