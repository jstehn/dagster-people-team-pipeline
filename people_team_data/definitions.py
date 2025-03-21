from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    Definitions,
    EnvVar,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from . import assets, resources

# 1. Load dbt project
dbt_project_directory = EnvVar("DBT_PROJECT_DIR").get_value()
dbt_profiles_directory = EnvVar("DBT_PROFILES_DIR").get_value()
dbt_project = DbtProject(
    project_dir=dbt_project_directory, profiles_dir=dbt_profiles_directory
)
print(f"Project directory: {dbt_project_directory}")

# 2. Prepare dbt project if dev
dbt_project.prepare_if_dev()


# 3. Define dbt assets
@dbt_assets(
    manifest=dbt_project.manifest_path,
)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# 4. Load all other assets
all_assets = load_assets_from_modules([assets])

# 5. Get environment resources
env = EnvVar("ENV").get_value()
env_resources = resources.get_environment_resources(env)
# 6. Create definitions object
defs = Definitions(
    assets=[*all_assets, dbt_models],  # Include dbt assets here
    resources={
        **env_resources,
    },
)
