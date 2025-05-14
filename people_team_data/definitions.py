import os
import pathlib

# Get the current working directory
cwd = os.getcwd()
print(f"--- CWD when definitions.py is loaded: {cwd}")

# You can also print the absolute path of the definitions.py file itself for reference
definitions_file_path = pathlib.Path(__file__).resolve()
print(f"--- Absolute path of definitions.py: {definitions_file_path}")
print(f"--- Parent directory of definitions.py: {definitions_file_path.parent}")

raise Exception(
    f"--- Absolute path of definitions.py: {definitions_file_path}\n--- Parent directory of definitions.py: {definitions_file_path.parent}\n--- CWD when definitions.py is loaded: {cwd}"
)

from dagster import (
    AssetExecutionContext,
    Definitions,
    EnvVar,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from . import assets, resources

# 1. Define the dbt project directory
# This should point to where your dbt_project.yml is.
# You confirmed it's 'people_team_data/assets/dbt' relative to your project root.
dbt_project_path = "people_team_data/assets/dbt"
print(
    f"Expected dbt project path: {dbt_project_path}"
)  # For local verification

# 2. Define DBT Target Configuration using Environment Variables
# This dictionary will hold the configuration for your dbt target (e.g., prod target)
# It will be populated from environment variables set in Dagster Cloud.

DBT_TARGET_CONFIG = {
    "type": EnvVar(
        "DBT_PROFILE_TYPE"
    ).get_value(),  # e.g., "bigquery", "postgres", "snowflake"
    "threads": EnvVar.int("DBT_THREADS").get_value(
        default=4
    ),  # Default to 4 threads if not set
    "retries": EnvVar.int("DBT_RETRIES").get_value(
        default=1
    ),  # Optional: default retries
    # Add other common dbt profile parameters if needed
}

# Populate connection-specific details based on the DBT_PROFILE_TYPE
# This logic helps keep your Dagster Cloud environment variables clean and specific.
profile_type = EnvVar("DBT_PROFILE_TYPE").get_value()

if profile_type == "bigquery":
    DBT_TARGET_CONFIG.update(
        {
            "project": EnvVar(
                "GCP_PROJECT"
            ).get_value(),  # Your Google Cloud Project ID
            "dataset": EnvVar(
                "GCP_DATASET"
            ).get_value(),  # Your default BigQuery dataset for dbt
            "location": EnvVar("BQ_LOCATION").get_value(
                default="us-west1"
            ),  # Optional: e.g., "US"
            "keyfile_json": EnvVar(
                "GCP_CREDS"
            ).get_value(),  # JSON content of your service account key
            # Store this as a multi-line secret in Dagster Cloud
            # Add any other BigQuery specific parameters: method, priority, etc.
        }
    )
elif profile_type == "postgres":
    DBT_TARGET_CONFIG.update(
        {
            "host": EnvVar("DBT_PG_HOST").get_value(),
            "user": EnvVar("DBT_PG_USER").get_value(),
            "password": EnvVar("DBT_PG_PASSWORD").get_value(),
            "port": EnvVar.int("DBT_PG_PORT").get_value(),
            "dbname": EnvVar("DBT_PG_DBNAME").get_value(),
            "schema": EnvVar(
                "DBT_PG_SCHEMA"
            ).get_value(),  # This is your dbt target schema
        }
    )
# Add elif blocks for other database types (snowflake, redshift, etc.) as needed

# 3. Initialize DbtProject using project_dir and target_config
dbt_project = DbtProject(
    project_dir=dbt_project_path,
    target_config=DBT_TARGET_CONFIG,
    # Optionally, specify the dbt target name if not using the default or if you want to be explicit
    # target=EnvVar("DBT_TARGET_NAME").get_value(default="prod")
)

# 4. Prepare dbt project if dev (this will now use the configured target_config)
# Ensure DAGSTER_DEV_ENVIRONMENT is set appropriately if you use this logic.
if EnvVar("DAGSTER_DEV_ENVIRONMENT").get_value(default="false") == "true":
    dbt_project.prepare_if_dev()


# 5. Define dbt assets
@dbt_assets(
    manifest=dbt_project.manifest_path,  # dbt_project.manifest_path will be generated
    # when Dagster parses the project with the target_config
    project=dbt_project,  # Pass the configured DbtProject object
)
def dbt_models_asset(
    context: AssetExecutionContext, dbt_cli: DbtCliResource
):  # Renamed dbt to dbt_cli for clarity
    yield from dbt_cli.cli(
        ["build"], context=context, project=dbt_project
    ).stream()


# 4. Load all other assets
all_assets = load_assets_from_modules([assets])
all_assets_job = define_asset_job(name="all_assets_job")

# 5. Get environment resources
env_resources = resources.get_environment_resources()
# 6. Create definitions object
defs = Definitions(
    assets=[*all_assets, dbt_models_asset],  # Include dbt assets here
    jobs=[all_assets_job],
    schedules=[],
    sensors=[],
    resources={
        **env_resources,
        "dbt_cli": DbtCliResource(
            project_dir=dbt_project_path, target_config=DBT_TARGET_CONFIG
        ),
    },
)
