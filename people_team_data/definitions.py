# people_team_data/definitions.py

from dagster import Definitions, EnvVar, load_assets_from_modules

from . import assets, resources  # , jobs, schedules, sensors

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Get the environment specific resources (prod, stage, dev)
env = EnvVar("ENV")
env_resources = resources.get_environment_resources(env)

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
    resources={
        "postgres_db": resources.postgres_db,
        "google_service_account": resources.google_service_account,
        "dlt": resources.dlt,
    },
)
