# people_team_data/definitions.py

from dagster import Definitions, load_assets_from_modules

from . import assets, resources  # , jobs, schedules, sensors

# Load all assets from the assets module
all_assets = load_assets_from_modules([assets])

# Create the Definitions object
defs = Definitions(
    assets=all_assets,
    # jobs=[jobs.my_job],
    # schedules=[schedules.my_schedule],
    # sensors=[sensors.my_sensor],
    resources={
        "postgres_db": resources.postgres_db,
        "google_service_account": resources.google_service_account,
        "dlt": resources.dlt,
    },
)
