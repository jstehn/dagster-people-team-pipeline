from dagster import Definitions, define_asset_job, load_assets_from_modules

from . import assets, resources

# 4. Load all other assets
all_assets = load_assets_from_modules([assets])
all_assets_job = define_asset_job(name="all_assets_job")

# 5. Get environment resources
env_resources = resources.get_environment_resources()
# 6. Create definitions object
defs = Definitions(
    assets=[*all_assets],
    jobs=[all_assets_job],
    schedules=[],
    sensors=[],
    resources={
        **env_resources,
    },
)
