from dagster import Definitions, define_asset_job, load_assets_from_modules

from . import assets
from .resources import all_resources  # Import the new all_resources dictionary
from .set_env import configure_secrets, secrets_configured

if not secrets_configured:
    configure_secrets()

# Load all assets
all_assets = load_assets_from_modules([assets])
all_assets_job = define_asset_job(name="all_assets_job")

# Create definitions object using the imported resources
defs = Definitions(
    assets=[*all_assets],
    jobs=[all_assets_job],
    schedules=[],
    sensors=[],
    resources=all_resources,  # Use the all_resources dictionary directly
)
