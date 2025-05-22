import os
import sys

project_root_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
if project_root_dir not in sys.path:
    sys.path.insert(0, project_root_dir)

from dagster import (  # noqa: E402
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

import set_env  # noqa: E402

from . import assets  # noqa: E402
from .resources import all_resources  # noqa: E402

# Load all assets
all_assets = load_assets_from_modules([assets])
all_assets_job = define_asset_job(name="all_assets_job")

set_env.configure_secrets()
# Create definitions object using the imported resources
defs = Definitions(
    assets=[*all_assets],
    jobs=[all_assets_job],
    schedules=[],
    sensors=[],
    resources=all_resources,  # Use the all_resources dictionary directly
)
