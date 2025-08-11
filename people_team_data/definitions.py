import os
import sys

project_root_dir = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
if project_root_dir not in sys.path:
    sys.path.insert(0, project_root_dir)

from dagster import Definitions, load_assets_from_modules  # noqa: E402

import set_env  # noqa: E402

from . import assets  # noqa: E402
from .jobs import (  # noqa: E402
    all_assets_job,
    bamboohr_job,
    dbt_job,
    dlt_source_job,
    paycom_job,
    position_control_job,
)
from .resources import all_resources  # noqa: E402
from .schedules import daily_all_assets_schedule  # noqa: E402

# Load all assets
all_assets = load_assets_from_modules([assets])

set_env.configure_secrets()
# Create definitions object using the imported resources
defs = Definitions(
    assets=[*all_assets],
    jobs=[
        all_assets_job,
        bamboohr_job,
        paycom_job,
        position_control_job,
        dlt_source_job,
        dbt_job,
    ],
    schedules=[daily_all_assets_schedule],
    sensors=[],
    resources=all_resources,  # Use the all_resources dictionary directly
)
