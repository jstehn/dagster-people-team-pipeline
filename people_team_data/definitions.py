"""
This module defines the Dagster pipeline configuration.

It includes:
- Loading all assets from the `assets` module.
- Configuring jobs, schedules, sensors, and resources.
- Setting up environment secrets for secure access.
"""

import os
import sys

# Add the project root directory to the system path for module resolution
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

# Load all assets from the `assets` module
all_assets = load_assets_from_modules([assets])

# Configure environment secrets for secure access
set_env.configure_secrets()

# Define the Dagster pipeline configuration
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
    resources=all_resources,
)
