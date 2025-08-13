"""
This module initializes the jobs package by defining all job configurations and shared retry policies.

It includes:
- Individual jobs for BambooHR, Paycom, and Position Control assets.
- A job for all DLT source assets.
- A job for all DBT model assets.
- A comprehensive job for all assets with dependency ordering.
- Shared retry policies for consistent error handling.
"""

from dagster import (
    AssetSelection,
    Backoff,
    Jitter,
    RetryPolicy,
    define_asset_job,
)

from ..assets.dbt.assets import dbt_models_dbt_assets
from ..assets.dlt_sources.dagster_assets import (
    dagster_bamboohr_assets,
    dagster_paycom_assets,
    dagster_position_control_assets,
)

# Shared retry policy for all jobs
DEFAULT_RETRY_POLICY = RetryPolicy(
    max_retries=3,
    delay=10,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS,
)

# Define individual asset jobs for each group of assets
bamboohr_job = define_asset_job(
    name="bamboohr_job",
    description="Job that runs BambooHR source assets",
    selection=AssetSelection.assets(dagster_bamboohr_assets),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)

paycom_job = define_asset_job(
    name="paycom_job",
    description="Job that runs Paycom source assets",
    selection=AssetSelection.assets(dagster_paycom_assets),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)

position_control_job = define_asset_job(
    name="position_control_job",
    description="Job that runs Position Control source assets",
    selection=AssetSelection.assets(dagster_position_control_assets),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)

dlt_source_job = define_asset_job(
    name="dlt_source_job",
    description="Job that runs all DLT source assets (BambooHR, Paycom, Position Control)",
    selection=AssetSelection.assets(
        dagster_bamboohr_assets,
        dagster_paycom_assets,
        dagster_position_control_assets,
    ),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)

dbt_job = define_asset_job(
    name="dbt_job",
    description="Job that runs all DBT model assets",
    selection=AssetSelection.assets(dbt_models_dbt_assets),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)

# Complete pipeline job that includes all assets with proper dependency ordering
all_assets_job = define_asset_job(
    name="all_assets_job",
    description="Job that runs all assets in the project with proper dependency ordering",
    selection=AssetSelection.all(),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)
