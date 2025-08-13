from dagster import (
    AssetSelection,
    Backoff,
    Jitter,
    RetryPolicy,
    define_asset_job,
)

# Import asset groups
from ..assets.dbt.assets import dbt_models_dbt_assets
from ..assets.dlt_sources.dagster_assets import (
    dagster_bamboohr_assets,
    dagster_paycom_assets,
    dagster_position_control_assets,
)

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

# Define dependency-aware jobs - these asset jobs implicitly respect the dependencies
# between assets due to how Dagster handles asset dependencies

# Complete pipeline job that includes all assets with proper dependency ordering
all_assets_job = define_asset_job(
    name="all_assets_job",
    description="Job that runs all assets in the project with proper dependency ordering",
    selection=AssetSelection.all(),
    op_retry_policy=DEFAULT_RETRY_POLICY,
)
