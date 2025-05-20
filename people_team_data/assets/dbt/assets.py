from dagster import AssetExecutionContext, get_dagster_logger
from dagster_dbt import DbtCliResource, dbt_assets

from .project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    logger = get_dagster_logger()
    logger.info("Starting dbt build process...")

    # Added --debug flag for verbose output
    dbt_build_cli_invocation = dbt.cli(["build", "--debug"], context=context)

    logger.info(
        f"Executing dbt CLI command: {' '.join(dbt_build_cli_invocation.process.args)}"
    )

    for event in dbt_build_cli_invocation.stream():
        logger.info(f"dbt event: {event}")
        yield event

    logger.info("Finished dbt build process.")
