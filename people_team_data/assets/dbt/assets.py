from dagster import AssetExecutionContext, get_dagster_logger
from dagster_dbt import DbtCliResource, dbt_assets

from .project import (
    KEYFILE_VALIDATION_ERROR_MESSAGE,
    central_keyfile_is_valid,
    dbt_project,
)


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    logger = get_dagster_logger()

    if not central_keyfile_is_valid:
        error_msg_for_dagster = (
            f"Halting dbt execution: GCP keyfile validation failed during module import. "
            f"Reason: {KEYFILE_VALIDATION_ERROR_MESSAGE}"
        )
        logger.error(error_msg_for_dagster)
        raise RuntimeError(error_msg_for_dagster)
    else:
        logger.info(
            "GCP keyfile was successfully validated during module import. Proceeding with dbt execution."
        )

    logger.info("Starting dbt build process...")

    dbt_build_cli_invocation = dbt.cli(["build", "--debug"], context=context)

    logger.info(
        f"Executing dbt CLI command: {' '.join(dbt_build_cli_invocation.process.args)}"
    )

    for event in dbt_build_cli_invocation.stream():
        logger.info(f"dbt event: {event}")
        yield event

    logger.info("Finished dbt build process.")
