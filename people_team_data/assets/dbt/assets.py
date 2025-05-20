import os  # Added for environment variables
from pathlib import Path  # Added for path operations

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

        # --- Start of Added Debug Logging ---
        logger.info("--- DBT Configuration Debugging ---")

        # Log dbt_project attributes
        try:
            logger.info(f"dbt_project.project_dir: {dbt_project.project_dir}")
            logger.info(f"dbt_project.profiles_dir: {dbt_project.profiles_dir}")

            # Log profiles.yml path and content
            if dbt_project.profiles_dir:
                profiles_yml_path = (
                    Path(dbt_project.profiles_dir) / "profiles.yml"
                )
                logger.info(f"Expected profiles.yml path: {profiles_yml_path}")
                if profiles_yml_path.exists():
                    try:
                        profiles_content = profiles_yml_path.read_text()
                        logger.info(
                            f"profiles.yml content:\\n{profiles_content}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Could not read profiles.yml content from {profiles_yml_path}: {e}"
                        )
                else:
                    logger.warning(
                        f"profiles.yml not found at: {profiles_yml_path}"
                    )
            else:
                logger.warning("dbt_project.profiles_dir is not set.")
        except Exception as e:
            logger.error(
                f"Error accessing dbt_project attributes or profiles.yml: {e}"
            )

        # Log Keyfile Path (from env var)
        keyfile_env_var = "DBT_BIGQUERY_KEYFILE_PATH"
        keyfile_path_from_env = os.environ.get(keyfile_env_var)
        logger.info(
            f"Value of environment variable {keyfile_env_var}: {keyfile_path_from_env}"
        )

        # Log Keyfile Content
        if keyfile_path_from_env:
            keyfile_actual_path = Path(keyfile_path_from_env)
            logger.info(
                f"Attempting to read keyfile from resolved path: {keyfile_actual_path}"
            )
            if keyfile_actual_path.exists():
                try:
                    keyfile_content = keyfile_actual_path.read_text()
                    # WARNING: Logging entire keyfile content is a security risk.
                    # Consider logging only a portion or a hash for production debugging.
                    logger.info(
                        f"Keyfile ({keyfile_actual_path}) content (WARNING: Contains sensitive data - first 256 chars):\\n{keyfile_content[:256]}..."
                    )
                except Exception as e:
                    logger.error(
                        f"Could not read keyfile content from {keyfile_actual_path}: {e}"
                    )
            else:
                logger.warning(
                    f"Keyfile not found at path from environment variable: {keyfile_actual_path}"
                )
        else:
            logger.warning(
                f"Environment variable {keyfile_env_var} is not set. Cannot attempt to read keyfile."
            )

        # --- Start of DbtCliResource Debug Logging ---
        logger.info("--- DbtCliResource Instance Debugging ---")
        try:
            logger.info(
                f"dbt (DbtCliResource) - project_dir: {dbt.project_dir}"
            )
            # DbtCliResource might have profiles_dir, profile, and target directly or via its internal dbt_project_instance
            # Accessing them safely:
            if hasattr(dbt, "profiles_dir") and dbt.profiles_dir:
                logger.info(
                    f"dbt (DbtCliResource) - profiles_dir: {dbt.profiles_dir}"
                )
            elif (
                hasattr(dbt, "_dbt_project_instance")
                and dbt._dbt_project_instance
                and hasattr(dbt._dbt_project_instance, "profiles_dir")
            ):
                logger.info(
                    f"dbt (DbtCliResource) - internal _dbt_project_instance.profiles_dir: {dbt._dbt_project_instance.profiles_dir}"
                )
            else:
                logger.info(
                    "dbt (DbtCliResource) - profiles_dir: Not directly available or set to None"
                )

            if hasattr(dbt, "profile") and dbt.profile:
                logger.info(f"dbt (DbtCliResource) - profile: {dbt.profile}")
            elif (
                hasattr(dbt, "_dbt_project_instance")
                and dbt._dbt_project_instance
                and hasattr(dbt._dbt_project_instance, "profile")
            ):
                logger.info(
                    f"dbt (DbtCliResource) - internal _dbt_project_instance.profile: {dbt._dbt_project_instance.profile}"
                )
            else:
                logger.info(
                    "dbt (DbtCliResource) - profile: Not directly available or set to None"
                )

            if hasattr(dbt, "target") and dbt.target:
                logger.info(f"dbt (DbtCliResource) - target: {dbt.target}")
            elif (
                hasattr(dbt, "_dbt_project_instance")
                and dbt._dbt_project_instance
                and hasattr(dbt._dbt_project_instance, "target")
            ):
                logger.info(
                    f"dbt (DbtCliResource) - internal _dbt_project_instance.target: {dbt._dbt_project_instance.target}"
                )
            else:
                logger.info(
                    "dbt (DbtCliResource) - target: Not directly available or set to None"
                )

        except Exception as e:
            logger.error(
                f"Error accessing dbt (DbtCliResource) attributes: {e}"
            )
        logger.info("--- End of DbtCliResource Instance Debugging ---")
        # --- End of DbtCliResource Debug Logging ---

        logger.info("--- End of DBT Configuration Debugging ---")
        # --- End of Added Debug Logging ---

    logger.info("Starting dbt build process...")

    dbt_build_cli_invocation = dbt.cli(["build", "--debug"], context=context)

    logger.info(
        f"Executing dbt CLI command: {' '.join(dbt_build_cli_invocation.process.args)}"
    )

    for event in dbt_build_cli_invocation.stream():
        logger.info(f"dbt event: {event}")
        yield event

    logger.info("Finished dbt build process.")
