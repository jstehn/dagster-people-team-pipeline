import json  # Added for keyfile JSON validation
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
        logger.info("--- DagsterDbtProject (`dbt_project`) Details ---")
        try:
            logger.info(f"dbt_project.project_dir: {dbt_project.project_dir}")
            logger.info(f"dbt_project.profiles_dir: {dbt_project.profiles_dir}")
            if hasattr(dbt_project, "profile"):
                logger.info(f"dbt_project.profile: {dbt_project.profile}")
            else:
                logger.info("dbt_project.profile attribute not found.")
            if hasattr(dbt_project, "target"):
                logger.info(f"dbt_project.target: {dbt_project.target}")
            else:
                logger.info("dbt_project.target attribute not found.")

            # Log profiles.yml path and content
            if dbt_project.profiles_dir:
                profiles_yml_path = (
                    Path(dbt_project.profiles_dir) / "profiles.yml"
                )
                logger.info(
                    f"Expected profiles.yml path (from dbt_project): {profiles_yml_path}"
                )
                if profiles_yml_path.exists():
                    try:
                        profiles_content = profiles_yml_path.read_text()
                        logger.info(
                            f"profiles.yml (from dbt_project) content:\\n{profiles_content}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Could not read profiles.yml content from {profiles_yml_path}: {e}"
                        )
                else:
                    logger.warning(
                        f"profiles.yml (from dbt_project) not found at: {profiles_yml_path}"
                    )
            else:
                logger.warning(
                    "dbt_project.profiles_dir is not set; cannot locate its profiles.yml."
                )
        except Exception as e:
            logger.error(
                f"Error accessing dbt_project attributes or its profiles.yml: {e}"
            )

        # Log Environment Variables
        logger.info("--- Relevant Environment Variables ---")
        keyfile_env_var = "DBT_BIGQUERY_KEYFILE_PATH"
        keyfile_path_from_env = os.environ.get(keyfile_env_var)
        logger.info(
            f"Environment variable {keyfile_env_var}: {keyfile_path_from_env}"
        )

        gcp_project_env_var = "GCP_PROJECT"
        gcp_project_from_env = os.environ.get(gcp_project_env_var)
        logger.info(
            f"Environment variable {gcp_project_env_var}: {gcp_project_from_env}"
        )

        # Log and Validate Keyfile
        logger.info("--- GCP Keyfile Validation (from env var) ---")
        if keyfile_path_from_env:
            keyfile_actual_path = Path(keyfile_path_from_env).resolve()
            logger.info(
                f"Attempting to read keyfile from resolved path: {keyfile_actual_path}"
            )
            if keyfile_actual_path.exists() and keyfile_actual_path.is_file():
                try:
                    keyfile_content = keyfile_actual_path.read_text()
                    logger.info(
                        f"Keyfile ({keyfile_actual_path}) content (first 256 chars):\\n{keyfile_content[:256]}..."
                    )
                    # Validate if keyfile content is valid JSON
                    try:
                        json.loads(keyfile_content)
                        logger.info(
                            f"Keyfile ({keyfile_actual_path}) IS valid JSON."
                        )
                    except json.JSONDecodeError as je:
                        logger.error(
                            f"Keyfile ({keyfile_actual_path}) IS NOT valid JSON. Error: {je}"
                        )
                except Exception as e:
                    logger.error(
                        f"Could not read keyfile content from {keyfile_actual_path}: {e}"
                    )
            else:
                logger.warning(
                    f"Keyfile not found or is not a file at resolved path: {keyfile_actual_path}"
                )
        else:
            logger.warning(
                f"Environment variable {keyfile_env_var} is not set. Cannot attempt to read or validate keyfile."
            )

        # Log DbtCliResource attributes
        logger.info("--- DbtCliResource (`dbt`) Instance Details ---")
        try:
            logger.info(
                f"dbt (DbtCliResource) - project_dir: {dbt.project_dir}"
            )

            dbt_profiles_dir = None
            if hasattr(dbt, "profiles_dir") and dbt.profiles_dir:
                dbt_profiles_dir = dbt.profiles_dir
                logger.info(
                    f"dbt (DbtCliResource) - explicit profiles_dir: {dbt_profiles_dir}"
                )
            elif (
                hasattr(dbt, "_dbt_project_instance")
                and dbt._dbt_project_instance
                and hasattr(dbt._dbt_project_instance, "profiles_dir")
                and dbt._dbt_project_instance.profiles_dir
            ):
                dbt_profiles_dir = dbt._dbt_project_instance.profiles_dir
                logger.info(
                    f"dbt (DbtCliResource) - internal _dbt_project_instance.profiles_dir: {dbt_profiles_dir}"
                )
            else:
                logger.info(
                    "dbt (DbtCliResource) - profiles_dir: Not explicitly set on resource or its internal project instance. DBT will use default search paths (e.g., ~/.dbt/, or project_dir if profiles.yml is there)."
                )

            dbt_profile_name = None
            if hasattr(dbt, "profile") and dbt.profile:
                dbt_profile_name = dbt.profile
                logger.info(
                    f"dbt (DbtCliResource) - explicit profile: {dbt_profile_name}"
                )
            elif (
                hasattr(dbt, "_dbt_project_instance")
                and dbt._dbt_project_instance
                and hasattr(dbt._dbt_project_instance, "profile")
                and dbt._dbt_project_instance.profile
            ):
                dbt_profile_name = dbt._dbt_project_instance.profile
                logger.info(
                    f"dbt (DbtCliResource) - internal _dbt_project_instance.profile: {dbt_profile_name}"
                )
            else:
                logger.info(
                    "dbt (DbtCliResource) - profile: Not explicitly set. DBT will use profile from dbt_project.yml or 'default'."
                )

            dbt_target_name = None
            if hasattr(dbt, "target") and dbt.target:
                dbt_target_name = dbt.target
                logger.info(
                    f"dbt (DbtCliResource) - explicit target: {dbt_target_name}"
                )
            elif (
                hasattr(dbt, "_dbt_project_instance")
                and dbt._dbt_project_instance
                and hasattr(dbt._dbt_project_instance, "target")
                and dbt._dbt_project_instance.target
            ):
                dbt_target_name = dbt._dbt_project_instance.target
                logger.info(
                    f"dbt (DbtCliResource) - internal _dbt_project_instance.target: {dbt_target_name}"
                )
            else:
                logger.info(
                    "dbt (DbtCliResource) - target: Not explicitly set. DBT will use target from dbt_project.yml or profile's default."
                )

        except Exception as e:
            logger.error(
                f"Error accessing dbt (DbtCliResource) attributes: {e}"
            )

        # --- DBT Configuration Consistency Checks ---
        logger.info("--- DBT Configuration Consistency Checks ---")
        try:
            # Profiles Dir
            dbt_project_pd = (
                Path(dbt_project.profiles_dir).resolve()
                if dbt_project.profiles_dir
                else None
            )
            # For dbt (DbtCliResource), dbt_profiles_dir might be None if dbt is to use default resolution.
            # If dbt_profiles_dir is captured above, resolve it. Otherwise, it's harder to compare directly here.
            dbt_resource_pd_str = (
                dbt_profiles_dir  # From DbtCliResource logging section
            )
            dbt_resource_pd = (
                Path(dbt_resource_pd_str).resolve()
                if dbt_resource_pd_str
                else None
            )

            logger.info(
                f"Consistency - Resolved dbt_project.profiles_dir: {dbt_project_pd}"
            )
            logger.info(
                f"Consistency - Resolved dbt (DbtCliResource) effective profiles_dir: {dbt_resource_pd if dbt_resource_pd else 'Default dbt behavior (e.g., ~/.dbt/, or project_dir)'}"
            )
            if dbt_project_pd and dbt_resource_pd:
                if dbt_project_pd == dbt_resource_pd:
                    logger.info("Consistency - profiles_dir: MATCH")
                else:
                    logger.warning("Consistency - profiles_dir: MISMATCH")
            elif (
                dbt_project_pd or dbt_resource_pd_str
            ):  # If one is set and the other implies default or is different
                logger.warning(
                    "Consistency - profiles_dir: POTENTIAL MISMATCH (one is specified, other might be default or different)"
                )
            else:  # both None or not set, dbt uses its defaults
                logger.info(
                    "Consistency - profiles_dir: Both dbt_project and DbtCliResource seem to rely on dbt's default profiles_dir resolution."
                )

            # Profile Name
            dbt_project_prof = getattr(dbt_project, "profile", None)
            # dbt_profile_name captured from DbtCliResource logging section
            logger.info(
                f"Consistency - dbt_project.profile: {dbt_project_prof}"
            )
            logger.info(
                f"Consistency - dbt (DbtCliResource) effective profile: {dbt_profile_name}"
            )
            if dbt_project_prof and dbt_profile_name:
                if dbt_project_prof == dbt_profile_name:
                    logger.info("Consistency - profile: MATCH")
                else:
                    logger.warning("Consistency - profile: MISMATCH")
            elif dbt_project_prof or dbt_profile_name:
                logger.warning(
                    "Consistency - profile: POTENTIAL MISMATCH (one is specified, other might be default or different)"
                )
            else:
                logger.info(
                    "Consistency - profile: Both seem to rely on dbt's default profile resolution."
                )

            # Target Name
            dbt_project_tgt = getattr(dbt_project, "target", None)
            # dbt_target_name captured from DbtCliResource logging section
            logger.info(f"Consistency - dbt_project.target: {dbt_project_tgt}")
            logger.info(
                f"Consistency - dbt (DbtCliResource) effective target: {dbt_target_name}"
            )
            if dbt_project_tgt and dbt_target_name:
                if dbt_project_tgt == dbt_target_name:
                    logger.info("Consistency - target: MATCH")
                else:
                    logger.warning("Consistency - target: MISMATCH")
            elif dbt_project_tgt or dbt_target_name:
                logger.warning(
                    "Consistency - target: POTENTIAL MISMATCH (one is specified, other might be default or different)"
                )
            else:
                logger.info(
                    "Consistency - target: Both seem to rely on dbt's default target resolution."
                )

        except Exception as e:
            logger.error(f"Error during consistency checks: {e}")

        logger.info("--- End of DBT Configuration Debugging ---")
        # --- End of Added Debug Logging ---

    # --- Start of dbt debug command ---
    logger.info("Running dbt debug to check connection and configurations...")
    try:
        dbt_debug_cli_invocation = dbt.cli(["debug"], context=context)
        logger.info(
            f"Executing dbt CLI command: {' '.join(dbt_debug_cli_invocation.process.args)}"
        )
        for event in dbt_debug_cli_invocation.stream():
            logger.info(f"dbt debug event: {event}")
        dbt_debug_cli_invocation.wait()  # Ensure debug command completes
        if dbt_debug_cli_invocation.is_successful():
            logger.info("dbt debug command completed successfully.")
        else:
            logger.error(
                "dbt debug command failed. Please check the logs above for details."
            )
            # Optionally, raise an exception or prevent build if debug fails
            # raise RuntimeError("dbt debug command failed, halting execution.")
    except Exception as e:
        logger.error(f"An error occurred while running dbt debug: {e}")
        # Optionally, re-raise or handle as needed
        # raise
    logger.info("--- Finished dbt debug command ---")
    # --- End of dbt debug command ---

    logger.info("Starting dbt build process...")

    dbt_build_cli_invocation = dbt.cli(["build", "--debug"], context=context)

    logger.info(
        f"Executing dbt CLI command: {' '.join(dbt_build_cli_invocation.process.args)}"
    )

    for event in dbt_build_cli_invocation.stream():
        logger.info(f"dbt event: {event}")
        yield event

    logger.info("Finished dbt build process.")
