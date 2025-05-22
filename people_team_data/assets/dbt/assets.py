import json  # Added for keyfile JSON validation
import os  # Added for environment variables
import subprocess  # Added for direct dbt executable call
from pathlib import Path  # Added for path operations

import yaml  # Added for parsing profiles.yml
from dagster import AssetExecutionContext, get_dagster_logger
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_dbt.errors import DagsterDbtCliRuntimeError  # Added import

from .project import dbt_project


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    logger = get_dagster_logger()

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
            profiles_yml_path = Path(dbt_project.profiles_dir) / "profiles.yml"
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

    gcp_project_env_var = "GCP_BASE_PROJECT"
    gcp_project_from_env = os.environ.get(gcp_project_env_var)
    logger.info(
        f"Environment variable {gcp_project_env_var}: {gcp_project_from_env}"
    )

    gac_env_var = "GOOGLE_APPLICATION_CREDENTIALS"
    gac_path_from_env = os.environ.get(gac_env_var)
    logger.info(f"Environment variable {gac_env_var}: {gac_path_from_env}")

    current_path_env_var = os.environ.get("PATH")
    logger.info(
        f"Current process PATH environment variable: {current_path_env_var}"
    )

    # Log and Validate Keyfile from profiles.yml
    logger.info("--- GCP Keyfile Validation (from profiles.yml) ---")
    keyfile_path_from_profiles = None
    profiles_yml_for_keyfile_path = None

    if hasattr(dbt_project, "profiles_dir") and dbt_project.profiles_dir:
        profiles_yml_for_keyfile_path = (
            Path(dbt_project.profiles_dir) / "profiles.yml"
        )
        logger.info(
            f"Attempting to load profiles.yml for keyfile from: {profiles_yml_for_keyfile_path}"
        )
        if profiles_yml_for_keyfile_path.exists():
            try:
                with open(profiles_yml_for_keyfile_path, "r") as f:
                    profiles_data = yaml.safe_load(f)

                # Determine profile and target for keyfile lookup
                profile_to_use = getattr(dbt, "profile", None)
                if not profile_to_use:
                    profile_to_use = getattr(dbt_project, "profile", None)
                if not profile_to_use:
                    # Fallback: use 'default' if it exists, or the first profile name if only one, else 'default'
                    if profiles_data and "default" in profiles_data:
                        profile_to_use = "default"
                    elif profiles_data and len(profiles_data) == 1:
                        profile_to_use = list(profiles_data.keys())[0]
                        logger.info(
                            f"Using the only profile found in profiles.yml: '{profile_to_use}'"
                        )
                    else:
                        profile_to_use = (
                            "default"  # Defaulting as per example structure
                        )
                        logger.warning(
                            f"Profile name not determined from dbt/dbt_project context, defaulting to '{profile_to_use}' for keyfile lookup."
                        )
                logger.info(
                    f"Using profile name '{profile_to_use}' for keyfile lookup."
                )

                target_to_use = getattr(dbt, "target", None)
                if not target_to_use:
                    target_to_use = getattr(dbt_project, "target", None)
                if not target_to_use:
                    # Try to get default target from the determined profile in profiles.yml
                    if (
                        profiles_data
                        and profile_to_use in profiles_data
                        and isinstance(profiles_data[profile_to_use], dict)
                        and profiles_data[profile_to_use].get("target")
                    ):
                        target_to_use = profiles_data[profile_to_use]["target"]
                        logger.info(
                            f"Using target '{target_to_use}' from profile '{profile_to_use}' in profiles.yml."
                        )
                    else:
                        # Fallback: use 'staff' as per example, or could be the first output key
                        target_to_use = (
                            "staff"  # Defaulting as per example structure
                        )
                        logger.warning(
                            f"Target name not determined from dbt/dbt_project context or profile, defaulting to '{target_to_use}' for keyfile lookup."
                        )
                logger.info(
                    f"Using target name '{target_to_use}' for keyfile lookup."
                )

                keyfile_relative_path_str = None
                if (
                    profiles_data
                    and profile_to_use in profiles_data
                    and isinstance(profiles_data[profile_to_use], dict)
                    and "outputs" in profiles_data[profile_to_use]
                    and isinstance(
                        profiles_data[profile_to_use]["outputs"], dict
                    )
                    and target_to_use
                    in profiles_data[profile_to_use]["outputs"]
                    and isinstance(
                        profiles_data[profile_to_use]["outputs"][target_to_use],
                        dict,
                    )
                    and "keyfile"
                    in profiles_data[profile_to_use]["outputs"][target_to_use]
                ):
                    keyfile_relative_path_str = profiles_data[profile_to_use][
                        "outputs"
                    ][target_to_use]["keyfile"]
                    logger.info(
                        f"Found keyfile path in profiles.yml ('{profile_to_use}' -> 'outputs' -> '{target_to_use}' -> 'keyfile'): {keyfile_relative_path_str}"
                    )
                else:
                    logger.warning(
                        f"Could not find 'keyfile' at expected path '{profile_to_use}.outputs.{target_to_use}.keyfile' in {profiles_yml_for_keyfile_path}"
                    )

                if keyfile_relative_path_str:
                    # The keyfile path in profiles.yml is relative to the directory of profiles.yml
                    keyfile_path_from_profiles = (
                        Path(dbt_project.profiles_dir)
                        / keyfile_relative_path_str
                    ).resolve()
                    logger.info(
                        f"Resolved keyfile path from profiles.yml: {keyfile_path_from_profiles}"
                    )
                else:
                    logger.warning(
                        f"Could not extract keyfile path string from {profiles_yml_for_keyfile_path} for profile '{profile_to_use}' and target '{target_to_use}'."
                    )

            except yaml.YAMLError as ye:
                logger.error(
                    f"Error parsing YAML from {profiles_yml_for_keyfile_path}: {ye}"
                )
            except Exception as e:
                logger.error(
                    f"Could not read or parse {profiles_yml_for_keyfile_path} for keyfile: {e}",
                    exc_info=True,
                )
        else:
            logger.warning(
                f"profiles.yml not found at: {profiles_yml_for_keyfile_path} (for keyfile extraction)"
            )
    else:
        logger.warning(
            "dbt_project.profiles_dir is not set; cannot locate profiles.yml to extract keyfile."
        )

    # Validate the keyfile obtained from profiles.yml
    if keyfile_path_from_profiles:
        keyfile_actual_path = keyfile_path_from_profiles  # Already resolved
        logger.info(
            f"Attempting to read keyfile from resolved profiles.yml path: {keyfile_actual_path}"
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
                f"Keyfile not found or is not a file at resolved path from profiles.yml: {keyfile_actual_path}"
            )
    else:
        logger.warning(
            "Keyfile path not found/extracted from profiles.yml. Cannot attempt to read or validate keyfile."
        )

    # Log DbtCliResource attributes
    logger.info("--- DbtCliResource (`dbt`) Instance Details ---")
    try:
        logger.info(f"dbt (DbtCliResource) - project_dir: {dbt.project_dir}")

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
        logger.error(f"Error accessing dbt (DbtCliResource) attributes: {e}")

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
            Path(dbt_resource_pd_str).resolve() if dbt_resource_pd_str else None
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
        logger.info(f"Consistency - dbt_project.profile: {dbt_project_prof}")
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

    # --- Start of dbt debug command (using subprocess) ---
    logger.info(
        "Running dbt debug via subprocess to check connection and configurations..."
    )
    try:
        dbt_executable_path = "dbt"

        # Base command
        command = [
            dbt_executable_path,  # Use the corrected variable
            "debug",
            "--no-version-check",
            "--project-dir",
            str(dbt.project_dir),  # Explicitly set project directory
        ]

        # Conditionally add other dbt arguments if configured on the DbtCliResource
        if dbt.profiles_dir:
            command.extend(["--profiles-dir", str(dbt.profiles_dir)])
        if dbt.profile:
            command.extend(["--profile", dbt.profile])
        if dbt.target:
            command.extend(["--target", dbt.target])

        # Prepare environment for subprocess
        # Start with a copy of the current environment
        sub_env = os.environ.copy()

        if gcp_project_from_env:
            sub_env["GCP_BASE_PROJECT"] = gcp_project_from_env
        else:
            logger.warning(
                "GCP_BASE_PROJECT not found in os.environ for subprocess."
            )

        logger.info(f"dbt executable for subprocess: {dbt_executable_path}")
        logger.info(f"dbt command for subprocess: {' '.join(command)}")
        logger.info(f"dbt project_dir for subprocess (cwd): {dbt.project_dir}")
        logger.info(f"PATH for dbt debug subprocess: {sub_env.get('PATH')}")

        sensitive_keys = ["KEYFILE", "TOKEN", "PASSWORD", "SECRET"]
        logged_env_vars = {
            k: (
                v
                if not any(s_key in k.upper() for s_key in sensitive_keys)
                else "****"
            )
            for k, v in sub_env.items()
            if "DBT_" in k or "GCP_" in k  # Log relevant vars
        }
        logger.info(
            f"Selected environment variables for dbt debug subprocess (potentially redacted): {logged_env_vars}"
        )

        process = subprocess.run(
            command,
            cwd=str(dbt.project_dir),
            capture_output=True,
            text=True,
            env=sub_env,  # Use the prepared environment
            check=False,
        )

        logger.info("--- dbt debug (subprocess) STDOUT ---")
        if process.stdout:
            logger.info(process.stdout)
        else:
            logger.info("No STDOUT from dbt debug subprocess.")

        logger.info("--- dbt debug (subprocess) STDERR ---")
        if process.stderr:
            # dbt debug often prints successful checks to stderr, so log as info unless return code is non-zero
            if process.returncode == 0:
                logger.info(
                    f"STDERR from dbt debug (return code 0):\n{process.stderr}"
                )
            else:
                logger.error(
                    f"STDERR from dbt debug (return code {process.returncode}):\n{process.stderr}"
                )
        else:
            logger.info("No STDERR from dbt debug subprocess.")

        if process.returncode == 0:
            logger.info("dbt debug (subprocess) completed successfully.")
        else:
            logger.error(
                f"dbt debug (subprocess) failed with return code {process.returncode}."
            )
            # Optionally, raise an exception or prevent build if debug fails
            # raise RuntimeError(f"dbt debug (subprocess) failed. Check logs.")

    except Exception as e:
        logger.error(
            f"An error occurred while preparing or running dbt debug (subprocess): {e}",
            exc_info=True,
        )
        # Optionally, re-raise or handle as needed
        # raise
    finally:
        logger.info(
            "--- Finished dbt debug (subprocess) command execution attempt ---"
        )

    # Attempt to read dbt.log from the project's standard logs directory
    # This is a general check, the run-specific log is more critical for failures.
    logger.info(
        "--- Attempting to read general dbt.log from project logs directory ---"
    )
    try:
        # dbt.project_dir should be the correct root of the dbt project
        dbt_project_log_file_path = Path(dbt.project_dir) / "logs" / "dbt.log"
        logger.info(f"Looking for dbt.log at: {dbt_project_log_file_path}")
        if (
            dbt_project_log_file_path.exists()
            and dbt_project_log_file_path.is_file()
        ):
            log_content = dbt_project_log_file_path.read_text()
            # Log a portion of the file, e.g., the last 2000 characters
            logger.info(
                f"Contents of {dbt_project_log_file_path} (last 2000 chars):\\n...{log_content[-2000:]}"
            )
        else:
            logger.warning(
                f"{dbt_project_log_file_path} not found or is not a file."
            )
    except Exception as e_log:
        logger.error(f"Could not read dbt.log from project logs: {e_log}")
    logger.info("--- Finished attempt to read dbt.log ---")

    logger.info("Starting dbt build process...")

    dbt_build_cli_invocation = None  # Initialize
    try:
        dbt_build_cli_invocation = dbt.cli(
            ["build", "--debug"], context=context
        )

        logger.info(
            f"Executing dbt CLI command: {' '.join(dbt_build_cli_invocation.process.args)}"
        )

        for event in dbt_build_cli_invocation.stream():
            logger.info(f"dbt event: {event}")
            yield event

        logger.info("Finished dbt build process.")

    except DagsterDbtCliRuntimeError as e:
        logger.error(
            f"dbt build command failed with DagsterDbtCliRuntimeError: {e}"
        )
        if dbt_build_cli_invocation:
            log_path = dbt_build_cli_invocation.target_path / "dbt.log"
            logger.info(
                f"Attempting to read dbt build specific log from: {log_path}"
            )
            if log_path.exists() and log_path.is_file():
                try:
                    log_content = log_path.read_text()
                    logger.info(
                        f"Contents of build-specific dbt.log ({log_path}):\\n{log_content}"
                    )
                except Exception as log_read_e:
                    logger.error(
                        f"Could not read build-specific dbt.log from {log_path}: {log_read_e}"
                    )
            else:
                logger.warning(
                    f"Build-specific dbt.log not found or is not a file at: {log_path}. Error object log_path: {getattr(e, 'log_path', 'N/A')}"
                )
        else:
            logger.warning(
                "dbt_build_cli_invocation was not initialized before the error occurred."
            )
        raise  # Re-raise the exception after logging
    except Exception as e_general:
        logger.error(
            f"An unexpected error occurred during dbt build: {e_general}"
        )
        if dbt_build_cli_invocation:
            log_path = dbt_build_cli_invocation.target_path / "dbt.log"
            logger.info(
                f"Attempting to read dbt build specific log (general exception) from: {log_path}"
            )
            if log_path.exists() and log_path.is_file():
                try:
                    log_content = log_path.read_text()
                    logger.info(
                        f"Contents of build-specific dbt.log ({log_path}) (general exception):\\n{log_content}"
                    )
                except Exception as log_read_e:
                    logger.error(
                        f"Could not read build-specific dbt.log from {log_path} (general exception): {log_read_e}"
                    )
            else:
                logger.warning(
                    f"Build-specific dbt.log not found or is not a file at: {log_path} (general exception)."
                )
        raise  # Re-raise the exception
