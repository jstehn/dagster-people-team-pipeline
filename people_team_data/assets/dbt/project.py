import json
import os
import sys
from pathlib import Path

from dagster import get_dagster_logger
from dagster_dbt import DbtProject

from ...set_env import configure_secrets, secrets_configured

if not secrets_configured:
    configure_secrets()

# --- Start of changes ---
# Global variables to store validation status and detailed message
# These will be updated by verify_central_keyfile()
central_keyfile_is_valid = False
# Provide a default message in case verify_central_keyfile somehow isn't called as expected
KEYFILE_VALIDATION_ERROR_MESSAGE = "GCP keyfile validation status unknown. verify_central_keyfile() may not have run."
# --- End of changes ---

DBT_PROJECT_ROOT_DIR = Path(__file__).joinpath("..").resolve()
DBT_PROFILES_DIR = Path(__file__).joinpath("..", ".dbt").resolve()

# Define the path to the centrally managed keyfile
# Renamed from BIGQUERY_KEYFILE_NAME for clarity as it's a Path object
BIGQUERY_KEYFILE_PATH = DBT_PROJECT_ROOT_DIR.joinpath(
    "..", "..", ".secrets", "gcp_bigquery.json"
).resolve()
os.environ["DBT_BIGQUERY_KEYFILE_PATH"] = str(BIGQUERY_KEYFILE_PATH)


def verify_central_keyfile():
    """
    Verifies the existence and validity of the centrally managed GCP keyfile.
    Sets global flags central_keyfile_is_valid and KEYFILE_VALIDATION_ERROR_MESSAGE.
    Logs issues to Dagster logger and prints to stderr.
    Returns True if a valid keyfile exists, False otherwise.
    """
    global central_keyfile_is_valid, KEYFILE_VALIDATION_ERROR_MESSAGE  # Allow modification of globals
    logger = get_dagster_logger()
    current_keyfile_path_str = str(BIGQUERY_KEYFILE_PATH)

    if not BIGQUERY_KEYFILE_PATH.exists():
        msg = (
            f"CRITICAL FAILURE: Central GCP keyfile does not exist at {current_keyfile_path_str}. "
            f"This file should have been created by setup.py from the GCP_CREDS environment variable. "
            f"Ensure GCP_CREDS is set and setup.py ran successfully during deployment/setup."
        )
        logger.error(msg)
        print(f"ERROR (project.py): {msg}", file=sys.stderr)
        central_keyfile_is_valid = False
        KEYFILE_VALIDATION_ERROR_MESSAGE = msg
        return False

    if BIGQUERY_KEYFILE_PATH.stat().st_size == 0:
        msg = (
            f"CRITICAL FAILURE: Central GCP keyfile at {current_keyfile_path_str} is empty. "
            f"This indicates an issue with its creation by setup.py (e.g., empty GCP_CREDS or write error)."
        )
        logger.error(msg)
        print(f"ERROR (project.py): {msg}", file=sys.stderr)
        central_keyfile_is_valid = False
        KEYFILE_VALIDATION_ERROR_MESSAGE = msg
        return False

    try:
        with open(BIGQUERY_KEYFILE_PATH, "r") as f:
            data = json.load(f)

        if not isinstance(data, dict):
            msg = f"CRITICAL FAILURE: Central GCP keyfile at {current_keyfile_path_str} does not contain a valid JSON object (e.g., not a dictionary)."
            logger.error(msg)
            print(f"ERROR (project.py): {msg}", file=sys.stderr)
            central_keyfile_is_valid = False
            KEYFILE_VALIDATION_ERROR_MESSAGE = msg
            return False

        required_keys = [
            "type",
            "project_id",
            "private_key_id",
            "private_key",
            "client_email",
            "client_id",
        ]
        missing_keys = [key for key in required_keys if key not in data]
        if missing_keys:
            msg = f"CRITICAL FAILURE: Central GCP keyfile at {current_keyfile_path_str} is missing required keys: {', '.join(missing_keys)}."
            logger.error(msg)
            print(f"ERROR (project.py): {msg}", file=sys.stderr)
            central_keyfile_is_valid = False
            KEYFILE_VALIDATION_ERROR_MESSAGE = msg
            return False

        if data.get("type") != "service_account":
            msg = f"CRITICAL FAILURE: Central GCP keyfile at {current_keyfile_path_str} is not of type 'service_account'. Found type: '{data.get('type')}'."
            logger.error(msg)
            print(f"ERROR (project.py): {msg}", file=sys.stderr)
            central_keyfile_is_valid = False
            KEYFILE_VALIDATION_ERROR_MESSAGE = msg
            return False

        success_msg = f"Successfully validated central GCP keyfile at {current_keyfile_path_str} as valid JSON and a service account key."
        logger.info(success_msg)
        # print(f"INFO (project.py): {success_msg}", file=sys.stdout) # Optional: print success to stdout
        central_keyfile_is_valid = True
        KEYFILE_VALIDATION_ERROR_MESSAGE = (
            None  # Clear error message on success
        )
        return True
    except json.JSONDecodeError as je:
        msg = (
            f"CRITICAL FAILURE: Central GCP keyfile at {current_keyfile_path_str} is not valid JSON. Error: {je}. "
            f"This file should have been created by setup.py."
        )
        logger.error(msg)
        print(f"ERROR (project.py): {msg}", file=sys.stderr)
        central_keyfile_is_valid = False
        KEYFILE_VALIDATION_ERROR_MESSAGE = msg
        return False
    except Exception as e:
        msg = f"CRITICAL FAILURE: An unexpected error occurred while validating central GCP keyfile at {current_keyfile_path_str}. Error: {e}"
        logger.error(msg)
        print(f"ERROR (project.py): {msg}", file=sys.stderr)
        central_keyfile_is_valid = False
        KEYFILE_VALIDATION_ERROR_MESSAGE = msg
        return False


# Verify the keyfile upon module load. This sets the global flags.
verify_central_keyfile()

# REMOVED the block that raised RuntimeError immediately based on central_keyfile_is_valid.
# The check will now happen inside the asset.

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    packaged_project_dir=DBT_PROJECT_ROOT_DIR.parent.joinpath(
        "dbt-project"
    ).resolve(),
)
dbt_project.prepare_if_dev()
dbt_project.prepare_if_dev()
