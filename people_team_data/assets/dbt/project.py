import json
import sys  # Import sys for stderr
from pathlib import Path

from dagster import get_dagster_logger
from dagster_dbt import DbtProject

DBT_PROJECT_ROOT_DIR = Path(__file__).joinpath("..").resolve()
DBT_PROFILES_DIR = Path(__file__).joinpath("..", ".dbt").resolve()

# Define the path to the centrally managed keyfile
# This keyfile is expected to be created by setup.py
BIGQUERY_KEYFILE_NAME = "gcp_bigquery.json"
# Path relative to the project root, then to people_team_data/.secrets/
# Assuming DBT_PROJECT_ROOT_DIR is /workspaces/PeopleTeamPipeline/people_team_data/assets/dbt
# We need to go up three levels to /workspaces/PeopleTeamPipeline, then into people_team_data/.secrets
BIGQUERY_KEYFILE_NAME = DBT_PROJECT_ROOT_DIR.joinpath(
    "..", "..", ".secrets", BIGQUERY_KEYFILE_NAME
).resolve()


def verify_central_keyfile():
    """
    Verifies the existence and validity of the centrally managed GCP keyfile.
    This file is expected to be created by the setup.py script.
    Returns True if a valid keyfile exists, False otherwise, logging errors.
    """
    logger = get_dagster_logger()

    if not BIGQUERY_KEYFILE_NAME.exists():
        logger.error(
            f"CRITICAL FAILURE: Central GCP keyfile does not exist at {BIGQUERY_KEYFILE_NAME}. "
            f"This file should have been created by setup.py from the GCP_CREDS environment variable. "
            f"Ensure GCP_CREDS is set and setup.py ran successfully during deployment/setup."
        )
        return False

    if BIGQUERY_KEYFILE_NAME.stat().st_size == 0:
        logger.error(
            f"CRITICAL FAILURE: Central GCP keyfile at {BIGQUERY_KEYFILE_NAME} is empty. "
            f"This indicates an issue with its creation by setup.py (e.g., empty GCP_CREDS or write error)."
        )
        return False

    try:
        with open(BIGQUERY_KEYFILE_NAME, "r") as f:
            json.load(f)  # Attempt to parse the JSON to validate its format
        logger.info(
            f"Successfully validated central GCP keyfile at {BIGQUERY_KEYFILE_NAME} as valid JSON."
        )
        return True  # Keyfile exists and is valid JSON
    except json.JSONDecodeError as je:
        logger.error(
            f"CRITICAL FAILURE: Central GCP keyfile at {BIGQUERY_KEYFILE_NAME} is not valid JSON. Error: {je}. "
            f"This file should have been created by setup.py."
        )
        return False
    except Exception as e:
        logger.error(
            f"CRITICAL FAILURE: An unexpected error occurred while validating central GCP keyfile at {BIGQUERY_KEYFILE_NAME}. Error: {e}"
        )
        return False


# Verify the keyfile upon module load
central_keyfile_is_valid = verify_central_keyfile()

if not central_keyfile_is_valid:
    logger = get_dagster_logger()  # Ensure logger is available
    error_message = (
        f"CRITICAL: Central GCP keyfile at {BIGQUERY_KEYFILE_NAME} is missing, invalid, or could not be validated. "
        "This file is essential for dbt operations and should be created by setup.py. "
        "Please check prior log messages for details on why keyfile validation failed. "
        "Halting execution as dbt cannot proceed without a valid keyfile."
    )
    logger.critical(error_message)
    print(f"ERROR (project.py): {error_message}", file=sys.stderr)
    raise RuntimeError(error_message)

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,  # dbt will look for profiles.yml in here
    # packaged_project_dir is for when the project is packaged, ensure it's correct for your setup
    # For local development, project_dir and profiles_dir are key.
    # If you have a separate structure for packaged deployments, adjust packaged_project_dir accordingly.
    # Assuming dbt-project is a sibling to the 'dbt' directory itself, which contains this project.py
    packaged_project_dir=DBT_PROJECT_ROOT_DIR.parent.joinpath(
        "dbt-project"
    ).resolve(),
)
dbt_project.prepare_if_dev()
