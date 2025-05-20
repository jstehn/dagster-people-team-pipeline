import base64
import json
from pathlib import Path

from dagster import EnvVar, get_dagster_logger
from dagster_dbt import DbtProject

DBT_PROJECT_ROOT_DIR = Path(__file__).joinpath("..").resolve()
DBT_PROFILES_DIR = Path(__file__).joinpath("..", ".dbt").resolve()
KEYFILE_NAME = "keyfile.json"
KEYFILE_PATH = DBT_PROFILES_DIR.joinpath(KEYFILE_NAME).resolve()


def create_keyfile_from_env():
    """
    Reads a Base64 encoded GCP credential from an environment variable,
    decodes it, and writes it to the specified keyfile path.
    The keyfile is only created if it doesn't exist or if the existing one is invalid.
    Returns True if a valid keyfile exists or was successfully created, False otherwise.
    """
    logger = get_dagster_logger()

    # Check if keyfile already exists and is valid
    if KEYFILE_PATH.exists():
        logger.info(f"Keyfile already exists at {KEYFILE_PATH}. Validating...")
        if KEYFILE_PATH.stat().st_size == 0:
            logger.warning(
                f"Existing keyfile {KEYFILE_PATH} is empty. Will attempt to recreate."
            )
        else:
            try:
                with open(KEYFILE_PATH, "r") as f:
                    json.load(f)  # Attempt to parse the JSON
                logger.info(
                    f"Existing keyfile {KEYFILE_PATH} is valid JSON. Skipping creation."
                )
                return True  # Keyfile exists and is valid
            except json.JSONDecodeError as je:
                logger.warning(
                    f"Existing keyfile {KEYFILE_PATH} is not valid JSON: {je}. Will attempt to recreate."
                )
            except Exception as e:
                logger.warning(
                    f"Error validating existing keyfile {KEYFILE_PATH}: {e}. Will attempt to recreate."
                )
    else:
        logger.info(
            f"Keyfile does not exist at {KEYFILE_PATH}. Attempting to create."
        )

    gcp_creds_base64 = EnvVar("GCP_CREDS").get_value()

    if not gcp_creds_base64:
        logger.info(
            "Environment variable GCP_CREDS not found. Keyfile cannot be created by this script."
        )
        # If the keyfile was expected because it didn't exist or was invalid,
        # and GCP_CREDS is not found, this is a failure condition.
        if not KEYFILE_PATH.exists() or (
            KEYFILE_PATH.exists() and KEYFILE_PATH.stat().st_size == 0
        ):
            logger.error(
                "GCP_CREDS environment variable is required to create/validate the keyfile, but it was not found."
            )
        return False  # GCP_CREDS not found, cannot proceed with creation/validation

    logger.info(
        f"Found environment variable GCP_CREDS. Attempting to decode and write keyfile to {KEYFILE_PATH}..."
    )
    try:
        DBT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)
        decoded_creds_bytes = base64.b64decode(gcp_creds_base64)
        decoded_creds_str = decoded_creds_bytes.decode("utf-8")

        with open(KEYFILE_PATH, "w") as f:
            f.write(decoded_creds_str)  # Actually write the decoded string
        logger.info(f"Successfully wrote decoded credentials to {KEYFILE_PATH}")

        if KEYFILE_PATH.stat().st_size == 0:
            logger.error(
                f"CRITICAL FAILURE: Keyfile {KEYFILE_PATH} is empty after attempting to write decoded credentials. "
                "This could be due to an empty GCP_CREDS variable or a write issue."
            )
            return False  # Failed to write content

        # Validate the newly written file
        try:
            with open(KEYFILE_PATH, "r") as f:
                json.load(f)  # Attempt to parse the newly written JSON
            logger.info(
                f"Successfully validated newly created keyfile {KEYFILE_PATH} as valid JSON."
            )
            return True  # Successfully created and validated
        except json.JSONDecodeError as inner_je:
            logger.error(
                f"CRITICAL FAILURE: The content written to keyfile {KEYFILE_PATH} from GCP_CREDS is NOT valid JSON. "
                f"Error details: {inner_je.msg} (Line: {inner_je.lineno}, Column: {inner_je.colno}, Character index: {inner_je.pos}).\\n"
                f"ACTION REQUIRED: Please meticulously verify the 'GCP_CREDS' environment variable. Ensure it contains the "
                f"correct, complete, and uncorrupted Base64 encoding of your entire GCP JSON service account key file. "
                f"The original JSON key must be UTF-8 encoded."
            )
            # Optionally, remove the invalid file: KEYFILE_PATH.unlink(missing_ok=True)
            return False  # JSON validation failed

    except base64.binascii.Error as b64e:
        logger.error(
            f"CRITICAL FAILURE: Failed to decode GCP_CREDS from Base64. "
            f"File path intended for key: {KEYFILE_PATH}. Error: {b64e}.\\n"
            f"ACTION REQUIRED: Ensure the 'GCP_CREDS' environment variable contains a valid Base64 string."
        )
        return False
    except UnicodeDecodeError as ude:
        logger.error(
            f"CRITICAL FAILURE: Failed to decode GCP_CREDS bytes as UTF-8 after Base64 decoding. "
            f"File path intended for key: {KEYFILE_PATH}. Error: {ude}.\\n"
            f"ACTION REQUIRED: Ensure the original GCP JSON key file (before Base64 encoding) was UTF-8 encoded."
        )
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during the creation or validation of keyfile {KEYFILE_PATH}: {e}"
        )
        logger.error(f"Underlying exception type: {type(e).__name__}")
        return False


keyfile_created_successfully = create_keyfile_from_env()

if not keyfile_created_successfully:
    # Given create_keyfile_from_env uses get_dagster_logger, it should be available.
    logger = get_dagster_logger()
    error_message = (
        f"CRITICAL: GCP keyfile at {KEYFILE_PATH} is missing, invalid, or could not be created. "
        "This is essential for dbt operations. Please check prior log messages from 'create_keyfile_from_env' "
        "for details on why keyfile creation/validation failed (e.g., missing GCP_CREDS, decoding errors, JSON errors). "
        "Halting execution as dbt cannot proceed without a valid keyfile."
    )
    logger.critical(error_message)
    # Also print to stderr for maximum visibility during definition loading
    import sys

    print(f"ERROR (project.py): {error_message}", file=sys.stderr)
    raise RuntimeError(error_message)

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    packaged_project_dir=Path(__file__).joinpath("..", "dbt-project").resolve(),
)
dbt_project.prepare_if_dev()
