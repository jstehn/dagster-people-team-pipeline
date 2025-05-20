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
                    json.load(f)
                logger.info(
                    f"Existing keyfile {KEYFILE_PATH} is valid JSON. Skipping creation."
                )
                return True  # Keyfile exists and is valid
            except json.JSONDecodeError as je:
                logger.warning(
                    f"Existing keyfile {KEYFILE_PATH} is NOT valid JSON. Error: {je.msg}. Will attempt to recreate."
                )
            except Exception as e:
                logger.warning(
                    f"An unexpected error occurred while validating existing keyfile {KEYFILE_PATH}: {e}. Will attempt to recreate."
                )
    else:
        logger.info(
            f"Keyfile does not exist at {KEYFILE_PATH}. Attempting to create."
        )

    gcp_creds_base64 = EnvVar("GCP_CREDS").get_value()

    if gcp_creds_base64:
        logger.info(
            f"Found environment variable GCP_CREDS. Attempting to decode and write keyfile to {KEYFILE_PATH}..."
        )
        try:
            # Create the .dbt directory if it doesn't exist
            DBT_PROFILES_DIR.mkdir(parents=True, exist_ok=True)

            decoded_creds_bytes = base64.b64decode(gcp_creds_base64)
            decoded_creds_str = decoded_creds_bytes.decode("utf-8")

            with open(KEYFILE_PATH, "w") as f:
                f.write(decoded_creds_str)
            logger.info(
                f"Successfully wrote decoded credentials to {KEYFILE_PATH}"
            )

            # Basic validation: Check if file is not empty and is valid JSON
            if KEYFILE_PATH.stat().st_size == 0:
                logger.warning(
                    f"Written keyfile {KEYFILE_PATH} is empty. This may cause issues downstream."
                )
                # Current logic returns False. If an empty keyfile should always be an error,
                # consider raising an exception here.
                return False

            # Inner try for JSON validation of the written file
            try:
                with open(KEYFILE_PATH, "r") as f:
                    json.load(f)
                logger.info(f"Keyfile {KEYFILE_PATH} appears to be valid JSON.")
            except (
                json.JSONDecodeError
            ) as inner_je:  # Inner except for json.load
                logger.warning(
                    f"Initial check: Written keyfile {KEYFILE_PATH} is NOT valid JSON. "
                    f"Error: {inner_je.msg} at line {inner_je.lineno} col {inner_je.colno}. "
                    "This often indicates an issue with the GCP_CREDS environment variable's content or encoding. Re-raising for detailed error logging."
                )
                raise  # Re-raise to be caught by the more specific outer handlers
            return True

        except (
            json.JSONDecodeError
        ) as je:  # Catches re-raised JSONDecodeError from the inner block
            logger.error(
                f"CRITICAL FAILURE: The content decoded from the GCP_CREDS environment variable is NOT valid JSON. "
                f"This was confirmed after attempting to load the keyfile: {KEYFILE_PATH}. "
                f"Error details: {je.msg} (Line: {je.lineno}, Column: {je.colno}, Character index: {je.pos}).\\n"
                f"ACTION REQUIRED: Please meticulously verify the 'GCP_CREDS' environment variable. Ensure it contains the "
                f"correct, complete, and uncorrupted Base64 encoding of your entire GCP JSON service account key file. "
                f"The original JSON key must be UTF-8 encoded."
            )
            raise
        except base64.binascii.Error as b64e:
            logger.error(
                f"CRITICAL FAILURE: Failed to decode GCP_CREDS from Base64. "
                f"File path intended for key: {KEYFILE_PATH}. Error: {b64e}.\\n"
                f"ACTION REQUIRED: Ensure the 'GCP_CREDS' environment variable contains a valid Base64 string."
            )
            raise
        except UnicodeDecodeError as ude:
            logger.error(
                f"CRITICAL FAILURE: Failed to decode GCP_CREDS bytes as UTF-8 after Base64 decoding. "
                f"File path intended for key: {KEYFILE_PATH}. Error: {ude}.\\n"
                f"ACTION REQUIRED: Ensure the original GCP JSON key file (before Base64 encoding) was UTF-8 encoded."
            )
            raise
        except (
            Exception
        ) as e:  # Catch-all for other unexpected errors during the process
            logger.error(
                f"An unexpected error occurred during the creation or validation of keyfile {KEYFILE_PATH}: {e}"
            )
            logger.error(f"Underlying exception type: {type(e).__name__}")
            raise
    else:
        logger.info(
            "Environment variable GCP_CREDS not found. Keyfile will not be created by this script."
        )
        # If the keyfile was expected because it didn't exist or was invalid,
        # and GCP_CREDS is not found, this is a failure condition for creating/fixing it.
        if not KEYFILE_PATH.exists() or (
            KEYFILE_PATH.exists() and KEYFILE_PATH.stat().st_size == 0
        ):
            logger.error(
                "GCP_CREDS environment variable is required to create the keyfile, but it was not found."
            )
            return False  # Explicitly return False if creation was needed but couldn't proceed
        return False  # Return False if GCP_CREDS is not found, maintaining previous behavior otherwise


keyfile_created_successfully = create_keyfile_from_env()

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    packaged_project_dir=Path(__file__).joinpath("..", "dbt-project").resolve(),
)
dbt_project.prepare_if_dev()
