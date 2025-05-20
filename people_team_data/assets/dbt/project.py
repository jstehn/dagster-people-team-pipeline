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
    """
    logger = get_dagster_logger()
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
                logger.warning(f"Written keyfile {KEYFILE_PATH} is empty.")
                return False
            try:
                with open(KEYFILE_PATH, "r") as f:
                    json.load(f)
                logger.info(f"Keyfile {KEYFILE_PATH} appears to be valid JSON.")
            except json.JSONDecodeError:
                logger.warning(
                    f"Written keyfile {KEYFILE_PATH} is NOT valid JSON."
                )
                raise
            return True

        except Exception as e:
            logger.error(f"Failed to decode or write keyfile: {e}")
            raise
    else:
        logger.info(
            "Environment variable GCP_CREDS not found. Keyfile will not be created by this script."
        )
        return False


keyfile_created_successfully = create_keyfile_from_env()

dbt_project = DbtProject(
    project_dir=DBT_PROJECT_ROOT_DIR,
    profiles_dir=DBT_PROFILES_DIR,
    packaged_project_dir=Path(__file__).joinpath("..", "dbt-project").resolve(),
)
dbt_project.prepare_if_dev()
