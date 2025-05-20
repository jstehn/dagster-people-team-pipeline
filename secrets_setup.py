import base64
import logging
import os
from pathlib import Path

# --- Configure logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
# --- End of logging configuration ---

# --- Configuration for creating keyfiles from environment variables ---
# The script will automatically process any environment variable starting with "KEYFILE_".
# The part of the environment variable name after "KEYFILE_" will be used as the
# filename stem (converted to lowercase), and a ".json" extension will be added.
# The value of the environment variable is expected to be the Base64 encoded JSON string.
KEYFILE_PREFIX = "KEYFILE_"
# --- End of keyfile configuration ---

# --- Code to handle creation of keyfiles ---
try:
    base_dir = Path(__file__).resolve().parent
    secrets_dir = base_dir / "people_team_data" / ".secrets"
    secrets_dir.mkdir(parents=True, exist_ok=True)  # Create secrets_dir once

    logger.info(
        f"Attempting to create keyfiles in: {secrets_dir} from env vars starting with '{KEYFILE_PREFIX}'"
    )
    found_keyfile_vars = False

    for env_var_name, env_var_value in os.environ.items():
        if env_var_name.startswith(KEYFILE_PREFIX):
            found_keyfile_vars = True
            filename_stem_from_env = env_var_name[len(KEYFILE_PREFIX) :]
            if not filename_stem_from_env:  # Handle case like KEYFILE_=.json
                logger.warning(
                    f"Environment variable {env_var_name} has an empty name after prefix. Skipping."
                )
                continue
            keyfile_name = f"{filename_stem_from_env.lower()}.json"
            keyfile_path = secrets_dir / keyfile_name

            logger.info(
                f"Processing environment variable {env_var_name} for keyfile {keyfile_path}..."
            )
            if env_var_value:  # Check if the value is not empty
                try:
                    decoded_creds_bytes = base64.b64decode(env_var_value)
                    decoded_creds_str = decoded_creds_bytes.decode("utf-8")

                    with open(keyfile_path, "w") as f:
                        f.write(decoded_creds_str)
                    logger.info(
                        f"Successfully created GCP keyfile: {keyfile_path} from env var {env_var_name}"
                    )

                except base64.binascii.Error as b64_err:
                    logger.error(
                        f"Environment variable {env_var_name} for {keyfile_name} contains invalid base64 data. {b64_err}"
                    )
                    logger.error(
                        f"GCP keyfile was NOT created at: {keyfile_path}"
                    )
                except UnicodeDecodeError as ude_err:
                    logger.error(
                        f"Failed to decode content from {env_var_name} for {keyfile_name} as UTF-8. {ude_err}"
                    )
                    logger.error(
                        f"GCP keyfile was NOT created at: {keyfile_path}"
                    )
                except Exception as e:
                    logger.error(
                        f"An unexpected error occurred while processing {env_var_name} for {keyfile_name}: {e}"
                    )
                    logger.error(
                        f"GCP keyfile was NOT created at: {keyfile_path}"
                    )
            else:
                logger.warning(
                    f"Environment variable {env_var_name} for {keyfile_name} is set but empty. Skipping keyfile creation."
                )

    if not found_keyfile_vars:
        logger.info(
            f"No environment variables found starting with the prefix '{KEYFILE_PREFIX}'. No keyfiles processed by this script."
        )

except Exception as path_calc_err:
    # This handles errors in Path(__file__) or initial path setup for secrets_dir
    logger.critical(
        f"Could not determine base path or create secrets directory. {path_calc_err}"
    )
    logger.critical("No GCP keyfiles were created.")
# --- End of keyfile creation code ---
