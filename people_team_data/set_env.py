"""
Handles the setup of secrets and environment variables for the project.

This script performs two main functions:
1.  Processes environment variables prefixed with 'KEYFILE_'. These variables
    are expected to contain base64 encoded JSON strings (typically service
    account keyfiles). The script decodes these strings and writes them as
    JSON files into a designated secrets directory ('people_team_data/.secrets/').
    It verifies that the written files are valid JSON.

2.  Processes an environment variable mapping configuration file (YAML format,
    typically '.dlt/env_mapping.yaml'). This file specifies how to create
    new environment variables based on the contents of other source environment
    variables (which are also expected to be base64 encoded JSON strings).
    The new environment variables are constructed by prefixing keys from the
    decoded JSON with a path derived from the YAML structure.

The script is designed to be run in a CI/CD environment (like GitHub Actions)
and will export any newly set environment variables via the $GITHUB_ENV file
if it exists, otherwise, it will set them in the current process's environment.
This ensures that these variables are available for subsequent steps in a
CI/CD pipeline, such as deployment to Dagster Cloud.
"""

import base64
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

# --- Constants ---
KEYFILE_ENV_PREFIX: str = "KEYFILE_"
# Relative path from the project root (where this script is expected to be)
SECRETS_DIR_RELATIVE_PATH: str = "people_team_data/.secrets"
# Relative path from the project root
ENV_MAPPING_FILE_RELATIVE_PATH: str = ".dlt/env_mapping.yaml"

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# --- Global Variables ---
GITHUB_ENV_FILE_PATH: Optional[str] = os.getenv("GITHUB_ENV")
secrets_configured = False  # Flag to indicate if secrets have been configured


# --- Helper Functions ---
def _escape_value(value: str) -> str:
    """
    Escapes special characters in a string for GitHub Actions environment variables.

    Args:
        value: The string to escape.

    Returns:
        The escaped string.
    """
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r")


def _export_to_github_env(
    name: str, value: str, github_env_file_path: str = GITHUB_ENV_FILE_PATH
) -> bool:
    """
    Exports an environment variable to the GitHub Actions environment file.

    Args:
        name: The name of the environment variable.
        value: The value of the environment variable. (Presumed already escaped)
        github_env_file_path: The path to the GitHub Actions environment file.

    Returns:
        True if the export was successful, False otherwise.
    """
    # Check if value contains actual newlines
    if github_env_file_path:
        try:
            with open(github_env_file_path, "a", encoding="utf-8") as f:
                f.write(f'{name}="{value}"\n')
            logger.info(f"Exported '{name}' to $GITHUB_ENV.")
            return True
        except OSError as e:
            logger.error(
                f"Failed to write to GITHUB_ENV file ('{github_env_file_path}'). Error: {e}"
            )
            return False
    else:
        logger.warning(
            "GITHUB_ENV is not set. Cannot export environment variables to GitHub Actions."
        )
        return False


def _get_value_logger_str(name: str, value: str) -> str:
    if (
        "PRIVATE" in name.upper()
        or "KEYFILE" in name.upper()
        or "SECRET" in name.upper()
    ):
        logger_value = f"*** LENGTH {len(value)} ***"
    else:
        logger_value = value[:20] + ("..." if len(value) > 20 else "")
    return logger_value


def _is_valid_env_var_name(name: str) -> bool:
    """
    Checks if the environment variable name is valid for GitHub Actions.
    GitHub Actions env var names must match ^[A-Za-z_][A-Za-z0-9_]*$ and be <= 32767 chars.
    """
    return (
        isinstance(name, str)
        and 0 < len(name) <= 32767
        and re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name) is not None
    )


def _export_env_var(
    name: str, value: str, github_env_file_path: str = GITHUB_ENV_FILE_PATH
) -> bool:
    """Sets an environment variable and exports it to the $GITHUB_ENV file if available and valid."""
    if "DAGSTER" in name.upper():
        logger.warning(
            f"Environment variable name '{name}' is reserved for Dagster. Skipping export."
        )
        return False
    if "GITHUB" in name.upper():
        logger.warning(
            f"Environment variable name '{name}' is reserved for GitHub Actions. Skipping export."
        )
        return False
    if not _is_valid_env_var_name(name):
        logger.warning(
            f"Environment variable name '{name}' is invalid for GitHub Actions. Skipping export."
        )
        return False

    if not isinstance(value, str):
        logger.warning(
            f"Value for env var '{name}' was not a string (type: {type(value)}). Converting to string."
        )
        value = str(value)
    escaped_value = _escape_value(value)
    logger_value = _get_value_logger_str(name, escaped_value)
    os.environ[name] = value
    logger.info(
        f'Successfully set environment variable: {name}="{logger_value}"'
    )
    if not github_env_file_path:
        return True
    try:
        exported_to_github = _export_to_github_env(
            name, escaped_value, github_env_file_path
        )
        logger.info(
            f'Successfully exported environmental variable to GITHUB_ENV: {name}="{logger_value}"'
        )
        return exported_to_github
    except Exception as e:
        logger.error(
            f'Failed to set environmental variable to GITHUB_ENV {name}="{logger_value}": {e}',
            exc_info=True,
        )
        return False


def _process_env_mapping_recursive(
    config_node: Any, current_prefix_parts: List[str]
) -> bool:
    """
    Recursively processes the environment mapping configuration.

    Args:
        config_node: The current node in the YAML configuration.
        current_prefix_parts: A list of strings representing the parts of the
                              environment variable prefix built so far.

    Returns:
        True if processing was successful for this branch, False otherwise.
    """
    overall_success = True
    if isinstance(config_node, dict):
        if "envvar" in config_node:
            source_env_var_name = config_node["envvar"]
            if (
                not isinstance(source_env_var_name, str)
                or not source_env_var_name.strip()
            ):
                logger.warning(
                    f"Invalid or empty 'envvar' value under prefix "
                    f"'{'__'.join(current_prefix_parts)}'. Skipping."
                )
                return False  # Critical error for this mapping entry

            base64_encoded_json_str = os.getenv(source_env_var_name)
            if base64_encoded_json_str is None:
                logger.warning(
                    f"Source environment variable '{source_env_var_name}' (for prefix "
                    f"'{'__'.join(current_prefix_parts)}') not set. Skipping this mapping."
                )
                return True  # Not a script failure, just a missing optional var

            decoded_json_dict = _decode_and_validate_json(
                base64_encoded_json_str, source_env_var_name
            )

            if decoded_json_dict is None:
                logger.error(
                    f"Failed to get valid JSON dictionary from '{source_env_var_name}' "
                    f"for prefix '{'__'.join(current_prefix_parts)}'. This mapping will be skipped."
                )
                return False  # Failed to process this specific mapping

            # Successfully decoded, now set environment variables based on its keys
            final_prefix = "__".join(current_prefix_parts)
            for key, value in decoded_json_dict.items():
                # Sanitize key for environment variable name
                sanitized_key = (
                    str(key).upper().replace("-", "_").replace(" ", "_")
                )
                new_env_var_name = f"{final_prefix}__{sanitized_key}"

                if isinstance(value, (dict, list)):
                    value_str = json.dumps(value)
                elif isinstance(value, bool):
                    value_str = str(value).lower()
                else:
                    value_str = str(value)

                _export_env_var(new_env_var_name, value_str)
            return True

        else:  # Traverse deeper
            for key, next_node in config_node.items():
                # Sanitize key for prefix part
                sanitized_key_part = (
                    str(key).upper().replace("-", "_").replace(" ", "_")
                )
                if not _process_env_mapping_recursive(
                    next_node, current_prefix_parts + [sanitized_key_part]
                ):
                    overall_success = False  # Propagate failure up
            return overall_success

    elif isinstance(config_node, list):
        logger.warning(
            f"Unexpected list found in YAML structure at prefix '{'__'.join(current_prefix_parts)}'. Skipping list items."
        )
        for i, item in enumerate(config_node):
            if not _process_env_mapping_recursive(
                item, current_prefix_parts + [f"ITEM_{i}"]
            ):  # Try to process items if they are dicts
                overall_success = False
        return overall_success
    else:
        # Leaf node that isn't an 'envvar' instruction, or an unexpected type.
        # This is fine if the YAML structure intends for some leaves to not be 'envvar's.
        # If 'envvar' was expected here, it's a config structure issue.
        logger.debug(
            f"Reached non-dictionary, non-list, non-'envvar' leaf or unexpected type at prefix "
            f"'{'__'.join(current_prefix_parts)}': {type(config_node)}. Value: '{str(config_node)[:50]}'."
        )
        return True


def _decode_and_validate_json(
    base64_encoded_str: str, source_description: str
) -> Optional[Dict[str, Any]]:
    """
    Decodes a base64 encoded string, then decodes it as UTF-8,
    and finally parses it as JSON, validating it's a dictionary.

    Args:
        base64_encoded_str: The base64 encoded string to process.
        source_description: A description of the source of the string
                             (e.g., an environment variable name) for logging.

    Returns:
        A dictionary if decoding and parsing are successful and the result
        is a dictionary, otherwise None.
    """
    if not base64_encoded_str.strip():
        logger.warning(
            f"Input string for '{source_description}' is empty. Cannot decode."
        )
        return None
    try:
        decoded_bytes = base64.b64decode(base64_encoded_str)
        json_str = decoded_bytes.decode("utf-8")
        data = json.loads(json_str)
        if not isinstance(data, dict):
            logger.error(
                f"Decoded JSON from '{source_description}' is not a dictionary (type: {type(data)})."
            )
            raise json.JSONDecodeError(
                "Decoded JSON is not a dictionary.", json_str, 0
            )
        return data
    except UnicodeDecodeError as e:
        logger.error(
            f"Failed to decode UTF-8 from base64 decoded string from '{source_description}'. Error: {e}"
        )
        raise
    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to parse JSON string from '{source_description}'. Error: {e}"
        )
        logger.debug(
            f"Problematic JSON string (first 100 chars): '{json_str[:100]}'"
        )
        raise
    except base64.binascii.Error as e:
        logger.error(
            f"Failed to decode base64 string from '{source_description}'. Error: {e}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Unexpected error processing base64 string from '{source_description}'. Error: {e}"
        )
        raise


def _write_keyfile(secrets_dir: Path, filename: str, content_str: str) -> bool:
    """
    Writes the given string content to a file in the specified secrets directory.
    Also verifies that the written content can be loaded as JSON.

    Args:
        secrets_dir: The directory where the file should be written.
        filename: The name of the file to write.
        content_str: The string content to write to the file.

    Returns:
        True if writing and JSON validation are successful, False otherwise.
    """
    keyfile_path = secrets_dir / filename
    try:
        secrets_dir.mkdir(parents=True, exist_ok=True)
        with open(keyfile_path, "w", encoding="utf-8") as f:
            f.write(content_str)
        logger.info(f"Successfully wrote keyfile to '{keyfile_path}'.")

        # Verify the written file is valid JSON
        with open(keyfile_path, "r", encoding="utf-8") as f_verify:
            json.load(f_verify)
        logger.info(f"Successfully validated JSON content of '{keyfile_path}'.")
        return True
    except OSError as e:
        logger.error(f"Failed to write keyfile to '{keyfile_path}'. Error: {e}")
    except json.JSONDecodeError as e:
        logger.error(
            f"Written keyfile '{keyfile_path}' is not valid JSON. Error: {e}"
        )
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during keyfile write or validation for '{keyfile_path}'. Error: {e}"
        )

    # Attempt to remove partially written or invalid file
    if keyfile_path.exists():
        try:
            keyfile_path.unlink()
            logger.info(
                f"Removed invalid or partially written keyfile '{keyfile_path}'."
            )
        except OSError as e_del:
            logger.error(
                f"Failed to remove problematic keyfile '{keyfile_path}'. Error: {e_del}"
            )
    return False


# --- Main Processing Functions ---


def process_keyfile_env_vars(project_root_dir: Path) -> bool:
    """
    Finds environment variables prefixed with KEYFILE_ENV_PREFIX,
    decodes their base64 JSON content (if needed), and writes them to JSON files
    in the SECRETS_DIR_RELATIVE_PATH.

    Args:
        project_root_dir: The root directory of the project.

    Returns:
        True if all found keyfiles were processed successfully, False otherwise.
    """
    logger.info(
        f"Starting processing of '{KEYFILE_ENV_PREFIX}*' environment variables."
    )
    secrets_dir_abs_path = project_root_dir / SECRETS_DIR_RELATIVE_PATH

    found_keyfile_vars_count = 0
    successful_keyfile_writes = 0
    overall_success = True

    for env_var_name, value in os.environ.items():
        if env_var_name.startswith(KEYFILE_ENV_PREFIX):
            found_keyfile_vars_count += 1
            logger.info(
                f"Found keyfile environment variable: '{env_var_name}'."
            )

            if not value:
                logger.warning(
                    f"Environment variable '{env_var_name}' is set but empty. Skipping."
                )
                overall_success = False
                continue

            filename_stem = env_var_name[len(KEYFILE_ENV_PREFIX) :].lower()
            if not filename_stem:
                logger.warning(
                    f"Environment variable '{env_var_name}' results in an empty filename stem. Skipping."
                )
                overall_success = False
                continue

            keyfile_name = f"{filename_stem}.json"

            # Try to decode as base64, but if that fails, treat as plain JSON string
            json_content_str = None
            try:
                # Check if value is valid base64 by trying to decode and then decode as utf-8
                decoded_bytes = base64.b64decode(value, validate=True)
                json_content_str = decoded_bytes.decode("utf-8")
                logger.info(f"'{env_var_name}' appears to be base64-encoded.")
            except (base64.binascii.Error, UnicodeDecodeError):
                # Not base64, treat as plain JSON string
                json_content_str = value
                logger.info(
                    f"'{env_var_name}' is not base64-encoded, treating as plain JSON string."
                )

            # Now validate this string is indeed JSON before writing
            try:
                json.loads(json_content_str)  # Validate if it's proper JSON
            except json.JSONDecodeError as e:
                logger.error(
                    f"Content of '{env_var_name}' is not valid JSON. Error: {e}. "
                    f"Problematic JSON string (first 100 chars): '{json_content_str[:100]}'"
                )
                overall_success = False
                continue

            if _write_keyfile(
                secrets_dir_abs_path, keyfile_name, json_content_str
            ):
                successful_keyfile_writes += 1
            else:
                overall_success = False  # _write_keyfile logs its own errors

    if found_keyfile_vars_count == 0:
        logger.info(
            f"No environment variables found with prefix '{KEYFILE_ENV_PREFIX}'."
        )
    else:
        logger.info(
            f"Processed {found_keyfile_vars_count} keyfile environment variables. "
            f"Successfully wrote {successful_keyfile_writes} keyfiles."
        )
    return overall_success


def process_env_mappings(project_root_dir: Path) -> bool:
    """
    Loads the YAML environment mapping file and processes it to set
    new environment variables.

    Args:
        project_root_dir: The root directory of the project.

    Returns:
        True if the mapping file was processed successfully (or if it
        doesn't exist, which is treated as a non-error), False if errors
        occurred during processing an existing file.
    """
    logger.info("Starting processing of environment mappings from YAML.")
    mapping_file_abs_path = project_root_dir / ENV_MAPPING_FILE_RELATIVE_PATH

    if not mapping_file_abs_path.is_file():
        logger.info(
            f"Environment mapping file '{mapping_file_abs_path}' not found. Skipping this step."
        )
        return True  # Not an error if the file is optional

    try:
        with open(mapping_file_abs_path, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logger.error(
            f"Failed to load or parse YAML mapping file '{mapping_file_abs_path}'. Error: {e}"
        )
        return False
    except OSError as e:
        logger.error(
            f"Failed to open YAML mapping file '{mapping_file_abs_path}'. Error: {e}"
        )
        return False

    if not isinstance(config_data, dict):
        logger.error(
            f"Root of YAML mapping file '{mapping_file_abs_path}' is not a dictionary. "
            f"Type found: {type(config_data)}. Cannot process."
        )
        return False

    logger.info(
        f"Successfully loaded YAML mapping file '{mapping_file_abs_path}'."
    )
    return _process_env_mapping_recursive(config_data, [])


def export_all_secrets_to_env(secrets_json_str: Optional[str]) -> None:
    """
    Decodes a JSON string containing all secrets and exports them as environment variables.

    Args:
        secrets_json_str: A JSON string where keys are secret names and values are secret values.
                          Typically sourced from GitHub Actions' `toJson(secrets)` context.
    """
    if not secrets_json_str:
        logger.info(
            "No JSON string provided for exporting all secrets (e.g., from ALL_SECRETS_JSON_FOR_ENV_EXPORT). Skipping."
        )
        return True

    logger.info(
        "Attempting to export all secrets from JSON string to environment variables."
    )
    try:
        all_secrets = json.loads(secrets_json_str)
        if not isinstance(all_secrets, dict):
            logger.error(
                f"Failed to parse ALL_SECRETS_JSON_FOR_ENV_EXPORT: Expected a JSON object (dictionary), but got {type(all_secrets)}."
            )
            return False

        for name, value in all_secrets.items():
            if isinstance(value, (str, int, float, bool)):
                _export_env_var(name, str(value))
            else:
                logger.warning(
                    f"Secret '{name}' has a complex type ({type(value)}) and will not be directly exported as an environment variable. "
                    "Only string, number, or boolean values are directly exported by this function."
                )
        logger.info("Finished exporting secrets from JSON string.")
        return True
    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to parse the JSON string from ALL_SECRETS_JSON_FOR_ENV_EXPORT. Error: {e}"
        )
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred while exporting all secrets from JSON. Error: {e}"
        )
        return False


# --- Main Execution ---


def configure_secrets() -> int:
    """
    Main function to orchestrate the secrets setup.

    Returns:
        0 if successful, 1 if any part of the process fails.
    """
    logger.info("--- Starting Secrets and Environment Configuration Script ---")

    # Determine project root directory (assuming this script is in the project root)
    project_root = Path(__file__).resolve().parent
    logger.info(f"Project root directory identified as: {project_root}")

    # Step 1: Process KEYFILE_* environment variables
    keyfile_success = process_keyfile_env_vars(project_root)

    # Step 2: Process environment mappings from YAML
    mapping_success = process_env_mappings(project_root)

    # Step 3: Export all secrets to environment variables
    secrets_export_success = export_all_secrets_to_env(
        os.getenv("ALL_SECRETS_JSON_FOR_ENV_EXPORT")
    )
    secrets_configured = (
        keyfile_success and mapping_success and secrets_export_success
    )

    if secrets_configured:
        logger.info(
            "--- Secrets and Environment Configuration Script Completed Successfully ---"
        )
        return 0
    else:
        logger.error(
            "--- Secrets and Environment Configuration Script Completed with Errors ---"
        )
        return 1


if __name__ == "__main__":
    sys.exit(configure_secrets())
