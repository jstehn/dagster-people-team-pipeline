# /workspaces/PeopleTeamPipeline/configure_secrets.py
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

# --- Helper Functions ---


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
    except base64.binascii.Error as e:
        logger.error(
            f"Failed to decode base64 string from '{source_description}'. Error: {e}"
        )
        return None

    try:
        json_str = decoded_bytes.decode("utf-8")
    except UnicodeDecodeError as e:
        logger.error(
            f"Failed to decode UTF-8 from base64 decoded string from '{source_description}'. Error: {e}"
        )
        return None

    try:
        data = json.loads(json_str)
        if not isinstance(data, dict):
            logger.error(
                f"Decoded JSON from '{source_description}' is not a dictionary (type: {type(data)})."
            )
            return None
        return data
    except json.JSONDecodeError as e:
        logger.error(
            f"Failed to parse JSON string from '{source_description}'. Error: {e}"
        )
        logger.debug(
            f"Problematic JSON string (first 100 chars): '{json_str[:100]}'"
        )
        return None


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


def _set_env_var(name: str, value: str) -> None:
    """
    Sets an environment variable. If GITHUB_ENV is defined, appends to it.
    Otherwise, sets it in the current process's environment.

    Args:
        name: The name of the environment variable.
        value: The value of the environment variable.
    """
    logger.info(f"Setting environment variable: {name}")
    if GITHUB_ENV_FILE_PATH:
        # Ensure the variable name is valid for GitHub Actions
        if not name.replace("_", "").isalnum() or name.startswith(
            ("GITHUB_", "RUNNER_")
        ):
            logger.warning(
                f"Environment variable name '{name}' might not be suitable for GitHub Actions. "
                "It should contain only letters, numbers, and underscores, and not start with GITHUB_ or RUNNER_."
            )

        # Properly format for GITHUB_ENV, especially for multi-line values
        if "\\n" in value or "\\r" in value:
            delimiter = f"__ENV_DELIM_{name.upper().replace('-', '_')}__"
            formatted_output = f"{name}<<{delimiter}\\n{value}\\n{delimiter}\\n"
        else:
            formatted_output = f"{name}={value}\\n"

        try:
            with open(GITHUB_ENV_FILE_PATH, "a", encoding="utf-8") as f:
                f.write(formatted_output)
            logger.info(f"Exported '{name}' to $GITHUB_ENV file.")
        except OSError as e:
            logger.error(
                f"Failed to write to GITHUB_ENV file ('{GITHUB_ENV_FILE_PATH}'). Error: {e}. "
                f"Setting '{name}' in current process environment as fallback."
            )
            os.environ[name] = value  # Fallback
    else:
        os.environ[name] = value
        logger.info(
            f"Set '{name}' in current process environment (GITHUB_ENV not found)."
        )


# --- Main Processing Functions ---


def process_keyfile_env_vars(project_root_dir: Path) -> bool:
    """
    Finds environment variables prefixed with KEYFILE_ENV_PREFIX,
    decodes their base64 JSON content, and writes them to JSON files
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

    for env_var_name, base64_encoded_value in os.environ.items():
        if env_var_name.startswith(KEYFILE_ENV_PREFIX):
            found_keyfile_vars_count += 1
            logger.info(
                f"Found keyfile environment variable: '{env_var_name}'."
            )

            if not base64_encoded_value:
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

            # Decode base64 and then UTF-8 to get the original JSON string
            # We need the string form to write to the file, not the parsed dict yet.
            try:
                decoded_bytes = base64.b64decode(base64_encoded_value)
                json_content_str = decoded_bytes.decode("utf-8")
            except base64.binascii.Error as e:
                logger.error(
                    f"Failed to decode base64 for '{env_var_name}'. Error: {e}"
                )
                overall_success = False
                continue
            except UnicodeDecodeError as e:
                logger.error(
                    f"Failed to decode UTF-8 for '{env_var_name}' after base64. Error: {e}"
                )
                overall_success = False
                continue

            # Now validate this string is indeed JSON before writing
            try:
                json.loads(json_content_str)  # Validate if it's proper JSON
            except json.JSONDecodeError as e:
                logger.error(
                    f"Content of '{env_var_name}' (after base64/UTF-8 decode) is not valid JSON. Error: {e}. "
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

                value_str: str
                if isinstance(value, (dict, list)):
                    value_str = json.dumps(value)
                elif isinstance(value, bool):
                    value_str = str(value).lower()
                else:
                    value_str = str(value)

                _set_env_var(new_env_var_name, value_str)
            return True  # This specific 'envvar' node processed

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
        return False

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
                _set_env_var(name, str(value))
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


def main() -> int:
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

    if keyfile_success and mapping_success and secrets_export_success:
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
    sys.exit(main())
