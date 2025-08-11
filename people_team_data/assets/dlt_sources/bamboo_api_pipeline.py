import logging
import time
from typing import Any, Dict, Iterator, List, Union

import dlt
from dlt.sources import DltResource
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.rest_api import rest_api_source
from dlt.sources.rest_api.typing import RESTAPIConfig

from .bamboohr.schema import get_bamboohr_fields

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants for rate limiting and retries
BATCH_SIZE = 30  # Number of fields per batch (plus employeeNumber)
RATE_LIMIT_DELAY = 5  # Seconds to wait between API calls
MAX_RETRIES = 5  # Maximum number of retries per batch
RETRY_DELAY = 10  # Base seconds to wait between retries


def batch_iterator(lst: List[Any], batch_size: int):
    """Yield successive batch_size chunks from lst."""
    for i in range(0, len(lst), batch_size):
        yield lst[i : i + batch_size]


def is_valid_response(
    data: Union[Iterator[Dict[str, Any]], DltResource, List[Dict[str, Any]]],
) -> bool:
    """
    Check if the response data is valid.

    Args:
        data: Data to validate - can be an Iterator, DltResource, or List of dictionaries

    Returns:
        bool: True if data appears valid, False if error detected
    """
    try:
        items = list(data)

        # Check if we got an empty response
        if not items:
            logging.warning("Empty response received")
            return False

        # Check for error messages in the response
        for item in items:
            if not isinstance(item, dict):
                logging.warning(f"Expected dictionary item, got {type(item)}")
                continue

            # Check for common API error patterns
            if "error" in item:
                logging.error(f"Error found in response: {item.get('error')}")
                return False

            if item.get("success") is False:
                logging.error(f"Response indicates failure: {item}")
                return False

            # Check for empty data in expected fields (customize this based on your API)
            if "data" in item and not item["data"]:
                logging.warning("Response contains empty data field")

        return True

    except Exception as e:
        logging.error(f"Error validating response: {str(e)}")
        return False


def process_batch_with_retry(
    config: RESTAPIConfig, batch_name: str, retry_count: int = 0
) -> DltResource | None:
    """
    Process a batch of fields with retry logic.

    Args:
        config: The API configuration
        batch_name: Name of the batch for logging
        retry_count: Current retry attempt

    Returns:
        Iterator of records or None if all retries failed
    """
    try:
        raw_source = rest_api_source(config)
        raw_data = raw_source.resources["raw_employee_data"]

        # Check if response contains valid data
        if not is_valid_response(raw_data):
            raise ValueError("Invalid or error response received from API")

        return raw_data

    except Exception as e:
        if retry_count < MAX_RETRIES:
            # Exponential backoff with jitter
            wait_time = RETRY_DELAY * (2**retry_count) + (retry_count * 2)
            logging.warning(
                "Error processing batch %s (attempt %d/%d). Waiting %d seconds before retry. Error: %s",
                batch_name,
                retry_count + 1,
                MAX_RETRIES,
                wait_time,
                str(e),
            )
            time.sleep(wait_time)
            return process_batch_with_retry(config, batch_name, retry_count + 1)
        else:
            logging.error(
                "Failed to process batch %s after %d retries. Error: %s",
                batch_name,
                MAX_RETRIES,
                str(e),
            )
            return None


@dlt.source
def bamboohr_source(
    bamboohr_api_key: str = dlt.secrets.value,
    bamboohr_company_domain: str = dlt.secrets.value,
):
    """
    DLT source for BambooHR API.

    Args:
        api_key (str): API key for BambooHR.
        company_domain (str): Company domain for BambooHR.

    Returns:
        dlt.Source: DLT source object.
    """
    logging.debug(
        "Initializing BambooHR source with company domain: %s",
        bamboohr_company_domain,
    )

    # Get fields from schema
    all_fields = get_bamboohr_fields()
    # Remove employeeNumber from the list as we'll add it to each batch
    if "employeeNumber" in all_fields:
        all_fields.remove("employeeNumber")
    logging.debug("Using fields from schema: %d fields", len(all_fields))

    @dlt.resource(name="raw_bamboohr", primary_key="employeeNumber")
    def employee_data():
        """
        Process employee data in batches of fields.

        Yields:
            dict: Complete employee records with all fields.
        """
        # Dictionary to store complete employee records
        employee_records: Dict[int, Dict[str, Any]] = {}

        # Track progress
        total_batches = (len(all_fields) + BATCH_SIZE - 1) // BATCH_SIZE
        current_batch = 0

        # Process fields in batches
        for field_batch in batch_iterator(all_fields, BATCH_SIZE):
            current_batch += 1
            batch_with_id = ["employeeNumber"] + field_batch

            # Log progress
            logging.info(
                "Processing batch %d/%d with %d fields: %s",
                current_batch,
                total_batches,
                len(batch_with_id),
                batch_with_id,
            )

            config: RESTAPIConfig = {
                "client": {
                    "base_url": f"https://api.bamboohr.com/api/gateway.php/{bamboohr_company_domain}/v1/",
                    "auth": HttpBasicAuth(bamboohr_api_key, "x"),
                },
                "resource_defaults": {
                    "primary_key": "employeeNumber",
                    "write_disposition": "merge",
                },
                "resources": [
                    {
                        "name": "raw_employee_data",
                        "endpoint": {
                            "path": "datasets/employee",
                            "method": "POST",
                            "json": {
                                "fields": batch_with_id,
                            },
                        },
                    },
                ],
            }

            # Process batch with retry logic
            raw_data = process_batch_with_retry(
                config, f"Batch {current_batch}/{total_batches}"
            )

            if raw_data is not None:
                # Process each record in the batch
                for record in raw_data:
                    employee_number = int(record.get("employeeNumber") or -1)
                    if employee_number >= 0:
                        # Initialize record if this is the first time we see this employee
                        if employee_number not in employee_records:
                            employee_records[employee_number] = {
                                "employeeNumber": employee_number
                            }

                        # Update record with fields from this batch
                        employee_records[employee_number].update(record)

            # Rate limiting delay between batches
            if current_batch < total_batches:  # Don't wait after the last batch
                logging.debug(
                    "Waiting %d seconds before next batch", RATE_LIMIT_DELAY
                )
                time.sleep(RATE_LIMIT_DELAY)

        # Log completion
        logging.info(
            "Completed processing all %d batches. Found %d employee records.",
            total_batches,
            len(employee_records),
        )

        # Yield complete records
        for record in employee_records.values():
            yield record

    return employee_data()
