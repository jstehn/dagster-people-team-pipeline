import logging
import time
from typing import Any, Dict, Iterator, List

import dlt
from dlt.sources.helpers.rest_client.auth import HttpBasicAuth
from dlt.sources.rest_api import RESTAPIConfig, rest_api_source

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


def is_valid_response(data: Iterator[Dict[str, Any]]) -> bool:
    """
    Check if the response data is valid.

    Args:
        data: Iterator of response data

    Returns:
        bool: True if data appears valid, False if error detected
    """
    try:
        # Convert iterator to list to inspect it
        data_list = list(data)

        # Check if data is empty
        if not data_list:
            return False

        # Check first record for error indicators
        first_record = data_list[0] if data_list else {}
        if "error" in first_record or "errors" in first_record:
            return False

        # If we got here, data looks valid
        return True

    except Exception as e:
        logging.error("Error checking response validity: %s", str(e))
        return False


def process_batch_with_retry(
    config: RESTAPIConfig, batch_name: str, retry_count: int = 0
) -> Iterator[Dict[str, Any]] | None:
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
    api_key: str = dlt.secrets.value,
    company_domain: str = dlt.secrets.value,
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
        "Initializing BambooHR source with company domain: %s", company_domain
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
                    "base_url": f"https://api.bamboohr.com/api/gateway.php/{company_domain}/v1/",
                    "auth": HttpBasicAuth(api_key, "x"),
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


def load_bamboohr() -> None:
    """Load data from BambooHR API into the destination for dlt."""
    logging.info("Starting BambooHR data load pipeline.")

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_bamboohr",
        destination="postgres",
        dataset_name="bamboohr_employee_data",
    )

    load_info = pipeline.run(bamboohr_source(), loader_file_format="csv")
    logging.info("Data load completed. Load info: %s", load_info)
    print(load_info)


if __name__ == "__main__":
    load_bamboohr()
