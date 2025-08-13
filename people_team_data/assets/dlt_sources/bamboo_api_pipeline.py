"""
DLT source for BambooHR API. Handles batch fetching, error isolation, and retries for employee data.
"""

import logging
import time
from typing import Any, Dict, List, Set

import dlt
import requests

from .bamboohr.schema import get_bamboohr_fields

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Constants for rate limiting and retries
BATCH_SIZE = 30  # Number of fields per batch (plus employeeNumber)
RATE_LIMIT_DELAY = 5  # Seconds to wait between successful batch API calls
MAX_RETRIES = 5  # Maximum number of retries per batch
RETRY_DELAY = 10  # Base seconds to wait between retries (exponential backoff)

# Cache of forbidden fields discovered this process so we skip them early
FORBIDDEN_FIELDS: Set[str] = set()


def batch_iterator(lst: List[Any], batch_size: int):
    """Yield successive batch_size chunks from a list."""
    for i in range(0, len(lst), batch_size):
        yield lst[i : i + batch_size]


def _post_dataset(
    domain: str, api_key: str, fields: List[str]
) -> List[Dict[str, Any]]:
    """
    Call BambooHR datasets/employee endpoint and return JSON list.
    Raises an exception with context on failure.
    """
    url = f"https://{domain}.bamboohr.com/api/v1/datasets/employee"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    payload = {"fields": fields}
    response = requests.post(
        url, json=payload, headers=headers, auth=(api_key, "x"), timeout=60
    )
    if response.status_code == 403:
        # Provide extra diagnostics
        raise PermissionError(
            f"403 Forbidden for dataset request. Fields={fields}. Body={response.text[:500]}"
        )
    try:
        response.raise_for_status()
    except Exception as e:
        raise RuntimeError(
            f"Error {response.status_code} calling BambooHR dataset: {response.text[:500]}"
        ) from e
    data = response.json()
    # API sometimes wraps list under {'data': [...]}
    if isinstance(data, dict):
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        # When error shape came through without triggering 4xx
        if "error" in data:
            raise RuntimeError(f"Dataset error payload: {data['error']}")
        raise ValueError(
            f"Unexpected dataset response type dict keys={list(data)[:5]} excerpt={str(data)[:200]}"
        )
    if isinstance(data, list):
        return data
    raise ValueError(
        f"Unexpected dataset response type {type(data)} excerpt={str(data)[:200]}"
    )


def _fetch_batch_with_retry(
    domain: str, api_key: str, fields: List[str], batch_name: str
) -> List[Dict[str, Any]] | None:
    """Fetch a batch of employee data with retries and forbidden field isolation on 403 errors."""
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            return _post_dataset(domain, api_key, fields)
        except PermissionError as pe:  # 403 - attempt to isolate bad field(s)
            logging.warning("%s -- attempting field isolation due to 403", pe)
            # Try each field (besides employeeNumber) individually to find forbidden ones
            bad_fields: List[str] = []
            good_fields: List[str] = []
            for f in fields:
                if f == "employeeNumber":
                    continue
                try:
                    _post_dataset(domain, api_key, ["employeeNumber", f])
                    good_fields.append(f)
                except PermissionError:
                    bad_fields.append(f)
            if bad_fields:
                logging.error(
                    "Forbidden fields detected and excluded from batch %s: %s",
                    batch_name,
                    bad_fields,
                )
                FORBIDDEN_FIELDS.update(bad_fields)
                # Re-run request with only good fields
                reduced_fields = ["employeeNumber"] + good_fields
                if len(reduced_fields) == 1:  # only employeeNumber
                    return []  # nothing else to add; skip
                return _post_dataset(domain, api_key, reduced_fields)
            # If no individual field caused 403, propagate original error
            raise
        except Exception as e:
            attempt += 1
            if attempt >= MAX_RETRIES:
                logging.error(
                    "Failed batch %s after %d attempts: %s",
                    batch_name,
                    attempt,
                    e,
                )
                return None
            wait = RETRY_DELAY * (2 ** (attempt - 1))
            logging.warning(
                "Error batch %s attempt %d/%d: %s. Sleeping %ds",
                batch_name,
                attempt,
                MAX_RETRIES,
                e,
                wait,
            )
            time.sleep(wait)


@dlt.source
def bamboohr_source(
    bamboohr_api_key: str = dlt.secrets.value,
    bamboohr_company_domain: str = dlt.secrets.value,
):
    """DLT source for BambooHR API. Returns employee data in batches."""
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
        """Yield complete employee records with all available fields, batched for API efficiency."""
        # Dictionary to store complete employee records
        employee_records: Dict[int, Dict[str, Any]] = {}

        # Track progress
        total_batches = (len(all_fields) + BATCH_SIZE - 1) // BATCH_SIZE
        current_batch = 0

        # Process fields in batches
        for field_batch in batch_iterator(all_fields, BATCH_SIZE):
            current_batch += 1
            # Remove any fields already identified as forbidden
            effective_fields = [
                f for f in field_batch if f not in FORBIDDEN_FIELDS
            ]
            if not effective_fields:
                logging.warning(
                    "Skipping batch %d entirely; all fields forbidden so far: %s",
                    current_batch,
                    field_batch,
                )
                continue
            batch_with_id = ["employeeNumber"] + effective_fields

            # Log progress
            logging.info(
                "Processing batch %d/%d with %d fields: %s",
                current_batch,
                total_batches,
                len(batch_with_id),
                batch_with_id,
            )

            batch_name = f"Batch {current_batch}/{total_batches}"
            batch_data = _fetch_batch_with_retry(
                bamboohr_company_domain,
                bamboohr_api_key,
                batch_with_id,
                batch_name,
            )
            if batch_data is not None:
                for record in batch_data:
                    try:
                        employee_number = int(
                            record.get("employeeNumber") or -1
                        )
                    except Exception:
                        continue
                    if employee_number < 0:
                        continue
                    if employee_number not in employee_records:
                        employee_records[employee_number] = {
                            "employeeNumber": employee_number
                        }
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
