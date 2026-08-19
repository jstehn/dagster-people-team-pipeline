"""Contains helper functions to extract data from spreadsheet API"""

import socket
from typing import Any, List, Tuple
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
)

from dlt.common.exceptions import MissingDependencyException
from dlt.common.typing import DictStrAny

from dlt.sources.credentials import GcpCredentials, GcpOAuthCredentials
from dlt.sources.helpers.requests.retry import DEFAULT_RETRY_STATUS

from .data_processing import ParsedRange, trim_range_top_left

try:
    from apiclient.discovery import build, Resource
except ImportError:
    raise MissingDependencyException("Google API Client", ["google-api-python-client"])


def is_retry_status_code(exception: BaseException) -> bool:
    """Retry condition on HttpError"""
    from googleapiclient.errors import HttpError  # type: ignore

    # print(f"RETRY ON {str(HttpError)} = {isinstance(exception, HttpError) and exception.resp.status in DEFAULT_RETRY_STATUS}")
    # if isinstance(exception, HttpError):
    #     print(exception.resp.status)
    #     print(DEFAULT_RETRY_STATUS)
    return (
        isinstance(exception, HttpError)
        and exception.resp.status in DEFAULT_RETRY_STATUS
    ) or isinstance(exception, (TimeoutError, ConnectionError))


# Bound retries by wall clock, not just attempt count. The Position Control
# workbook is large enough that a single read can take ~30s, so the previous
# schedule (10 attempts, backoff up to 120s) could spend ~12 minutes per
# resource before surfacing a failure. Capping total retry time keeps a
# transient 429/503 from turning into a silent multi-minute stall.
retry_deco = retry(
    # Retry on rate limits (429), server errors (5xx) and network timeouts
    retry=retry_if_exception(is_retry_status_code),
    # Exponential backoff, capped so a single sleep cannot dominate the budget
    wait=wait_exponential(multiplier=1.5, min=5, max=30),
    # Give up after ~3 minutes of retrying, or 5 attempts, whichever comes first
    stop=(stop_after_delay(180) | stop_after_attempt(5)),
    # Raise the original error rather than tenacity's RetryError
    reraise=True,
)


def api_auth(credentials: GcpCredentials, max_api_retries: int) -> Resource:
    """
    Uses GCP credentials to authenticate with Google Sheets API.

    Args:
        credentials (GcpCredentials): Credentials needed to log in to GCP.
        max_api_retries (int): Max number of retires to google sheets API. Actual behavior is internal to google client.

    Returns:
        Resource: Object needed to make API calls to Google Sheets API.
    """
    if isinstance(credentials, GcpOAuthCredentials):
        credentials.auth("https://www.googleapis.com/auth/spreadsheets.readonly")
    socket.setdefaulttimeout(300)
    # Build the service object for Google sheets api.
    service = build(
        "sheets",
        "v4",
        credentials=credentials.to_native_credentials(),
        num_retries=max_api_retries,
    )
    return service


@retry_deco
def get_meta_for_ranges(
    service: Resource, spreadsheet_id: str, range_names: List[str]
) -> Any:
    """Retrieves `spreadsheet_id` cell metadata for `range_names`"""
    return (
        service.spreadsheets()
        .get(
            spreadsheetId=spreadsheet_id,
            ranges=range_names,
            includeGridData=True,
        )
        .execute()
    )


@retry_deco
def get_known_range_names(
    spreadsheet_id: str, service: Resource
) -> Tuple[List[str], List[str], str]:
    """
    Retrieves spreadsheet metadata and extracts a list of sheet names and named ranges

    Args:
        spreadsheet_id (str): The ID of the spreadsheet.
        service (Resource): Resource object used to make API calls to Google Sheets API.

    Returns:
        Tuple[List[str], List[str], str] sheet names, named ranges, spreadheet title
    """
    metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_names: List[str] = [s["properties"]["title"] for s in metadata["sheets"]]
    named_ranges: List[str] = [r["name"] for r in metadata.get("namedRanges", {})]
    title: str = metadata["properties"]["title"]
    return sheet_names, named_ranges, title


@retry_deco
def get_data_for_ranges(
    service: Resource, spreadsheet_id: str, range_names: List[str]
) -> List[Tuple[str, ParsedRange, ParsedRange, List[List[Any]]]]:
    """
    Calls Google Sheets API to get data in a batch. This is the most efficient way to get data for multiple ranges inside a spreadsheet.

    Args:
        service (Resource): Object to make API calls to Google Sheets.
        spreadsheet_id (str): The ID of the spreadsheet.
        range_names (List[str]): List of range names.

    Returns:
        List[DictStrAny]: A list of ranges with data in the same order as `range_names`
    """
    range_batch_resp = (
        service.spreadsheets()
        .values()
        .batchGet(
            spreadsheetId=spreadsheet_id,
            ranges=range_names,
            # un formatted returns typed values
            valueRenderOption="UNFORMATTED_VALUE",
            # will return formatted dates as a serial number
            dateTimeRenderOption="SERIAL_NUMBER",
        )
        .execute()
    )
    # if there are not ranges to be loaded, there's no "valueRanges"
    range_batch: List[DictStrAny] = range_batch_resp.get("valueRanges", [])
    # trim the empty top rows and columns from the left
    rv = []
    for name, range_ in zip(range_names, range_batch):
        parsed_range = ParsedRange.parse_range(range_["range"])
        values: List[List[Any]] = range_.get("values", None)
        if values:
            parsed_range, values = trim_range_top_left(parsed_range, values)
        # Sample more than just the first data row for cell-format-based type
        # inference (see get_data_types): a single blank or unformatted row
        # in that first position -- a placeholder/template row, for example
        # -- would otherwise prevent detecting a date/datetime column for
        # the whole range, silently corrupting every other row's date
        # conversion. Bounded to keep the metadata payload small.
        type_inference_sample_rows = 20
        meta_range = parsed_range._replace(
            end_row=min(
                parsed_range.start_row + type_inference_sample_rows,
                parsed_range.end_row,
            )
        )
        # print(f"{name}:{parsed_range}:{meta_range}")
        rv.append((name, parsed_range, meta_range, values))
    return rv
