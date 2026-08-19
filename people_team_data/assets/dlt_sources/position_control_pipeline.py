"""
DLT source for Position Control data. This module defines a source and multiple resources for processing data from Google Sheets.
"""

from typing import Any, Callable, Dict

import dlt

from .google_sheets import google_spreadsheet


def ensure_str(value: Any) -> str:
    """Convert a value to a string, returning an empty string if the value is None."""
    return str(value) if value is not None else ""


def ensure_float(value: Any) -> float | None:
    """Convert a value to a float, returning None if the conversion fails."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def is_valid_row(row: Dict[str, Any], primary_key: str) -> bool:
    """
    Check if a row is valid by ensuring it is not empty and does not duplicate the header.

    Args:
        row (Dict[str, Any]): The row data dictionary.
        primary_key (str): The primary key field name to check.

    Returns:
        bool: True if the row is valid, False otherwise.
    """
    return bool(
        row
        and row.get(primary_key) is not None
        and row[primary_key] != primary_key
    )


def convert_types(
    row: Dict[str, Any], conversions: Dict[str, Callable[[Any], Any]]
) -> Dict[str, Any]:
    """
    Apply type conversions to specified fields in a row.

    Args:
        row (Dict[str, Any]): The row data dictionary.
        conversions (Dict[str, Callable[[Any], Any]]): A dictionary mapping field names to conversion functions.

    Returns:
        Dict[str, Any]: The row with converted field values.
    """
    if not row:
        return row

    print(f"\nProcessing row with ID: {row.get('Adjustment_ID')}")  # Debug
    for field, converter in conversions.items():
        if field in row:
            print(f"Converting field {field}: {row[field]!r}")  # Debug
            row[field] = converter(row[field])
            print(f"Converted value: {row[field]!r}")  # Debug
    return row


# Tabs read from the Position Control workbook, fetched in a single batch.
POSITION_CONTROL_TABS = [
    "Positions",
    "Employees",
    "Adjustments",
    "Stipends",
    "Assignments",
]

# Columns each tab's row-validity filter below looks up by name. If one of
# these disappears from the sheet -- renamed, removed, or lost to a header
# parsing failure -- every row for that tab silently fails its filter and
# the resource loads zero rows. That happened for a week undetected when
# Assignments' header row got corrupted upstream. Validating these
# immediately after reading turns a real column rename into a loud, visible
# run failure instead of a silent, empty load.
REQUIRED_COLUMNS = {
    "Positions": ["Position_ID"],
    "Employees": ["Employee_ID"],
    "Adjustments": ["Adjustment_ID"],
    "Stipends": ["Stipend_ID", "Employee_ID"],
    "Assignments": ["Assignment_ID", "Employee_ID", "Position_ID"],
}


def _validate_required_columns(tab_name: str, rows: list) -> None:
    if not rows:
        return
    present = set(rows[0].keys())
    missing = [c for c in REQUIRED_COLUMNS.get(tab_name, []) if c not in present]
    if missing:
        raise ValueError(
            f"Position Control tab '{tab_name}' is missing expected column(s) "
            f"{missing}. Check for a renamed, removed, or duplicated header in "
            f"the sheet. Columns found: {sorted(present)}"
        )


@dlt.source(name="position_control_source")
def position_control_source(
    position_control_sheet_id: str = dlt.config.value,
):
    """
    Define a DLT source for Position Control data from Google Sheets.

    Args:
        position_control_sheet_id (str): The ID of the Google Sheet containing Position Control data.

    Returns:
        List[dlt.Resource]: A list of resources for processing Position Control data.
    """
    # Cache of tab name -> rows, populated on first use by _tab_rows.
    tab_cache: Dict[str, list] = {}

    def _tab_rows(tab_name: str):
        """
        Yield rows for one tab, reading every tab in a single batched request.

        Each resource used to call google_spreadsheet itself, so one pipeline run
        re-authenticated, re-fetched workbook metadata and issued a separate
        values request five times over. The Position Control workbook is large
        (~1.3M cells across 21 tabs) and Google re-evaluates it on every read, so
        that overhead was paid five times per run. Fetching all ranges in one
        batchGet pays it once.

        The read is deferred until a resource is first iterated: position_control_source()
        is evaluated at import time by the Dagster asset definitions, and hitting
        the Sheets API there would make loading the code location slow and
        failure-prone.
        """
        if not tab_cache:
            sheets = google_spreadsheet(
                position_control_sheet_id=position_control_sheet_id,
                range_names=POSITION_CONTROL_TABS,
                get_sheets=False,
                get_named_ranges=False,
            )
            for name in POSITION_CONTROL_TABS:
                tab_cache[name] = list(sheets.resources[name])
                _validate_required_columns(name, tab_cache[name])
        yield from tab_cache.get(tab_name, [])

    @dlt.resource(
        name="raw_position_control_positions",
        primary_key="Position_ID",
        write_disposition="merge",
    )
    def position_control_positions():
        """
        Process Position Control positions data from the Google Sheet.

        Yields:
            Dict[str, Any]: Parsed and converted rows of positions data.
        """
        data = _tab_rows("Positions")
        # Define type conversions
        conversions = {
            "Position_Count": ensure_str,
            "Salary_Scale": ensure_str,
        }
        for row in data:
            if is_valid_row(row, "Position_ID"):
                yield convert_types(row, conversions)

    @dlt.resource(
        name="raw_position_control_employees",
        primary_key="Employee_ID",
        write_disposition="merge",
    )
    def position_control_employees():
        """
        Process Position Control employees data from the Google Sheet.

        Yields:
            Dict[str, Any]: Parsed rows of employees data.
        """
        data = _tab_rows("Employees")
        for row in data:
            if is_valid_row(row, "Employee_ID"):
                yield row

    @dlt.resource(
        name="raw_position_control_adjustments",
        primary_key="Adjustment_ID",
        write_disposition="merge",
    )
    def position_control_adjustments():
        """
        Process Position Control adjustments data from the Google Sheet.

        Yields:
            Dict[str, Any]: Parsed and converted rows of adjustments data.
        """
        data = _tab_rows("Adjustments")
        # Define type conversions
        conversions = {
            "Adjustment_PPP": ensure_float,
            "Adjustment_Total": ensure_float,
            "Adjustment_Salary": ensure_float,
            "Adjustment_Hourly": ensure_float,
        }
        for row in data:
            if is_valid_row(row, "Adjustment_ID"):
                yield convert_types(row, conversions)

    @dlt.resource(
        name="raw_position_control_stipends",
        primary_key="Stipend_ID",
        write_disposition="merge",
    )
    def position_control_stipends():
        """
        Process Position Control stipends data from the Google Sheet.

        Yields:
            Dict[str, Any]: Parsed rows of stipends data.
        """
        data = _tab_rows("Stipends")
        for row in data:
            if is_valid_row(row, "Stipend_ID") and row.get("Employee_ID"):
                yield row

    @dlt.resource(
        name="raw_position_control_assignments",
        primary_key="Assignment_ID",
        write_disposition="merge",
    )
    def position_control_assignments():
        """
        Process Position Control assignments data from the Google Sheet.

        Yields:
            Dict[str, Any]: Parsed and converted rows of assignments data.
        """
        data = _tab_rows("Assignments")
        # Define type conversions
        conversions = {
            "Assignment_Scale": ensure_str,
            "Assignment_Calendar": ensure_float,
            "Assignment_Salary": ensure_float,
            "Assignment_Wage": ensure_float,
            "Assignment_FTE": ensure_float,
            "Assignment_PPP": ensure_float,
            "Employee_ID": int,
        }
        for row in data:
            if (
                is_valid_row(row, "Assignment_ID")
                and row.get("Employee_ID") is not None
                and row.get("Position_ID") is not None
            ):
                yield convert_types(row, conversions)

    return [
        position_control_positions,
        position_control_employees,
        position_control_adjustments,
        position_control_stipends,
        position_control_assignments,
    ]
