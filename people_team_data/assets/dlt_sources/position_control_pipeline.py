from typing import Any, Dict

import dlt

from .google_sheets import google_spreadsheet


def ensure_str(value: Any) -> str:
    """Convert value to string, empty string if None"""
    return str(value) if value is not None else ""


def ensure_float(value: Any) -> float | None:
    """Convert value to float, None if invalid"""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def convert_types(
    row: Dict[str, Any], conversions: Dict[str, callable]
) -> Dict[str, Any]:
    """Apply type conversions to specified fields in a row"""
    if not row:
        return row

    print(f"\nProcessing row with ID: {row.get('Adjustment_ID')}")  # Debug
    for field, converter in conversions.items():
        if field in row:
            print(f"Converting field {field}: {row[field]!r}")  # Debug
            row[field] = converter(row[field])
            print(f"Converted value: {row[field]!r}")  # Debug
    return row


@dlt.source(name="position_control_source")
def position_control_source(
    spreadsheet_url_or_id: str = dlt.config.value,
):
    @dlt.resource(
        name="raw_position_control_positions",
        primary_key="Position_ID",
        write_disposition="merge",
    )
    def position_control_positions():
        data = google_spreadsheet(
            spreadsheet_url_or_id=spreadsheet_url_or_id,
            range_names=["Positions"],
            get_sheets=False,
            get_named_ranges=False,
        )
        # Define type conversions
        conversions = {
            "Position_Count": ensure_str,
        }
        for row in data:
            if row.get("Position_ID") is not None:
                yield convert_types(row, conversions)

    @dlt.resource(
        name="raw_position_control_employees",
        primary_key="Employee_ID",
        write_disposition="merge",
    )
    def position_control_employees():
        data = google_spreadsheet(
            spreadsheet_url_or_id=spreadsheet_url_or_id,
            range_names=["Employees"],
            get_sheets=False,
            get_named_ranges=False,
        )
        yield from (row for row in data if row.get("Employee_ID") is not None)

    @dlt.resource(
        name="raw_position_control_adjustments",
        primary_key="Adjustment_ID",
        write_disposition="merge",
        columns={
            "Adjustment_Begin_Payroll": {
                "name": "Adjustment_Begin_Payroll",
                "data_type": "text",
            },
            "Adjustment_End_Payroll": {
                "name": "Adjustment_End_Payroll",
                "data_type": "text",
            },
        },
    )
    def position_control_adjustments():
        data = google_spreadsheet(
            spreadsheet_url_or_id=spreadsheet_url_or_id,
            range_names=["Adjustments"],
            get_sheets=False,
            get_named_ranges=False,
        )
        # Define type conversions
        conversions = {
            "Adjustment_PPP": ensure_float,
            "Adjustment_Total": ensure_float,
        }
        for row in data:
            if row.get("Adjustment_ID") is not None:
                yield convert_types(row, conversions)

    @dlt.resource(
        name="raw_position_control_stipends",
        primary_key="Stipend_ID",
        write_disposition="merge",
    )
    def position_control_stipends():
        data = google_spreadsheet(
            spreadsheet_url_or_id=spreadsheet_url_or_id,
            range_names=["Stipends"],
            get_sheets=False,
            get_named_ranges=False,
        )
        yield from (
            row
            for row in data
            if row.get("Stipend_ID") is not None and row.get("Employee_ID")
        )

    @dlt.resource(
        name="raw_position_control_assignments",
        primary_key="Assignment_ID",
        write_disposition="merge",
    )
    def position_control_assignments():
        data = google_spreadsheet(
            spreadsheet_url_or_id=spreadsheet_url_or_id,
            range_names=["Assignments"],
            get_sheets=False,
            get_named_ranges=False,
        )
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
                row.get("Assignment_ID") is not None
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


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="position_control_pipeline",
        destination="postgres",
        dataset_name="raw_position_control_data",
    )
    info = pipeline.run(position_control_source())
    print(info)
