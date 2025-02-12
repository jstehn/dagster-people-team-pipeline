import dlt

from .google_sheets import google_spreadsheet


@dlt.source(name="position_control_source")
def position_control_source(
    spreadsheet_url_or_id: str = dlt.config.value,
):
    @dlt.resource(
        name="position_control_positions",
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
        yield from (row for row in data if row.get("Position_ID") is not None)

    @dlt.resource(
        name="position_control_employees",
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
        name="position_control_adjustments",
        primary_key="Adjustment_ID",
        write_disposition="merge",
    )
    def position_control_adjustments():
        data = google_spreadsheet(
            spreadsheet_url_or_id=spreadsheet_url_or_id,
            range_names=["Adjustments"],
            get_sheets=False,
            get_named_ranges=False,
        )
        yield from (row for row in data if row.get("Adjustment_ID") is not None)

    @dlt.resource(
        name="position_control_stipends",
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
        yield from (row for row in data if row.get("Stipend_ID") is not None)

    @dlt.resource(
        name="position_control_assignments",
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
        yield from (row for row in data if row.get("Assignment_ID") is not None)

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
        dataset_name="position_control_data",
    )
    info = pipeline.run(position_control_source())
    print(info)
    print(info)
