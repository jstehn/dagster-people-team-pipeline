from dagster import ScheduleDefinition

from ..jobs import all_assets_job

# Schedule for running all assets daily at 2 AM
daily_all_assets_schedule = ScheduleDefinition(
    name="daily_all_assets",
    cron_schedule="0 5 * * *",  # Runs daily at 05:00
    job=all_assets_job,
    execution_timezone="America/Los_Angeles",
)
