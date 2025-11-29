from dagster import Definitions, ScheduleDefinition, define_asset_job

from .assets import airbyte_assets, dbt_project_assets, resources

# Define a job that runs all assets
all_assets_job = define_asset_job(
    name="all_assets_job",
    selection="*",  # Select all assets
)

# Define hourly schedule
hourly_schedule = ScheduleDefinition(
    name="hourly_sync",
    job=all_assets_job,
    cron_schedule="0 * * * *",  # Every hour at minute 0
)

defs = Definitions(
    assets=[
        *airbyte_assets,
        dbt_project_assets,
    ],
    resources=resources,
    jobs=[all_assets_job],
    schedules=[hourly_schedule],
)

__all__ = ["defs"]
