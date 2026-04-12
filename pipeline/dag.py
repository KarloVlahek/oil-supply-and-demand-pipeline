from datetime import date, timedelta
from typing import Optional

from prefect import flow, task

from ingest import run_incremental_ingest
from load import incremental_load


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def ingest_task(ingest_date: date) -> list[str]:
    return run_incremental_ingest(ingest_date)


@task(retries=3, retry_delay_seconds=60, log_prints=True)
def load_task(ingest_date: date) -> None:
    from datetime import date as date_type
    # Load all months in the 90-day window
    lookback_start = date_type.today() - timedelta(days=90)
    first_of_current = date_type(date_type.today().year, date_type.today().month, 1)

    current = date_type(lookback_start.year, lookback_start.month, 1)
    while current < first_of_current:
        incremental_load(current)
        if current.month == 12:
            current = date_type(current.year + 1, 1, 1)
        else:
            current = date_type(current.year, current.month + 1, 1)

@flow(
    name="cems-daily-pipeline",
    description="Daily EPA CEMS ingestion pipeline: API → GCS → BigQuery",
)
def cems_daily_pipeline(ingest_date: Optional[date] = None) -> None:
    if ingest_date is None:
        ingest_date = date.today() - timedelta(days=1)

    # Adjust to last completed month if needed
    first_of_current_month = date(date.today().year, date.today().month, 1)
    if ingest_date >= first_of_current_month:
        last_completed = first_of_current_month - timedelta(days=1)
        ingest_date = date(last_completed.year, last_completed.month, 1)
        print(f"Adjusted to last completed month: {ingest_date}")

    print(f"Running pipeline for date: {ingest_date}")

    uploaded = ingest_task(ingest_date)

    if uploaded:
        load_task(ingest_date)
    else:
        print("No files uploaded — skipping BigQuery load")


if __name__ == "__main__":
    cems_daily_pipeline.serve(
        name="cems-daily-pipeline-deployment",
        cron="0 6 * * *",        # 6am UTC daily
        parameters={"ingest_date": None}
    )