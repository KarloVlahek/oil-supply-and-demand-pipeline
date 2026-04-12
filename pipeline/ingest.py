import os
import io
from datetime import date, timedelta

import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

from cems_client import fetch_state_month, STATES

load_dotenv()

GCS_BUCKET_NAME  = os.getenv("GCS_BUCKET_NAME")
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "credentials", "gcp-key.json")


def get_gcs_client():
    return storage.Client.from_service_account_json(CREDENTIALS_PATH)


def upload_parquet_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str) -> None:
    """Upload a DataFrame as Parquet to GCS."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    blob.upload_from_file(buffer, content_type="application/octet-stream")
    print(f"  Uploaded: gs://{bucket_name}/{blob_name}")


def run_incremental_ingest(ingest_date: date = None) -> list[str]:
    """
    Fetch CEMS data for all 50 states for a given month and
    upload to GCS. Returns list of GCS URIs uploaded.

    For daily scheduling we re-upload the current full month's file
    AND the previous two closed months for each state since the EPA
    publishes data with a lag. Re-uploading ensures we always have the
    latest available data.

    Args:
        ingest_date: Date to ingest (defaults to yesterday)

    Returns:
        List of GCS blob paths uploaded
    """
    if ingest_date is None:
        ingest_date = date.today() - timedelta(days=1)

    # Build list of completed months within 90-day lookback window
    first_of_current_month = date(date.today().year, date.today().month, 1)
    lookback_start         = date.today() - timedelta(days=90)

    months_to_fetch = set()
    current = date(lookback_start.year, lookback_start.month, 1)
    while current < first_of_current_month:
        months_to_fetch.add((current.year, current.month))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    months_to_fetch = sorted(months_to_fetch)

    print(f"=== Incremental Ingest: {len(months_to_fetch)} months in 90-day window ===")
    print(f"  Months: {[f'{y}-{m:02d}' for y, m in months_to_fetch]}\n")

    uploaded = []

    for year, month in months_to_fetch:
        print(f"  --- {year}-{month:02d} ---")
        for state in STATES:
            print(f"    Fetching {state}...", end=" ", flush=True)
            df = fetch_state_month(state, year, month)

            if df.empty:
                print("empty — skipping")
                continue

            blob_name = (
                f"raw/cems/state={state}/year={year}/"
                f"month={month:02d}/cems_{state}_{year}_{month:02d}.parquet"
            )
            upload_parquet_to_gcs(df, GCS_BUCKET_NAME, blob_name)
            uploaded.append(blob_name)
            print(f"done ({len(df):,} rows)")

    print(f"\n=== Ingest Complete: {len(uploaded)} files uploaded ===")
    return uploaded


if __name__ == "__main__":
    run_incremental_ingest()