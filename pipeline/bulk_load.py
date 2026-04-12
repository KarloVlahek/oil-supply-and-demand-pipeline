import os
import io
import json
import time
from datetime import date, timedelta

import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

from cems_client import fetch_state_month, generate_state_month_chunks

load_dotenv()

GCS_BUCKET_NAME    = os.getenv("GCS_BUCKET_NAME")
CREDENTIALS_PATH   = os.path.join(os.path.dirname(__file__), "..", "credentials", "gcp-key.json")
CHECKPOINT_FILE    = os.path.join(os.path.dirname(__file__), "..", "checkpoint.json")


# ── GCS helpers ────────────────────────────────────────────────────────────────

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


def blob_exists(bucket_name: str, blob_name: str) -> bool:
    """Check if a blob already exists in GCS."""
    client = get_gcs_client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(blob_name)
    return blob.exists()


# ── Checkpoint helpers ──────────────────────────────────────────────────────────

def load_checkpoint() -> set:
    """
    Load the set of completed chunks from the checkpoint file.
    Each entry is a string like 'TX-2024-01'.
    """
    if not os.path.exists(CHECKPOINT_FILE):
        return set()
    with open(CHECKPOINT_FILE, "r") as f:
        return set(json.load(f))


def save_checkpoint(completed: set) -> None:
    """Save the completed chunks set to the checkpoint file."""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(list(completed), f)


# ── Core bulk load ──────────────────────────────────────────────────────────────

def run_bulk_load(
    start_year:  int = 2016,
    start_month: int = 1,
) -> None:
    """
    Bulk load all CEMS data from start_year/start_month to yesterday
    for all 50 states. Skips already completed chunks via checkpointing.
    """
    chunks    = generate_state_month_chunks(start_year, start_month)
    completed = load_checkpoint()

    total     = len(chunks)
    skipped   = 0
    succeeded = 0
    failed    = []

    print(f"=== CEMS Bulk Load Started ===")
    print(f"Total chunks : {total:,} ({len(chunks) // 50} months × 50 states)")
    print(f"Already done : {len(completed):,}")
    print(f"Remaining    : {total - len(completed):,}\n")

    for i, (state, year, month) in enumerate(chunks, 1):
        chunk_key = f"{state}-{year}-{month:02d}"
        blob_name = f"raw/cems/state={state}/year={year}/month={month:02d}/cems_{state}_{year}_{month:02d}.parquet"

        # Skip if already completed
        if chunk_key in completed:
            skipped += 1
            continue

        print(f"[{i:>5}/{total}] {chunk_key} ...", end=" ", flush=True)

        try:
            # Fetch and aggregate
            df = fetch_state_month(state, year, month)

            if df.empty:
                print("empty — skipping")
                completed.add(chunk_key)
                save_checkpoint(completed)
                continue

            # Upload to GCS
            upload_parquet_to_gcs(df, GCS_BUCKET_NAME, blob_name)
            completed.add(chunk_key)
            save_checkpoint(completed)

            succeeded += 1
            print(f"done ({len(df):,} rows → gs://{GCS_BUCKET_NAME}/{blob_name})")

        except Exception as e:
            failed.append(chunk_key)
            print(f"FAILED: {e}")

        # Be polite to the API
        time.sleep(0.5)

    print(f"\n=== Bulk Load Complete ===")
    print(f"Succeeded : {succeeded:,}")
    print(f"Skipped   : {skipped:,}")
    print(f"Failed    : {len(failed):,}")

    if failed:
        print(f"\nFailed chunks:")
        for chunk in failed:
            print(f"  {chunk}")
        print(f"\nRe-run the script to retry failed chunks.")


if __name__ == "__main__":
    run_bulk_load(start_year=2016, start_month=1)