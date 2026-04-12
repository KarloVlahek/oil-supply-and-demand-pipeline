import os
from datetime import date, timedelta

from google.cloud import bigquery, storage
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID   = os.getenv("GCP_PROJECT_ID")
BQ_DATASET_ID    = os.getenv("BQ_DATASET_ID")
GCS_BUCKET_NAME  = os.getenv("GCS_BUCKET_NAME")
CREDENTIALS_PATH = os.path.join(os.path.dirname(__file__), "..", "credentials", "gcp-key.json")
TABLE_ID         = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.raw_cems_daily"


def get_bq_client():
    return bigquery.Client.from_service_account_json(CREDENTIALS_PATH)


def load_gcs_to_bq(gcs_uri: str, write_disposition: str = "WRITE_APPEND") -> None:
    """
    Load Parquet file(s) from GCS into BigQuery raw_cems_daily table.

    Args:
        gcs_uri:           GCS URI or wildcard URI to load from
        write_disposition: WRITE_APPEND (default) or WRITE_TRUNCATE
    """
    client = get_bq_client()

    job_config = bigquery.LoadJobConfig(
        source_format     = bigquery.SourceFormat.PARQUET,
        write_disposition = write_disposition,
        autodetect        = True,
    )

    # Handle both single URI string and list of URIs
    if isinstance(gcs_uri, str):
        source = [gcs_uri]
    else:
        source = gcs_uri

    print(f"  Loading {len(source)} file(s) → {TABLE_ID}")

    load_job = client.load_table_from_uri(
        source,
        TABLE_ID,
        job_config=job_config,
    )

    load_job.result()

    table = client.get_table(TABLE_ID)
    print(f"  Done — total rows in table: {table.num_rows:,}")


def bulk_load() -> None:
    """
    Load all historical Parquet files from GCS into BigQuery.
    Uses a wildcard URI to load all files in one BigQuery job.
    Truncates the table first to avoid duplicates on re-run.
    """
    print("=== Bulk Load: GCS → BigQuery ===\n")

    gcs_uri = f"gs://{GCS_BUCKET_NAME}/raw/cems/*"

    load_gcs_to_bq(gcs_uri, write_disposition="WRITE_TRUNCATE")

    print("\n=== Bulk Load Complete ===")


def incremental_load(load_date: date = None) -> None:
    """
    Load a single day's Parquet files for all states into BigQuery.
    Used by the daily Prefect DAG.

    Args:
        load_date: Date to load (defaults to yesterday)
    """
    if load_date is None:
        load_date = date.today() - timedelta(days=1)
        
    year  = load_date.year
    month = load_date.month

    print(f"=== Incremental Load: {load_date} ===\n")

    # List all parquet files for this year/month from GCS
    storage_client = storage.Client.from_service_account_json(CREDENTIALS_PATH)
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    prefix = f"raw/cems/"

    blobs = bucket.list_blobs(prefix=prefix)
    uris = [
        f"gs://{GCS_BUCKET_NAME}/{blob.name}"
        for blob in blobs
        if f"year={year}/month={month:02d}/" in blob.name
        and blob.name.endswith(".parquet")
    ]

    if not uris:
        print(f"No files found for {year}-{month:02d} — skipping")
        return

    print(f"  Found {len(uris)} files for {year}-{month:02d}")

    # BigQuery accepts a list of URIs directly
    load_gcs_to_bq(uris, write_disposition="WRITE_APPEND")

    print(f"\n=== Incremental Load Complete ===")


if __name__ == "__main__":
    bulk_load()