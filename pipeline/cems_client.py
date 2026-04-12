import os
import time
import requests
import pandas as pd
from datetime import date, timedelta
from dotenv import load_dotenv

load_dotenv()

CEMS_API_KEY = os.getenv("CEMS_API_KEY")
BASE_URL = "https://api.epa.gov/easey/streaming-services/emissions/apportioned/hourly"

STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
]

# STATES = ["NV"]

def fetch_state_month(
    state: str,
    year: int,
    month: int,
    retries: int = 3
) -> pd.DataFrame:
    """
    Fetch all hourly CEMS data for a given state and month,
    aggregate to daily level, and return as a cleaned DataFrame.

    Args:
        state:   Two-letter state code (e.g. "TX")
        year:    Four-digit year (e.g. 2024)
        month:   Month as integer (e.g. 1 for January)
        retries: Number of retry attempts on failure

    Returns:
        DataFrame with columns:
            state, facility_name, facility_id, unit_id, date,
            gross_load_mw, so2_mass_lbs, co2_mass_tons,
            nox_mass_lbs, heat_input_mmbtu, primary_fuel,
            unit_type, ingested_at
    """
    # Build begin and end dates for the month
    begin_date = date(year, month, 1)
    if month == 12:
        end_date = date(year, 12, 31)
    else:
        end_date = date(year, month + 1, 1) - timedelta(days=1)

    params = {
        "api_key":    CEMS_API_KEY,
        "stateCode":  state,
        "beginDate":  begin_date.strftime("%Y-%m-%d"),
        "endDate":    end_date.strftime("%Y-%m-%d"),
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()
            break
        except Exception as e:
            if attempt == retries:
                print(f"    ERROR: {state} {year}-{month:02d} failed after {retries} attempts: {e}")
                return pd.DataFrame()
            print(f"    Retry {attempt}/{retries} for {state} {year}-{month:02d}...")
            time.sleep(5 * attempt)

    if not data:
        print(f"    WARNING: No data for {state} {year}-{month:02d}")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Select and rename only the columns we need
    col_map = {
        "stateCode":    "state",
        "facilityName": "facility_name",
        "facilityId":   "facility_id",
        "unitId":       "unit_id",
        "date":         "date",
        "grossLoad":    "gross_load_mw",
        "so2Mass":      "so2_mass_lbs",
        "co2Mass":      "co2_mass_tons",
        "noxMass":      "nox_mass_lbs",
        "heatInput":    "heat_input_mmbtu",
        "primaryFuelInfo": "primary_fuel",
        "unitType":     "unit_type",
    }

    df = df[[c for c in col_map.keys() if c in df.columns]].copy()
    df = df.rename(columns=col_map)

    # Cast types
    df["date"]         = pd.to_datetime(df["date"]).dt.date
    df["facility_id"]  = pd.to_numeric(df["facility_id"], errors="coerce")
    df["gross_load_mw"]     = pd.to_numeric(df["gross_load_mw"],     errors="coerce")
    df["so2_mass_lbs"]      = pd.to_numeric(df["so2_mass_lbs"],      errors="coerce")
    df["co2_mass_tons"]     = pd.to_numeric(df["co2_mass_tons"],      errors="coerce")
    df["nox_mass_lbs"]      = pd.to_numeric(df["nox_mass_lbs"],      errors="coerce")
    df["heat_input_mmbtu"]  = pd.to_numeric(df["heat_input_mmbtu"],  errors="coerce")

    # Aggregate hourly → daily
    agg_cols = ["gross_load_mw", "so2_mass_lbs", "co2_mass_tons",
                "nox_mass_lbs", "heat_input_mmbtu"]

    df = (
        df.groupby(
            ["state", "facility_name", "facility_id", "unit_id",
             "date", "primary_fuel", "unit_type"],
            dropna=False
        )
        .agg({col: "sum" for col in agg_cols})
        .reset_index()
    )

    for col in agg_cols:
        df[col] = df[col].astype("float64")
        
    df["ingested_at"] = pd.Timestamp.now("UTC")

    return df


def generate_state_month_chunks(
    start_year: int  = 2016,
    start_month: int = 1,
    end_date: date   = None
) -> list[tuple[str, int, int]]:
    """
    Generate a list of (state, year, month) tuples to process.

    Args:
        start_year:  Year to start from
        start_month: Month to start from
        end_date:    Date to stop at (defaults to yesterday)

    Returns:
        List of (state, year, month) tuples
    """
    if end_date is None:
        # Stop at the last fully completed month
        today      = date.today()
        first_of_month = date(today.year, today.month, 1)
        last_completed = first_of_month - timedelta(days=1)  # last day of previous month
        end_date   = date(last_completed.year, last_completed.month, 1)

    chunks  = []
    current = date(start_year, start_month, 1)

    while current <= end_date:
        for state in STATES:
            chunks.append((state, current.year, current.month))
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)

    return chunks


if __name__ == "__main__":
    # Quick test — fetch one state, one month
    print("Testing cems_client.py — fetching TX January 2024...\n")
    df = fetch_state_month("TX", 2024, 1)

    if not df.empty:
        print(f"Rows returned : {len(df):,}")
        print(f"Date range    : {df['date'].min()} to {df['date'].max()}")
        print(f"Columns       : {df.columns.tolist()}")
        print(f"\nSample:")
        print(df.head(5).to_string(index=False))
    else:
        print("No data returned — check your API key and connection")