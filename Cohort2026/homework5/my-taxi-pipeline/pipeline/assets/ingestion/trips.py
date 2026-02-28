"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: vendor_id
    type: integer
    description: Vendor identifier.
  - name: pickup_datetime
    type: timestamp
    description: Trip pickup timestamp.
  - name: dropoff_datetime
    type: timestamp
    description: Trip dropoff timestamp.
  - name: payment_type
    type: integer
    description: Payment type id.
  - name: total_amount
    type: double
    description: Total charged amount.
  - name: extracted_at
    type: timestamp
    description: Extraction timestamp.

@bruin"""

import io
import json
import os
from datetime import date, datetime, timezone

import pandas as pd
import pyarrow.parquet as pq
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def _parse_vars() -> list[str]:
    raw = os.getenv("BRUIN_VARS", "{}")
    parsed = json.loads(raw)
    taxi_types = parsed.get("taxi_types", ["yellow", "green"])
    if isinstance(taxi_types, str):
        taxi_types = [taxi_types]
    return [str(t).lower() for t in taxi_types]


def _month_starts(start_date: date, end_date: date):
    cursor = date(start_date.year, start_date.month, 1)
    while cursor < end_date:
        yield cursor
        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _load_month(taxi_type: str, month_start: date) -> pd.DataFrame:
    url = f"{BASE_URL}/{taxi_type}_tripdata_{month_start:%Y-%m}.parquet"
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    table = pq.read_table(io.BytesIO(response.content))
    raw = table.to_pandas()

    vendor_col = _pick_col(raw, ["VendorID", "vendor_id"])
    pickup_col = _pick_col(raw, ["tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime"])
    dropoff_col = _pick_col(raw, ["tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime"])
    payment_col = _pick_col(raw, ["payment_type"])
    total_col = _pick_col(raw, ["total_amount"])

    if pickup_col is None:
        return pd.DataFrame()

    normalized = pd.DataFrame(
        {
            "vendor_id": raw[vendor_col] if vendor_col else pd.NA,
            "pickup_datetime": pd.to_datetime(raw[pickup_col], errors="coerce"),
            "dropoff_datetime": pd.to_datetime(raw[dropoff_col], errors="coerce") if dropoff_col else pd.NaT,
            "payment_type": raw[payment_col] if payment_col else pd.NA,
            "total_amount": pd.to_numeric(raw[total_col], errors="coerce") if total_col else pd.NA,
        }
    )
    return normalized


def materialize():
    start_date = datetime.strptime(os.environ["BRUIN_START_DATE"], "%Y-%m-%d").date()
    end_date = datetime.strptime(os.environ["BRUIN_END_DATE"], "%Y-%m-%d").date()
    start_ts = pd.Timestamp(start_date)
    end_ts = pd.Timestamp(end_date)

    taxi_types = _parse_vars()
    frames = []
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None)

    for taxi_type in taxi_types:
        for month_start in _month_starts(start_date, end_date):
            try:
                monthly = _load_month(taxi_type, month_start)
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code == 404:
                    continue
                raise
            if monthly.empty:
                continue
            monthly = monthly[(monthly["pickup_datetime"] >= start_ts) & (monthly["pickup_datetime"] < end_ts)]
            if monthly.empty:
                continue
            monthly["extracted_at"] = extracted_at
            frames.append(monthly)

    if not frames:
        return pd.DataFrame(
            columns=[
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "payment_type",
                "total_amount",
                "extracted_at",
            ]
        )

    result = pd.concat(frames, ignore_index=True)
    result["vendor_id"] = pd.to_numeric(result["vendor_id"], errors="coerce").astype("Int64")
    result["payment_type"] = pd.to_numeric(result["payment_type"], errors="coerce").astype("Int64")
    result["total_amount"] = pd.to_numeric(result["total_amount"], errors="coerce")
    result["pickup_datetime"] = pd.to_datetime(result["pickup_datetime"], errors="coerce")
    result["dropoff_datetime"] = pd.to_datetime(result["dropoff_datetime"], errors="coerce")
    return result
