import csv
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

SOURCE_URL = "https://raw.githubusercontent.com/orzice/DeltaForcePrice/master/price.json"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

def fetch_snapshot() -> pd.DataFrame:
    r = requests.get(SOURCE_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # snapshot_time = when we recorded it (GitHub runner time)
    now = datetime.now(timezone.utc)
    df["snapshot_time"] = now.isoformat()

    # commit_time = prefer is_get_time if present, else snapshot_time
    if "is_get_time" in df.columns:
        df["commit_time"] = pd.to_datetime(df["is_get_time"], unit="s", utc=True, errors="coerce")
        df["commit_time"] = df["commit_time"].fillna(pd.to_datetime(df["snapshot_time"], utc=True))
    else:
        df["commit_time"] = pd.to_datetime(df["snapshot_time"], utc=True)

    return df

def append_daily(df: pd.DataFrame) -> Path:
    if df.empty:
        return None

    day = pd.to_datetime(df["commit_time"], utc=True).dt.date.iloc[0].isoformat()
    out = DATA_DIR / f"{day}.csv"

    write_header = not out.exists()
    df.to_csv(
        out,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
    )
    return out

if __name__ == "__main__":
    df = fetch_snapshot()
    out = append_daily(df)
    if out:
        print(f"Appended {len(df)} rows -> {out}")
    else:
        print("No data returned; nothing appended.")
