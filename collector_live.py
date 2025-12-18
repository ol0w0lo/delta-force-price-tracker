import pathlib
import csv
import requests
import pandas as pd
from datetime import datetime, timezone

# =========================
# CONFIG
# =========================
SOURCE_URL = "https://raw.githubusercontent.com/orzice/DeltaForcePrice/master/price.json"
LIVE_FILE = pathlib.Path("delta_price_live.csv")

# =========================
# FETCH + APPEND
# =========================
def fetch_snapshot() -> pd.DataFrame:
    r = requests.get(SOURCE_URL, timeout=20)
    r.raise_for_status()
    data = r.json()
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Ensure time column exists
    if "is_get_time" in df.columns:
        df["commit_time"] = pd.to_datetime(
            df["is_get_time"], unit="s", utc=True, errors="coerce"
        )
    else:
        df["commit_time"] = datetime.now(timezone.utc)

    return df


def append_to_live(df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    if LIVE_FILE.exists():
        df.to_csv(
            LIVE_FILE,
            mode="a",
            header=False,
            index=False,
            encoding="utf-8-sig",
            quoting=csv.QUOTE_ALL,
        )
    else:
        df.to_csv(
            LIVE_FILE,
            index=False,
            encoding="utf-8-sig",
            quoting=csv.QUOTE_ALL,
        )

    return len(df)


if __name__ == "__main__":
    df = fetch_snapshot()
    n = append_to_live(df)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] appended {n} rows")
