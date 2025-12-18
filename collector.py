import time
import pathlib
import requests
import pandas as pd

# Data source from the GitHub repo
URL = "https://raw.githubusercontent.com/orzice/DeltaForcePrice/master/price.json"

# Local history file
OUT_FILE = pathlib.Path("delta_price_history.csv")


def fetch_once() -> int:
    """
    Fetch current prices and append to local CSV.
    Returns number of rows saved.
    """
    print("Fetching latest prices...")
    resp = requests.get(URL, timeout=10)
    resp.raise_for_status()

    data = resp.json()  # list of dicts
    if not data:
        print("No data received.")
        return 0

    df = pd.DataFrame(data)

    # Convert Unix seconds → datetime (UTC)
    if "is_get_time" in df.columns:
        df["is_get_time"] = pd.to_datetime(df["is_get_time"], unit="s", utc=True)
    else:
        raise KeyError("Column 'is_get_time' not found in JSON data.")

    # Append to CSV (create with header if first time)
    if OUT_FILE.exists():
        df.to_csv(OUT_FILE, mode="a", header=False, index=False, encoding="utf-8-sig")
    else:
        df.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    print(f"Saved {len(df)} rows to {OUT_FILE}")
    return len(df)


if __name__ == "__main__":
    # ---------- MODE 1: run once ----------
    rows = fetch_once()
    print(f"Done. {rows} new records.")

    # ---------- MODE 2: run in loop (optional) ----------
    # Uncomment this if you want the script itself to loop forever.
    """
    INTERVAL_SECONDS = 600  # 10 minutes

    while True:
        try:
            rows = fetch_once()
            print(f"Fetched {rows} rows. Sleeping {INTERVAL_SECONDS} seconds...")
        except Exception as e:
            print("Error while fetching:", e)
        time.sleep(INTERVAL_SECONDS)
    """
