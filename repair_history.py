import csv
import pandas as pd
from pathlib import Path

SRC = Path("delta_price_history.csv")
OUT = Path("delta_price_history_clean.csv")

def main():
    # Read with python engine, skip bad lines (we'll lose only the broken lines)
    df = pd.read_csv(
        SRC,
        encoding="utf-8-sig",
        engine="python",
        on_bad_lines="skip",  # pandas>=1.3
    )

    # Rewrite with strong quoting so commas in names never break CSV again
    df.to_csv(
        OUT,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
    )

    print("Done.")
    print("Original:", SRC, "rows:", len(df))
    print("Cleaned :", OUT)

if __name__ == "__main__":
    main()
