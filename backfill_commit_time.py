import os
import time
import csv
import requests
import pandas as pd

OWNER = "orzice"
REPO = "DeltaForcePrice"
PATH = "price.json"

SINCE_ISO = "2025-06-01T00:00:00Z"  # change earlier if you want
OUT_FILE = "delta_price_backfill_only.csv"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN and GITHUB_TOKEN != "YOUR_TOKEN":
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

def list_commits(since_iso: str):
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/commits"
    params = {"path": PATH, "since": since_iso, "per_page": 100, "page": 1}
    commits = []

    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        print(f"Fetched page {params['page']} -> {len(batch)} commits")
        if not batch:
            break
        commits.extend(batch)
        params["page"] += 1
        time.sleep(0.15)

    return commits

def pick_one_commit_per_day(commits):
    picked = []
    seen_days = set()
    # commits newest -> oldest
    for c in commits:
        dt = c["commit"]["committer"]["date"]  # ISO string
        day = dt[:10]
        if day not in seen_days:
            picked.append(c)
            seen_days.add(day)
    return picked

def fetch_price_json_at_sha(sha: str):
    raw = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{sha}/{PATH}"
    r = requests.get(raw, timeout=30)
    r.raise_for_status()
    return r.json()

def append_snapshot(rows, commit_iso: str, write_header: bool):
    df = pd.DataFrame(rows)
    df["commit_time"] = pd.to_datetime(commit_iso, utc=True, errors="coerce")

    # If is_get_time exists, parse it; else set it to commit_time
    if "is_get_time" in df.columns:
        parsed = pd.to_datetime(df["is_get_time"], unit="s", utc=True, errors="coerce")
        if parsed.isna().all():
            parsed = pd.to_datetime(df["is_get_time"], utc=True, errors="coerce")
        df["is_get_time"] = parsed.fillna(df["commit_time"])
    else:
        df["is_get_time"] = df["commit_time"]

    df.to_csv(
        OUT_FILE,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
        quoting=csv.QUOTE_ALL,
    )

def main():
    print("Listing commits since:", SINCE_ISO)
    commits = list_commits(SINCE_ISO)
    print("Total commits touching price.json:", len(commits))

    if not commits:
        print("No commits found.")
        return

    picked = pick_one_commit_per_day(commits)
    print("Picked daily snapshots:", len(picked))

    # Overwrite output file fresh each run
    open(OUT_FILE, "w", encoding="utf-8-sig").close()

    total_rows = 0
    for i, c in enumerate(picked, 1):
        sha = c["sha"]
        dt = c["commit"]["committer"]["date"]
        print(f"[{i}/{len(picked)}] {dt} sha={sha[:7]}")

        data = fetch_price_json_at_sha(sha)
        append_snapshot(data, dt, write_header=(i == 1))
        total_rows += len(data)
        time.sleep(0.2)

    print("Done. Wrote rows:", total_rows)
    print("Output:", OUT_FILE)

if __name__ == "__main__":
    main()
