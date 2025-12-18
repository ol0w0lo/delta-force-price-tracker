import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

OWNER = "orzice"
REPO = "DeltaForcePrice"
PATH = "price.json"

# Change this: how far back you want
SINCE_ISO = "2025-06-01T00:00:00Z"# YYYY-MM-DDT00:00:00Z

OUT_FILE = "delta_price_history.csv"

# Optional but strongly recommended to avoid GitHub rate limits:
# Create a GitHub token and set it:
#   setx GITHUB_TOKEN "xxxx"
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")

HEADERS = {"Accept": "application/vnd.github+json"}
if GITHUB_TOKEN:
    HEADERS["Authorization"] = f"Bearer {GITHUB_TOKEN}"

def list_commits(since_iso: str):
    """List commits that touched price.json since the given time."""
    url = f"https://api.github.com/repos/{OWNER}/{REPO}/commits"
    params = {"path": PATH, "since": since_iso, "per_page": 100, "page": 1}
    commits = []

    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=20)
        r.raise_for_status()
        batch = r.json()

        print(f"Fetched page {params['page']} -> {len(batch)} commits")

        if not batch:
            break

        commits.extend(batch)
        params["page"] += 1

        # be polite
        time.sleep(0.15)

    return commits


def fetch_price_json_at_sha(sha: str):
    raw = f"https://raw.githubusercontent.com/{OWNER}/{REPO}/{sha}/{PATH}"
    r = requests.get(raw, timeout=20)
    r.raise_for_status()
    return r.json()


def pick_one_commit_per_day(commits):
    """
    Commits are returned newest -> oldest.
    We'll pick the newest commit for each calendar day (UTC).
    """
    picked = []
    seen_days = set()

    for c in commits:
        dt = c["commit"]["committer"]["date"]  # ISO
        day = dt[:10]  # YYYY-MM-DD
        if day not in seen_days:
            picked.append(c)
            seen_days.add(day)

    return picked

def append_rows(rows, commit_iso: str):
    """
    Append one snapshot to CSV.
    We use commit time (always exists) as the timeline for historical backfill.
    """
    df = pd.DataFrame(rows)

    # Always add commit_time
    df["commit_time"] = pd.to_datetime(commit_iso, utc=True)

    # If is_get_time exists, parse it; otherwise fill with commit_time
    if "is_get_time" in df.columns:
        # some versions store seconds, some may store strings; be robust
        df["is_get_time"] = pd.to_datetime(df["is_get_time"], unit="s", utc=True, errors="coerce")
        df["is_get_time"] = df["is_get_time"].fillna(df["commit_time"])
    else:
        df["is_get_time"] = df["commit_time"]

    existing = os.path.exists(OUT_FILE)
    df.to_csv(
        OUT_FILE,
        mode="a" if existing else "w",
        header=not existing,
        index=False,
        encoding="utf-8-sig",
    )


def main():
    print("Listing commits since:", SINCE_ISO)
    commits = list_commits(SINCE_ISO)
    print("Total commits touching price.json:", len(commits))

    if not commits:
        print("No commits found.")
        return

    # choose daily snapshots to keep it manageable
    picked = pick_one_commit_per_day(commits)
    print("Picked daily snapshots:", len(picked))

    # Fetch each snapshot and append
    total_rows = 0
    for i, c in enumerate(picked, 1):
        sha = c["sha"]
        dt = c["commit"]["committer"]["date"]
        print(f"[{i}/{len(picked)}] {dt} sha={sha[:7]} ...")

        data = fetch_price_json_at_sha(sha)
        append_rows(data, dt)
        total_rows += len(data)

        time.sleep(0.3)

    print("Done. Appended rows:", total_rows)
    print("History file:", OUT_FILE)

if __name__ == "__main__":
    main()
