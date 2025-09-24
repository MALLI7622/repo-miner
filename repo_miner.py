#!/usr/bin/env python3
"""
repo_miner.py (flat layout)

Implements:
    fetch_commits(repo_full_name: str, max_commits: int | None = None) -> pd.DataFrame

CLI:
    python -m repo_miner fetch-commits --repo owner/repo [--max 100] --out commits.csv
or
    python repo_miner.py fetch-commits --repo owner/repo [--max 100] --out commits.csv
"""

from __future__ import annotations

import os
import argparse
from typing import Optional, List, Dict
import pandas as pd
from github import Github


def _normalize_commit(commit) -> Dict[str, str]:
    """Normalize a PyGitHub Commit object into our schema."""
    sha = getattr(commit, "sha", None)
    c = getattr(commit, "commit", None)

    author = email = date_iso = message = None
    if c is not None:
        a = getattr(c, "author", None)
        if a is not None:
            author = getattr(a, "name", None)
            email = getattr(a, "email", None)
            date = getattr(a, "date", None)
            if date is not None:
                try:
                    date_iso = date.isoformat()
                except Exception:
                    date_iso = str(date)
        msg = getattr(c, "message", "") or ""
        message = msg.splitlines()[0] if msg else ""

    return {
        "sha": sha or "",
        "author": author or "",
        "email": email or "",
        "date": date_iso or "",
        "message": message or "",
    }


def fetch_commits(repo_full_name: str, max_commits: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch up to `max_commits` commits from the GitHub repo `owner/repo`.

    Returns a DataFrame with columns: sha, author, email, date (ISO-8601), message (first line).
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is not set")

    gh = Github(token)
    repo = gh.get_repo(repo_full_name)

    records: List[Dict[str, str]] = []
    count = 0

    for commit in repo.get_commits():
        records.append(_normalize_commit(commit))
        count += 1
        if max_commits is not None and count >= max_commits:
            break

    return pd.DataFrame.from_records(records, columns=["sha", "author", "email", "date", "message"])


# Optional stubs to keep future imports happy
def fetch_issues(*args, **kwargs):
    raise NotImplementedError("fetch_issues is not implemented for RM1.")

def merge_and_summarize(*args, **kwargs):
    raise NotImplementedError("merge_and_summarize is not implemented for RM1.")


def main():
    parser = argparse.ArgumentParser(prog="repo_miner", description="Fetch GitHub commits and export to CSV")
    subparsers = parser.add_subparsers(dest="command", required=True)

    c1 = subparsers.add_parser("fetch-commits", help="Fetch commits and save to CSV")
    c1.add_argument("--repo", required=True, help="owner/repo")
    c1.add_argument("--max", type=int, dest="max_commits", help="Max number of commits")
    c1.add_argument("--out", required=True, help="Output CSV path")

    args = parser.parse_args()

    if args.command == "fetch-commits":
        df = fetch_commits(args.repo, args.max_commits)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} commits to {args.out}")


if __name__ == "__main__":
    main()
