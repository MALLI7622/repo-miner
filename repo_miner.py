#!/usr/bin/env python3
"""
repo_miner.py (flat layout)

Implements:
    fetch_commits(repo_full_name: str, max_commits: int | None = None) -> pd.DataFrame
    fetch_issues(repo_full_name: str, state: str="all", max_issues: int | None = None) -> pd.DataFrame

CLI:
    python -m repo_miner fetch-commits --repo owner/repo [--max 100] --out commits.csv
    python -m repo_miner fetch-issues  --repo owner/repo [--state all|open|closed] [--max 50] --out issues.csv
or
    python repo_miner.py <subcommand> ...
"""

from __future__ import annotations

import os
import argparse
from typing import Optional, List, Dict, Any
import pandas as pd
Github = None

def _get_github_class():
    global Github
    if Github is None:
        from github import Github as _Github
        Github = _Github
    return Github



# -------------------------- Commits --------------------------

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

    gh = _get_github_class()(token)
    repo = gh.get_repo(repo_full_name)

    records: List[Dict[str, str]] = []
    count = 0

    for commit in repo.get_commits():
        records.append(_normalize_commit(commit))
        count += 1
        if max_commits is not None and count >= max_commits:
            break

    return pd.DataFrame.from_records(records, columns=["sha", "author", "email", "date", "message"])


# -------------------------- Issues --------------------------

def _iso(dt: Any) -> str:
    if dt is None:
        return ""
    try:
        return dt.isoformat()
    except Exception:
        return str(dt)


def _normalize_issue(issue) -> Dict[str, Any]:
    """Normalize a PyGitHub Issue object into our schema + derived fields."""
    # Skip PRs: PyGithub Issue has .pull_request set for PRs
    if getattr(issue, "pull_request", None):
        return {}

    iid = getattr(issue, "id", "")
    number = getattr(issue, "number", "")
    title = getattr(issue, "title", "") or ""
    user_obj = getattr(issue, "user", None)
    user_login = getattr(user_obj, "login", "") if user_obj is not None else ""
    state = getattr(issue, "state", "") or ""
    created_at = getattr(issue, "created_at", None)
    closed_at = getattr(issue, "closed_at", None)
    comments = getattr(issue, "comments", 0)

    created_iso = _iso(created_at)
    closed_iso = _iso(closed_at)

    open_days = None
    if created_at is not None and closed_at is not None:
        try:
            # floor to whole days per requirements
            delta = closed_at - created_at
            open_days = delta.days
        except Exception:
            open_days = None

    return {
        "id": iid,
        "number": number,
        "title": title,
        "user": user_login,
        "state": state,
        "created_at": created_iso,
        "closed_at": closed_iso,
        "comments": comments,
        "open_duration_days": open_days,
    }


def fetch_issues(repo_full_name: str, state: str = "all", max_issues: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch up to `max_issues` issues (excluding PRs) from the GitHub repo `owner/repo`.
    Returns a DataFrame with columns:
        id, number, title, user, state, created_at, closed_at, comments, open_duration_days
    """
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN environment variable is not set")

    if state not in {"all", "open", "closed"}:
        raise ValueError("state must be one of: 'all', 'open', 'closed'")

    gh = _get_github_class()(token)
    repo = gh.get_repo(repo_full_name)

    records: List[Dict[str, Any]] = []
    count = 0
    for issue in repo.get_issues(state=state):
        if getattr(issue, "pull_request", None):
            # PRs should be skipped
            continue

        rec = _normalize_issue(issue)
        if rec:
            records.append(rec)
            count += 1
            if max_issues is not None and count >= max_issues:
                break

    cols = ["id", "number", "title", "user", "state", "created_at", "closed_at", "comments", "open_duration_days"]
    return pd.DataFrame.from_records(records, columns=cols)


# Optional stub kept for forward-compat
def merge_and_summarize(*args, **kwargs):
    raise NotImplementedError("merge_and_summarize is not implemented for this milestone.")


def main():
    parser = argparse.ArgumentParser(prog="repo_miner", description="Fetch GitHub commits/issues and export to CSV")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # fetch-commits
    c1 = subparsers.add_parser("fetch-commits", help="Fetch commits and save to CSV")
    c1.add_argument("--repo", required=True, help="owner/repo")
    c1.add_argument("--max", type=int, dest="max_commits", help="Max number of commits")
    c1.add_argument("--out", required=True, help="Output CSV path")

    # fetch-issues
    c2 = subparsers.add_parser("fetch-issues", help="Fetch issues and save to CSV")
    c2.add_argument("--repo", required=True, help="owner/repo")
    c2.add_argument("--state", choices=["all", "open", "closed"], default="all", help="Issue state filter")
    c2.add_argument("--max", type=int, dest="max_issues", help="Max number of issues")
    c2.add_argument("--out", required=True, help="Output CSV path")

    args = parser.parse_args()

    if args.command == "fetch-commits":
        df = fetch_commits(args.repo, args.max_commits)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} commits to {args.out}")

    elif args.command == "fetch-issues":
        df = fetch_issues(args.repo, args.state, args.max_issues)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} issues to {args.out}")


if __name__ == "__main__":
    main()
