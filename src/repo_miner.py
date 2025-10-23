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


def merge_and_summarize(commits_df: pd.DataFrame, issues_df: pd.DataFrame) -> None:
    """
    Takes two DataFrames (commits and issues) and prints:
      - Top 5 committers by commit count
      - Issue close rate (closed/total)
      - Average open duration for closed issues (in days)

    Additionally, normalizes date columns and computes a by-day join of commit/issue activity.
    """
    # --- copies so we don't mutate caller-owned DFs ---
    commits = commits_df.copy()
    issues  = issues_df.copy()

    # 1) Normalize date/time columns to pandas datetime
    commits['date'] = pd.to_datetime(commits['date'], errors='coerce', utc=True)
    issues['created_at'] = pd.to_datetime(issues['created_at'], errors='coerce', utc=True)
    # Some inputs may have None/NaN or empty string; coerce handles that.
    issues['closed_at']  = pd.to_datetime(issues['closed_at'],  errors='coerce', utc=True)

    # 2) Top 5 committers
    print("Top 5 committers")
    if 'author' in commits and not commits.empty:
        top_committers = commits['author'].value_counts().head(5)
        for author, count in top_committers.items():
            print(f"- {author}: {count} commits")
    else:
        print("- (no commits)")

    # 3) Issue close rate (closed/total) with 2-decimal precision
    if not issues.empty and 'state' in issues:
        closed_cnt = issues['state'].astype(str).str.lower().eq('closed').sum()
        total_cnt  = len(issues)
        close_rate = (closed_cnt / total_cnt) if total_cnt > 0 else 0.0
        print(f"Issue close rate: {close_rate:.2f}")
    else:
        print("Issue close rate: 0.00")

    # 4) Average open duration (days) for closed issues
    # Only consider issues that have a valid closed_at and created_at
    if not issues.empty:
        closed_mask = issues['closed_at'].notna() & issues['created_at'].notna()
        durations = (issues.loc[closed_mask, 'closed_at'] - issues.loc[closed_mask, 'created_at']).dt.total_seconds() / 86400.0
        if not durations.empty:
            avg_days = float(durations.mean())
            print(f"Avg. issue open duration: {avg_days:.2f} days")
        else:
            print("Avg. issue open duration: N/A")
    else:
        print("Avg. issue open duration: N/A")

    # 5) (Optional) Join commits & issues by day (not required for the printed metrics, but useful)
    #    We compute it silently to respect the test output, but keep here for future use.
    if not commits.empty or not issues.empty:
        commits_by_day = (
            commits.assign(day=commits['date'].dt.date)
                   .groupby('day', dropna=True)
                   .size()
                   .rename('commits')
        )
        issues_by_day = (
            issues.assign(day=issues['created_at'].dt.date)
                  .groupby('day', dropna=True)
                  .size()
                  .rename('issues_created')
        )
        # Left/right outer join to cover all active days
        by_day = pd.concat([commits_by_day, issues_by_day], axis=1).fillna(0)
        # If you want to inspect later, you can return or log `by_day`; we avoid printing to keep output minimal.




def main():
    parser = argparse.ArgumentParser(prog="repo_miner", description="Fetch GitHub commits/issues and export or summarize")
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

    # summarize  <-- add this block
    c3 = subparsers.add_parser("summarize", help="Summarize commits and issues")
    c3.add_argument("--commits", required=True, help="Path to commits CSV file")
    c3.add_argument("--issues",  required=True, help="Path to issues CSV file")

    args = parser.parse_args()

    if args.command == "fetch-commits":
        df = fetch_commits(args.repo, args.max_commits)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} commits to {args.out}")

    elif args.command == "fetch-issues":
        df = fetch_issues(args.repo, args.state, args.max_issues)
        df.to_csv(args.out, index=False)
        print(f"Saved {len(df)} issues to {args.out}")

    elif args.command == "summarize":  # <-- add this handler
        commits_df = pd.read_csv(args.commits)
        issues_df  = pd.read_csv(args.issues)
        merge_and_summarize(commits_df, issues_df)
