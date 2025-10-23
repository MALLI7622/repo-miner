# test_repo.py (flat layout)
import os
import pandas as pd
import pytest
from datetime import datetime, timedelta

from repo_miner import fetch_commits, fetch_issues        # direct import of functions
import repo_miner as rm                                   # module alias used by fixture


# --- Dummy GitHub API objects to avoid real network calls ---
class DummyAuthor:
    def __init__(self, name, email, date):
        self.name = name
        self.email = email
        self.date = date

class DummyCommitCommit:
    def __init__(self, author, message):
        self.author = author
        self.message = message

class DummyCommit:
    def __init__(self, sha, author, email, date, message):
        self.sha = sha
        self.commit = DummyCommitCommit(DummyAuthor(author, email, date), message)

# Users and Issues
class DummyUser:
    def __init__(self, login):
        self.login = login

class DummyIssue:
    def __init__(self, iid, number, title, user_login, state, created_at, closed_at, comments, is_pr=False):
        self.id = iid
        self.number = number
        self.title = title
        self.user = DummyUser(user_login)
        self.state = state
        self.created_at = created_at
        self.closed_at = closed_at
        self.comments = comments
        # PyGithub exposes .pull_request for PRs; we emulate that
        if is_pr:
            self.pull_request = object()

class DummyRepo:
    def __init__(self, commits=None, issues=None):
        self._commits = commits or []
        self._issues = issues or []

    def get_commits(self):
        return self._commits

    def get_issues(self, state="all"):
        if state == "all":
            return self._issues
        return [i for i in self._issues if i.state == state]

class DummyGithub:
    def __init__(self, token):
        assert token == "fake-token"
        self._repo = None
    def get_repo(self, repo_name):
        return self._repo


@pytest.fixture(autouse=True)
def patch_env_and_github(monkeypatch):
    """
    Automatically patch environment and the Github class so tests never
    touch the real network. Each test can set gh_instance._repo.
    """
    # Set fake token
    monkeypatch.setenv("GITHUB_TOKEN", "fake-token")

    # Provide a shared DummyGithub instance
    gh_instance = DummyGithub("fake-token")

    # Patch the Github class inside repo_miner
    monkeypatch.setattr(rm, "Github", lambda token: gh_instance)

    yield gh_instance


# ---------------- Commits ----------------

def test_fetch_commits_basic(patch_env_and_github):
    gh_instance = patch_env_and_github
    now = datetime.now()
    commits = [
        DummyCommit("sha1", "Alice", "a@example.com", now, "Initial commit\nDetails"),
        DummyCommit("sha2", "Bob", "b@example.com", now - timedelta(days=1), "Bug fix"),
    ]
    gh_instance._repo = DummyRepo(commits=commits)

    df = fetch_commits("any/repo")
    assert list(df.columns) == ["sha", "author", "email", "date", "message"]
    assert len(df) == 2
    assert df.iloc[0]["message"] == "Initial commit"
    assert "T" in df.iloc[0]["date"]  # ISO-8601 format


def test_fetch_commits_limit(patch_env_and_github):
    gh_instance = patch_env_and_github
    now = datetime.now()
    commits = [
        DummyCommit(f"sha{i}", f"User{i}", f"u{i}@ex.com", now, f"msg {i}") for i in range(5)
    ]
    gh_instance._repo = DummyRepo(commits=commits)

    df = fetch_commits("any/repo", max_commits=3)
    assert len(df) == 3
    assert list(df["sha"]) == ["sha0", "sha1", "sha2"]


def test_fetch_commits_empty(patch_env_and_github):
    gh_instance = patch_env_and_github
    gh_instance._repo = DummyRepo(commits=[])

    df = fetch_commits("any/repo")
    assert df.empty
    assert list(df.columns) == ["sha", "author", "email", "date", "message"]


# ---------------- Issues ----------------

def test_fetch_issues_excludes_prs(patch_env_and_github):
    gh_instance = patch_env_and_github
    now = datetime.now()
    issues = [
        DummyIssue(1, 101, "Issue A", "alice", "open", now, None, 0, is_pr=False),
        DummyIssue(2, 102, "PR should be skipped", "bob", "open", now, None, 1, is_pr=True),
        DummyIssue(3, 103, "Issue B", "carol", "closed", now - timedelta(days=2), now, 2, is_pr=False),
    ]
    gh_instance._repo = DummyRepo(commits=[], issues=issues)

    df = fetch_issues("any/repo", state="all")
    assert set(["id", "number", "title", "user", "state", "created_at", "closed_at", "comments", "open_duration_days"]) == set(df.columns)
    # Only 2 real issues, PR excluded
    assert len(df) == 2
    assert not (df["title"] == "PR should be skipped").any()


def test_fetch_issues_dates_and_duration(patch_env_and_github):
    gh_instance = patch_env_and_github
    created = datetime(2025, 9, 25, 15, 0, 0)
    closed = datetime(2025, 9, 28, 10, 0, 0)  # 2 days + 19 hours -> 2 days after flooring
    issues = [DummyIssue(10, 110, "Closed bug", "dave", "closed", created, closed, 5)]
    gh_instance._repo = DummyRepo(issues=issues)

    df = fetch_issues("any/repo", state="all")
    assert len(df) == 1
    row = df.iloc[0]
    # ISO-8601 check
    assert "T" in row["created_at"]
    assert "T" in row["closed_at"]
    # Duration in days (floor)
    assert row["open_duration_days"] == 2


def test_fetch_issues_state_filter_open_only(patch_env_and_github):
    gh_instance = patch_env_and_github
    now = datetime.now()
    issues = [
        DummyIssue(1, 101, "Open 1", "eve", "open", now, None, 0),
        DummyIssue(2, 102, "Closed 1", "frank", "closed", now - timedelta(days=1), now, 1),
        DummyIssue(3, 103, "Open 2", "gina", "open", now, None, 2),
    ]
    gh_instance._repo = DummyRepo(issues=issues)

    df = fetch_issues("any/repo", state="open")
    assert len(df) == 2
    assert set(df["title"]) == {"Open 1", "Open 2"}
