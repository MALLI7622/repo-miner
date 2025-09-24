# test_repo.py (flat layout)
import os
import pandas as pd
import pytest
from datetime import datetime, timedelta

from repo_miner import fetch_commits         # direct import of function
import repo_miner as rm                      # module alias used by fixture


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

class DummyRepo:
    def __init__(self, commits):
        self._commits = commits
    def get_commits(self):
        return self._commits

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


def test_fetch_commits_basic(patch_env_and_github):
    gh_instance = patch_env_and_github
    now = datetime.now()
    commits = [
        DummyCommit("sha1", "Alice", "a@example.com", now, "Initial commit\nDetails"),
        DummyCommit("sha2", "Bob", "b@example.com", now - timedelta(days=1), "Bug fix"),
    ]
    gh_instance._repo = DummyRepo(commits)

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
    gh_instance._repo = DummyRepo(commits)

    df = fetch_commits("any/repo", max_commits=3)
    assert len(df) == 3
    assert list(df["sha"]) == ["sha0", "sha1", "sha2"]


def test_fetch_commits_empty(patch_env_and_github):
    gh_instance = patch_env_and_github
    gh_instance._repo = DummyRepo([])

    df = fetch_commits("any/repo")
    assert df.empty
    assert list(df.columns) == ["sha", "author", "email", "date", "message"]
