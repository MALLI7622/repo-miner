# Repo Miner – RM1: Commit Fetcher

Implements `fetch_commits(repo_full_name: str, max_commits: int=None) -> pd.DataFrame` and a CLI.

## Install

```bash
python -m venv .venv && source .venv/bin/activate  # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## CLI

```bash
export GITHUB_TOKEN=...   # set your token; on Windows: set GITHUB_TOKEN=...
python -m src.repo_miner fetch-commits --repo octocat/Hello-World --max 10 --out commits.csv
```

## Tests

Offline tests use dummy objects (no network).

```bash
pytest -q
```

## Output Schema

CSV columns: `sha, author, email, date (ISO-8601), message (first line)`
