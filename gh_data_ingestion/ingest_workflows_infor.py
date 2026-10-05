"""
Download Actions workflow records for repos in a CSV.

Join key to the gh-aw corpus: ``repo`` + ``yml_filename`` (basename of ``path``,
e.g. ``issue-enhancer.lock.yml``). ``state`` is the workflow status
(``active``, ``disabled_manually``, ``disabled_inactivity``, ``disabled_fork``,
``deleted``), not a run conclusion.

Auth: ``GH_AW_INGESTION`` / ``GITHUB_TOKEN`` / ``GH_TOKEN`` in
``gh_data_ingestion/.env``.

Examples:
  python -m gh_data_ingestion.ingest_workflows_infor --limit 3
  python -m gh_data_ingestion.ingest_workflows_infor
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

API_VERSION = "2022-11-28"
BASE = "https://api.github.com"
PKG_DIR = Path(__file__).resolve().parent
REPO_ROOT = PKG_DIR.parent
DEFAULT_REPOS = REPO_ROOT / "notebooks" / "unique_repo.csv"
DEFAULT_OUT = REPO_ROOT / "data" / "workflow_info"
TOKEN_ENV_KEYS = ("GH_AW_INGESTION", "GITHUB_TOKEN", "GH_TOKEN")

CSV_COLUMNS = (
    "repo",
    "yml_filename",
    "state",
    "path",
    "name",
    "workflow_id",
    "created_at",
    "updated_at",
    "html_url",
    "badge_url",
    "url",
    "node_id",
    "deleted_at",
)


def load_repos(path: Path) -> list[str]:
    """Read ``owner/name`` values from a CSV with a ``repo`` column."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or "repo" not in reader.fieldnames:
            raise SystemExit(f"{path} needs a 'repo' column")
        repos: list[str] = []
        seen: set[str] = set()
        for row in reader:
            repo = (row.get("repo") or "").strip()
            if repo and repo not in seen:
                seen.add(repo)
                repos.append(repo)
    return repos


def _load_env() -> None:
    load_dotenv(PKG_DIR / ".env")
    load_dotenv(REPO_ROOT / ".env")


def _token() -> str:
    _load_env()
    for key in TOKEN_ENV_KEYS:
        token = os.environ.get(key)
        if token:
            return token.strip()
    keys = ", ".join(TOKEN_ENV_KEYS)
    sys.exit(f"Set one of [{keys}] in gh_data_ingestion/.env (or the environment).")


def _session(token: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "msr-2027-workflow-info",
        }
    )
    return session


def _request(session: requests.Session, url: str, params: dict[str, Any]) -> requests.Response:
    for attempt in range(8):
        resp = session.get(url, params=params, timeout=120)
        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 1
            print(f"  rate limited; sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if resp.status_code in (429, 502, 503, 504):
            wait = min(2**attempt, 120)
            print(f"  HTTP {resp.status_code}; retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        return resp
    return resp


def list_workflows(session: requests.Session, owner: str, repo: str) -> list[dict[str, Any]]:
    """Every Actions workflow in the repo, including disabled ones."""
    url = f"{BASE}/repos/{owner}/{repo}/actions/workflows"
    page = 1
    workflows: list[dict[str, Any]] = []
    while True:
        resp = _request(session, url, {"per_page": 100, "page": page})
        if resp.status_code >= 400:
            raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:300]}")
        batch = resp.json().get("workflows") or []
        workflows.extend(batch)
        if len(batch) < 100:
            return workflows
        page += 1


def yml_filename(path: str | None) -> str:
    if not path:
        return ""
    return path.rstrip("/").rsplit("/", 1)[-1]


def workflow_row(repo: str, workflow: dict[str, Any]) -> dict[str, Any]:
    path = workflow.get("path") or ""
    return {
        "repo": repo,
        "yml_filename": yml_filename(path),
        "state": workflow.get("state") or "",
        "path": path,
        "name": workflow.get("name") or "",
        "workflow_id": workflow.get("id") or "",
        "created_at": workflow.get("created_at") or "",
        "updated_at": workflow.get("updated_at") or "",
        "html_url": workflow.get("html_url") or "",
        "badge_url": workflow.get("badge_url") or "",
        "url": workflow.get("url") or "",
        "node_id": workflow.get("node_id") or "",
        "deleted_at": workflow.get("deleted_at") or "",
    }


def repo_path(out_dir: Path, repo: str) -> Path:
    owner, _, name = repo.partition("/")
    return out_dir / "repos" / f"{owner}__{name}.json"


def save_repo(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def fetch_repo(session: requests.Session, repo: str) -> dict[str, Any]:
    owner, _, name = repo.partition("/")
    if not owner or not name or "/" in name:
        raise RuntimeError(f"repo must be owner/name, got {repo!r}")
    workflows = list_workflows(session, owner, name)
    return {
        "repo": repo,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "error": None,
        "workflow_count": len(workflows),
        "workflows": workflows,
    }


def write_tables(out_dir: Path) -> tuple[int, int]:
    """Rebuild workflows.csv and repos.csv from saved repo JSON files."""
    workflow_rows: list[dict[str, Any]] = []
    repo_rows: list[dict[str, str]] = []
    for path in sorted((out_dir / "repos").glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        repo = payload.get("repo") or ""
        repo_rows.append(
            {
                "repo": repo,
                "workflow_count": str(payload.get("workflow_count") or 0),
                "error": payload.get("error") or "",
                "fetched_at": payload.get("fetched_at") or "",
            }
        )
        for workflow in payload.get("workflows") or []:
            workflow_rows.append(workflow_row(repo, workflow))

    _write_csv(out_dir / "workflows.csv", CSV_COLUMNS, workflow_rows)
    _write_csv(
        out_dir / "repos.csv",
        ("repo", "workflow_count", "error", "fetched_at"),
        repo_rows,
    )
    return len(repo_rows), len(workflow_rows)


def _write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def ingest(repos: list[str], out_dir: Path, session: requests.Session) -> None:
    for index, repo in enumerate(repos, start=1):
        dest = repo_path(out_dir, repo)
        if dest.exists():
            print(f"[{index}/{len(repos)}] skip {repo}", file=sys.stderr)
            continue
        print(f"[{index}/{len(repos)}] {repo}", file=sys.stderr)
        try:
            payload = fetch_repo(session, repo)
        except RuntimeError as exc:
            payload = {
                "repo": repo,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "error": str(exc),
                "workflow_count": 0,
                "workflows": [],
            }
            print(f"  {exc}", file=sys.stderr)
        save_repo(dest, payload)
    repo_count, workflow_count = write_tables(out_dir)
    print(
        f"wrote {out_dir / 'workflows.csv'} ({workflow_count} workflows, {repo_count} repos)",
        file=sys.stderr,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repos", type=Path, default=DEFAULT_REPOS)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--limit", type=int, default=None, help="Only the first N repos")
    args = parser.parse_args()

    repos = load_repos(args.repos)
    if args.limit is not None:
        repos = repos[: args.limit]
    ingest(repos, args.out, _session(_token()))


if __name__ == "__main__":
    main()
