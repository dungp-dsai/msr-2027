"""
Extract GitHub Actions workflow-run data (metadata, jobs, logs, artifacts).

What we pull per run (GitHub REST API):
  - Run metadata          GET /repos/{o}/{r}/actions/runs/{run_id}
  - Billable usage        GET /repos/{o}/{r}/actions/runs/{run_id}/timing
  - Jobs + steps          GET /repos/{o}/{r}/actions/runs/{run_id}/jobs
  - Console logs (zip)    GET /repos/{o}/{r}/actions/runs/{run_id}/logs
  - Per-job logs (txt)    GET /repos/{o}/{r}/actions/jobs/{job_id}/logs
  - Artifacts list+zips   GET /repos/{o}/{r}/actions/runs/{run_id}/artifacts
  - Attempt metadata      GET .../runs/{run_id}/attempts/{n}  (if run_attempt > 1)
  - Attempt logs          GET .../runs/{run_id}/attempts/{n}/logs
  - Approval history      GET .../runs/{run_id}/approvals

Auth: loads `gh_data_ingestion/.env` (or a repo-root `.env`). Tokens are read
from GH_AW_INGESTION, GH_AW_INGESTION_2, GH_AW_INGESTION_3, ...,
GH_AW_INGESTION_TOKENS (comma-separated), GITHUB_TOKEN, or GH_TOKEN.
When one token hits a rate limit, the next token is used. If every token is
limited, the script sleeps until the earliest reset. Needs `actions:read`
(+ `repo` for private repos).

Resume: a workflow is appended to ``<out>/.progress/done_workflows.txt`` only
after all of its runs are saved. Re-run the same command to continue. Runs
that already have ``summary.json`` are skipped too. ``--force`` ignores both.

The Actions page for a lock file is:

  https://github.com/{owner}/{repo}/actions/workflows/{lock.yml}

Runs are listed from the matching API route, then each run is extracted.

Examples:
  python -m gh_data_ingestion.ingest_workflows_details \\
    0GiS0/github-agentic-workflows-demos issue-enhancer.lock.yml

  python -m gh_data_ingestion.ingest_workflows_details --gh-aw
  python -m gh_data_ingestion.ingest_workflows_details --non-gh-aw

  # metadata, jobs, timing, artifacts — no console or job logs
  python -m gh_data_ingestion.ingest_workflows_details --gh-aw --skip-logs
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from tqdm import tqdm

API_VERSION = "2022-11-28"
BASE = "https://api.github.com"
PKG_DIR = Path(__file__).resolve().parent
REPO_ROOT = PKG_DIR.parent
GH_AW_CSV = REPO_ROOT / "notebooks" / "gh_aw_workflows_df.csv"
NON_GH_AW_CSV = REPO_ROOT / "notebooks" / "non_gh_aw_workflows_df.csv"
GH_AW_OUT = Path("data/workflow_details/gh_aw_workflows")
NON_GH_AW_OUT = Path("data/workflow_details/non_gh_aw_workflows")

# Common gh-aw artifact names (download all by default; filter with --artifact-names)
GH_AW_ARTIFACT_NAMES = {
    "agent",
    "activation",
    "firewall",
    "firewall-audit-logs",
    "detection",
    "experiment",
    "usage",
    "evals",
}


def _log(message: str) -> None:
    tqdm.write(str(message))


def _load_env() -> None:
    """Load .env next to this package, then repo-root .env (without overriding)."""
    load_dotenv(PKG_DIR / ".env")
    load_dotenv(PKG_DIR.parent / ".env")


def _add_token(found: list[str], seen: set[str], raw: str | None) -> None:
    if not raw:
        return
    for part in raw.split(","):
        token = part.strip()
        if token and token not in seen:
            seen.add(token)
            found.append(token)


def _tokens() -> list[str]:
    _load_env()
    found: list[str] = []
    seen: set[str] = set()
    _add_token(found, seen, os.environ.get("GH_AW_INGESTION_TOKENS"))
    _add_token(found, seen, os.environ.get("GH_AW_INGESTION"))
    numbered: list[tuple[int, str]] = []
    for key, value in os.environ.items():
        suffix = key.removeprefix("GH_AW_INGESTION_")
        if key.startswith("GH_AW_INGESTION_") and suffix.isdigit():
            numbered.append((int(suffix), value))
    for _, value in sorted(numbered):
        _add_token(found, seen, value)
    for key in ("GITHUB_TOKEN", "GH_TOKEN"):
        _add_token(found, seen, os.environ.get(key))
    if not found:
        sys.exit(
            "Set GH_AW_INGESTION and GH_AW_INGESTION_2 "
            "(or GH_AW_INGESTION_TOKENS) in gh_data_ingestion/.env"
        )
    return found


class TokenPool:
    """Share one session and move to the next token when the current one is limited."""

    def __init__(self, tokens: list[str], pool_size: int) -> None:
        self.tokens = tokens
        self.index = 0
        self.cooldown: dict[int, float] = {}
        self._lock = threading.Lock()
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=pool_size, pool_maxsize=pool_size)
        self.session.mount("https://", adapter)
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "msr-2027-workflow-extractor",
            }
        )
        self.session.token_pool = self

    def authorization(self) -> tuple[int, str]:
        with self._lock:
            return self.index, f"Bearer {self.tokens[self.index]}"

    def on_rate_limit(self, token_index: int, reset_at: float | None) -> float:
        """Cool down ``token_index``. Return seconds to sleep, or 0 if another token is ready."""
        with self._lock:
            now = time.time()
            self.cooldown[token_index] = reset_at if reset_at and reset_at > now else now + 60
            if self.index == token_index:
                for step in range(1, len(self.tokens) + 1):
                    nxt = (token_index + step) % len(self.tokens)
                    if self.cooldown.get(nxt, 0) <= now:
                        self.index = nxt
                        _log(f"rate limited; switched to token {nxt + 1}/{len(self.tokens)}")
                        return 0.0
            ready = [self.cooldown.get(i, 0) for i in range(len(self.tokens))]
            soonest = min(ready)
            if soonest <= now:
                return 0.0
            return max(soonest - now, 1)


def _is_rate_limited(resp: requests.Response) -> bool:
    if resp.status_code not in (403, 429):
        return False
    if resp.headers.get("X-RateLimit-Remaining") == "0":
        return True
    body = resp.text.lower()
    return "rate limit" in body


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    allow_redirects: bool = True,
    stream: bool = False,
    max_retries: int = 8,
) -> requests.Response:
    """GET with rate-limit rotation. Other tokens are tried before sleeping."""
    pool: TokenPool = session.token_pool
    attempt = 0
    resp: requests.Response | None = None
    while attempt < max_retries:
        token_index, auth = pool.authorization()
        resp = session.request(
            method,
            url,
            params=params,
            headers={"Authorization": auth},
            allow_redirects=allow_redirects,
            stream=stream,
            timeout=120,
        )
        if _is_rate_limited(resp):
            reset_raw = resp.headers.get("X-RateLimit-Reset") or resp.headers.get("Retry-After")
            reset_at = None
            if reset_raw and reset_raw.isdigit():
                reset_at = float(reset_raw)
                if reset_at < 10_000:
                    reset_at = time.time() + reset_at
            wait = pool.on_rate_limit(token_index, reset_at)
            resp.close()
            if wait:
                _log(f"all tokens rate limited; sleeping {wait:.0f}s")
                time.sleep(wait)
            continue
        if resp.status_code in (502, 503, 504):
            wait = min(2**attempt, 60)
            _log(f"  HTTP {resp.status_code}; retry in {wait}s")
            resp.close()
            time.sleep(wait)
            attempt += 1
            continue
        return resp
    assert resp is not None
    return resp


def _api_json(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
) -> Any:
    url = path if path.startswith("http") else f"{BASE}{path}"
    resp = _request(session, "GET", url, params=params)
    if resp.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")
    if not resp.content:
        return None
    return resp.json()


def _paginate(
    session: requests.Session,
    path: str,
    *,
    list_key: str,
    params: dict[str, Any] | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    params = dict(params or {})
    params.setdefault("per_page", 100)
    page = 1
    out: list[dict[str, Any]] = []
    while True:
        params["page"] = page
        data = _api_json(session, path, params=params)
        if isinstance(data, list):
            batch = data
        else:
            batch = data.get(list_key, [])
        out.extend(batch)
        if limit is not None and len(out) >= limit:
            return out[:limit]
        if len(batch) < params["per_page"]:
            break
        page += 1
    return out


def _write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, default=str) + "\n", encoding="utf-8")


def _download_binary(
    session: requests.Session,
    url: str,
    dest: Path,
) -> bool:
    """Follow redirects and write body to dest. Returns False on 404/410."""
    resp = _request(session, "GET", url, allow_redirects=True, stream=True)
    if resp.status_code in (404, 410):
        resp.close()
        return False
    if resp.status_code >= 400:
        resp.close()
        raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            if chunk:
                f.write(chunk)
    return True


def _unzip(zip_path: Path, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)


def _unzip_bytes(content: bytes, dest_dir: Path) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(content)) as zf:
        zf.extractall(dest_dir)


def parse_repo(repo: str) -> tuple[str, str]:
    repo = repo.strip().removeprefix("https://github.com/").removesuffix(".git")
    if repo.count("/") != 1:
        raise argparse.ArgumentTypeError("repo must be owner/name")
    owner, name = repo.split("/")
    return owner, name


def parse_lock_yml(lock_yml: str) -> str:
    """File name of a compiled workflow, e.g. ``issue-enhancer.lock.yml``."""
    name = lock_yml.strip().rstrip("/").rsplit("/", 1)[-1]
    if not name.endswith(".lock.yml"):
        raise argparse.ArgumentTypeError("lock file must end with .lock.yml")
    return name


def load_workflow_rows(path: Path) -> list[tuple[str, str]]:
    """Read ``repo,yml_filename`` rows. ``yml_filename`` is the Actions file name."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames or not {"repo", "yml_filename"} <= set(reader.fieldnames):
            raise SystemExit(f"{path} needs repo and yml_filename columns")
        rows: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for row in reader:
            repo = (row.get("repo") or "").strip()
            filename = (row.get("yml_filename") or "").strip().rstrip("/").rsplit("/", 1)[-1]
            key = (repo, filename)
            if not repo or not filename or key in seen:
                continue
            seen.add(key)
            rows.append(key)
    return rows


def load_gh_aw_workflows(path: Path = GH_AW_CSV) -> list[tuple[str, str]]:
    return load_workflow_rows(path)


def load_non_gh_aw_workflows(path: Path = NON_GH_AW_CSV) -> list[tuple[str, str]]:
    return load_workflow_rows(path)


def progress_path(out_dir: Path) -> Path:
    return out_dir / ".progress" / "done_workflows.txt"


def load_done(out_dir: Path) -> set[tuple[str, str]]:
    path = progress_path(out_dir)
    if not path.is_file():
        return set()
    done: set[tuple[str, str]] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        repo, sep, filename = line.partition("\t")
        if sep and repo and filename:
            done.add((repo, filename))
    return done


def mark_done(out_dir: Path, repo: str, workflow_file: str) -> None:
    path = progress_path(out_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"{repo}\t{workflow_file}\n")
        handle.flush()


def workflow_actions_url(owner: str, repo: str, lock_yml: str) -> str:
    return f"https://github.com/{owner}/{repo}/actions/workflows/{lock_yml}"


def list_runs(
    session: requests.Session,
    owner: str,
    repo: str,
    *,
    limit: int | None,
    status: str | None,
    event: str | None,
    branch: str | None,
    workflow: str | None,
    created: str | None,
) -> list[dict[str, Any]]:
    params: dict[str, Any] = {}
    if status:
        params["status"] = status
    if event:
        params["event"] = event
    if branch:
        params["branch"] = branch
    if created:
        params["created"] = created

    if workflow:
        # workflow can be numeric id or filename (e.g. agent.yml)
        path = f"/repos/{owner}/{repo}/actions/workflows/{workflow}/runs"
    else:
        path = f"/repos/{owner}/{repo}/actions/runs"

    return _paginate(session, path, list_key="workflow_runs", params=params, limit=limit)


def logs_still_kept(run: dict[str, Any], retention_days: int) -> bool:
    """GitHub deletes logs and artifacts after the repo retention window (often 90 days)."""
    if retention_days < 0:
        return True
    raw = run.get("updated_at") or run.get("created_at")
    if not isinstance(raw, str):
        return True
    try:
        when = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return True
    return datetime.now(timezone.utc) - when <= timedelta(days=retention_days)


def extract_run(
    session: requests.Session,
    owner: str,
    repo: str,
    run_id: int,
    out_dir: Path,
    *,
    run: dict[str, Any] | None = None,
    download_logs: bool = True,
    download_job_logs: bool = True,
    download_artifacts: bool = True,
    artifact_names: set[str] | None = None,
    keep_zips: bool = False,
    log_retention_days: int = -1,
) -> Path:
    """
    Extract one workflow run into out_dir / {owner}__{repo}__{run_id}/.

    Layout:
      run.json
      timing.json
      jobs.json
      approvals.json          (if any)
      attempts/{n}/run.json   (older attempts when run_attempt > 1)
      console_logs/           (unzipped run logs)
      job_logs/{job_id}.txt
      artifacts/{name}/       (unzipped)
      raw/                    (optional zip archives if --keep-zips)
    """
    run_root = out_dir / f"{owner}__{repo}__{run_id}"
    run_root.mkdir(parents=True, exist_ok=True)
    _log(f"[{owner}/{repo}#{run_id}] -> {run_root}")

    # The list-runs payload is the same run record, so skip a second GET.
    if run is None:
        run = _api_json(session, f"/repos/{owner}/{repo}/actions/runs/{run_id}")
    _write_json(run_root / "run.json", run)

    logs_kept = logs_still_kept(run, log_retention_days)
    fetch_logs = download_logs and logs_kept
    fetch_job_logs = download_job_logs and logs_kept
    if (download_logs or download_job_logs) and not logs_kept:
        _log(f"  skip logs (older than {log_retention_days} days)")

    try:
        timing = _api_json(session, f"/repos/{owner}/{repo}/actions/runs/{run_id}/timing")
        _write_json(run_root / "timing.json", timing)
    except RuntimeError as e:
        _log(f"  timing unavailable: {e}")

    jobs = _paginate(
        session,
        f"/repos/{owner}/{repo}/actions/runs/{run_id}/jobs",
        list_key="jobs",
        params={"filter": "all"},
    )
    _write_json(run_root / "jobs.json", {"total_count": len(jobs), "jobs": jobs})

    try:
        approvals = _api_json(session, f"/repos/{owner}/{repo}/actions/runs/{run_id}/approvals")
        if approvals:
            _write_json(run_root / "approvals.json", approvals)
    except RuntimeError:
        pass

    # --- prior attempts (metadata + logs) ---
    attempt_n = int(run.get("run_attempt") or 1)
    if attempt_n > 1:
        for n in range(1, attempt_n):
            attempt_dir = run_root / "attempts" / str(n)
            try:
                attempt = _api_json(
                    session,
                    f"/repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{n}",
                )
                _write_json(attempt_dir / "run.json", attempt)
            except RuntimeError as e:
                _log(f"  attempt {n} metadata: {e}")
                continue
            if fetch_logs:
                zip_path = attempt_dir / "console_logs.zip"
                ok = _download_binary(
                    session,
                    f"{BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/attempts/{n}/logs",
                    zip_path,
                )
                if ok:
                    _unzip(zip_path, attempt_dir / "console_logs")
                    if not keep_zips:
                        zip_path.unlink(missing_ok=True)
                else:
                    _log(f"  attempt {n} logs missing (expired/deleted)")

    # --- console logs (latest attempt) ---
    if fetch_logs:
        zip_path = run_root / "raw" / "console_logs.zip"
        ok = _download_binary(
            session,
            f"{BASE}/repos/{owner}/{repo}/actions/runs/{run_id}/logs",
            zip_path,
        )
        if ok:
            _unzip(zip_path, run_root / "console_logs")
            if not keep_zips:
                zip_path.unlink(missing_ok=True)
                # clean empty raw/ if we created it only for this
                raw = run_root / "raw"
                if raw.exists() and not any(raw.iterdir()):
                    raw.rmdir()
            _log(f"  console logs: {run_root / 'console_logs'}")
        else:
            _log("  console logs missing (expired/deleted or still running)")
            fetch_job_logs = False

    # --- per-job plain-text logs ---
    if fetch_job_logs and jobs:
        job_logs_dir = run_root / "job_logs"
        for job in jobs:
            job_id = job["id"]
            name = job.get("name", job_id)
            dest = job_logs_dir / f"{job_id}_{_safe_name(name)}.txt"
            ok = _download_binary(
                session,
                f"{BASE}/repos/{owner}/{repo}/actions/jobs/{job_id}/logs",
                dest,
            )
            if not ok:
                _log(f"  job log missing: {name} ({job_id})")
        _log(f"  job logs: {job_logs_dir}")

    # --- artifacts ---
    if download_artifacts:
        artifacts = _paginate(
            session,
            f"/repos/{owner}/{repo}/actions/runs/{run_id}/artifacts",
            list_key="artifacts",
        )
        _write_json(
            run_root / "artifacts.json",
            {"total_count": len(artifacts), "artifacts": artifacts},
        )
        for art in artifacts:
            name = art.get("name") or str(art["id"])
            if artifact_names is not None and name not in artifact_names:
                continue
            if art.get("expired"):
                _log(f"  artifact expired: {name}")
                continue
            dl = art.get("archive_download_url")
            if not dl:
                continue
            zip_path = run_root / "raw" / f"artifact_{art['id']}_{_safe_name(name)}.zip"
            ok = _download_binary(session, dl, zip_path)
            if not ok:
                _log(f"  artifact download failed: {name}")
                continue
            _unzip(zip_path, run_root / "artifacts" / _safe_name(name))
            if not keep_zips:
                zip_path.unlink(missing_ok=True)
            _log(f"  artifact: {name}")
        raw = run_root / "raw"
        if raw.exists() and not any(raw.iterdir()):
            raw.rmdir()

    # --- index summary ---
    summary = {
        "owner": owner,
        "repo": repo,
        "run_id": run_id,
        "name": run.get("name"),
        "display_title": run.get("display_title"),
        "status": run.get("status"),
        "conclusion": run.get("conclusion"),
        "event": run.get("event"),
        "head_branch": run.get("head_branch"),
        "head_sha": run.get("head_sha"),
        "path": run.get("path"),
        "html_url": run.get("html_url"),
        "created_at": run.get("created_at"),
        "updated_at": run.get("updated_at"),
        "run_attempt": run.get("run_attempt"),
        "job_count": len(jobs),
        "logs_skipped": (download_logs or download_job_logs) and not logs_kept,
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "output_dir": str(run_root),
    }
    _write_json(run_root / "summary.json", summary)
    return run_root


def _safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in name)[:120]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Extract every run of one gh-aw lock workflow (metadata, jobs, logs, artifacts).",
    )
    p.add_argument("repo", nargs="?", type=parse_repo, help="owner/repo")
    p.add_argument(
        "lock_yml",
        nargs="?",
        type=parse_lock_yml,
        help="Lock file name, e.g. issue-enhancer.lock.yml",
    )
    group = p.add_mutually_exclusive_group()
    group.add_argument(
        "--gh-aw",
        action="store_true",
        help=f"Ingest {GH_AW_CSV.name} into {GH_AW_OUT}",
    )
    group.add_argument(
        "--non-gh-aw",
        action="store_true",
        help=f"Ingest {NON_GH_AW_CSV.name} into {NON_GH_AW_OUT}",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max runs to extract (default: all runs of this lock file).",
    )
    p.add_argument("--status", help="Filter: queued|in_progress|completed|...")
    p.add_argument("--event", help="Filter by trigger event (push, pull_request, ...)")
    p.add_argument("--branch", help="Filter by branch")
    p.add_argument("--created", help="Created date filter, e.g. '>=2025-01-01'")
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Output directory (default: data/workflow_details, or the gh-aw / non-gh-aw folder).",
    )
    p.add_argument(
        "--skip-logs",
        action="store_true",
        help="Skip console logs and per-job logs. Metadata, jobs, timing, and artifacts are still saved.",
    )
    p.add_argument("--no-logs", action="store_true", help="Skip the console log zip only")
    p.add_argument("--no-job-logs", action="store_true", help="Skip per-job plain-text logs only")
    p.add_argument("--no-artifacts", action="store_true", help="Skip artifact downloads")
    p.add_argument(
        "--gh-aw-artifacts-only",
        action="store_true",
        help=f"Only download known gh-aw artifacts: {sorted(GH_AW_ARTIFACT_NAMES)}",
    )
    p.add_argument(
        "--artifact-names",
        nargs="+",
        help="Only download artifacts with these exact names",
    )
    p.add_argument("--keep-zips", action="store_true", help="Keep raw zip archives under raw/")
    p.add_argument(
        "--workers",
        type=int,
        default=8,
        help="How many runs to download at once (default 8).",
    )
    p.add_argument(
        "--log-retention-days",
        type=int,
        default=-1,
        help="Always download logs when GitHub still has them. Set a positive number of days to skip older runs.",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Re-download a run even if summary.json is already there.",
    )
    p.add_argument(
        "--list-only",
        action="store_true",
        help="Only list matching runs (write runs_index.json); do not extract",
    )
    return p


def ingest_workflow(session, owner: str, repo: str, workflow_file: str, out_dir: Path, args) -> int:
    workers = max(1, args.workers)
    out_dir.mkdir(parents=True, exist_ok=True)
    page_url = workflow_actions_url(owner, repo, workflow_file)
    _log(page_url)
    try:
        runs_meta = list_runs(
            session,
            owner,
            repo,
            limit=args.limit,
            status=args.status,
            event=args.event,
            branch=args.branch,
            workflow=workflow_file,
            created=args.created,
        )
    except RuntimeError as exc:
        _log(f"FAILED {owner}/{repo} {workflow_file}: {exc}")
        return 1

    lock_yml = workflow_file
    run_ids = [r["id"] for r in runs_meta]
    index_path = out_dir / f"{owner}__{repo}__{lock_yml}__runs_index.json"
    _write_json(
        index_path,
        {
            "repo": f"{owner}/{repo}",
            "lock_yml": lock_yml,
            "workflow_url": page_url,
            "run_count": len(runs_meta),
            "runs": runs_meta,
        },
    )
    _log(f"listed {len(run_ids)} runs -> {index_path}")

    if args.list_only:
        for r in runs_meta:
            _log(
                f"{r.get('id')}\t{r.get('conclusion') or r.get('status')}\t"
                f"{r.get('name')}\t{r.get('html_url')}"
            )
        return 0

    artifact_names: set[str] | None = None
    if args.artifact_names:
        artifact_names = set(args.artifact_names)
    elif args.gh_aw_artifacts_only:
        artifact_names = set(GH_AW_ARTIFACT_NAMES)

    pending: list[dict[str, Any]] = []
    extracted: list[str] = []
    run_bar = tqdm(
        total=len(runs_meta),
        desc="runs",
        unit="run",
        leave=False,
        file=sys.stderr,
    )
    for run in runs_meta:
        rid = int(run["id"])
        dest = out_dir / f"{owner}__{repo}__{rid}" / "summary.json"
        if dest.is_file() and not args.force:
            extracted.append(str(dest.parent))
            run_bar.update(1)
            continue
        pending.append(run)

    def _one(run: dict[str, Any]) -> Path:
        return extract_run(
            session,
            owner,
            repo,
            int(run["id"]),
            out_dir,
            run=run,
            download_logs=not (args.skip_logs or args.no_logs),
            download_job_logs=not (args.skip_logs or args.no_job_logs),
            download_artifacts=not args.no_artifacts,
            artifact_names=artifact_names,
            keep_zips=args.keep_zips,
            log_retention_days=args.log_retention_days,
        )

    if pending and workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_one, run): run["id"] for run in pending}
            for future in as_completed(futures):
                rid = futures[future]
                try:
                    extracted.append(str(future.result()))
                except Exception as exc:
                    _log(f"FAILED run {rid}: {exc}")
                run_bar.update(1)
    else:
        for run in pending:
            try:
                extracted.append(str(_one(run)))
            except Exception as exc:
                _log(f"FAILED run {run.get('id')}: {exc}")
            run_bar.update(1)
    run_bar.close()

    _log(f"done: {len(extracted)}/{len(run_ids)} runs extracted under {out_dir}")
    return 0 if len(extracted) == len(run_ids) else 1


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    workers = max(1, args.workers)
    tokens = _tokens()
    _log(f"using {len(tokens)} token(s)")
    session = TokenPool(tokens, pool_size=workers).session

    if args.gh_aw:
        rows = load_gh_aw_workflows()
        out_dir = args.out or GH_AW_OUT
    elif args.non_gh_aw:
        rows = load_non_gh_aw_workflows()
        out_dir = args.out or NON_GH_AW_OUT
    else:
        if args.repo is None or args.lock_yml is None:
            build_parser().error("repo and lock_yml are required, or pass --gh-aw / --non-gh-aw")
        owner, repo = args.repo
        rows = [(f"{owner}/{repo}", args.lock_yml)]
        out_dir = args.out or Path("data/workflow_details")

    if args.force:
        done: set[tuple[str, str]] = set()
    else:
        done = load_done(out_dir)
        remaining = [row for row in rows if row not in done]
        _log(f"resume: {len(rows) - len(remaining)} workflows already done, {len(remaining)} left")
        rows = remaining

    failed = 0
    workflow_bar = tqdm(rows, desc="workflows", unit="workflow", file=sys.stderr)
    for repo_name, workflow_file in workflow_bar:
        workflow_bar.set_postfix_str(f"{repo_name} {workflow_file}", refresh=False)
        try:
            owner, repo = parse_repo(repo_name)
        except argparse.ArgumentTypeError as exc:
            _log(f"FAILED {repo_name}: {exc}")
            failed += 1
            continue
        code = ingest_workflow(session, owner, repo, workflow_file, out_dir, args)
        if code == 0:
            mark_done(out_dir, repo_name, workflow_file)
        else:
            failed += 1
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
