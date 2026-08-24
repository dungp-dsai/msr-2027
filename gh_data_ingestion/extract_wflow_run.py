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

Auth: loads `gh_data_ingestion/.env` (or a repo-root `.env`). Token is read
from GH_AW_INGESTION, GITHUB_TOKEN, or GH_TOKEN. Needs `actions:read`
(+ `repo` for private repos).

Examples:
  python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123
  python -m gh_data_ingestion.extract_wflow_run owner/repo --limit 20
  python -m gh_data_ingestion.extract_wflow_run owner/repo --workflow agent.yml --status completed
"""

from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

import requests
from dotenv import load_dotenv

API_VERSION = "2022-11-28"
BASE = "https://api.github.com"
PKG_DIR = Path(__file__).resolve().parent
TOKEN_ENV_KEYS = ("GH_AW_INGESTION", "GITHUB_TOKEN", "GH_TOKEN")

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


def _load_env() -> None:
    """Load .env next to this package, then repo-root .env (without overriding)."""
    load_dotenv(PKG_DIR / ".env")
    load_dotenv(PKG_DIR.parent / ".env")


def _token() -> str:
    _load_env()
    for key in TOKEN_ENV_KEYS:
        token = os.environ.get(key)
        if token:
            return token.strip()
    keys = ", ".join(TOKEN_ENV_KEYS)
    sys.exit(f"Set one of [{keys}] in gh_data_ingestion/.env (or the environment).")


def _session(token: str) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "msr-2027-workflow-extractor",
        }
    )
    return s


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    allow_redirects: bool = True,
    stream: bool = False,
    max_retries: int = 5,
) -> requests.Response:
    """GET/POST with basic rate-limit / secondary-limit backoff."""
    for attempt in range(max_retries):
        resp = session.request(
            method,
            url,
            params=params,
            allow_redirects=allow_redirects,
            stream=stream,
            timeout=120,
        )
        if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
            reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
            wait = max(reset - int(time.time()), 1) + 1
            print(f"  rate limited; sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        if resp.status_code in (429, 502, 503, 504):
            wait = 2**attempt
            print(f"  HTTP {resp.status_code}; retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
            continue
        return resp
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


def extract_run(
    session: requests.Session,
    owner: str,
    repo: str,
    run_id: int,
    out_dir: Path,
    *,
    download_logs: bool = True,
    download_job_logs: bool = True,
    download_artifacts: bool = True,
    artifact_names: set[str] | None = None,
    keep_zips: bool = False,
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
    print(f"[{owner}/{repo}#{run_id}] -> {run_root}")

    # --- metadata ---
    run = _api_json(session, f"/repos/{owner}/{repo}/actions/runs/{run_id}")
    _write_json(run_root / "run.json", run)

    try:
        timing = _api_json(session, f"/repos/{owner}/{repo}/actions/runs/{run_id}/timing")
        _write_json(run_root / "timing.json", timing)
    except RuntimeError as e:
        print(f"  timing unavailable: {e}", file=sys.stderr)

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
                print(f"  attempt {n} metadata: {e}", file=sys.stderr)
                continue
            if download_logs:
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
                    print(f"  attempt {n} logs missing (expired/deleted)", file=sys.stderr)

    # --- console logs (latest attempt) ---
    if download_logs:
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
            print(f"  console logs: {run_root / 'console_logs'}")
        else:
            print("  console logs missing (expired/deleted or still running)", file=sys.stderr)

    # --- per-job plain-text logs ---
    if download_job_logs and jobs:
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
                print(f"  job log missing: {name} ({job_id})", file=sys.stderr)
        print(f"  job logs: {job_logs_dir}")

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
                print(f"  artifact expired: {name}", file=sys.stderr)
                continue
            dl = art.get("archive_download_url")
            if not dl:
                continue
            zip_path = run_root / "raw" / f"artifact_{art['id']}_{_safe_name(name)}.zip"
            ok = _download_binary(session, dl, zip_path)
            if not ok:
                print(f"  artifact download failed: {name}", file=sys.stderr)
                continue
            _unzip(zip_path, run_root / "artifacts" / _safe_name(name))
            if not keep_zips:
                zip_path.unlink(missing_ok=True)
            print(f"  artifact: {name}")
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
        "extracted_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "output_dir": str(run_root),
    }
    _write_json(run_root / "summary.json", summary)
    return run_root


def _safe_name(name: str) -> str:
    return "".join(c if c.isalnum() or c in "-._" else "_" for c in name)[:120]


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Extract GitHub Actions workflow runs (metadata, jobs, logs, artifacts).",
    )
    p.add_argument("repo", type=parse_repo, help="owner/repo")
    p.add_argument(
        "--run-id",
        type=int,
        action="append",
        dest="run_ids",
        help="Specific run id(s). Repeatable. If omitted, list runs with filters.",
    )
    p.add_argument("--limit", type=int, default=10, help="Max runs to list when --run-id omitted (default 10).")
    p.add_argument("--status", help="Filter: queued|in_progress|completed|...")
    p.add_argument("--event", help="Filter by trigger event (push, pull_request, ...)")
    p.add_argument("--branch", help="Filter by branch")
    p.add_argument("--workflow", help="Workflow id or filename (e.g. agent.yml)")
    p.add_argument("--created", help="Created date filter, e.g. '>=2025-01-01'")
    p.add_argument(
        "--out",
        type=Path,
        default=Path("data/workflow_runs"),
        help="Output directory (default: data/workflow_runs)",
    )
    p.add_argument("--no-logs", action="store_true", help="Skip console log zip download")
    p.add_argument("--no-job-logs", action="store_true", help="Skip per-job plain-text logs")
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
        "--list-only",
        action="store_true",
        help="Only list matching runs (write runs_index.json); do not extract",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    owner, repo = args.repo
    session = _session(_token())
    out_dir: Path = args.out
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.run_ids:
        run_ids = args.run_ids
        runs_meta = [
            {"id": rid, "html_url": f"https://github.com/{owner}/{repo}/actions/runs/{rid}"}
            for rid in run_ids
        ]
    else:
        runs_meta = list_runs(
            session,
            owner,
            repo,
            limit=args.limit,
            status=args.status,
            event=args.event,
            branch=args.branch,
            workflow=args.workflow,
            created=args.created,
        )
        run_ids = [r["id"] for r in runs_meta]
        _write_json(out_dir / f"{owner}__{repo}__runs_index.json", runs_meta)
        print(f"listed {len(run_ids)} runs -> {out_dir / f'{owner}__{repo}__runs_index.json'}")

    if args.list_only:
        for r in runs_meta:
            print(
                f"{r.get('id')}\t{r.get('conclusion') or r.get('status')}\t"
                f"{r.get('name')}\t{r.get('html_url')}"
            )
        return 0

    artifact_names: set[str] | None = None
    if args.artifact_names:
        artifact_names = set(args.artifact_names)
    elif args.gh_aw_artifacts_only:
        artifact_names = set(GH_AW_ARTIFACT_NAMES)

    extracted: list[str] = []
    for rid in run_ids:
        try:
            path = extract_run(
                session,
                owner,
                repo,
                int(rid),
                out_dir,
                download_logs=not args.no_logs,
                download_job_logs=not args.no_job_logs,
                download_artifacts=not args.no_artifacts,
                artifact_names=artifact_names,
                keep_zips=args.keep_zips,
            )
            extracted.append(str(path))
        except Exception as e:
            print(f"FAILED run {rid}: {e}", file=sys.stderr)

    print(f"done: {len(extracted)}/{len(run_ids)} runs extracted under {out_dir}")
    return 0 if len(extracted) == len(run_ids) else 1


if __name__ == "__main__":
    raise SystemExit(main())
