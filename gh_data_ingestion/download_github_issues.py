"""
Download only issues touched by issue-triage gh-aw workflows.

Flow (no full-repo issue dump):
  1. For each triage workflow in df_iss_triage_new, list Actions runs
     for its ``*.lock.yml``.
  2. Resolve the linked issue number per run (issues / issue_comment /
     dispatch-with-issue_number).
  3. Fetch those issues (+ per-issue timeline label events) only.

Auth: ``GH_AW_INGESTION`` / ``GITHUB_TOKEN`` / ``GH_TOKEN`` in
``gh_data_ingestion/.env``.

Examples:
  python -m gh_data_ingestion.download_github_issues --from-triage --skills-only
  python -m gh_data_ingestion.download_github_issues --from-triage --limit-workflows 5
  python -m gh_data_ingestion.download_github_issues --repo cli/cli --workflow issue-triage
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import requests
from dotenv import load_dotenv

API_VERSION = "2022-11-28"
BASE = "https://api.github.com"
PKG_DIR = Path(__file__).resolve().parent
REPO_ROOT = PKG_DIR.parent
DEFAULT_OUT = REPO_ROOT / "data" / "github_issues"
TOKEN_ENV_KEYS = ("GH_AW_INGESTION", "GITHUB_TOKEN", "GH_TOKEN")

WF_CSV = REPO_ROOT / "data" / "gh_aw_workflows" / "ai_agentic_workflows.csv"
TASK_CSV = REPO_ROOT / "data" / "gh_aw_workflows" / "task_categories_llm.csv"
SKILLS_CSV = REPO_ROOT / "data" / "gh_aw_workflows" / "skills_usage_issue_triage.csv"

# Runs triggered by these events usually map 1:1 to an issue.
ISSUE_EVENTS = frozenset({"issues", "issue_comment"})
ISSUE_NUM_RE = re.compile(r"#(\d+)\b")
ISSUE_NUM_INPUT_RE = re.compile(
    r"(?i)(?:issue[_\s-]?number|issue)\s*[:=]\s*#?(\d+)"
)


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


def _session(token: str, *, use_env_proxy: bool = False) -> requests.Session:
    s = requests.Session()
    s.trust_env = use_env_proxy
    s.headers.update(
        {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "msr-2027-issue-downloader",
        }
    )
    return s


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    max_retries: int = 8,
) -> requests.Response:
    for attempt in range(max_retries):
        resp = session.request(
            method, url, params=params, headers=headers, timeout=120
        )
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


def _api_json(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> Any:
    url = path if path.startswith("http") else f"{BASE}{path}"
    resp = _request(session, "GET", url, params=params, headers=headers)
    if resp.status_code >= 400:
        raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:500]}")
    if not resp.content:
        return None
    return resp.json()


def _paginate(
    session: requests.Session,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    list_key: str | None = None,
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
        elif list_key:
            batch = data.get(list_key, [])
        else:
            raise RuntimeError(f"Expected list from {path}")
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


def _repo_dir(out_root: Path, repo: str) -> Path:
    return out_root / repo.replace("/", "__")


def load_triage_workflows(
    *,
    skills_only: bool = False,
    no_skills_only: bool = False,
    repos: set[str] | None = None,
    workflow: str | None = None,
) -> list[dict[str, Any]]:
    """Rows equivalent to df_iss_triage_new (optionally filtered)."""
    if not WF_CSV.exists() or not TASK_CSV.exists():
        sys.exit(f"Missing {WF_CSV} or {TASK_CSV}")

    cats: dict[tuple[str, str], str] = {}
    with TASK_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            cats[(row["repo"], row["workflow_name"])] = row["task_category"]

    skills: dict[tuple[str, str], bool] = {}
    if SKILLS_CSV.exists():
        with SKILLS_CSV.open(newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                val = str(row.get("uses_skills", "")).strip().lower()
                skills[(row["repo"], row["workflow_name"])] = val in ("1", "true", "yes")

    out: list[dict[str, Any]] = []
    with WF_CSV.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            key = (row["repo"], row["workflow_name"])
            if cats.get(key) != "issue triage":
                continue
            uses = skills.get(key, False)
            if skills_only and not uses:
                continue
            if no_skills_only and uses:
                continue
            if repos is not None and row["repo"] not in repos:
                continue
            if workflow and row["workflow_name"] != workflow:
                continue
            lock_path = row.get("lock_path") or f".github/workflows/{row['workflow_name']}.lock.yml"
            out.append(
                {
                    "repo": row["repo"],
                    "workflow_name": row["workflow_name"],
                    "lock_path": lock_path,
                    "lock_file": Path(lock_path).name,
                    "uses_skills": uses,
                }
            )
    return out


def list_workflow_runs(
    session: requests.Session,
    repo: str,
    lock_file: str,
    *,
    limit: int | None,
    event: str | None = None,
) -> list[dict[str, Any]]:
    owner, name = repo.split("/", 1)
    params: dict[str, Any] = {"per_page": 100}
    if event:
        params["event"] = event
    path = f"/repos/{owner}/{name}/actions/workflows/{lock_file}/runs"
    try:
        return _paginate(
            session, path, params=params, list_key="workflow_runs", limit=limit
        )
    except RuntimeError as exc:
        # Workflow file may be missing / renamed on default branch
        if "404" in str(exc):
            print(f"  workflow not found: {lock_file}", file=sys.stderr)
            return []
        raise


def _issue_number_from_text(*texts: str | None) -> int | None:
    for text in texts:
        if not text:
            continue
        m = ISSUE_NUM_RE.search(text)
        if m:
            return int(m.group(1))
        m = ISSUE_NUM_INPUT_RE.search(text)
        if m:
            return int(m.group(1))
    return None


def resolve_issue_number(run: dict[str, Any]) -> tuple[int | None, str]:
    """
    Map a workflow run → issue number.

    Prefer explicit ``#N`` in display_title / name (common for issue triggers).
    Returns (number_or_None, how).
    """
    event = (run.get("event") or "").strip()
    title = run.get("display_title") or ""
    name = run.get("name") or ""

    n = _issue_number_from_text(title, name)
    if n is not None:
        return n, "run_title"

    # issues / issue_comment: display_title is often the issue title without #N.
    # Keep a marker so callers can try timeline/search fallbacks later if needed.
    if event in ISSUE_EVENTS:
        return None, f"event:{event}:unresolved"

    if event == "workflow_dispatch":
        return None, "dispatch:unresolved"

    return None, f"skip:{event or 'unknown'}"


def resolve_issue_via_run_url_search(
    session: requests.Session, repo: str, run_id: int
) -> int | None:
    """Find an issue that mentions this Actions run URL (gh-aw comments often do)."""
    q = f'repo:{repo} "actions/runs/{run_id}"'
    try:
        data = _api_json(
            session,
            "/search/issues",
            params={"q": q, "per_page": 5},
        )
    except RuntimeError as exc:
        print(f"  search failed for run {run_id}: {exc}", file=sys.stderr)
        return None
    items = data.get("items") or []
    pure = [it for it in items if "pull_request" not in it]
    pick = pure[0] if pure else (items[0] if items else None)
    return int(pick["number"]) if pick else None


def fetch_issue(session: requests.Session, repo: str, number: int) -> dict[str, Any]:
    return _api_json(session, f"/repos/{repo}/issues/{number}")


def fetch_issue_timeline_label_events(
    session: requests.Session,
    repo: str,
    number: int,
) -> list[dict[str, Any]]:
    """Labeled / unlabeled events on one issue (not whole-repo events)."""
    headers = {"Accept": "application/vnd.github+json"}
    # timeline API needs the special preview accept historically; modern API
    # accepts application/vnd.github+json for /timeline as well.
    try:
        events = _paginate(
            session,
            f"/repos/{repo}/issues/{number}/timeline",
            params={"per_page": 100},
        )
    except RuntimeError:
        # Fallback: issue events endpoint is repo-wide only — skip
        return []

    out: list[dict[str, Any]] = []
    for ev in events:
        et = ev.get("event")
        if et not in ("labeled", "unlabeled"):
            continue
        out.append(
            {
                "issue_number": number,
                "event": et,
                "label": (ev.get("label") or {}).get("name"),
                "actor": (ev.get("actor") or {}).get("login"),
                "created_at": ev.get("created_at"),
                "commit_id": ev.get("commit_id"),
            }
        )
    return out


def download_workflow_touched_issues(
    session: requests.Session,
    wf: dict[str, Any],
    out_root: Path,
    *,
    runs_limit: int | None,
    force: bool = False,
    resolve_search: bool = True,
) -> dict[str, Any]:
    repo = wf["repo"]
    wname = wf["workflow_name"]
    out_dir = _repo_dir(out_root, repo) / "workflows" / wname
    meta_path = out_dir / "metadata.json"
    if meta_path.exists() and not force:
        print(f"=== Skipping {repo}::{wname} (already downloaded) ===")
        return json.loads(meta_path.read_text(encoding="utf-8"))

    print(f"=== {repo} :: {wname} ({wf['lock_file']}) skills={wf['uses_skills']} ===")
    out_dir.mkdir(parents=True, exist_ok=True)

    # Collect runs across issue-related events (+ dispatch). Paginate each event
    # filter separately so we don't pull push/PR noise.
    runs_by_id: dict[int, dict[str, Any]] = {}
    for event in ("issues", "issue_comment", "workflow_dispatch"):
        batch = list_workflow_runs(
            session, repo, wf["lock_file"], limit=runs_limit, event=event
        )
        for run in batch:
            runs_by_id[int(run["id"])] = run
        print(f"  runs event={event}: {len(batch)}")

    # If filters returned nothing (permissions / old API), try unfiltered once.
    if not runs_by_id:
        batch = list_workflow_runs(
            session, repo, wf["lock_file"], limit=runs_limit, event=None
        )
        for run in batch:
            if (run.get("event") or "") in ISSUE_EVENTS | {"workflow_dispatch", "schedule"}:
                runs_by_id[int(run["id"])] = run
        print(f"  runs (fallback filtered): {len(runs_by_id)}")

    runs = sorted(runs_by_id.values(), key=lambda r: r.get("created_at") or "")
    slim_runs = [
        {
            "id": r.get("id"),
            "event": r.get("event"),
            "status": r.get("status"),
            "conclusion": r.get("conclusion"),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            "html_url": r.get("html_url"),
            "display_title": r.get("display_title"),
            "name": r.get("name"),
            "actor": (r.get("actor") or {}).get("login"),
            "path": r.get("path"),
        }
        for r in runs
    ]
    _write_json(out_dir / "runs.json", slim_runs)

    run_issue_map: list[dict[str, Any]] = []
    issue_numbers: set[int] = set()
    unresolved = 0
    for r in runs:
        num, how = resolve_issue_number(r)
        if num is None and resolve_search and (
            how.startswith("event:") or how.startswith("dispatch:")
        ):
            found = resolve_issue_via_run_url_search(session, repo, int(r["id"]))
            if found is not None:
                num, how = found, "search_run_url"
            else:
                unresolved += 1
        elif num is None and (
            how.startswith("event:") or how.startswith("dispatch:")
        ):
            unresolved += 1

        rec = {
            "run_id": r.get("id"),
            "event": r.get("event"),
            "issue_number": num,
            "resolve": how,
            "display_title": r.get("display_title"),
            "created_at": r.get("created_at"),
            "conclusion": r.get("conclusion"),
            "html_url": r.get("html_url"),
        }
        run_issue_map.append(rec)
        if num is not None:
            issue_numbers.add(num)

    _write_json(out_dir / "run_issue_map.json", run_issue_map)
    print(
        f"  mapped issues={len(issue_numbers)} "
        f"unresolved_runs={unresolved} total_runs={len(runs)}"
    )

    # Shared issue cache at repo level (multiple triage workflows may overlap)
    repo_dir = _repo_dir(out_root, repo)
    issues_dir = repo_dir / "issues"
    timelines_dir = repo_dir / "timelines"
    issues_dir.mkdir(parents=True, exist_ok=True)
    timelines_dir.mkdir(parents=True, exist_ok=True)

    issues: list[dict[str, Any]] = []
    all_label_events: list[dict[str, Any]] = []
    for num in sorted(issue_numbers):
        iss_path = issues_dir / f"{num}.json"
        if iss_path.exists() and not force:
            iss = json.loads(iss_path.read_text(encoding="utf-8"))
        else:
            try:
                iss = fetch_issue(session, repo, num)
            except RuntimeError as exc:
                print(f"  issue #{num} fetch failed: {exc}", file=sys.stderr)
                continue
            # Drop PR stubs if a run somehow pointed at a PR number
            if iss.get("pull_request"):
                print(f"  skip #{num} (pull request)")
                continue
            _write_json(iss_path, iss)

        issues.append(iss)

        tl_path = timelines_dir / f"{num}.json"
        if tl_path.exists() and not force:
            label_ev = json.loads(tl_path.read_text(encoding="utf-8"))
        else:
            label_ev = fetch_issue_timeline_label_events(session, repo, num)
            _write_json(tl_path, label_ev)
        all_label_events.extend(label_ev)

    # Workflow-scoped index of touched issues (numbers + final labels)
    touched = []
    for iss in issues:
        labels = [
            lab.get("name")
            for lab in (iss.get("labels") or [])
            if isinstance(lab, dict) and lab.get("name")
        ]
        touched.append(
            {
                "repo": repo,
                "workflow_name": wname,
                "uses_skills": wf["uses_skills"],
                "issue_number": iss.get("number"),
                "state": iss.get("state"),
                "title": iss.get("title"),
                "created_at": iss.get("created_at"),
                "closed_at": iss.get("closed_at"),
                "final_labels": labels,
                "html_url": iss.get("html_url"),
            }
        )
    _write_json(out_dir / "touched_issues.json", touched)
    _write_json(out_dir / "label_events.json", all_label_events)

    closed_n = sum(1 for t in touched if t.get("state") == "closed")
    meta = {
        "repo": repo,
        "workflow_name": wname,
        "lock_file": wf["lock_file"],
        "uses_skills": wf["uses_skills"],
        "downloaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mode": "workflow_touched_only",
        "run_count": len(runs),
        "issue_count": len(touched),
        "issue_closed_count": closed_n,
        "unresolved_run_count": unresolved,
        "label_event_count": len(all_label_events),
    }
    _write_json(meta_path, meta)
    print(f"  saved issues={len(touched)} closed={closed_n} label_events={len(all_label_events)}")
    return meta


def _parse_workflow_list(args: argparse.Namespace) -> list[dict[str, Any]]:
    repo_set: set[str] | None = None
    if args.repo:
        repo_set = set(args.repo)
    if args.repos_file:
        text = Path(args.repos_file).read_text(encoding="utf-8")
        extra = {
            line.strip()
            for line in text.splitlines()
            if line.strip() and not line.startswith("#") and "/" in line
        }
        repo_set = (repo_set or set()) | extra

    if args.from_triage or repo_set is not None or args.workflow:
        wfs = load_triage_workflows(
            skills_only=args.skills_only,
            no_skills_only=args.no_skills_only,
            repos=repo_set,
            workflow=args.workflow,
        )
    else:
        sys.exit("Pass --from-triage and/or --repo / --repos-file")

    if not wfs:
        sys.exit("No matching issue-triage workflows")
    return wfs


def main(argv: Iterable[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--repo", action="append", default=[], help="OWNER/REPO (repeatable)")
    p.add_argument("--repos-file", type=Path, help="Text file with one OWNER/REPO per line")
    p.add_argument("--workflow", help="Only this workflow_name")
    p.add_argument(
        "--from-triage",
        action="store_true",
        help="All issue-triage workflows (df_iss_triage_new)",
    )
    p.add_argument("--skills-only", action="store_true")
    p.add_argument("--no-skills-only", action="store_true")
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument(
        "--runs-limit",
        type=int,
        default=None,
        help="Max runs per event filter per workflow (default: all)",
    )
    p.add_argument(
        "--limit-workflows",
        type=int,
        default=None,
        help="Stop after N workflows (smoke test)",
    )
    p.add_argument(
        "--no-resolve-search",
        action="store_true",
        help="Do not use Search API fallback for runs without #N in the title",
    )
    p.add_argument("--force", action="store_true")
    p.add_argument("--list-only", action="store_true")
    p.add_argument("--use-env-proxy", action="store_true")
    args = p.parse_args(list(argv) if argv is not None else None)

    wfs = _parse_workflow_list(args)
    if args.limit_workflows is not None:
        wfs = wfs[: args.limit_workflows]

    print(
        f"workflows={len(wfs)} out={args.out} mode=workflow_touched_only",
        file=sys.stderr,
    )
    if args.list_only:
        for wf in wfs:
            print(
                f"{wf['repo']}\t{wf['workflow_name']}\t"
                f"skills={wf['uses_skills']}\t{wf['lock_file']}"
            )
        return 0

    session = _session(_token(), use_env_proxy=args.use_env_proxy)
    ok = 0
    err = 0
    for i, wf in enumerate(wfs, 1):
        print(f"\n[{i}/{len(wfs)}]")
        try:
            download_workflow_touched_issues(
                session,
                wf,
                args.out,
                runs_limit=args.runs_limit,
                force=args.force,
                resolve_search=not args.no_resolve_search,
            )
            ok += 1
        except Exception as exc:
            err += 1
            print(f"  ERROR: {exc}", file=sys.stderr)
            err_path = (
                _repo_dir(args.out, wf["repo"])
                / "workflows"
                / wf["workflow_name"]
                / "error.json"
            )
            _write_json(
                err_path,
                {
                    "repo": wf["repo"],
                    "workflow_name": wf["workflow_name"],
                    "error": str(exc),
                    "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )

    print(f"\nFinished. ok={ok} errors={err} out={args.out}")
    return 0 if err == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
