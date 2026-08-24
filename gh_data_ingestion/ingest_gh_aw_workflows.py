"""
Ingest gh-aw workflow pairs (.md + .lock.yml) for every repo in data/repos.

Same on-disk layout as data/2026-08-11, but one corpus (no year-month split):

  data/gh_aw_workflows/
    ai_agentic_workflows.csv
    workflow-markdowns/{owner}_{repo}_{workflow}.md
    workflow-yamls/{owner}_{repo}_{workflow}.lock.yml
    .progress/scanned_repos.txt      # flushed after EACH repo
    .progress/pairs_index.jsonl      # flushed after EACH saved pair
    .progress/state.json             # running summary (updated often)

Resume is always on (Ctrl-C / network drop safe):
  - Re-run the same command; already-scanned repos are skipped.
  - Already-saved pairs are skipped even if the repo was mid-fetch.
  - Failed GraphQL batches are NOT marked scanned (retry next run).

Auth: gh_data_ingestion/.env
  GH_AW_INGESTION / GITHUB_TOKEN / GH_TOKEN          (single token)
  GH_AW_INGESTION_TOKENS=ghp_a,ghp_b                 (comma-separated pool)

Examples:
  python -m gh_data_ingestion.ingest_gh_aw_workflows --limit 20
  python -m gh_data_ingestion.ingest_gh_aw_workflows
  python -m gh_data_ingestion.ingest_gh_aw_workflows --out data/gh_aw_workflows --batch-size 100
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TextIO

import pandas as pd
import requests
from dotenv import load_dotenv

API_VERSION = "2022-11-28"
GRAPHQL_URL = "https://api.github.com/graphql"
CONTENTS_URL = "https://api.github.com/repos/{repo}/contents/{path}"
PKG_DIR = Path(__file__).resolve().parent
REPO_ROOT = PKG_DIR.parent
DEFAULT_REPOS = REPO_ROOT / "data" / "repos"
DEFAULT_OUT = REPO_ROOT / "data" / "gh_aw_workflows"

TOKEN_ENV_KEYS = ("GH_AW_INGESTION", "GITHUB_TOKEN", "GH_TOKEN")
TOKEN_POOL_KEY = "GH_AW_INGESTION_TOKENS"

CSV_FIELDS = [
    "repo",
    "workflow_name",
    "md_path",
    "lock_path",
    "md_url",
    "lock_url",
    "md_sha",
    "lock_sha",
]

LIST_QUERY_TEMPLATE = """
query {{
{aliases}
}}
"""

REPO_ALIAS = """
  r{i}: repository(owner: {owner}, name: {name}) {{
    nameWithOwner
    object(expression: "HEAD:.github/workflows") {{
      ... on Tree {{
        entries {{
          name
          type
        }}
      }}
    }}
  }}
"""


def _load_env() -> None:
    load_dotenv(PKG_DIR / ".env")
    load_dotenv(REPO_ROOT / ".env")


def _tokens() -> list[str]:
    _load_env()
    pool = os.environ.get(TOKEN_POOL_KEY, "")
    tokens = [t.strip() for t in pool.split(",") if t.strip()]
    if tokens:
        return tokens
    for key in TOKEN_ENV_KEYS:
        val = os.environ.get(key)
        if val and val.strip():
            return [val.strip()]
    keys = ", ".join((TOKEN_POOL_KEY, *TOKEN_ENV_KEYS))
    sys.exit(f"Set one of [{keys}] in gh_data_ingestion/.env")


def _graphql_quote(s: str) -> str:
    return json.dumps(s)


class TokenPool:
    """Rotate GitHub tokens on primary rate limit."""

    def __init__(self, tokens: list[str]) -> None:
        if not tokens:
            raise ValueError("no tokens")
        self.tokens = tokens
        self.index = 0
        self.session = requests.Session()
        self._apply_headers()

    def _apply_headers(self) -> None:
        token = self.tokens[self.index]
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": API_VERSION,
                "User-Agent": "msr-2027-ingest-gh-aw-workflows",
            }
        )

    def rotate(self) -> bool:
        """Advance to next token. Returns False if wrapped back to start."""
        nxt = (self.index + 1) % len(self.tokens)
        wrapped = nxt == 0 and len(self.tokens) > 1 or (len(self.tokens) == 1)
        if len(self.tokens) == 1:
            return False
        self.index = nxt
        self._apply_headers()
        return not wrapped or self.index != 0

    def graphql(self, query: str, *, retries: int = 8) -> dict[str, Any]:
        for attempt in range(retries):
            try:
                resp = self.session.post(GRAPHQL_URL, json={"query": query}, timeout=90)
            except requests.RequestException as exc:
                wait = min(2**attempt, 60)
                print(f"  graphql network error: {exc}; retry in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue

            if resp.status_code == 401:
                raise RuntimeError("GitHub token unauthorized (401)")

            if resp.status_code in (403, 429) or (
                resp.status_code == 200 and _graphql_rate_limited(resp)
            ):
                if self._maybe_rotate_or_sleep(resp, attempt):
                    continue
            if resp.status_code in (502, 503, 504):
                wait = min(2**attempt, 60)
                print(f"  HTTP {resp.status_code}; retry in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"GraphQL HTTP {resp.status_code}: {resp.text[:400]}")
            data = resp.json()
            if not isinstance(data, dict):
                raise RuntimeError("GraphQL returned non-object JSON")
            return data
        raise RuntimeError("GraphQL failed after retries")

    def get_json(self, url: str, *, retries: int = 8) -> tuple[int, Any]:
        for attempt in range(retries):
            try:
                resp = self.session.get(url, timeout=60)
            except requests.RequestException as exc:
                wait = min(2**attempt, 60)
                print(f"  GET network error: {exc}; retry in {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if resp.status_code in (403, 429):
                if self._maybe_rotate_or_sleep(resp, attempt):
                    continue
            if resp.status_code in (502, 503, 504):
                time.sleep(min(2**attempt, 60))
                continue
            if resp.status_code == 200:
                return 200, resp.json()
            return resp.status_code, None
        return 0, None

    def _maybe_rotate_or_sleep(self, resp: requests.Response, attempt: int) -> bool:
        remaining = resp.headers.get("X-RateLimit-Remaining")
        reset = resp.headers.get("X-RateLimit-Reset")
        if remaining == "0" or resp.status_code in (403, 429):
            if len(self.tokens) > 1:
                prev = self.index
                self.index = (self.index + 1) % len(self.tokens)
                self._apply_headers()
                print(
                    f"  rate limit on token[{prev}]; switching to token[{self.index}]",
                    file=sys.stderr,
                )
                if self.index != 0 or prev != len(self.tokens) - 1:
                    return True
            reset_ts = int(reset or (time.time() + 60))
            wait = max(reset_ts - int(time.time()), 1) + 1
            print(f"  rate limited; sleeping {wait}s", file=sys.stderr)
            time.sleep(wait)
            return True
        time.sleep(min(2**attempt, 30))
        return True


def _graphql_rate_limited(resp: requests.Response) -> bool:
    try:
        body = resp.json()
    except Exception:
        return False
    errors = body.get("errors") if isinstance(body, dict) else None
    if not isinstance(errors, list):
        return False
    text = json.dumps(errors).lower()
    return "rate limit" in text or "api rate limit" in text


def load_repo_full_names(repos_dir: Path) -> list[str]:
    files = sorted(repos_dir.glob("*.parquet"))
    if not files:
        sys.exit(f"No parquet files in {repos_dir}")
    frames = [pd.read_parquet(p, columns=["full_name"]) for p in files]
    series = pd.concat(frames, ignore_index=True)["full_name"].dropna().astype(str)
    names = sorted({n.strip() for n in series if n.strip() and "/" in n.strip()})
    return names


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fsync_file(fh: TextIO) -> None:
    fh.flush()
    try:
        os.fsync(fh.fileno())
    except OSError:
        pass


def load_scanned(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def pair_key(repo: str, workflow: str) -> str:
    return f"{repo}::{workflow}"


class Checkpoint:
    """Durable per-step resume state (flush + fsync after every write)."""

    def __init__(self, progress_dir: Path, csv_path: Path) -> None:
        self.progress_dir = progress_dir
        self.scanned_path = progress_dir / "scanned_repos.txt"
        self.pairs_path = progress_dir / "pairs_index.jsonl"
        self.state_path = progress_dir / "state.json"
        self.csv_path = csv_path
        progress_dir.mkdir(parents=True, exist_ok=True)
        ensure_csv(csv_path)
        self.scanned = load_scanned(self.scanned_path)
        self.done_pairs = load_done_pairs(self.pairs_path)
        self.fetched_this_run = 0
        self.failed_fetch = 0
        self.pairs_found = 0
        self.repos_scanned_this_run = 0
        self._write_state(note="loaded")

    def mark_repo_scanned(self, repo: str) -> None:
        """Append one fully-processed repo and sync to disk immediately."""
        if repo in self.scanned:
            return
        with self.scanned_path.open("a", encoding="utf-8") as fh:
            fh.write(repo + "\n")
            _fsync_file(fh)
        self.scanned.add(repo)
        self.repos_scanned_this_run += 1
        self._write_state(last_repo=repo, note="repo_scanned")

    def save_pair(self, row: dict[str, str]) -> None:
        """Append CSV + jsonl for one pair and sync immediately."""
        pk = pair_key(row["repo"], row["workflow_name"])
        if pk in self.done_pairs:
            return
        with self.csv_path.open("a", encoding="utf-8", newline="") as fh:
            csv.DictWriter(fh, fieldnames=CSV_FIELDS).writerow(row)
            _fsync_file(fh)
        with self.pairs_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            _fsync_file(fh)
        self.done_pairs.add(pk)
        self.fetched_this_run += 1
        self._write_state(
            last_repo=row["repo"],
            last_workflow=row["workflow_name"],
            note="pair_saved",
        )

    def _write_state(
        self,
        *,
        last_repo: str | None = None,
        last_workflow: str | None = None,
        note: str = "",
    ) -> None:
        payload: dict[str, Any] = {
            "updated_at": _utc_now(),
            "note": note,
            "scanned_repos_total": len(self.scanned),
            "pairs_saved_total": len(self.done_pairs),
            "repos_scanned_this_run": self.repos_scanned_this_run,
            "pairs_fetched_this_run": self.fetched_this_run,
            "pairs_found_this_run": self.pairs_found,
            "fetch_failures_this_run": self.failed_fetch,
        }
        if last_repo is not None:
            payload["last_repo"] = last_repo
        if last_workflow is not None:
            payload["last_workflow"] = last_workflow
        tmp = self.state_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.state_path)


def safe_stem(repo: str, workflow: str) -> str:
    return f"{repo.replace('/', '_')}_{workflow.replace('/', '_')}"


def github_blob_url(repo: str, sha: str, path: str) -> str:
    return f"https://github.com/{repo}/blob/{sha}/{path}"


def match_pairs(entries: list[dict[str, Any]]) -> list[str]:
    files = {
        e["name"]
        for e in entries
        if isinstance(e, dict)
        and isinstance(e.get("name"), str)
        and str(e.get("type") or "").lower() in {"blob", "file"}
    }
    bases: list[str] = []
    for name in sorted(files):
        if name.endswith(".md"):
            base = name[: -len(".md")]
            if f"{base}.lock.yml" in files:
                bases.append(base)
    return bases


def build_list_query(repos: list[str]) -> str:
    aliases: list[str] = []
    for i, full in enumerate(repos):
        owner, _, name = full.partition("/")
        aliases.append(
            REPO_ALIAS.format(i=i, owner=_graphql_quote(owner), name=_graphql_quote(name))
        )
    return LIST_QUERY_TEMPLATE.format(aliases="".join(aliases))


def parse_list_result(
    data: dict[str, Any], batch: list[str]
) -> dict[str, list[str]]:
    """Return repo -> workflow base names that have md+lock pairs."""
    payload = data.get("data") if isinstance(data, dict) else None
    out: dict[str, list[str]] = {repo: [] for repo in batch}
    if not isinstance(payload, dict):
        return out
    for i, repo in enumerate(batch):
        node = payload.get(f"r{i}")
        if not isinstance(node, dict):
            continue
        obj = node.get("object")
        if not isinstance(obj, dict):
            continue
        entries = obj.get("entries")
        if not isinstance(entries, list):
            continue
        out[repo] = match_pairs(entries)
    return out


def decode_contents_payload(payload: dict[str, Any]) -> tuple[str, str] | None:
    encoding = payload.get("encoding")
    content = payload.get("content")
    sha = payload.get("sha")
    if not isinstance(sha, str) or not sha:
        return None
    if encoding == "base64" and isinstance(content, str):
        try:
            text = base64.b64decode(content).decode("utf-8")
        except Exception:
            return None
        return text, sha
    if isinstance(content, str) and content:
        return content, sha
    return None


def fetch_file(pool: TokenPool, repo: str, path: str) -> tuple[str, str] | None:
    url = CONTENTS_URL.format(repo=repo, path=path)
    status, body = pool.get_json(url)
    if status != 200 or not isinstance(body, dict):
        return None
    decoded = decode_contents_payload(body)
    if decoded:
        return decoded
    download = body.get("download_url")
    if isinstance(download, str) and download:
        status2, raw = pool.get_json(download)
        if status2 == 200 and isinstance(raw, str):
            sha = str(body.get("sha") or "")
            return raw, sha
        if status2 == 200 and isinstance(raw, dict):
            return decode_contents_payload(raw)
    return None


def write_collected_file(
    dest: Path,
    *,
    repo: str,
    workflow: str,
    md_url: str,
    lock_url: str,
    sha: str,
    body: str,
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    collected = datetime.now(timezone.utc).isoformat()
    header = (
        f"# Repo: {repo}\n"
        f"# Workflow: {workflow}\n"
        f"# MD URL: {md_url}\n"
        f"# Lock URL: {lock_url}\n"
        f"# SHA: {sha}\n"
        f"# Collected: {collected}\n\n"
        + ("=" * 80)
        + "\n\n"
    )
    dest.write_text(header + body, encoding="utf-8")


def ensure_csv(path: Path) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        csv.DictWriter(fh, fieldnames=CSV_FIELDS).writeheader()
        _fsync_file(fh)


def load_done_pairs(jsonl_path: Path) -> set[str]:
    done: set[str] = set()
    if not jsonl_path.exists():
        return done
    with jsonl_path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            repo = rec.get("repo")
            wf = rec.get("workflow_name")
            if repo and wf:
                done.add(pair_key(str(repo), str(wf)))
    return done


def ingest(args: argparse.Namespace) -> None:
    repos = load_repo_full_names(args.repos)
    if args.offset:
        repos = repos[args.offset :]
    if args.limit is not None:
        repos = repos[: args.limit]
    print(f"repos to scan: {len(repos)} (from {args.repos})", file=sys.stderr)

    out: Path = args.out
    md_dir = out / "workflow-markdowns"
    yml_dir = out / "workflow-yamls"
    progress_dir = out / ".progress"
    csv_path = out / "ai_agentic_workflows.csv"

    md_dir.mkdir(parents=True, exist_ok=True)
    yml_dir.mkdir(parents=True, exist_ok=True)

    if args.no_resume:
        # Start fresh progress files (keep any already-written workflow files).
        for name in ("scanned_repos.txt", "pairs_index.jsonl", "state.json"):
            p = progress_dir / name
            if p.exists():
                p.unlink()
        if csv_path.exists():
            csv_path.unlink()
        print("resume disabled: cleared progress files", file=sys.stderr)

    ckpt = Checkpoint(progress_dir, csv_path)
    print(
        f"resume: {len(ckpt.scanned)} repos already scanned, "
        f"{len(ckpt.done_pairs)} pairs saved",
        file=sys.stderr,
    )

    pending = [r for r in repos if r not in ckpt.scanned]
    print(f"pending repos: {len(pending)}", file=sys.stderr)
    if not pending:
        print("nothing to do", file=sys.stderr)
        return

    pool = TokenPool(_tokens())
    print(f"tokens in pool: {len(pool.tokens)}", file=sys.stderr)

    try:
        for start in range(0, len(pending), args.batch_size):
            batch = pending[start : start + args.batch_size]
            query = build_list_query(batch)
            try:
                result = pool.graphql(query)
            except Exception as exc:
                print(f"  batch graphql failed ({batch[0]}…): {exc}", file=sys.stderr)
                time.sleep(2)
                try:
                    result = pool.graphql(query)
                except Exception as exc2:
                    # Do NOT mark scanned — leave batch for next resume.
                    print(
                        f"  leaving batch unscanned for resume: {exc2}",
                        file=sys.stderr,
                    )
                    ckpt._write_state(last_repo=batch[0], note="batch_failed")
                    continue

            repo_pairs = parse_list_result(result, batch)

            for repo in batch:
                bases = repo_pairs.get(repo) or []
                if bases:
                    print(
                        f"  {repo}: {len(bases)} gh-aw pair(s): {', '.join(bases)}",
                        file=sys.stderr,
                    )
                for workflow in bases:
                    ckpt.pairs_found += 1
                    pk = pair_key(repo, workflow)
                    if pk in ckpt.done_pairs:
                        continue
                    md_path = f".github/workflows/{workflow}.md"
                    lock_path = f".github/workflows/{workflow}.lock.yml"
                    md = fetch_file(pool, repo, md_path)
                    lock = fetch_file(pool, repo, lock_path)
                    if not md or not lock:
                        print(f"    failed fetch {repo}/{workflow}", file=sys.stderr)
                        ckpt.failed_fetch += 1
                        ckpt._write_state(
                            last_repo=repo,
                            last_workflow=workflow,
                            note="pair_fetch_failed",
                        )
                        continue
                    md_text, md_sha = md
                    lock_text, lock_sha = lock
                    md_url = github_blob_url(repo, md_sha, md_path)
                    lock_url = github_blob_url(repo, lock_sha, lock_path)
                    stem = safe_stem(repo, workflow)
                    write_collected_file(
                        md_dir / f"{stem}.md",
                        repo=repo,
                        workflow=workflow,
                        md_url=md_url,
                        lock_url=lock_url,
                        sha=md_sha,
                        body=md_text,
                    )
                    write_collected_file(
                        yml_dir / f"{stem}.lock.yml",
                        repo=repo,
                        workflow=workflow,
                        md_url=md_url,
                        lock_url=lock_url,
                        sha=lock_sha,
                        body=lock_text,
                    )
                    ckpt.save_pair(
                        {
                            "repo": repo,
                            "workflow_name": workflow,
                            "md_path": md_path,
                            "lock_path": lock_path,
                            "md_url": md_url,
                            "lock_url": lock_url,
                            "md_sha": md_sha,
                            "lock_sha": lock_sha,
                        }
                    )
                    time.sleep(args.sleep)

                # Only after all pairs for this repo are attempted.
                ckpt.mark_repo_scanned(repo)

            done_n = start + len(batch)
            print(
                f"[{done_n}/{len(pending)}] scanned +{len(batch)} "
                f"(pairs fetched this run: {ckpt.fetched_this_run}, "
                f"fetch failures: {ckpt.failed_fetch})",
                file=sys.stderr,
            )
            ckpt._write_state(last_repo=batch[-1], note="batch_done")
            time.sleep(args.batch_sleep)
    except KeyboardInterrupt:
        ckpt._write_state(note="interrupted")
        print(
            "\ninterrupted — progress flushed; re-run the same command to resume",
            file=sys.stderr,
        )
        raise SystemExit(130) from None

    print("=" * 72, file=sys.stderr)
    print(f"pending scanned this run: {ckpt.repos_scanned_this_run}", file=sys.stderr)
    print(f"pairs found (incl. already saved): {ckpt.pairs_found}", file=sys.stderr)
    print(f"newly fetched: {ckpt.fetched_this_run}", file=sys.stderr)
    print(f"fetch failures: {ckpt.failed_fetch}", file=sys.stderr)
    print(f"output: {out}", file=sys.stderr)
    print(f"  csv: {csv_path}", file=sys.stderr)
    print(f"  markdowns: {md_dir}", file=sys.stderr)
    print(f"  lock yamls: {yml_dir}", file=sys.stderr)
    print(f"  progress: {progress_dir}", file=sys.stderr)
    ckpt._write_state(note="finished")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--repos",
        type=Path,
        default=DEFAULT_REPOS,
        help="Directory of parquet files with a full_name column (default: data/repos)",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT,
        help="Output root (default: data/gh_aw_workflows)",
    )
    p.add_argument("--limit", type=int, default=None, help="Scan only the first N repos (after offset)")
    p.add_argument("--offset", type=int, default=0, help="Skip the first N repos in the sorted list")
    p.add_argument("--batch-size", type=int, default=100, help="Repos per GraphQL listing query (default: 100)")
    p.add_argument("--sleep", type=float, default=0.1, help="Pause after each file-pair fetch")
    p.add_argument("--batch-sleep", type=float, default=0.15, help="Pause after each GraphQL batch")
    p.add_argument(
        "--no-resume",
        action="store_true",
        help="Clear progress files and start scan tracking from scratch",
    )
    return p.parse_args()


def main() -> None:
    ingest(parse_args())


if __name__ == "__main__":
    main()
