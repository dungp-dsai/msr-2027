"""
Find GitHub repos using Agentic Workflows (gh-aw) that declare skills.

Detection rules (aligned with gh-aw-workflows.py):
  - A repo uses gh-aw when it has a matching pair:
      .github/workflows/{name}.md
      .github/workflows/{name}.lock.yml
  - Skills are detected from YAML frontmatter in the .md file:
      skills:
        - .github/skills/my-skill

Modes:
  local   Scan collected snapshot folders (data/YYYY-MM-DD/workflow-markdowns)
  live    Query GitHub Code Search API, match md/lock pairs, fetch .md content
          Writes hits to CSV as they are found; saves progress for --resume.

Output CSV columns: repo_name, repo_url

Live mode (streaming):
  1. Index .lock.yml files (checkpointed; required for matching)
  2. Stream .md files — on each match with lock index → fetch → check skills → append CSV

Sidecar files (live mode):
  {out}.progress.json     checked pairs + repos written
  {out}.lock_index.json   lock file index (enables match-as-you-go)

Examples:
  python -m gh_data_ingestion.find_skills_repos --data-root data --out gh_data_ingestion/repos_with_skills.csv
  python -m gh_data_ingestion.find_skills_repos --snapshots data/2026-08-11 data/2026-06-20
  python -m gh_data_ingestion.find_skills_repos --live --out gh_data_ingestion/repos_with_skills_live.csv
  python -m gh_data_ingestion.find_skills_repos --live --no-resume
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

PKG_DIR = Path(__file__).resolve().parent
TOKEN_ENV_KEYS = ("GH_AW_INGESTION", "GITHUB_TOKEN", "GH_TOKEN")

COLLECTION_SEP_RE = re.compile(r"(?m)^={10,}\s*$")
FRONTMATTER_RE = re.compile(r"---\s*\r?\n(.*?)\r?\n---\s*(?:\r?\n|$)", re.DOTALL)
SKILLS_KEY_RE = re.compile(r"(?m)^skills\s*:")


def _load_env() -> None:
    load_dotenv(PKG_DIR / ".env")
    load_dotenv(PKG_DIR.parent / ".env")


def _token() -> str | None:
    _load_env()
    for key in TOKEN_ENV_KEYS:
        val = os.environ.get(key)
        if val:
            return val.strip()
    return None


def _workflow_body(text: str) -> str:
    m = COLLECTION_SEP_RE.search(text)
    body = text[m.end() :] if m else text
    return body.lstrip("\r\n")


def _extract_frontmatter(text: str) -> str | None:
    body = _workflow_body(text)
    m = FRONTMATTER_RE.match(body) or FRONTMATTER_RE.search(body)
    return m.group(1) if m else None


def _parse_skills_from_frontmatter(fm: str) -> list[str]:
    if not SKILLS_KEY_RE.search(fm):
        return []
    key = re.search(r"(?m)^skills\s*:\s*(.*)$", fm)
    if not key:
        return []
    first = (key.group(1) or "").strip()
    rest = fm[key.end() :]
    items: list[str] = []
    if first and first not in ("[]", "~", "null", ""):
        if first.startswith("[") and "]" in first:
            inner = first.strip()[1 : first.rfind("]")].strip()
            if inner:
                items = [x.strip().strip("'\"") for x in inner.split(",") if x.strip()]
        else:
            items = [first.strip("'\"")]
    for line in rest.splitlines():
        if re.match(r"^[A-Za-z_][\w-]*\s*:", line):
            break
        im = re.match(r"^\s*-\s+(.+)$", line)
        if im:
            items.append(im.group(1).strip().strip("'\""))
    return items


def has_skills_frontmatter(text: str) -> bool:
    fm = _extract_frontmatter(text)
    if fm is None:
        return False
    return bool(_parse_skills_from_frontmatter(fm))


def repo_url(repo_name: str) -> str:
    return f"https://github.com/{repo_name.strip().removeprefix('https://github.com/')}"


def _repo_from_collection_header(text: str) -> str | None:
    m = re.search(r"(?m)^# Repo:\s*(.+)$", text)
    return m.group(1).strip() if m else None


def scan_workflow_markdown(path: Path) -> tuple[str | None, bool, list[str]]:
    text = path.read_text(encoding="utf-8", errors="replace")
    repo = _repo_from_collection_header(text)
    fm = _extract_frontmatter(text)
    skills = _parse_skills_from_frontmatter(fm) if fm else []
    return repo, bool(skills), skills


def scan_snapshot_dir(snapshot_dir: Path) -> list[dict[str, Any]]:
    md_dir = snapshot_dir / "workflow-markdowns"
    if not md_dir.is_dir():
        raise FileNotFoundError(f"Missing workflow-markdowns in {snapshot_dir}")

    hits: list[dict[str, Any]] = []
    for md_path in sorted(md_dir.glob("*.md")):
        repo, ok, skills = scan_workflow_markdown(md_path)
        if not ok or not repo:
            continue
        hits.append(
            {
                "repo_name": repo,
                "repo_url": repo_url(repo),
                "snapshot": snapshot_dir.name,
                "workflow_file": md_path.name,
                "skills": ";".join(skills),
            }
        )
    return hits


def collect_from_snapshots(snapshot_dirs: list[Path]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for d in snapshot_dirs:
        print(f"scanning {d} ...")
        rows.extend(scan_snapshot_dir(d))

    if not rows:
        return pd.DataFrame(columns=["repo_name", "repo_url"])

    df = pd.DataFrame(rows)
    # Keep latest snapshot per repo (folder names sort as YYYY-MM-DD)
    df = df.sort_values(["repo_name", "snapshot"]).drop_duplicates("repo_name", keep="last")
    return df[["repo_name", "repo_url"]].sort_values("repo_name").reset_index(drop=True)


def discover_snapshot_dirs(data_root: Path) -> list[Path]:
    dirs = sorted(p for p in data_root.iterdir() if p.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", p.name))
    return [d for d in dirs if (d / "workflow-markdowns").is_dir()]


# --- Live GitHub API collection (based on gh-aw-workflows.py) ---

SIZE_SPLITS: dict[str, list[str] | None] = {
    "<1000": ["<100", "100..200", "200..300", "300..500", "500..1000"],
    "1000..5000": ["1000..2000", "2000..3000", "3000..4000", "4000..5000"],
    "5000..10000": ["5000..6000", "6000..7000", "7000..8000", "8000..9000", "9000..10000"],
    "10000..50000": ["10000..20000", "20000..30000", "30000..40000", "40000..50000"],
    ">50000": ["50000..100000", "100000..500000", ">500000"],
    "<100": ["<10", "10..20", "20..30", "30..40", "40..50", "50..60", "60..70", "70..80", "80..90", "90..100"],
    "100..200": ["100..120", "120..140", "140..160", "160..180", "180..200"],
    "200..300": ["200..225", "225..250", "250..275", "275..300"],
    "300..500": ["300..350", "350..400", "400..450", "450..500"],
    "500..1000": ["500..600", "600..700", "700..800", "800..900", "900..1000"],
    "50000..100000": None,
    "100000..500000": None,
    ">500000": None,
    "<10": ["<1", "1..2", "2..3", "3..4", "4..5", "5..6", "6..7", "7..8", "8..9", "9..10"],
    "10..20": ["10..12", "12..14", "14..16", "16..18", "18..20"],
    "20..30": ["20..22", "22..24", "24..26", "26..28", "28..30"],
    "30..40": ["30..32", "32..34", "34..36", "36..38", "38..40"],
    "40..50": ["40..42", "42..44", "44..46", "46..48", "48..50"],
    "50..60": ["50..52", "52..54", "54..56", "56..58", "58..60"],
    "60..70": ["60..62", "62..64", "64..66", "66..68", "68..70"],
    "70..80": ["70..72", "72..74", "74..76", "76..78", "78..80"],
    "80..90": ["80..82", "82..84", "84..86", "86..88", "88..90"],
    "90..100": ["90..92", "92..94", "94..96", "96..98", "98..100"],
    "<1": None,
    "1..2": None,
}


def _item_repo_full_name(item: dict[str, Any]) -> str | None:
    """Normalize repository field from code-search items."""
    repo = item.get("repository")
    if isinstance(repo, dict):
        name = repo.get("full_name")
        return str(name) if name else None
    if isinstance(repo, str):
        # API sometimes returns repo URL string instead of object.
        return repo.removeprefix("https://api.github.com/repos/").strip("/")
    return None


def _valid_code_item(item: Any, *, suffix: str) -> bool:
    if not isinstance(item, dict):
        return False
    name = item.get("name")
    if not isinstance(name, str) or not name.endswith(suffix):
        return False
    if not _item_repo_full_name(item):
        return False
    if not item.get("path") or not item.get("sha"):
        return False
    return True


class GitHubClient:
    def __init__(self, token: str) -> None:
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "User-Agent": "msr-2027-find-skills-repos",
            }
        )

    def get_json(self, url: str, *, params: dict[str, Any] | None = None, retries: int = 5) -> Any:
        for attempt in range(retries):
            resp = self.session.get(url, params=params, timeout=60)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
                reset = int(resp.headers.get("X-RateLimit-Reset", time.time() + 60))
                wait = max(reset - int(time.time()), 1) + 1
                print(f"rate limited; sleeping {wait}s", file=sys.stderr)
                time.sleep(wait)
                continue
            if resp.status_code in (429, 502, 503, 504):
                time.sleep(2**attempt)
                continue
            if resp.status_code == 422:
                return {"total_count": 0, "items": []}
            raise RuntimeError(f"GET {url} -> {resp.status_code}: {resp.text[:300]}")
        raise RuntimeError(f"GET {url} failed after retries")

    def search_code_count(self, query: str) -> int:
        data = self.get_json(
            "https://api.github.com/search/code",
            params={"q": query, "per_page": 1},
        )
        return int(data.get("total_count", 0)) if isinstance(data, dict) else 0

    def search_code_page(self, query: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        page = 1
        while page <= 10:  # GitHub caps code search at 1000 results (10 pages)
            data = self.get_json(
                "https://api.github.com/search/code",
                params={"q": query, "per_page": 100, "page": page},
            )
            if not isinstance(data, dict):
                break
            items = data.get("items", [])
            if not isinstance(items, list):
                break
            out.extend(i for i in items if isinstance(i, dict))
            if len(items) < 100:
                break
            page += 1
            time.sleep(2)
        return out

    def search_code_with_size_splitting(
        self,
        base_query: str,
        *,
        suffix: str,
        size_range: str | None = None,
        depth: int = 0,
    ) -> list[dict[str, Any]]:
        return list(self.iter_search_code_with_size_splitting(
            base_query, suffix=suffix, size_range=size_range, depth=depth
        ))

    def iter_search_code_with_size_splitting(
        self,
        base_query: str,
        *,
        suffix: str,
        size_range: str | None = None,
        depth: int = 0,
    ):
        """Yield code-search items, splitting by file size to bypass the 1000-result cap."""
        indent = "  " * depth
        if size_range is None:
            top_ranges = ["<1000", "1000..5000", "5000..10000", "10000..50000", ">50000"]
            for sr in top_ranges:
                yield from self.iter_search_code_with_size_splitting(
                    base_query, suffix=suffix, size_range=sr, depth=depth
                )
                time.sleep(1)
            return

        query = f"{base_query} size:{size_range}"
        count = self.search_code_count(query)
        print(f"{indent}[size:{size_range}] {count} results", flush=True)

        if count == 0:
            return

        if count <= 1000:
            print(f"{indent}  fetching {count} ...", flush=True)
            for item in self.search_code_page(query):
                if _valid_code_item(item, suffix=suffix):
                    yield item
            return

        sub_ranges = SIZE_SPLITS.get(size_range)
        if not sub_ranges:
            print(f"{indent}  cannot split further; fetching first 1000", flush=True)
            for item in self.search_code_page(query):
                if _valid_code_item(item, suffix=suffix):
                    yield item
            return

        print(f"{indent}  splitting ...", flush=True)
        for sub in sub_ranges:
            yield from self.iter_search_code_with_size_splitting(
                base_query, suffix=suffix, size_range=sub, depth=depth + 1
            )
            time.sleep(1)

    def iter_search_code(self, query: str, *, suffix: str):
        print("  using size-based splitting (GitHub code search caps at 1000/query)", flush=True)
        yield from self.iter_search_code_with_size_splitting(query, suffix=suffix)

    def search_code(self, query: str, *, suffix: str) -> list[dict[str, Any]]:
        return list(self.iter_search_code(query, suffix=suffix))

    def fetch_file_content(self, repo: str, path: str) -> str | None:
        data = self.get_json(f"https://api.github.com/repos/{repo}/contents/{path}")
        if not data or "content" not in data:
            return None
        try:
            return base64.b64decode(data["content"]).decode("utf-8")
        except Exception:
            return None


def write_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"wrote {len(df)} repos -> {out_path}")


def _progress_path(out_path: Path) -> Path:
    return out_path.with_name(out_path.name + ".progress.json")


def _lock_index_path(out_path: Path) -> Path:
    return out_path.with_name(out_path.name + ".lock_index.json")


def _pair_key(repo: str, md: dict[str, Any]) -> str:
    return f"{repo}:{md.get('path', '')}:{md.get('sha', '')}"


class LockIndexStore:
    """repo:workflow_base -> True for gh-aw lock files."""

    def __init__(self, path: Path, *, resume: bool) -> None:
        self.path = path
        self.keys: set[str] = set()
        self.complete = False
        self.seen_shas: set[str] = set()
        if resume and path.exists():
            self._load()

    def _load(self) -> None:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            self.keys = set(data.get("keys", []))
            self.complete = bool(data.get("complete", False))
            self.seen_shas = set(data.get("seen_shas", []))
            print(
                f"loaded lock index: {len(self.keys)} entries "
                f"({'complete' if self.complete else 'partial'})",
                flush=True,
            )
        except (json.JSONDecodeError, OSError) as e:
            print(f"warning: lock index load failed ({e})", file=sys.stderr)

    def add(self, repo: str, base_name: str, sha: str) -> None:
        if sha in self.seen_shas:
            return
        self.seen_shas.add(sha)
        self.keys.add(f"{repo}:{base_name}")

    def has(self, repo: str, base_name: str) -> bool:
        return f"{repo}:{base_name}" in self.keys

    def save(self, *, complete: bool | None = None) -> None:
        if complete is not None:
            self.complete = complete
        payload = {
            "complete": self.complete,
            "keys": sorted(self.keys),
            "seen_shas": sorted(self.seen_shas),
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def maybe_save(self, *, every: int, count: int) -> None:
        if count % every == 0:
            self.save()


class LiveResultWriter:
    """Append skill hits to CSV immediately; persist progress for resume."""

    COLUMNS = ["repo_name", "repo_url"]

    def __init__(self, out_path: Path, *, resume: bool) -> None:
        self.out_path = out_path
        self.progress_path = _progress_path(out_path)
        self.lock_index_path = _lock_index_path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        self.repos_written: set[str] = set()
        self.processed_pairs: set[str] = set()
        self.matched_pairs = 0
        self._file_exists = out_path.exists() and out_path.stat().st_size > 0

        if resume:
            self._load_progress()
            self._load_repos_from_csv()
        else:
            for p in (self.out_path, self.progress_path, self.lock_index_path):
                if p.exists():
                    p.unlink()

        if not self._file_exists or not resume:
            with self.out_path.open("w", encoding="utf-8", newline="") as f:
                csv.DictWriter(f, fieldnames=self.COLUMNS).writeheader()

    def _load_repos_from_csv(self) -> None:
        if not self.out_path.exists():
            return
        with self.out_path.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                name = (row.get("repo_name") or "").strip()
                if name:
                    self.repos_written.add(name)

    def _load_progress(self) -> None:
        if not self.progress_path.exists():
            return
        try:
            data = json.loads(self.progress_path.read_text(encoding="utf-8"))
            self.processed_pairs = set(data.get("processed_pairs", []))
            self.repos_written.update(data.get("repos_written", []))
            self.matched_pairs = int(data.get("matched_pairs", 0))
            print(
                f"resuming: {len(self.repos_written)} repos in CSV, "
                f"{len(self.processed_pairs)} pairs checked, "
                f"{self.matched_pairs} pairs matched so far",
                file=sys.stderr,
            )
        except (json.JSONDecodeError, OSError) as e:
            print(f"warning: could not load progress ({e}); starting fresh", file=sys.stderr)

    def save_progress(self) -> None:
        payload = {
            "repos_written": sorted(self.repos_written),
            "processed_pairs": sorted(self.processed_pairs),
            "matched_pairs": self.matched_pairs,
            "updated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.progress_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    def append_hit(self, repo_name: str) -> bool:
        if repo_name in self.repos_written:
            return False
        row = {"repo_name": repo_name, "repo_url": repo_url(repo_name)}
        with self.out_path.open("a", encoding="utf-8", newline="") as f:
            csv.DictWriter(f, fieldnames=self.COLUMNS).writerow(row)
        self.repos_written.add(repo_name)
        print(f"  + skills repo: {repo_name} -> {self.out_path}", flush=True)
        self.save_progress()
        return True

    def mark_checked(self, key: str) -> None:
        self.processed_pairs.add(key)

    def flush_progress(self, *, every: int = 10, count: int = 0) -> None:
        if count % every == 0:
            self.save_progress()


def _build_lock_index(gh: GitHubClient, lock_store: LockIndexStore) -> None:
    print("=== step 1/2: index .lock.yml files ===", flush=True)
    n = 0
    for item in gh.iter_search_code(
        "path:.github/workflows filename:.lock.yml",
        suffix=".lock.yml",
    ):
        repo = _item_repo_full_name(item)
        if not repo:
            continue
        base = item["name"].removesuffix(".lock.yml")
        lock_store.add(repo, base, item["sha"])
        n += 1
        lock_store.maybe_save(every=100, count=n)
    lock_store.save(complete=True)
    print(f"lock index ready: {len(lock_store.keys)} gh-aw lock files", flush=True)


def _check_matched_md(
    gh: GitHubClient,
    writer: LiveResultWriter,
    repo: str,
    md: dict[str, Any],
) -> None:
    key = _pair_key(repo, md)
    if key in writer.processed_pairs:
        return

    writer.matched_pairs += 1

    if repo in writer.repos_written:
        writer.mark_checked(key)
        writer.save_progress()
        return

    if writer.matched_pairs == 1 or writer.matched_pairs % 25 == 0:
        print(
            f"  matched={writer.matched_pairs} "
            f"checked={len(writer.processed_pairs)} "
            f"skills_hits={len(writer.repos_written)} "
            f"{repo} {md.get('path', '')}",
            flush=True,
        )

    content = gh.fetch_file_content(repo, md["path"])
    if content and has_skills_frontmatter(content):
        writer.append_hit(repo)

    writer.mark_checked(key)
    writer.flush_progress(every=10, count=len(writer.processed_pairs))
    time.sleep(0.5)


def collect_live(token: str, out_path: Path, *, resume: bool = True) -> pd.DataFrame:
    writer = LiveResultWriter(out_path, resume=resume)
    gh = GitHubClient(token)
    lock_store = LockIndexStore(_lock_index_path(out_path), resume=resume)

    if not lock_store.complete:
        _build_lock_index(gh, lock_store)
    else:
        print(
            f"=== step 1/2: lock index already complete ({len(lock_store.keys)} entries) ===",
            flush=True,
        )

    print("=== step 2/2: stream .md → match → check skills → write CSV ===", flush=True)
    seen_md_sha: set[str] = set()
    pending_md: list[tuple[str, dict[str, Any]]] = []

    for item in gh.iter_search_code("path:.github/workflows filename:.md", suffix=".md"):
        sha = item.get("sha")
        if not sha or sha in seen_md_sha:
            continue
        seen_md_sha.add(sha)

        repo = _item_repo_full_name(item)
        if not repo:
            continue
        base = item["name"].removesuffix(".md")
        if lock_store.has(repo, base):
            _check_matched_md(gh, writer, repo, item)
        else:
            pending_md.append((repo, item))

    # Lock index was built first; re-check md seen before a lock entry landed (edge case).
    for repo, md in pending_md:
        base = md["name"].removesuffix(".md")
        if not lock_store.has(repo, base):
            continue
        _check_matched_md(gh, writer, repo, md)

    writer.save_progress()
    print(
        f"finished: matched={writer.matched_pairs} "
        f"checked={len(writer.processed_pairs)} "
        f"skills_hits={len(writer.repos_written)}",
        flush=True,
    )
    if not out_path.exists():
        return pd.DataFrame(columns=["repo_name", "repo_url"])
    return pd.read_csv(out_path)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Find gh-aw repos whose workflow .md declares skills.")
    p.add_argument(
        "--data-root",
        type=Path,
        default=Path("data"),
        help="Root folder containing YYYY-MM-DD snapshots (default: data)",
    )
    p.add_argument(
        "--snapshots",
        nargs="+",
        type=Path,
        help="Explicit snapshot dirs (e.g. data/2026-08-11). Default: all under --data-root.",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=PKG_DIR / "repos_with_skills.csv",
        help="Output CSV path (default: gh_data_ingestion/repos_with_skills.csv)",
    )
    p.add_argument(
        "--live",
        action="store_true",
        help="Query GitHub API live (needs token in gh_data_ingestion/.env)",
    )
    p.add_argument(
        "--no-resume",
        action="store_true",
        help="Live mode: ignore checkpoint files and start fresh",
    )
    p.add_argument(
        "--include-details",
        action="store_true",
        help="Keep snapshot/skills columns (local mode only)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.live:
        token = _token()
        if not token:
            sys.exit("Live mode requires GH_AW_INGESTION / GITHUB_TOKEN / GH_TOKEN in .env")
        out_path = args.out.resolve()
        df = collect_live(token, out_path, resume=not args.no_resume)
        print(f"done: {len(df)} repos in {out_path}")
        print(f"progress: {_progress_path(out_path)}")
        print(f"lock index: {_lock_index_path(out_path)}")
        return 0

    if args.snapshots:
        snapshot_dirs = [p.resolve() for p in args.snapshots]
    else:
        snapshot_dirs = discover_snapshot_dirs(args.data_root.resolve())
        if not snapshot_dirs:
            sys.exit(f"No snapshot folders found under {args.data_root}")

    print(f"using {len(snapshot_dirs)} snapshot(s)")
    detail_rows: list[dict[str, Any]] = []
    for d in snapshot_dirs:
        detail_rows.extend(scan_snapshot_dir(d))

    if not detail_rows:
        df = pd.DataFrame(columns=["repo_name", "repo_url"])
    else:
        df_detail = pd.DataFrame(detail_rows).sort_values(["repo_name", "snapshot"])
        df = df_detail.drop_duplicates("repo_name", keep="last")
        if args.include_details:
            df = df[["repo_name", "repo_url", "snapshot", "workflow_file", "skills"]]
        else:
            df = df[["repo_name", "repo_url"]]

    write_csv(df.reset_index(drop=True), args.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
