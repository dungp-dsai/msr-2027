"""Load collected gh-aw workflow markdown and lock YAML files."""

import hashlib
import math
import re
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]

MARKDOWN_DIR = "workflow-markdowns"
YAML_DIR = "workflow-yamls"
MARKDOWN_SUFFIX = ".md"
YAML_SUFFIX = ".lock.yml"
COLUMNS = ("workflow_markdown", "workflow_yml")


def resolve_folder(path: str) -> Path:
    folder = Path(path)
    if not folder.is_absolute():
        folder = REPO_ROOT / folder
    folder = folder.resolve()
    if not folder.is_dir():
        raise FileNotFoundError(folder)
    return folder


def is_snapshot(folder: Path) -> bool:
    return (folder / MARKDOWN_DIR).is_dir() and (folder / YAML_DIR).is_dir()


def snapshot_folders(folder: Path) -> list[Path]:
    if is_snapshot(folder):
        return [folder]
    return sorted(
        child for child in folder.iterdir() if child.is_dir() and is_snapshot(child)
    )


def file_stem(name: str) -> str:
    if name.endswith(YAML_SUFFIX):
        return name[: -len(YAML_SUFFIX)]
    if name.endswith(MARKDOWN_SUFFIX):
        return name[: -len(MARKDOWN_SUFFIX)]
    return name


def index_files(directory: Path, suffix: str) -> dict[str, Path]:
    return {
        file_stem(path.name): path
        for path in directory.iterdir()
        if path.is_file() and path.name.endswith(suffix)
    }


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def read_snapshot(folder: Path) -> list[tuple[str | None, str | None]]:
    markdowns = index_files(folder / MARKDOWN_DIR, MARKDOWN_SUFFIX)
    yamls = index_files(folder / YAML_DIR, YAML_SUFFIX)
    rows = []
    for stem in sorted(set(markdowns) | set(yamls)):
        markdown = read_text(markdowns[stem]) if stem in markdowns else None
        yaml = read_text(yamls[stem]) if stem in yamls else None
        rows.append((markdown, yaml))
    return rows


def load_gh_aw_config_files(path: str) -> pd.DataFrame:
    """Read every markdown/yaml pair under ``path`` (repo-relative or absolute).

    ``path`` may be one snapshot (``.../2026-04-16``) or a parent of snapshots
    (``data/gh-aw_raw``). Each row is one workflow file pair.
    """
    rows: list[tuple[str | None, str | None]] = []
    for snapshot in snapshot_folders(resolve_folder(path)):
        rows.extend(read_snapshot(snapshot))
    return pd.DataFrame(rows, columns=list(COLUMNS))


def load_workflows_info(path: str = "data/workflow_info/workflows.csv") -> pd.DataFrame:
    """Load Actions workflow records. Join to gh-aw on ``repo`` and ``yml_filename``."""
    file = Path(path)
    if not file.is_absolute():
        file = REPO_ROOT / file
    return pd.read_csv(file)


# Header written by data/gh-aw_raw/gh-aw-workflows.py before the file body.
HEADER_SEPARATOR = "=" * 80
_HEADER_LINE = re.compile(r"^# ([^:\n]+):\s*(.*?)\s*$", re.MULTILINE)


def _as_text(text) -> str | None:
    if isinstance(text, str):
        return text
    if text is None or text is pd.NA:
        return None
    try:
        if pd.isna(text):
            return None
    except (TypeError, ValueError):
        return None
    return None


def _header_block(text: str) -> str:
    index = text.find(HEADER_SEPARATOR)
    if index == -1:
        return text
    return text[:index]


def _header_field(text, label: str):
    body = _as_text(text)
    if body is None:
        return math.nan
    for match in _HEADER_LINE.finditer(_header_block(body)):
        if match.group(1) == label:
            value = match.group(2)
            return value if value else math.nan
    return math.nan


def parse_repo(text):
    return _header_field(text, "Repo")


def parse_workflow(text):
    return _header_field(text, "Workflow")


def parse_md_url(text):
    return _header_field(text, "MD URL")


def create_gh_aw_id(md_content: str):
    """Stable id for a markdown workflow. Same content always hashes to the same id."""
    markdown = _as_text(md_content)
    if not markdown:
        return math.nan
    return hashlib.sha256(markdown.encode("utf-8")).hexdigest()


def parse_yml_url(text):
    return _header_field(text, "Lock URL")


def parse_yml_filename(yml_url: str):
    """Last path segment of a lock URL, including ``.lock.yml``."""
    text = _as_text(yml_url)
    if not text:
        return math.nan
    path = text.split("?", 1)[0].split("#", 1)[0].rstrip("/")
    name = path.rsplit("/", 1)[-1]
    return name if name else math.nan


def parse_md_SHA(text):
    """SHA stored on the markdown file (`# SHA:` is that file's blob SHA)."""
    return _header_field(text, "SHA")


def parse_yml_SHA(text):
    """SHA stored on the lock YAML file (`# SHA:` is that file's blob SHA)."""
    return _header_field(text, "SHA")


def parse_colected_date(text):
    return _header_field(text, "Collected")


def clean_headers(text):
    """Drop the collector header, through the 80-equals separator and the blank line after it."""
    body = _as_text(text)
    if body is None:
        return math.nan
    for marker in (HEADER_SEPARATOR + "\r\n\r\n", HEADER_SEPARATOR + "\n\n"):
        index = body.find(marker)
        if index != -1:
            return body[index + len(marker) :]
    return math.nan
