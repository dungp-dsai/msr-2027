# gh-aw workflow corpus (all repos)

Pull `.md` + `.lock.yml` pairs for every repo in `data/repos` (same layout as `data/2026-08-11`, **one folder**, no year-month split).

```bash
# Smoke test
python -m gh_data_ingestion.ingest_gh_aw_workflows --limit 20

# Full corpus (~282k repos in data/repos/*.parquet)
python -m gh_data_ingestion.ingest_gh_aw_workflows

# Faster listing (100 repos/GraphQL query by default; tune if rate-limited)
python -m gh_data_ingestion.ingest_gh_aw_workflows --batch-size 100 --batch-sleep 0.1
```

Output: `data/gh_aw_workflows/{ai_agentic_workflows.csv,workflow-markdowns/,workflow-yamls/}`

Resume is automatic and durable: after **every repo** and **every saved pair**, progress is flushed (`scanned_repos.txt`, `pairs_index.jsonl`, `state.json`). Ctrl-C / network drop → re-run the same command. Failed GraphQL batches stay unscanned and retry. `--no-resume` clears progress files. Optional multi-PAT: `GH_AW_INGESTION_TOKENS=ghp_a,ghp_b` in `gh_data_ingestion/.env`.

---

# Workflow run extraction

Extract GitHub Actions workflow runs (metadata, jobs, console logs, job logs, artifacts) via `extract_wflow_run.py`.

## Setup

1. Put a PAT in `gh_data_ingestion/.env`:

```env
GH_AW_INGESTION=ghp_...   # or github_pat_...
```

Also accepted: `GITHUB_TOKEN` / `GH_TOKEN`.

2. Token choice:

| Goal | Token |
|------|--------|
| Other people's **public** repos | **Classic** (`ghp_...`) — no special scopes needed for public read |
| Your own / selected private repos | **Fine-grained** (`github_pat_...`) with **Actions: Read** (+ **Contents: Read** if private) |

Fine-grained tokens only work on repos you explicitly grant access to. They cannot mine arbitrary third-party public repos.

3. Install deps from repo root:

```bash
uv sync
```

## Commands

From the repo root:

```bash
# One run (logs + artifacts)
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123456789

# Multiple specific runs
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 111 --run-id 222

# Last N runs (default limit 10)
python -m gh_data_ingestion.extract_wflow_run owner/repo --limit 20

# Filter by workflow file / status / event / branch / created
python -m gh_data_ingestion.extract_wflow_run owner/repo \
  --workflow agent.yml --status completed --limit 20

python -m gh_data_ingestion.extract_wflow_run owner/repo \
  --event pull_request --branch main --created '>=2025-01-01' --limit 50

# List matching runs only (writes runs_index.json, no download)
python -m gh_data_ingestion.extract_wflow_run owner/repo --limit 50 --list-only

# Only known gh-aw artifacts (agent, activation, firewall-audit-logs, ...)
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 --gh-aw-artifacts-only

# Custom artifact names
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 \
  --artifact-names agent activation

# Skip parts of the download
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 --no-artifacts
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 --no-job-logs
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 --no-logs

# Keep raw zip archives under raw/
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 --keep-zips

# Custom output directory
python -m gh_data_ingestion.extract_wflow_run owner/repo --run-id 123 --out data/workflow_runs
```

## Output layout

Default: `data/workflow_runs/{owner}__{repo}__{run_id}/`

```
run.json              # run metadata
timing.json           # billable usage
jobs.json             # jobs + steps
approvals.json        # if present
summary.json          # short index of what was extracted
console_logs/         # unzipped Actions console logs
job_logs/             # per-job plain-text logs
artifacts/{name}/     # unzipped artifacts
artifacts.json        # artifact listing
attempts/{n}/         # prior attempts (if re-run)
raw/                  # zip archives (only with --keep-zips)
```

Listing without `--run-id` also writes:

```
data/workflow_runs/{owner}__{repo}__runs_index.json
```

## Notes

- Logs and artifacts expire after GitHub retention (often ~90 days). Missing ones are skipped with a warning.
- Authenticated requests have higher rate limits; the script backs off on 403/429.
