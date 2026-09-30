# Introduction

Replication package for research on GitHub agentic workflows.

# Setup

Requires Python 3.12+. From the repo root:

```bash
uv sync
```

Put a GitHub personal access token in `gh_data_ingestion/.env`:

```env
GH_AW_INGESTION=ghp_...
```

`GITHUB_TOKEN` and `GH_TOKEN` are also accepted. A classic token (`ghp_...`) is enough to read public repositories. A fine-grained token (`github_pat_...`) only works on repositories you grant it access to (Actions: Read, plus Contents: Read for private repos).

Pipeline commands are in [`gh_data_ingestion/instruction.md`](gh_data_ingestion/instruction.md).
