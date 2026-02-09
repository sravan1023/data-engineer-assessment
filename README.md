# Data Engineering Take-Home Assessment

A production-grade data engineering pipeline with two components:

1. **Product Documentation Pipeline** — Extract, consolidate, and ingest Snowflake documentation from XML sitemaps
2. **GitHub Contributor Analytics** — Analyze Apache Airflow contributors with scoring and tier ranking

## Features

- **Idempotent UPSERT operations** — Safe for repeated runs
- **Content change detection** — SHA-256 hashing with skip-on-unchanged
- **Observability & alerting** — PIPELINE_METRICS + ALERTS tables with 4 alert categories
- **Query optimization examples** — Cost vs. latency, compute vs. parallelism, complexity vs. speed
- **92 passing tests** — Unit + integration + data quality coverage
-  **Dual storage** — SQLite (local dev) / Snowflake (production)

## Setup


### 1. Configure Environment

Create a `.env` file:

```env
GITHUB_TOKEN="your_github_token_here"
SPREADSHEET_ID="your_google_spreadsheet_id"
```

### 2. Add Google Service Account

Place your `service_account.json` in the project root for Google Sheets API access.

## Usage

### Run the Pipeline

Open and execute `Tasks.ipynb` in Jupyter or VS Code.

**Part 1 — Documentation Pipeline:**
- Task 1: Sitemap extraction (6,620 URLs)
- Task 2: UPSERT consolidation
- Task 3: Content ingestion with throttling
- Task 4: Analytics queries (4a–4e)
- Task 5: Query optimization scenarios
- Task 6: pytest test suite
- Task 7: Observability & alerting
- Task 8: Google Sheets export

**Part 2 — GitHub Analytics:**
- Ingest 5 endpoints (commits, PRs, comments, issues, reviews)
- Transform: contributor scoring with tier ranking
- Export to Google Sheets


## Architecture

```
pipeline/
  ├── sitemap.py         # XML parsing with recursive traversal
  ├── normalize.py       # URL normalization
  ├── hashing.py         # SHA-256 content hashing
  ├── ingest.py          # HTTP fetching with retry/backoff
  ├── consolidate.py     # MERGE staging → master
  ├── throttle.py        # Rate-limiting constants
  ├── observability.py   # Metrics lifecycle + alerts
  ├── db.py              # DDL helpers, table schemas
  └── sheets_export.py   # Google Sheets API integration

tests/
  └── 6 test modules covering unit, integration, and data quality
```

## Key Results

| Metric | Value |
|--------|------:|
| URLs extracted | 6,620 |
| Fetch success rate | 100% |
| Test coverage | 92 tests passing |
| Contributors analyzed | 327 |
| Core contributors | 25 (7.6%) |


## License

Private assessment project.
