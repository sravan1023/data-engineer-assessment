"""
pipeline — Sitemap-to-Snowflake ingestion pipeline package.

Re-exports every public symbol so that existing code using
``import pipeline`` or ``from pipeline import parse_sitemap``
continues to work without changes.
"""

from pipeline.sitemap import (
    SITEMAP_NS,
    HEADERS,
    fetch_xml,
    is_sitemap_index,
    parse_sitemap,
)

from pipeline.normalize import normalize_url

from pipeline.hashing import compute_hash

from pipeline.throttle import (
    FETCH_BATCH_SIZE,
    MAX_WORKERS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    BACKOFF_BASE,
    THROTTLE_DELAY,
    MAX_CONTENT_SIZE,
    MAX_CONSECUTIVE_FAILURES,
    TRANSIENT_STATUS_CODES,
    FETCH_HEADERS,
)

from pipeline.ingest import fetch_document

from pipeline.db import (
    STAGING_TABLE,
    MASTER_TABLE,
    CONTENT_TABLE,
    METRICS_TABLE,
    ALERTS_TABLE,
    create_staging_table,
    create_master_table,
    create_content_table,
    create_metrics_table,
    create_alerts_table,
    save_metrics,
    save_alerts,
    get_historical_avg_duration,
)

from pipeline.consolidate import merge_staging_to_master

from pipeline.observability import (
    FAILURE_RATE_WARNING,
    FAILURE_RATE_CRITICAL,
    STALENESS_WARNING_HOURS,
    STALENESS_CRITICAL_HOURS,
    EMPTY_RESULT_MIN_ROWS,
    PERF_DEGRADATION_FACTOR,
    start_pipeline_run,
    finish_pipeline_run,
    evaluate_alerts,
    evaluate_staleness_alert,
    _make_alert,
)

from pipeline.sheets_export import (
    SCOPES,
    QUERY_TITLES,
    export_to_google_sheets,
)
