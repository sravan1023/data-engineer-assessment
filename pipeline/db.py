"""
pipeline.db — Snowflake DDL helpers, metrics/alerts persistence, table names.
"""

from typing import Optional

STAGING_TABLE = "CANDIDATE_SSP_SITEMAP_STAGING"
MASTER_TABLE = "CANDIDATE_SSP_DOCS_MASTER"
CONTENT_TABLE = "CANDIDATE_SSP_DOCUMENT_CONTENT"
METRICS_TABLE = "PIPELINE_METRICS"
ALERTS_TABLE = "ALERTS"


def create_staging_table(cursor):
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (
        LOC              VARCHAR(2000) NOT NULL,
        LASTMOD          VARCHAR(50),
        SOURCE_SITEMAP   VARCHAR(500),
        SITEMAP_TYPE     VARCHAR(50),
        EXTRACTED_AT     TIMESTAMP_NTZ
    )
    """)


def create_master_table(cursor):
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {MASTER_TABLE} (
        LOC            VARCHAR(2000) NOT NULL PRIMARY KEY,
        LASTMOD        VARCHAR(50),
        SOURCES        VARCHAR(4000),
        FIRST_SEEN_AT  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
        LAST_SEEN_AT   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
    )
    """)


def create_content_table(cursor):
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {CONTENT_TABLE} (
        LOC                   VARCHAR(2000) NOT NULL PRIMARY KEY,
        CONTENT               VARCHAR(16777216),
        CONTENT_HASH          VARCHAR(64),
        CONTENT_SIZE_BYTES    NUMBER,
        HTTP_STATUS           NUMBER,
        FETCH_STATUS          VARCHAR(30),
        RETRY_COUNT           NUMBER DEFAULT 0,
        CONSECUTIVE_FAILURES  NUMBER DEFAULT 0,
        FIRST_FETCHED_AT      TIMESTAMP_NTZ,
        LAST_FETCHED_AT       TIMESTAMP_NTZ,
        LAST_SUCCESS_AT       TIMESTAMP_NTZ
    )
    """)



def create_metrics_table(cursor):
    """Create PIPELINE_METRICS to capture run-level statistics."""
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {METRICS_TABLE} (
        RUN_ID                VARCHAR(36)    NOT NULL PRIMARY KEY,
        RUN_START             TIMESTAMP_NTZ  NOT NULL,
        RUN_END               TIMESTAMP_NTZ,
        DURATION_SECONDS      NUMBER,
        STAGE                 VARCHAR(50)    NOT NULL,
        URLS_DISCOVERED       NUMBER         DEFAULT 0,
        URLS_INSERTED         NUMBER         DEFAULT 0,
        URLS_UPDATED          NUMBER         DEFAULT 0,
        FETCH_SUCCESS         NUMBER         DEFAULT 0,
        FETCH_FAILED          NUMBER         DEFAULT 0,
        FETCH_TIMEOUT         NUMBER         DEFAULT 0,
        FETCH_SKIPPED         NUMBER         DEFAULT 0,
        FAILURE_RATE_PCT      FLOAT,
        AVG_RESPONSE_MS       FLOAT,
        STATUS                VARCHAR(20)    DEFAULT 'running',
        ERROR_MESSAGE         VARCHAR(4000)
    )
    """)


def create_alerts_table(cursor):
    """Create ALERTS to store triggered alert records."""
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {ALERTS_TABLE} (
        ALERT_ID       VARCHAR(36)    NOT NULL PRIMARY KEY,
        RUN_ID         VARCHAR(36),
        CREATED_AT     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
        SEVERITY       VARCHAR(10)    NOT NULL,
        CATEGORY       VARCHAR(50)    NOT NULL,
        CONDITION_NAME VARCHAR(100)   NOT NULL,
        MESSAGE        VARCHAR(4000)  NOT NULL,
        METRIC_VALUE   FLOAT,
        THRESHOLD      FLOAT,
        ACKNOWLEDGED   BOOLEAN        DEFAULT FALSE
    )
    """)



def save_metrics(cursor, metrics: dict):
    """INSERT a finalised metrics dict into PIPELINE_METRICS."""
    cursor.execute(
        f"""
        INSERT INTO {METRICS_TABLE} (
            RUN_ID, RUN_START, RUN_END, DURATION_SECONDS, STAGE,
            URLS_DISCOVERED, URLS_INSERTED, URLS_UPDATED,
            FETCH_SUCCESS, FETCH_FAILED, FETCH_TIMEOUT, FETCH_SKIPPED,
            FAILURE_RATE_PCT, AVG_RESPONSE_MS, STATUS, ERROR_MESSAGE
        ) VALUES (
            %(run_id)s, %(run_start)s, %(run_end)s, %(duration_seconds)s, %(stage)s,
            %(urls_discovered)s, %(urls_inserted)s, %(urls_updated)s,
            %(fetch_success)s, %(fetch_failed)s, %(fetch_timeout)s, %(fetch_skipped)s,
            %(failure_rate_pct)s, %(avg_response_ms)s, %(status)s, %(error_message)s
        )
        """,
        metrics,
    )


def save_alerts(cursor, alerts: list[dict]):
    """Batch-INSERT alert dicts into ALERTS table."""
    for alert in alerts:
        cursor.execute(
            f"""
            INSERT INTO {ALERTS_TABLE} (
                ALERT_ID, RUN_ID, CREATED_AT, SEVERITY, CATEGORY,
                CONDITION_NAME, MESSAGE, METRIC_VALUE, THRESHOLD, ACKNOWLEDGED
            ) VALUES (
                %(alert_id)s, %(run_id)s, %(created_at)s, %(severity)s, %(category)s,
                %(condition_name)s, %(message)s, %(metric_value)s, %(threshold)s, FALSE
            )
            """,
            alert,
        )


def get_historical_avg_duration(cursor) -> Optional[float]:
    """Return average DURATION_SECONDS of the last 10 completed runs, or None."""
    cursor.execute(f"""
        SELECT AVG(DURATION_SECONDS)
        FROM (
            SELECT DURATION_SECONDS
            FROM {METRICS_TABLE}
            WHERE STATUS = 'completed' AND DURATION_SECONDS IS NOT NULL
            ORDER BY RUN_END DESC
            LIMIT 10
        )
    """)
    row = cursor.fetchone()
    return float(row[0]) if row and row[0] is not None else None
