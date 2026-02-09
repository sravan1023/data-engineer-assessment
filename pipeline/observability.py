"""
pipeline.observability — Metrics lifecycle and alert evaluation.

Threshold Rationale
-------------------
FAILURE_RATE_WARNING  (10 %)   — Healthy crawls succeed >95 %. 10 % signals
    intermittent issues (CDN flaps, rate-limiting) worth investigating.
FAILURE_RATE_CRITICAL (25 %)   — Coverage is materially incomplete at 25 %.
STALENESS_WARNING_HOURS  (24)  — Pipeline runs daily; 24 h = missed schedule.
STALENESS_CRITICAL_HOURS (72)  — 3 days stale = unreliable for downstream.
EMPTY_RESULT_MIN_ROWS    (1)   — Zero rows always suspicious.
PERF_DEGRADATION_FACTOR (2.0)  — 2× filters noise while catching real slowdowns.
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from pipeline.db import METRICS_TABLE

FAILURE_RATE_WARNING = 10.0
FAILURE_RATE_CRITICAL = 25.0
STALENESS_WARNING_HOURS = 24
STALENESS_CRITICAL_HOURS = 72
EMPTY_RESULT_MIN_ROWS = 1
PERF_DEGRADATION_FACTOR = 2.0



def start_pipeline_run(stage: str) -> dict:
    """Begin a new pipeline run.  Returns a metrics dict to populate."""
    return {
        "run_id": str(uuid.uuid4()),
        "run_start": datetime.now(timezone.utc),
        "run_end": None,
        "duration_seconds": None,
        "stage": stage,
        "urls_discovered": 0,
        "urls_inserted": 0,
        "urls_updated": 0,
        "fetch_success": 0,
        "fetch_failed": 0,
        "fetch_timeout": 0,
        "fetch_skipped": 0,
        "failure_rate_pct": None,
        "avg_response_ms": None,
        "status": "running",
        "error_message": None,
    }


def finish_pipeline_run(metrics: dict) -> dict:
    """Finalise metrics: compute duration, failure rate, mark completed."""
    metrics["run_end"] = datetime.now(timezone.utc)
    elapsed = (metrics["run_end"] - metrics["run_start"]).total_seconds()
    metrics["duration_seconds"] = round(elapsed, 2)

    total_fetches = (
        metrics["fetch_success"]
        + metrics["fetch_failed"]
        + metrics["fetch_timeout"]
    )
    if total_fetches > 0:
        metrics["failure_rate_pct"] = round(
            (metrics["fetch_failed"] + metrics["fetch_timeout"]) / total_fetches * 100,
            2,
        )
    else:
        metrics["failure_rate_pct"] = 0.0

    if metrics["status"] == "running":
        metrics["status"] = "completed"

    return metrics



def _make_alert(run_id, severity, category, condition, message, metric_value, threshold):
    """Build a single alert dict."""
    return {
        "alert_id": str(uuid.uuid4()),
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc),
        "severity": severity,
        "category": category,
        "condition_name": condition,
        "message": message,
        "metric_value": metric_value,
        "threshold": threshold,
    }


def evaluate_alerts(metrics: dict, historical_avg_duration: Optional[float] = None) -> list[dict]:
    """
    Evaluate alert conditions against a finished metrics dict.
    Returns zero or more alert dicts.
    """
    alerts: list[dict] = []
    run_id = metrics["run_id"]
    failure_rate = metrics.get("failure_rate_pct", 0.0) or 0.0

    # 1. Anomalous failure rate
    if failure_rate >= FAILURE_RATE_CRITICAL:
        alerts.append(_make_alert(
            run_id, "CRITICAL", "failure_rate", "failure_rate_critical",
            f"Failure rate {failure_rate:.1f}% exceeds critical threshold "
            f"({FAILURE_RATE_CRITICAL}%)",
            failure_rate, FAILURE_RATE_CRITICAL,
        ))
    elif failure_rate >= FAILURE_RATE_WARNING:
        alerts.append(_make_alert(
            run_id, "WARNING", "failure_rate", "failure_rate_warning",
            f"Failure rate {failure_rate:.1f}% exceeds warning threshold "
            f"({FAILURE_RATE_WARNING}%)",
            failure_rate, FAILURE_RATE_WARNING,
        ))

    # 2. Empty result set
    total_rows = metrics["urls_inserted"] + metrics["urls_updated"]
    if total_rows < EMPTY_RESULT_MIN_ROWS:
        alerts.append(_make_alert(
            run_id, "CRITICAL", "empty_results", "empty_result_set",
            f"Pipeline produced {total_rows} rows (minimum expected: "
            f"{EMPTY_RESULT_MIN_ROWS})",
            float(total_rows), float(EMPTY_RESULT_MIN_ROWS),
        ))

    # 3. Performance degradation
    duration = metrics.get("duration_seconds")
    if duration is not None and historical_avg_duration is not None and historical_avg_duration > 0:
        ratio = duration / historical_avg_duration
        if ratio >= PERF_DEGRADATION_FACTOR:
            alerts.append(_make_alert(
                run_id, "WARNING", "performance", "performance_degradation",
                f"Run took {duration:.1f}s — {ratio:.1f}× the historical "
                f"average ({historical_avg_duration:.1f}s)",
                duration, historical_avg_duration * PERF_DEGRADATION_FACTOR,
            ))

    return alerts


def evaluate_staleness_alert(cursor) -> list[dict]:
    """
    Check whether the most recent pipeline run is stale.
    Returns zero or more alerts.
    """
    cursor.execute(f"""
        SELECT MAX(RUN_END) AS LAST_RUN
        FROM {METRICS_TABLE}
        WHERE STATUS = 'completed'
    """)
    row = cursor.fetchone()

    if row is None or row[0] is None:
        return [_make_alert(
            None, "CRITICAL", "staleness", "no_completed_runs",
            "No completed pipeline runs found in PIPELINE_METRICS",
            None, None,
        )]

    last_run = row[0]
    now = datetime.now(timezone.utc)

    if hasattr(last_run, "tzinfo") and last_run.tzinfo is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    hours_since = (now - last_run).total_seconds() / 3600
    alerts: list[dict] = []

    if hours_since >= STALENESS_CRITICAL_HOURS:
        alerts.append(_make_alert(
            None, "CRITICAL", "staleness", "pipeline_stale_critical",
            f"Last successful run was {hours_since:.1f}h ago "
            f"(critical threshold: {STALENESS_CRITICAL_HOURS}h)",
            hours_since, float(STALENESS_CRITICAL_HOURS),
        ))
    elif hours_since >= STALENESS_WARNING_HOURS:
        alerts.append(_make_alert(
            None, "WARNING", "staleness", "pipeline_stale_warning",
            f"Last successful run was {hours_since:.1f}h ago "
            f"(warning threshold: {STALENESS_WARNING_HOURS}h)",
            hours_since, float(STALENESS_WARNING_HOURS),
        ))

    return alerts
