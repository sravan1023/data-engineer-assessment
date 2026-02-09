"""
Integration tests — End-to-end consolidation, idempotency, observability & alerting.
"""

import hashlib
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.sitemap import parse_sitemap
from pipeline.db import (
    STAGING_TABLE,
    MASTER_TABLE,
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
    start_pipeline_run,
    finish_pipeline_run,
    evaluate_alerts,
    evaluate_staleness_alert,
    _make_alert,
)
from tests.conftest import URLSET_XML


class TestEndToEndConsolidation:
    """
    Integration test: parse sitemaps → create tables → merge staging → master.
    All HTTP and Snowflake calls are mocked.
    """

    @patch("pipeline.sitemap.time.sleep")
    @patch("pipeline.sitemap.fetch_xml")
    def test_full_pipeline_mock(self, mock_fetch_xml, mock_sleep):
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        records = parse_sitemap("https://example.com/sitemap.xml")
        assert len(records) == 2

        cursor = MagicMock()
        create_staging_table(cursor)
        create_master_table(cursor)
        create_content_table(cursor)
        assert cursor.execute.call_count == 3

        cursor.reset_mock()
        cursor.fetchone.return_value = (2, 0)
        result = merge_staging_to_master(cursor)
        assert cursor.execute.call_count == 1
        assert result == (2, 0)

    @patch("pipeline.sitemap.time.sleep")
    @patch("pipeline.sitemap.fetch_xml")
    def test_merge_sql_references_correct_tables(self, mock_fetch_xml, mock_sleep):
        cursor = MagicMock()
        cursor.fetchone.return_value = (0, 0)
        merge_staging_to_master(cursor)
        sql_executed = cursor.execute.call_args[0][0]
        assert STAGING_TABLE in sql_executed
        assert MASTER_TABLE in sql_executed


class TestIdempotency:
    """Running the full consolidation twice produces the same row counts."""

    @patch("pipeline.sitemap.time.sleep")
    @patch("pipeline.sitemap.fetch_xml")
    def test_double_run_same_row_count(self, mock_fetch_xml, mock_sleep):
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)

        records_run1 = parse_sitemap("https://example.com/sitemap.xml")
        cursor = MagicMock()
        cursor.fetchone.return_value = (2, 0)
        result1 = merge_staging_to_master(cursor)

        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        records_run2 = parse_sitemap("https://example.com/sitemap.xml")
        cursor.reset_mock()
        cursor.fetchone.return_value = (0, 2)
        result2 = merge_staging_to_master(cursor)

        assert len(records_run1) == len(records_run2)
        assert result1[0] == 2
        assert result2[0] == 0

    @patch("pipeline.sitemap.time.sleep")
    @patch("pipeline.sitemap.fetch_xml")
    def test_parse_sitemap_idempotent(self, mock_fetch_xml, mock_sleep):
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        run1 = parse_sitemap("https://example.com/sitemap.xml")
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        run2 = parse_sitemap("https://example.com/sitemap.xml")
        assert run1 == run2


class TestMetricsTableDDL:
    def test_create_metrics_table(self):
        cursor = MagicMock()
        create_metrics_table(cursor)
        ddl = cursor.execute.call_args[0][0].upper()
        assert "PIPELINE_METRICS" in ddl
        assert "PRIMARY KEY" in ddl
        for col in [
            "RUN_ID", "RUN_START", "RUN_END", "DURATION_SECONDS", "STAGE",
            "URLS_DISCOVERED", "URLS_INSERTED", "URLS_UPDATED",
            "FETCH_SUCCESS", "FETCH_FAILED", "FETCH_TIMEOUT", "FETCH_SKIPPED",
            "FAILURE_RATE_PCT", "AVG_RESPONSE_MS", "STATUS", "ERROR_MESSAGE",
        ]:
            assert col in ddl, f"Missing column {col}"


class TestAlertsTableDDL:
    def test_create_alerts_table(self):
        cursor = MagicMock()
        create_alerts_table(cursor)
        ddl = cursor.execute.call_args[0][0].upper()
        assert "ALERTS" in ddl
        assert "PRIMARY KEY" in ddl
        for col in [
            "ALERT_ID", "RUN_ID", "CREATED_AT", "SEVERITY", "CATEGORY",
            "CONDITION_NAME", "MESSAGE", "METRIC_VALUE", "THRESHOLD",
            "ACKNOWLEDGED",
        ]:
            assert col in ddl, f"Missing column {col}"

    def test_alerts_severity_not_null(self):
        cursor = MagicMock()
        create_alerts_table(cursor)
        ddl = cursor.execute.call_args[0][0].upper()
        assert "SEVERITY" in ddl
        assert "NOT NULL" in ddl


class TestMetricsLifecycle:

    def test_start_returns_running_status(self):
        m = start_pipeline_run("extraction")
        assert m["status"] == "running"
        assert m["stage"] == "extraction"
        assert m["run_id"] is not None
        assert m["run_start"] is not None

    def test_finish_sets_completed(self):
        m = start_pipeline_run("consolidation")
        m["fetch_success"] = 90
        m["fetch_failed"] = 10
        m = finish_pipeline_run(m)
        assert m["status"] == "completed"
        assert m["run_end"] is not None
        assert m["duration_seconds"] >= 0

    def test_failure_rate_computed(self):
        m = start_pipeline_run("ingestion")
        m["fetch_success"] = 80
        m["fetch_failed"] = 15
        m["fetch_timeout"] = 5
        m = finish_pipeline_run(m)
        assert m["failure_rate_pct"] == 20.0

    def test_failure_rate_zero_when_no_fetches(self):
        m = start_pipeline_run("extraction")
        m = finish_pipeline_run(m)
        assert m["failure_rate_pct"] == 0.0

    def test_save_metrics_executes_insert(self):
        m = start_pipeline_run("test")
        m = finish_pipeline_run(m)
        cursor = MagicMock()
        save_metrics(cursor, m)
        assert cursor.execute.call_count == 1
        sql = cursor.execute.call_args[0][0].upper()
        assert "INSERT INTO" in sql
        assert "PIPELINE_METRICS" in sql


class TestEvaluateAlerts:

    def _make_metrics(self, **overrides):
        m = start_pipeline_run("test")
        m["urls_inserted"] = 100
        m["urls_updated"] = 0
        m["fetch_success"] = 100
        m["fetch_failed"] = 0
        m["fetch_timeout"] = 0
        m.update(overrides)
        return finish_pipeline_run(m)

    def test_no_alert_on_healthy_run(self):
        m = self._make_metrics()
        alerts = evaluate_alerts(m)
        assert len(alerts) == 0

    def test_warning_on_moderate_failure_rate(self):
        m = self._make_metrics(fetch_success=85, fetch_failed=15)
        alerts = evaluate_alerts(m)
        fr_alerts = [a for a in alerts if a["category"] == "failure_rate"]
        assert len(fr_alerts) == 1
        assert fr_alerts[0]["severity"] == "WARNING"

    def test_critical_on_high_failure_rate(self):
        m = self._make_metrics(fetch_success=70, fetch_failed=30)
        alerts = evaluate_alerts(m)
        fr_alerts = [a for a in alerts if a["category"] == "failure_rate"]
        assert len(fr_alerts) == 1
        assert fr_alerts[0]["severity"] == "CRITICAL"

    def test_boundary_exactly_at_warning(self):
        m = self._make_metrics(fetch_success=90, fetch_failed=10)
        alerts = evaluate_alerts(m)
        fr_alerts = [a for a in alerts if a["category"] == "failure_rate"]
        assert len(fr_alerts) == 1
        assert fr_alerts[0]["severity"] == "WARNING"

    def test_boundary_exactly_at_critical(self):
        m = self._make_metrics(fetch_success=75, fetch_failed=25)
        alerts = evaluate_alerts(m)
        fr_alerts = [a for a in alerts if a["category"] == "failure_rate"]
        assert len(fr_alerts) == 1
        assert fr_alerts[0]["severity"] == "CRITICAL"

    def test_empty_result_set_triggers_critical(self):
        m = self._make_metrics(urls_inserted=0, urls_updated=0)
        alerts = evaluate_alerts(m)
        empty_alerts = [a for a in alerts if a["category"] == "empty_results"]
        assert len(empty_alerts) == 1
        assert empty_alerts[0]["severity"] == "CRITICAL"

    def test_no_empty_alert_when_rows_present(self):
        m = self._make_metrics(urls_inserted=5, urls_updated=10)
        alerts = evaluate_alerts(m)
        empty_alerts = [a for a in alerts if a["category"] == "empty_results"]
        assert len(empty_alerts) == 0

    def test_perf_degradation_alert(self):
        m = self._make_metrics()
        m["duration_seconds"] = 120.0
        alerts = evaluate_alerts(m, historical_avg_duration=50.0)
        perf_alerts = [a for a in alerts if a["category"] == "performance"]
        assert len(perf_alerts) == 1
        assert perf_alerts[0]["severity"] == "WARNING"

    def test_no_perf_alert_within_normal_range(self):
        m = self._make_metrics()
        m["duration_seconds"] = 55.0
        alerts = evaluate_alerts(m, historical_avg_duration=50.0)
        perf_alerts = [a for a in alerts if a["category"] == "performance"]
        assert len(perf_alerts) == 0

    def test_no_perf_alert_when_no_history(self):
        m = self._make_metrics()
        m["duration_seconds"] = 999.0
        alerts = evaluate_alerts(m, historical_avg_duration=None)
        perf_alerts = [a for a in alerts if a["category"] == "performance"]
        assert len(perf_alerts) == 0

    def test_multiple_alerts_at_once(self):
        m = self._make_metrics(
            fetch_success=50, fetch_failed=50,
            urls_inserted=0, urls_updated=0,
        )
        alerts = evaluate_alerts(m)
        categories = {a["category"] for a in alerts}
        assert "failure_rate" in categories
        assert "empty_results" in categories


class TestStalenessAlert:

    def test_no_runs_triggers_critical(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (None,)
        alerts = evaluate_staleness_alert(cursor)
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "CRITICAL"
        assert alerts[0]["condition_name"] == "no_completed_runs"

    def test_recent_run_no_alert(self):
        cursor = MagicMock()
        recent = datetime.now(timezone.utc) - timedelta(hours=1)
        cursor.fetchone.return_value = (recent,)
        alerts = evaluate_staleness_alert(cursor)
        assert len(alerts) == 0

    def test_stale_warning(self):
        cursor = MagicMock()
        stale = datetime.now(timezone.utc) - timedelta(hours=30)
        cursor.fetchone.return_value = (stale,)
        alerts = evaluate_staleness_alert(cursor)
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "WARNING"
        assert "stale_warning" in alerts[0]["condition_name"]

    def test_stale_critical(self):
        cursor = MagicMock()
        very_stale = datetime.now(timezone.utc) - timedelta(hours=80)
        cursor.fetchone.return_value = (very_stale,)
        alerts = evaluate_staleness_alert(cursor)
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "CRITICAL"
        assert "stale_critical" in alerts[0]["condition_name"]

    def test_naive_datetime_treated_as_utc(self):
        cursor = MagicMock()
        naive = datetime.utcnow() - timedelta(hours=30)
        cursor.fetchone.return_value = (naive,)
        alerts = evaluate_staleness_alert(cursor)
        assert len(alerts) == 1
        assert alerts[0]["severity"] == "WARNING"


class TestHistoricalAvgDuration:

    def test_returns_float(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (42.5,)
        avg = get_historical_avg_duration(cursor)
        assert avg == 42.5

    def test_returns_none_when_no_data(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (None,)
        avg = get_historical_avg_duration(cursor)
        assert avg is None

    def test_sql_queries_metrics_table(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (10.0,)
        get_historical_avg_duration(cursor)
        sql = cursor.execute.call_args[0][0].upper()
        assert "PIPELINE_METRICS" in sql
        assert "AVG" in sql


class TestSaveAlerts:

    def test_no_alerts_no_inserts(self):
        cursor = MagicMock()
        save_alerts(cursor, [])
        cursor.execute.assert_not_called()

    def test_multiple_alerts_inserted(self):
        cursor = MagicMock()
        alerts = [
            _make_alert("r1", "WARNING", "cat1", "cond1", "msg1", 1.0, 2.0),
            _make_alert("r2", "CRITICAL", "cat2", "cond2", "msg2", 3.0, 4.0),
        ]
        save_alerts(cursor, alerts)
        assert cursor.execute.call_count == 2
        for call_args in cursor.execute.call_args_list:
            sql = call_args[0][0].upper()
            assert "INSERT INTO" in sql
            assert "ALERTS" in sql


class TestAlertDictStructure:

    def test_all_keys_present(self):
        a = _make_alert("run1", "WARNING", "test", "cond", "msg", 1.0, 2.0)
        expected_keys = {
            "alert_id", "run_id", "created_at", "severity", "category",
            "condition_name", "message", "metric_value", "threshold",
        }
        assert expected_keys == set(a.keys())

    def test_alert_id_is_uuid(self):
        import uuid
        a = _make_alert("run1", "CRITICAL", "test", "cond", "msg", 0, 0)
        uuid.UUID(a["alert_id"])

    def test_created_at_is_utc(self):
        a = _make_alert("run1", "WARNING", "test", "cond", "msg", 0, 0)
        assert a["created_at"].tzinfo == timezone.utc
