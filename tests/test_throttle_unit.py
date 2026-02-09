"""
Unit tests — Throttling / rate-limiting behavior (fetch_document).
"""

from unittest.mock import MagicMock, patch

import requests

from pipeline.ingest import fetch_document
from pipeline.throttle import BACKOFF_BASE, MAX_RETRIES, THROTTLE_DELAY


class TestFetchDocumentThrottling:
    """pipeline.ingest.fetch_document — delay / backoff behavior."""

    @patch("pipeline.ingest.time.sleep")
    @patch("pipeline.ingest.requests.get")
    def test_success_includes_throttle_delay(self, mock_get, mock_sleep):
        """After a successful fetch, time.sleep(THROTTLE_DELAY) is called."""
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.status_code = 200
        ctx.iter_content = MagicMock(return_value=["<html>OK</html>"])
        mock_get.return_value = ctx

        result = fetch_document("https://example.com/page")
        assert result["fetch_status"] == "success"
        mock_sleep.assert_any_call(THROTTLE_DELAY)

    @patch("pipeline.ingest.time.sleep")
    @patch("pipeline.ingest.requests.get")
    def test_transient_error_triggers_backoff(self, mock_get, mock_sleep):
        """A 503 on first attempt → backoff sleep before retry."""
        ctx_fail = MagicMock()
        ctx_fail.__enter__ = MagicMock(return_value=ctx_fail)
        ctx_fail.__exit__ = MagicMock(return_value=False)
        ctx_fail.status_code = 503

        ctx_ok = MagicMock()
        ctx_ok.__enter__ = MagicMock(return_value=ctx_ok)
        ctx_ok.__exit__ = MagicMock(return_value=False)
        ctx_ok.status_code = 200
        ctx_ok.iter_content = MagicMock(return_value=["OK"])

        mock_get.side_effect = [ctx_fail, ctx_ok]

        result = fetch_document("https://example.com/page")
        assert result["fetch_status"] == "success"
        assert result["retry_count"] == 1
        mock_sleep.assert_any_call(BACKOFF_BASE ** 1)

    @patch("pipeline.ingest.time.sleep")
    @patch("pipeline.ingest.requests.get")
    def test_all_retries_exhausted_returns_failed(self, mock_get, mock_sleep):
        """All attempts return transient 500 → failed with MAX_RETRIES."""
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.status_code = 500

        mock_get.return_value = ctx

        result = fetch_document("https://example.com/page")
        assert result["fetch_status"] == "failed"
        assert result["retry_count"] == MAX_RETRIES

    @patch("pipeline.ingest.time.sleep")
    @patch("pipeline.ingest.requests.get", side_effect=requests.exceptions.Timeout("timed out"))
    def test_timeout_classified_correctly(self, mock_get, mock_sleep):
        result = fetch_document("https://example.com/slow")
        assert result["fetch_status"] == "timeout"
        assert result["content"] is None

    @patch("pipeline.ingest.time.sleep")
    @patch("pipeline.ingest.requests.get")
    def test_non_transient_error_no_retry(self, mock_get, mock_sleep):
        """404 is non-transient → single attempt, no retries."""
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.status_code = 404

        mock_get.return_value = ctx

        result = fetch_document("https://example.com/missing")
        assert result["fetch_status"] == "failed"
        assert result["retry_count"] == 0
        assert result["http_status"] == 404
