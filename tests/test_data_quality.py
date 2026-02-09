"""
Data-quality tests — Schema validation, null handling, constraint enforcement.
"""

import hashlib
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

from pipeline.sitemap import parse_sitemap
from pipeline.hashing import compute_hash
from pipeline.ingest import fetch_document
from pipeline.db import (
    create_staging_table,
    create_master_table,
    create_content_table,
)
from pipeline.consolidate import merge_staging_to_master
from tests.conftest import SITEMAP_NS_URI


class TestSchemaValidation:
    """Verify that DDL produced by pipeline helpers conforms to expected schema."""

    def _capture_ddl(self, create_fn):
        cursor = MagicMock()
        create_fn(cursor)
        return cursor.execute.call_args[0][0].upper()

    def test_staging_table_has_not_null_on_loc(self):
        ddl = self._capture_ddl(create_staging_table)
        assert "LOC" in ddl
        assert "NOT NULL" in ddl

    def test_master_table_has_primary_key(self):
        ddl = self._capture_ddl(create_master_table)
        assert "PRIMARY KEY" in ddl

    def test_content_table_has_primary_key(self):
        ddl = self._capture_ddl(create_content_table)
        assert "PRIMARY KEY" in ddl

    def test_staging_table_columns(self):
        ddl = self._capture_ddl(create_staging_table)
        for col in ["LOC", "LASTMOD", "SOURCE_SITEMAP", "SITEMAP_TYPE", "EXTRACTED_AT"]:
            assert col in ddl, f"Missing column {col} in staging DDL"

    def test_master_table_columns(self):
        ddl = self._capture_ddl(create_master_table)
        for col in ["LOC", "LASTMOD", "SOURCES", "FIRST_SEEN_AT", "LAST_SEEN_AT"]:
            assert col in ddl, f"Missing column {col} in master DDL"

    def test_content_table_columns(self):
        ddl = self._capture_ddl(create_content_table)
        for col in [
            "LOC", "CONTENT", "CONTENT_HASH", "CONTENT_SIZE_BYTES",
            "HTTP_STATUS", "FETCH_STATUS", "RETRY_COUNT",
            "CONSECUTIVE_FAILURES", "FIRST_FETCHED_AT", "LAST_FETCHED_AT",
            "LAST_SUCCESS_AT",
        ]:
            assert col in ddl, f"Missing column {col} in content DDL"

    def test_content_table_default_values(self):
        ddl = self._capture_ddl(create_content_table)
        assert "DEFAULT 0" in ddl


class TestNullHandling:
    """Verify pipeline functions handle None / missing data gracefully."""

    @patch("pipeline.sitemap.fetch_xml")
    def test_missing_lastmod_yields_none(self, mock_fetch_xml):
        xml = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="{SITEMAP_NS_URI}">
  <url><loc>https://example.com/no-mod</loc></url>
</urlset>""".encode("utf-8")
        mock_fetch_xml.return_value = ET.fromstring(xml)
        results = parse_sitemap("https://example.com/sitemap.xml")
        assert results[0]["lastmod"] is None

    @patch("pipeline.sitemap.fetch_xml")
    def test_missing_loc_element_skipped(self, mock_fetch_xml):
        xml = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="{SITEMAP_NS_URI}">
  <url><lastmod>2025-01-01</lastmod></url>
  <url><loc>https://example.com/ok</loc></url>
</urlset>""".encode("utf-8")
        mock_fetch_xml.return_value = ET.fromstring(xml)
        results = parse_sitemap("https://example.com/sitemap.xml")
        assert len(results) == 1
        assert results[0]["loc"] == "https://example.com/ok"

    def test_compute_hash_empty_content(self):
        h = compute_hash("")
        assert h == hashlib.sha256(b"").hexdigest()

    @patch("pipeline.ingest.time.sleep")
    @patch("pipeline.ingest.requests.get")
    def test_fetch_document_failure_returns_none_content(self, mock_get, mock_sleep):
        ctx = MagicMock()
        ctx.__enter__ = MagicMock(return_value=ctx)
        ctx.__exit__ = MagicMock(return_value=False)
        ctx.status_code = 403

        mock_get.return_value = ctx
        result = fetch_document("https://example.com/forbidden")
        assert result["content"] is None
        assert result["content_hash"] is None
        assert result["content_size_bytes"] == 0


class TestConstraintEnforcement:
    """Verify MERGE and DDL enforce logical constraints."""

    def test_merge_uses_upsert_pattern(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (0, 0)
        merge_staging_to_master(cursor)
        sql = cursor.execute.call_args[0][0].upper()
        assert "WHEN MATCHED" in sql
        assert "WHEN NOT MATCHED" in sql

    def test_merge_deduplicates_sources(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (0, 0)
        merge_staging_to_master(cursor)
        sql = cursor.execute.call_args[0][0].upper()
        assert "DISTINCT" in sql

    def test_master_table_loc_is_varchar_2000(self):
        ddl = self._capture_ddl(create_master_table)
        assert "VARCHAR(2000)" in ddl

    def _capture_ddl(self, create_fn):
        cursor = MagicMock()
        create_fn(cursor)
        return cursor.execute.call_args[0][0].upper()
