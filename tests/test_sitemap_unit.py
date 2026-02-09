"""
Unit tests — Sitemap XML parsing (fetch_xml, is_sitemap_index, parse_sitemap).
"""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipeline.sitemap import fetch_xml, is_sitemap_index, parse_sitemap
from tests.conftest import (
    URLSET_XML,
    SITEMAP_INDEX_XML,
    CHILD_URLSET_XML,
    SITEMAP_NS_URI,
    mock_http_response,
)


class TestFetchXml:
    """pipeline.sitemap.fetch_xml — HTTP fetch + XML parse."""

    @patch("pipeline.sitemap.requests.get")
    def test_valid_xml_returns_element(self, mock_get):
        mock_get.return_value = mock_http_response(URLSET_XML)
        root = fetch_xml("https://example.com/sitemap.xml")
        assert root is not None
        assert isinstance(root, ET.Element)

    @patch("pipeline.sitemap.requests.get")
    def test_http_error_returns_none(self, mock_get):
        mock_get.return_value = mock_http_response(b"", 500)
        root = fetch_xml("https://example.com/bad.xml")
        assert root is None

    @patch("pipeline.sitemap.requests.get", side_effect=requests.ConnectionError("refused"))
    def test_connection_error_returns_none(self, mock_get):
        root = fetch_xml("https://example.com/down.xml")
        assert root is None


class TestIsSitemapIndex:
    """pipeline.sitemap.is_sitemap_index — tag detection."""

    def test_urlset_returns_false(self):
        root = ET.fromstring(URLSET_XML)
        assert is_sitemap_index(root) is False

    def test_sitemapindex_returns_true(self):
        root = ET.fromstring(SITEMAP_INDEX_XML)
        assert is_sitemap_index(root) is True


class TestParseSitemap:
    """pipeline.sitemap.parse_sitemap — recursive sitemap parsing."""

    @patch("pipeline.sitemap.fetch_xml")
    def test_urlset_extracts_all_urls(self, mock_fetch_xml):
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        results = parse_sitemap("https://example.com/sitemap.xml")

        assert len(results) == 2
        locs = {r["loc"] for r in results}
        assert "https://docs.snowflake.com/en/page1" in locs
        assert "https://docs.snowflake.com/en/page2" in locs

    @patch("pipeline.sitemap.fetch_xml")
    def test_lastmod_parsed_when_present(self, mock_fetch_xml):
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        results = parse_sitemap("https://example.com/sitemap.xml")

        page1 = next(r for r in results if "page1" in r["loc"])
        page2 = next(r for r in results if "page2" in r["loc"])
        assert page1["lastmod"] == "2025-12-01"
        assert page2["lastmod"] is None

    @patch("pipeline.sitemap.time.sleep")
    @patch("pipeline.sitemap.fetch_xml")
    def test_sitemap_index_recurses(self, mock_fetch_xml, mock_sleep):
        """A sitemap-index with 2 children → fetch_xml called 3 times total."""
        call_count = {"n": 0}
        roots = [
            ET.fromstring(SITEMAP_INDEX_XML),
            ET.fromstring(CHILD_URLSET_XML),
            ET.fromstring(CHILD_URLSET_XML),
        ]

        def _side(url):
            idx = call_count["n"]
            call_count["n"] += 1
            return roots[idx]

        mock_fetch_xml.side_effect = _side
        results = parse_sitemap("https://example.com/sitemap.xml")
        assert len(results) == 2
        assert mock_fetch_xml.call_count == 3

    @patch("pipeline.sitemap.fetch_xml", return_value=None)
    def test_unreachable_url_returns_empty(self, mock_fetch_xml):
        results = parse_sitemap("https://example.com/gone.xml")
        assert results == []

    @patch("pipeline.sitemap.fetch_xml")
    def test_source_sitemap_field_set(self, mock_fetch_xml):
        mock_fetch_xml.return_value = ET.fromstring(URLSET_XML)
        url = "https://example.com/sitemap.xml"
        results = parse_sitemap(url)
        for r in results:
            assert r["source_sitemap"] == url
            assert r["sitemap_type"] == "urlset"
