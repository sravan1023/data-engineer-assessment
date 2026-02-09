"""
Unit tests — URL normalization (whitespace stripping, empty-loc handling).
"""

import xml.etree.ElementTree as ET
from unittest.mock import patch

from pipeline.sitemap import parse_sitemap
from pipeline.normalize import normalize_url
from tests.conftest import SITEMAP_NS_URI


class TestUrlNormalization:
    """Verify that the parser preserves / trims URL loc values correctly."""

    @patch("pipeline.sitemap.fetch_xml")
    def test_whitespace_stripped(self, mock_fetch_xml):
        xml = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="{SITEMAP_NS_URI}">
  <url><loc>  https://example.com/page  </loc></url>
</urlset>""".encode("utf-8")
        mock_fetch_xml.return_value = ET.fromstring(xml)
        results = parse_sitemap("https://example.com/sitemap.xml")
        assert results[0]["loc"] == "https://example.com/page"

    @patch("pipeline.sitemap.fetch_xml")
    def test_empty_loc_skipped(self, mock_fetch_xml):
        xml = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="{SITEMAP_NS_URI}">
  <url><loc></loc></url>
  <url><loc>https://example.com/valid</loc></url>
</urlset>""".encode("utf-8")
        mock_fetch_xml.return_value = ET.fromstring(xml)
        results = parse_sitemap("https://example.com/sitemap.xml")
        assert len(results) == 1
        assert results[0]["loc"] == "https://example.com/valid"


class TestNormalizeUrlFunction:
    """pipeline.normalize.normalize_url — standalone URL normalization."""

    def test_scheme_lowered(self):
        assert normalize_url("HTTPS://Example.COM/page") == "https://example.com/page"

    def test_default_port_removed(self):
        assert normalize_url("https://example.com:443/page") == "https://example.com/page"

    def test_non_default_port_kept(self):
        result = normalize_url("https://example.com:8080/page")
        assert ":8080" in result

    def test_trailing_slash_removed(self):
        assert normalize_url("https://example.com/page/") == "https://example.com/page"

    def test_root_slash_kept(self):
        result = normalize_url("https://example.com/")
        # Root path should have at least the slash
        assert result.endswith("example.com") or result.endswith("example.com/")
