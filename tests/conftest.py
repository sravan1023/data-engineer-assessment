"""
tests/conftest.py — Shared fixtures and sample XML payloads for the test suite.
"""

import xml.etree.ElementTree as ET
from unittest.mock import MagicMock

import pytest
import requests

SITEMAP_NS_URI = "http://www.sitemaps.org/schemas/sitemap/0.9"

URLSET_XML = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="{SITEMAP_NS_URI}">
  <url>
    <loc>https://docs.snowflake.com/en/page1</loc>
    <lastmod>2025-12-01</lastmod>
  </url>
  <url>
    <loc>https://docs.snowflake.com/en/page2</loc>
  </url>
</urlset>
""".encode("utf-8")

SITEMAP_INDEX_XML = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="{SITEMAP_NS_URI}">
  <sitemap>
    <loc>https://docs.snowflake.com/sitemap-child1.xml</loc>
  </sitemap>
  <sitemap>
    <loc>https://docs.snowflake.com/sitemap-child2.xml</loc>
  </sitemap>
</sitemapindex>
""".encode("utf-8")

CHILD_URLSET_XML = f"""\
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="{SITEMAP_NS_URI}">
  <url>
    <loc>https://docs.snowflake.com/en/child-page</loc>
    <lastmod>2025-11-15</lastmod>
  </url>
</urlset>
""".encode("utf-8")



def mock_http_response(content: bytes, status_code: int = 200):
    """Return a mock ``requests.Response`` with the given content."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.content = content
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp



@pytest.fixture
def mock_cursor():
    """A fresh ``MagicMock`` mimicking a Snowflake cursor."""
    return MagicMock()


@pytest.fixture
def urlset_root():
    """Parsed ET.Element for a simple urlset."""
    return ET.fromstring(URLSET_XML)


@pytest.fixture
def sitemap_index_root():
    """Parsed ET.Element for a sitemap index."""
    return ET.fromstring(SITEMAP_INDEX_XML)


@pytest.fixture
def child_urlset_root():
    """Parsed ET.Element for a child urlset."""
    return ET.fromstring(CHILD_URLSET_XML)
