"""
pipeline.sitemap — Sitemap XML parsing: fetch, detect index/urlset, recurse.
"""

import time
import xml.etree.ElementTree as ET
from typing import Optional

import requests

# XML namespace used in the sitemap protocol
SITEMAP_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; SitemapBot/1.0)"}


def fetch_xml(url: str) -> Optional[ET.Element]:
    """Fetch and parse an XML document from a URL."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return ET.fromstring(resp.content)
    except Exception as e:
        print(f"  [ERROR] Failed to fetch {url}: {e}")
        return None


def is_sitemap_index(root: ET.Element) -> bool:
    """Check if the XML root is a sitemap index (contains nested sitemaps)."""
    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
    return tag == "sitemapindex"


def parse_sitemap(url: str, depth: int = 0) -> list[dict]:
    """
    Recursively parse a sitemap URL.
    Returns a list of dicts with keys: loc, lastmod, source_sitemap, sitemap_type
    """
    indent = "  " * depth
    print(f"{indent}Processing: {url}")

    root = fetch_xml(url)
    if root is None:
        return []

    results = []

    if is_sitemap_index(root):
        print(f"{indent}  -> Sitemap Index (contains nested sitemaps)")
        sitemap_entries = root.findall("sm:sitemap", SITEMAP_NS)
        print(f"{indent}  -> Found {len(sitemap_entries)} child sitemap(s)")

        for sitemap in sitemap_entries:
            loc_elem = sitemap.find("sm:loc", SITEMAP_NS)
            if loc_elem is not None and loc_elem.text:
                child_url = loc_elem.text.strip()
                results.extend(parse_sitemap(child_url, depth + 1))
                time.sleep(0.2)
    else:
        url_entries = root.findall("sm:url", SITEMAP_NS)
        print(f"{indent}  -> URL Set with {len(url_entries)} URL(s)")

        for entry in url_entries:
            loc_elem = entry.find("sm:loc", SITEMAP_NS)
            lastmod_elem = entry.find("sm:lastmod", SITEMAP_NS)

            loc = loc_elem.text.strip() if loc_elem is not None and loc_elem.text else None
            lastmod = lastmod_elem.text.strip() if lastmod_elem is not None and lastmod_elem.text else None

            if loc:
                results.append({
                    "loc": loc,
                    "lastmod": lastmod,
                    "source_sitemap": url,
                    "sitemap_type": "urlset",
                })

    return results
