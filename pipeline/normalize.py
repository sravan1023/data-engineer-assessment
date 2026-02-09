"""
pipeline.normalize — URL normalization utilities.
"""

from urllib.parse import urlparse, urlunparse, quote, unquote


def normalize_url(url: str) -> str:
    """
    Normalize a URL for consistent deduplication:
      - Strip leading/trailing whitespace
      - Lowercase the scheme and host
      - Remove default ports (80 for http, 443 for https)
      - Remove trailing slash on path (unless root)
      - Collapse duplicate slashes in path
      - Re-encode the path for consistency
    """
    url = url.strip()
    if not url:
        return url

    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    host = parsed.hostname.lower() if parsed.hostname else ""

    # Remove default ports
    port = parsed.port
    if (scheme == "http" and port == 80) or (scheme == "https" and port == 443):
        port = None
    netloc = host if port is None else f"{host}:{port}"

    # Collapse duplicate slashes and remove trailing slash (keep root "/")
    path = parsed.path
    while "//" in path:
        path = path.replace("//", "/")
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    # Re-encode path for consistency (decode then re-encode)
    path = quote(unquote(path), safe="/:@!$&'()*+,;=-._~")

    return urlunparse((scheme, netloc, path, parsed.params, parsed.query, ""))
