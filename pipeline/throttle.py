"""
pipeline.throttle — Rate-limiting constants and configuration.
"""

FETCH_BATCH_SIZE = 500
MAX_WORKERS = 5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
BACKOFF_BASE = 2
THROTTLE_DELAY = 0.3
MAX_CONTENT_SIZE = 5 * 1024 * 1024        # 5 MB
MAX_CONSECUTIVE_FAILURES = 5
TRANSIENT_STATUS_CODES = {408, 429, 500, 502, 503, 504}

FETCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; DocIngestionBot/1.0)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
