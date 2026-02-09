"""
pipeline.ingest — Fetch document content (master → document_content).
"""

import time

import requests

from pipeline.hashing import compute_hash
from pipeline.throttle import (
    BACKOFF_BASE,
    FETCH_HEADERS,
    MAX_CONTENT_SIZE,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    THROTTLE_DELAY,
    TRANSIENT_STATUS_CODES,
)


def fetch_document(url: str) -> dict:
    """
    Fetch a single document with streaming, retries, and throttling.
    """
    last_exception = None
    http_status = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            if attempt > 0:
                wait = BACKOFF_BASE ** attempt
                time.sleep(wait)

            with requests.get(url, headers=FETCH_HEADERS, timeout=REQUEST_TIMEOUT, stream=True) as resp:
                http_status = resp.status_code

                if resp.status_code >= 400 and resp.status_code not in TRANSIENT_STATUS_CODES:
                    return {
                        "content": None,
                        "content_hash": None,
                        "content_size_bytes": 0,
                        "http_status": http_status,
                        "fetch_status": "failed",
                        "retry_count": attempt,
                    }

                if resp.status_code in TRANSIENT_STATUS_CODES:
                    last_exception = f"HTTP {resp.status_code}"
                    continue

                chunks = []
                total_size = 0
                truncated = False
                for chunk in resp.iter_content(chunk_size=64 * 1024, decode_unicode=True):
                    if chunk:
                        total_size += len(chunk.encode("utf-8", errors="replace"))
                        if total_size <= MAX_CONTENT_SIZE:
                            chunks.append(chunk)
                        else:
                            truncated = True
                            break

                content = "".join(chunks)
                if truncated:
                    content += f"\n\n[TRUNCATED at {MAX_CONTENT_SIZE / (1024*1024):.0f} MB]"
                    total_size = len(content.encode("utf-8", errors="replace"))

                content_hash = compute_hash(content)
                time.sleep(THROTTLE_DELAY)

                return {
                    "content": content,
                    "content_hash": content_hash,
                    "content_size_bytes": total_size,
                    "http_status": http_status,
                    "fetch_status": "success",
                    "retry_count": attempt,
                }

        except requests.exceptions.Timeout:
            last_exception = "timeout"
            http_status = None
        except requests.exceptions.ConnectionError as e:
            last_exception = f"connection_error: {e}"
            http_status = None
        except Exception as e:
            last_exception = str(e)
            http_status = None

    is_timeout = "timeout" in str(last_exception).lower()
    return {
        "content": None,
        "content_hash": None,
        "content_size_bytes": 0,
        "http_status": http_status,
        "fetch_status": "timeout" if is_timeout else "failed",
        "retry_count": MAX_RETRIES,
    }
