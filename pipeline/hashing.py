"""
pipeline.hashing — Content hashing for change detection.
"""

import hashlib


def compute_hash(content: str) -> str:
    """Compute SHA-256 hash of content for change detection."""
    return hashlib.sha256(content.encode("utf-8", errors="replace")).hexdigest()
