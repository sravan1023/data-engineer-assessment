"""
Unit tests — Hash generation (compute_hash).
"""

import hashlib

from pipeline.hashing import compute_hash


class TestComputeHash:
    """pipeline.hashing.compute_hash — SHA-256 hashing."""

    def test_deterministic(self):
        h1 = compute_hash("hello world")
        h2 = compute_hash("hello world")
        assert h1 == h2

    def test_different_content_different_hash(self):
        assert compute_hash("a") != compute_hash("b")

    def test_matches_stdlib_sha256(self):
        content = "sample document body"
        expected = hashlib.sha256(content.encode("utf-8")).hexdigest()
        assert compute_hash(content) == expected

    def test_empty_string(self):
        h = compute_hash("")
        assert isinstance(h, str) and len(h) == 64

    def test_unicode_content(self):
        h = compute_hash("日本語テキスト 🚀")
        assert isinstance(h, str) and len(h) == 64
