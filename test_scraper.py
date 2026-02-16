#!/usr/bin/env python3
"""
test_scraper.py — Standalone tests for scraper components.
Tests hashing, URL normalization, tab naming, config, and auto-tagging.
No API keys or network access required.
"""

import sys
import os
import tempfile
import hashlib

sys.path.insert(0, os.path.dirname(__file__))
os.environ["GEMINI_API_KEY"] = "test"
os.environ["GOOGLE_SVC_JSON"] = "/dev/null"


def test_compute_file_hash():
    """Test full file SHA256 hash."""
    from hash_utils import compute_file_hash

    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as f:
        data = b"test video content 12345"
        f.write(data)
        f.flush()
        path = f.name

    expected = hashlib.sha256(data).hexdigest()
    got_hash, method = compute_file_hash(path)
    os.unlink(path)
    assert got_hash == expected, f"Hash mismatch: {got_hash} != {expected}"
    assert method == "full"
    print("  PASS: File hash (full)")


def test_metadata_hash():
    """Test metadata-based hash."""
    from hash_utils import compute_metadata_hash

    h1, m1 = compute_metadata_hash("Title", 30, "2026-01-01", "https://youtube.com/shorts/abc")
    h2, m2 = compute_metadata_hash("Title", 30, "2026-01-01", "https://youtube.com/shorts/abc")
    h3, m3 = compute_metadata_hash("Different", 30, "2026-01-01", "https://youtube.com/shorts/abc")

    assert h1 == h2, "Same input should produce same hash"
    assert h1 != h3, "Different input should produce different hash"
    assert m1 == "metadata_hash"
    print("  PASS: Metadata hash")


def test_normalize_youtube_urls():
    """Test YouTube URL normalization."""
    from hash_utils import normalize_url

    cases = [
        ("https://youtube.com/shorts/abc123", "https://youtube.com/shorts/abc123"),
        ("https://www.youtube.com/shorts/abc123/", "https://youtube.com/shorts/abc123"),
        ("https://youtu.be/abc123", "https://youtube.com/shorts/abc123"),
        ("https://www.youtube.com/watch?v=abc123&t=5", "https://youtube.com/shorts/abc123"),
        ("https://youtube.com/shorts/abc123?si=xyz", "https://youtube.com/shorts/abc123"),
    ]
    for input_url, expected in cases:
        got = normalize_url(input_url)
        assert got == expected, f"normalize({input_url}) = {got}, expected {expected}"
    print("  PASS: YouTube URL normalization")


def test_normalize_instagram_urls():
    """Test Instagram URL normalization."""
    from hash_utils import normalize_url

    cases = [
        ("https://www.instagram.com/reel/ABC123/", "https://www.instagram.com/reel/ABC123"),
        ("https://instagram.com/reel/ABC123?utm=x", "https://www.instagram.com/reel/ABC123"),
    ]
    for input_url, expected in cases:
        got = normalize_url(input_url)
        assert got == expected, f"normalize({input_url}) = {got}, expected {expected}"
    print("  PASS: Instagram URL normalization")


def test_clean_tab_name():
    """Test tab name sanitization."""
    from hash_utils import clean_tab_name

    assert clean_tab_name("FunnyClips_2024") == "source__funnyclips_2024"
    assert clean_tab_name("My Channel !@#$") == "source__my_channel"
    assert clean_tab_name("A" * 60) == f"source__{'a' * 40}"
    assert clean_tab_name("simple") == "source__simple"
    print("  PASS: Tab name sanitization")


def test_scraper_config_schema():
    """Test that scraper config has correct number of headers."""
    from scraper_config import SCRAPER_HEADERS, MASTER_INDEX_HEADERS

    assert len(SCRAPER_HEADERS) == 23, f"Expected 23 headers, got {len(SCRAPER_HEADERS)}"
    assert SCRAPER_HEADERS[0] == "row_id"
    assert SCRAPER_HEADERS[4] == "source_url"
    assert SCRAPER_HEADERS[11] == "content_hash"
    assert SCRAPER_HEADERS[14] == "status"
    assert SCRAPER_HEADERS[-1] == "dest_mapping_tags"
    assert len(MASTER_INDEX_HEADERS) == 6
    print("  PASS: Scraper config schema (23 headers)")


def test_auto_tags():
    """Test auto-tagging from title."""
    sys.path.insert(0, os.path.dirname(__file__))
    # Import from scraper directly
    from scraper import _auto_tags_from_title

    tags = _auto_tags_from_title("Incredible cricket catch by the dog")
    assert "cricket" in tags
    assert "animals" in tags  # "dog" maps to "animals"
    assert "incredible" in tags

    tags2 = _auto_tags_from_title("Random boring video")
    assert len(tags2) == 0
    print("  PASS: Auto-tagging from title")


def test_iso_duration_parsing():
    """Test ISO 8601 duration parsing."""
    from scraper import _parse_iso_duration

    assert _parse_iso_duration("PT30S") == 30
    assert _parse_iso_duration("PT1M30S") == 90
    assert _parse_iso_duration("PT1H2M3S") == 3723
    assert _parse_iso_duration("PT0S") == 0
    assert _parse_iso_duration("invalid") == 0
    print("  PASS: ISO duration parsing")


def test_key_rotator():
    """Test Scrapingdog key rotation."""
    from scraper import KeyRotator

    rotator = KeyRotator(["key1", "key2", "key3"])
    k1 = rotator.next_key()
    k2 = rotator.next_key()
    k3 = rotator.next_key()
    k4 = rotator.next_key()
    assert k1 == "key1"
    assert k2 == "key2"
    assert k3 == "key3"
    assert k4 == "key1"  # wraps around
    print("  PASS: Key rotation")

    # Empty keys
    empty = KeyRotator([])
    assert empty.next_key() == ""
    print("  PASS: Empty key rotation")


def test_sources_yaml():
    """Test that sources.yaml is valid and parseable."""
    import yaml
    with open(os.path.join(os.path.dirname(__file__), "sources.yaml")) as f:
        sources = yaml.safe_load(f)
    assert isinstance(sources, list)
    assert len(sources) >= 1
    for s in sources:
        assert "source_tab" in s
        assert "source_type" in s
        assert "source_id" in s
        assert s["source_tab"].startswith("source__")
    print(f"  PASS: sources.yaml valid ({len(sources)} sources)")


if __name__ == "__main__":
    tests = [
        test_compute_file_hash,
        test_metadata_hash,
        test_normalize_youtube_urls,
        test_normalize_instagram_urls,
        test_clean_tab_name,
        test_scraper_config_schema,
        test_auto_tags,
        test_iso_duration_parsing,
        test_key_rotator,
        test_sources_yaml,
    ]
    print(f"Running {len(tests)} scraper tests...\n")
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1
    print(f"\nResults: {passed} passed, {failed} failed out of {len(tests)} tests.")
    sys.exit(1 if failed else 0)
