#!/usr/bin/env python3
"""
test_validation.py — Standalone validation test (no API keys needed).
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

# Override env before importing config
os.environ["GEMINI_API_KEY"] = "test_key_not_real"

from ai_agent import validate_response, _build_user_message

def test_valid_response():
    """Test that a correct response passes validation."""
    data = {
        "agent_version": "ai_agent_v1.0",
        "row_id": 451,
        "ai_title": "Last-Minute Cricket Catch - Unbelievable!",
        "ai_description": (
            "A jaw-dropping last-minute cricket catch that stunned the crowd. "
            "Watch the play that changed the match in seconds. "
            "Follow for more epic highlights on our channel."
        ),
        "ai_hashtags": [
            "#cricket", "#amazingcatch", "#sports", "#highlights",
            "#viral", "#shorts", "#crickethighlight", "#mustwatch",
            "#fyp", "#instashorts", "#sportsclips", "#matchmoment",
        ],
        "ai_hashtags_csv": "#cricket,#amazingcatch,#sports,#highlights",
        "ai_tags": "cricket,catch,highlights,viral,shorts",
        "category": "Sports",
        "priority_score": 88,
        "priority_reason": "High view_count plus short duration",
        "suggested_ffmpeg_cmd": "ffmpeg -y -i input.mp4 -ss 0.5 output.mp4",
        "ffmpeg_reason": "Trim to alter fingerprint",
        "flagged_for_review": False,
        "review_reasons": [],
        "notes": "Normalized title.",
        "content_hash": "sha256:abcd",
        "output_language": "en",
        "timestamp_utc": "2026-02-14T00:00:00Z",
    }
    is_valid, issues = validate_response(data)
    assert is_valid, f"Expected valid, got issues: {issues}"
    print("  PASS: Valid response accepted")


def test_title_too_long():
    """Title > 60 chars should flag."""
    data = _make_base()
    data["ai_title"] = "A" * 65
    is_valid, issues = validate_response(data)
    assert not is_valid
    assert any("ai_title exceeds 60" in i for i in issues)
    print("  PASS: Long title detected")


def test_short_description():
    """Description < 100 chars should flag."""
    data = _make_base()
    data["ai_description"] = "Too short."
    is_valid, issues = validate_response(data)
    assert not is_valid
    assert any("too short" in i for i in issues)
    print("  PASS: Short description detected")


def test_too_few_hashtags():
    """Fewer than 8 hashtags should flag."""
    data = _make_base()
    data["ai_hashtags"] = ["#one", "#two", "#three"]
    is_valid, issues = validate_response(data)
    assert not is_valid
    assert any("Too few hashtags" in i for i in issues)
    print("  PASS: Too few hashtags detected")


def test_invalid_category():
    """Invalid category should flag."""
    data = _make_base()
    data["category"] = "Gaming"
    is_valid, issues = validate_response(data)
    assert not is_valid
    assert any("Invalid category" in i for i in issues)
    print("  PASS: Invalid category detected")


def test_non_ascii():
    """Non-ASCII in title should flag for review."""
    data = _make_base()
    data["ai_title"] = "Cricket \u0905\u0926\u094d\u092d\u0941\u0924!"
    is_valid, issues = validate_response(data)
    assert any("Non-ASCII" in i for i in issues)
    assert data["flagged_for_review"] is True
    print("  PASS: Non-ASCII detection works")


def test_priority_out_of_range():
    """Priority score > 100 should flag."""
    data = _make_base()
    data["priority_score"] = 150
    is_valid, issues = validate_response(data)
    assert not is_valid
    assert any("priority_score out of range" in i for i in issues)
    print("  PASS: Priority out of range detected")


def test_missing_field():
    """Missing required field should flag."""
    data = _make_base()
    del data["ai_title"]
    is_valid, issues = validate_response(data)
    assert not is_valid
    assert any("Missing field: ai_title" in i for i in issues)
    print("  PASS: Missing field detected")


def test_user_message_builder():
    """Test that _build_user_message produces valid JSON."""
    import json
    row = {
        "row_id": "42",
        "source_channel": "TestCh",
        "source_channel_tab": "tab",
        "source_url": "https://example.com",
        "original_title": "Test Title",
        "duration_seconds": "30",
        "view_count": "1000",
        "thumbnail_url": "",
        "content_hash": "hash123",
    }
    msg = _build_user_message(row)
    assert "42" in msg or '"row_id": 42' in msg
    # Verify JSON is parseable
    json_part = msg.split("\n\n", 1)[1]
    parsed = json.loads(json_part)
    assert parsed["row_id"] == 42
    assert parsed["duration_seconds"] == 30
    print("  PASS: User message builder works")


def _make_base():
    """Return a minimal valid response dict for modification."""
    return {
        "agent_version": "ai_agent_v1.0",
        "row_id": 1,
        "ai_title": "Cricket Catch Unbelievable Moment",
        "ai_description": (
            "A jaw-dropping last-minute cricket catch that stunned the crowd. "
            "Watch the play that changed the match in seconds. "
            "Follow for more epic highlights on our channel."
        ),
        "ai_hashtags": [
            "#cricket", "#catch", "#sports", "#highlights",
            "#viral", "#shorts", "#mustwatch", "#fyp",
            "#sportsclips", "#matchmoment",
        ],
        "ai_hashtags_csv": "#cricket,#catch",
        "ai_tags": "cricket,catch",
        "category": "Sports",
        "priority_score": 88,
        "priority_reason": "test",
        "suggested_ffmpeg_cmd": None,
        "ffmpeg_reason": None,
        "flagged_for_review": False,
        "review_reasons": [],
        "notes": "",
        "content_hash": "test",
        "output_language": "en",
        "timestamp_utc": "2026-02-14T00:00:00Z",
    }


if __name__ == "__main__":
    tests = [
        test_valid_response,
        test_title_too_long,
        test_short_description,
        test_too_few_hashtags,
        test_invalid_category,
        test_non_ascii,
        test_priority_out_of_range,
        test_missing_field,
        test_user_message_builder,
    ]
    print(f"Running {len(tests)} validation tests...\n")
    passed = 0
    failed = 0
    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  FAIL: {test.__name__}: {e}")
            failed += 1
    print(f"\nResults: {passed} passed, {failed} failed out of {len(tests)} tests.")
    sys.exit(1 if failed else 0)
