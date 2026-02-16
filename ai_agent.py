"""
ai_agent.py — Gemini AI integration for content optimization.
Sends video metadata to Gemini and validates the JSON response.
"""

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

import google.generativeai as genai

import config

logger = logging.getLogger(__name__)

# ── System prompt (from user specification) ───────────────────────
SYSTEM_PROMPT = """You are a content optimization assistant for short-form video reposting. Your job: given metadata of a short video (source URL, original title, duration, view_count, thumbnail_url, source_channel, language_hint), generate professional, SEO-oriented, ENGLISH-ONLY outputs that will be used to upload the video to destination channels. Strict rules:

1. LANGUAGE & STYLE:
   - Output must be in US English only. Do NOT use Hindi, Hinglish, emojis, slang, or non-ASCII punctuation characters.
   - No profanity, no political content. If content appears political or sensitive, set "flagged_for_review": true and explain reason in "review_reasons".

2. OUTPUT FORMAT:
   - Return **ONLY** a single JSON object (no surrounding text). The JSON must follow the exact schema in the "required_schema" instructions below. Fields not applicable must be null.
   - Use arrays where requested; strings must avoid line breaks unless explicitly allowed.

3. SEO & PLATFORM RULES:
   - Title length: <= 60 characters.
   - Description: 100–400 characters. Include ONE short 1–2 sentence hook + 2–3 line context. End with 1 short CTA (e.g., "Watch more on @[channel]" — must be English).
   - Provide between 8–15 hashtags (English only). Format: array of strings, each starts with '#' and only letters/numbers/underscores.
   - Provide tags as a comma-separated string (max 15 tags).
   - Provide one category from: Sports, Entertainment, Education, Lifestyle, Tech, Music, Comedy, Other.

4. DUPLICATE-FINGERPRINT MITIGATION:
   - Suggest a single, safe ffmpeg command string to perform minimal transformation before upload (trim 0.0–0.7s, re-encode, scale or slight crop). Keep commands short and deterministic.
   - Provide `suggested_ffmpeg_cmd` (string) and a brief `ffmpeg_reason`.

5. RISK / SAFETY:
   - Run checks for obvious copyright/responsible content indicators (e.g., copyrighted music, logos, watermarks). If high risk, set "flagged_for_review": true and explain in "review_reasons".
   - If video duration > 120s or aspect ratio not vertical, set "flagged_for_review".

6. PRIORITY:
   - Compute `priority_score` 0–100 (higher means recommended for immediate upload). Base on view_count, duration (shorter >), and virality clues in title (keywords like "goal", "funny", "viral", "amazing"). Provide transparent reasons in `priority_reason`.

7. SHEET MAPPING:
   Map outputs to Google Sheet columns:
     H -> ai_title
     I -> ai_description
     J -> ai_hashtags (comma-joined string) and also return as array
     K -> ai_tags (comma-separated)
     L -> category
     U -> content_hash (NOT generated here; provided to you)
     Additional: suggested_ffmpeg_cmd, priority_score, flagged_for_review, review_reasons

8. FAILURE / EDGE CASES:
   - If input metadata is missing (e.g., original_title or duration) still attempt sensible defaults and set "notes" explaining assumptions.
   - If you cannot generate safe outputs (e.g., clearly copyrighted movie clip), set "flagged_for_review": true with reasons.

9. VERSION:
   - Add "agent_version": "ai_agent_v1.0" to the JSON.

10. JSON STRICT SCHEMA:
   - Follow the "required_schema" exactly.

REQUIRED JSON SCHEMA (you must return EXACTLY this JSON object):
{
  "agent_version": "ai_agent_v1.0",
  "row_id": 0,
  "ai_title": "",
  "ai_description": "",
  "ai_hashtags": ["#example"],
  "ai_hashtags_csv": "",
  "ai_tags": "",
  "category": "",
  "priority_score": 0,
  "priority_reason": "",
  "suggested_ffmpeg_cmd": "",
  "ffmpeg_reason": "",
  "flagged_for_review": false,
  "review_reasons": [],
  "notes": "",
  "content_hash": "",
  "output_language": "en",
  "timestamp_utc": "2026-02-14T00:00:00Z"
}"""


def _configure_genai():
    """Configure the Gemini SDK with the API key."""
    genai.configure(api_key=config.GEMINI_API_KEY)


def _build_user_message(row_data: dict[str, Any]) -> str:
    """Build the user message from a sheet row dict."""
    payload = {
        "row_id": _safe_int(row_data.get("row_id", 0)),
        "source_channel": row_data.get("source_channel", ""),
        "source_channel_tab": row_data.get("source_channel_tab", ""),
        "source_url": row_data.get("source_url", ""),
        "original_title": row_data.get("original_title", ""),
        "duration_seconds": _safe_int(row_data.get("duration_seconds", 0)),
        "view_count": _safe_int(row_data.get("view_count", 0)),
        "thumbnail_url": row_data.get("thumbnail_url", ""),
        "content_hash": row_data.get("content_hash", ""),
        "language_hint": row_data.get("language_hint", "unknown"),
    }
    return (
        "Here is the source row data. Respond ONLY with the JSON described "
        "in the system message.\n\n" + json.dumps(payload, indent=2)
    )


def _safe_int(val) -> int:
    """Convert a value to int safely, defaulting to 0."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


# ── Response validation ───────────────────────────────────────────

REQUIRED_FIELDS = {
    "agent_version": str,
    "row_id": int,
    "ai_title": str,
    "ai_description": str,
    "ai_hashtags": list,
    "ai_hashtags_csv": str,
    "ai_tags": str,
    "category": str,
    "priority_score": int,
    "priority_reason": str,
    "suggested_ffmpeg_cmd": (str, type(None)),
    "ffmpeg_reason": (str, type(None)),
    "flagged_for_review": bool,
    "review_reasons": list,
    "notes": (str, type(None)),
    "content_hash": str,
    "output_language": str,
    "timestamp_utc": str,
}

VALID_CATEGORIES = {
    "Sports", "Entertainment", "Education", "Lifestyle",
    "Tech", "Music", "Comedy", "Other",
}


def _contains_non_ascii(text: str) -> bool:
    """Check if text contains non-ASCII characters (excluding common punctuation)."""
    try:
        text.encode("ascii")
        return False
    except UnicodeEncodeError:
        return True


def validate_response(data: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Validate the AI response against the required schema.
    Returns (is_valid, list_of_issues).
    """
    issues = []

    # Check required fields exist and have correct types
    for field, expected_type in REQUIRED_FIELDS.items():
        if field not in data:
            issues.append(f"Missing field: {field}")
            continue
        val = data[field]
        if isinstance(expected_type, tuple):
            if not isinstance(val, expected_type):
                issues.append(
                    f"Field '{field}' has type {type(val).__name__}, "
                    f"expected one of {[t.__name__ for t in expected_type]}"
                )
        else:
            if not isinstance(val, expected_type):
                # Allow int/float coercion for numeric fields
                if expected_type == int and isinstance(val, (float, str)):
                    try:
                        data[field] = int(float(val))
                    except (ValueError, TypeError):
                        issues.append(f"Field '{field}' cannot be converted to int")
                else:
                    issues.append(
                        f"Field '{field}' has type {type(val).__name__}, "
                        f"expected {expected_type.__name__}"
                    )

    # Title length
    title = data.get("ai_title", "")
    if isinstance(title, str) and len(title) > 60:
        issues.append(f"ai_title exceeds 60 chars ({len(title)})")

    # Description length
    desc = data.get("ai_description", "")
    if isinstance(desc, str):
        if len(desc) < 100:
            issues.append(f"ai_description too short ({len(desc)} chars, min 100)")
        if len(desc) > 400:
            issues.append(f"ai_description too long ({len(desc)} chars, max 400)")

    # Hashtags count
    hashtags = data.get("ai_hashtags", [])
    if isinstance(hashtags, list):
        if len(hashtags) < 8:
            issues.append(f"Too few hashtags ({len(hashtags)}, min 8)")
        if len(hashtags) > 15:
            issues.append(f"Too many hashtags ({len(hashtags)}, max 15)")
        for tag in hashtags:
            if isinstance(tag, str) and not re.match(r"^#[A-Za-z0-9_]+$", tag):
                issues.append(f"Invalid hashtag format: {tag}")

    # Category
    cat = data.get("category", "")
    if cat and cat not in VALID_CATEGORIES:
        issues.append(f"Invalid category: {cat}")

    # Priority score range
    score = data.get("priority_score", 0)
    if isinstance(score, (int, float)) and not (0 <= score <= 100):
        issues.append(f"priority_score out of range: {score}")

    # Non-ASCII check on text fields
    for text_field in ["ai_title", "ai_description", "ai_tags", "ai_hashtags_csv"]:
        val = data.get(text_field, "")
        if isinstance(val, str) and _contains_non_ascii(val):
            issues.append(f"Non-ASCII characters in {text_field}")
            data["flagged_for_review"] = True
            if "review_reasons" not in data or not isinstance(data["review_reasons"], list):
                data["review_reasons"] = []
            data["review_reasons"].append(f"Non-ASCII detected in {text_field}")

    # Output language must be "en"
    if data.get("output_language") != "en":
        issues.append(f"output_language is '{data.get('output_language')}', expected 'en'")

    is_valid = len(issues) == 0
    return is_valid, issues


def _extract_json(text: str) -> dict[str, Any]:
    """Extract JSON from model response text, handling markdown fences."""
    text = text.strip()
    # Remove markdown code fences if present
    if text.startswith("```"):
        # Remove first line (```json or ```)
        lines = text.split("\n")
        lines = lines[1:]  # drop opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]  # drop closing fence
        text = "\n".join(lines).strip()
    return json.loads(text)


# ── Main processing function ─────────────────────────────────────

def process_row(row_data: dict[str, Any]) -> dict[str, Any]:
    """
    Process a single row through Gemini and return validated output.
    Raises ValueError on unrecoverable errors.
    """
    _configure_genai()

    model = genai.GenerativeModel(
        model_name=config.GEMINI_MODEL,
        system_instruction=SYSTEM_PROMPT,
        generation_config=genai.GenerationConfig(
            temperature=config.GEMINI_TEMPERATURE,
            max_output_tokens=config.GEMINI_MAX_TOKENS,
        ),
    )

    user_msg = _build_user_message(row_data)
    logger.info("Sending row %s to Gemini...", row_data.get("row_id", "?"))

    response = model.generate_content(user_msg)

    raw_text = response.text
    logger.debug("Raw Gemini response: %s", raw_text[:500])

    # Log raw response to file
    row_id = row_data.get("row_id", "unknown")
    log_file = config.LOG_DIR / f"row_{row_id}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    log_file.write_text(raw_text, encoding="utf-8")
    logger.info("Logged raw response to %s", log_file)

    # Parse JSON
    try:
        result = _extract_json(raw_text)
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse model JSON: {e}\nRaw: {raw_text[:300]}")

    # Validate
    is_valid, issues = validate_response(result)
    if not is_valid:
        logger.warning("Validation issues for row %s: %s", row_id, issues)
        # For non-critical issues (like char limits), still proceed but add notes
        result.setdefault("notes", "")
        if result["notes"]:
            result["notes"] += " | "
        result["notes"] += f"Validation warnings: {'; '.join(issues)}"

    # Ensure timestamp
    if not result.get("timestamp_utc"):
        result["timestamp_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    return result
