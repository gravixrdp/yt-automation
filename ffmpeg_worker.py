"""
ffmpeg_worker.py — Video transformation and validation for uploads.
Applies suggested ffmpeg commands to mitigate duplicate fingerprints.
Includes branding pipeline: watermark, crop variation, intro/outro.
"""

import logging
import random
import subprocess
import os
from pathlib import Path

import scheduler_config

logger = logging.getLogger(__name__)


def get_video_duration(path: str) -> float:
    """Get video duration in seconds using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries", "format=duration",
        "-of", "default=noprint_wrappers=1:nokey=1",
        path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and result.stdout.strip():
            return float(result.stdout.strip())
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError):
        pass
    return 0.0


def get_video_dimensions(path: str) -> tuple[int, int]:
    """Get (width, height) of video using ffprobe."""
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "csv=s=x:p=0",
        path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0 and "x" in result.stdout:
            w, h = result.stdout.strip().split("x")
            return int(w), int(h)
    except (subprocess.TimeoutExpired, ValueError, FileNotFoundError):
        pass
    return 0, 0


def transform_video(
    input_path: str,
    suggested_cmd: str = "",
    max_duration: int = 60,
    dest_account_id: str = "",
) -> dict:
    """
    Apply ffmpeg transformation to video.
    Returns dict with: success, output_path, error, duration.
    """
    result = {"success": False, "output_path": None, "error": None, "duration": 0.0}
    input_p = Path(input_path)

    if not input_p.exists():
        result["error"] = f"input file not found: {input_path}"
        return result

    output_path = str(input_p.parent / f"upload_{input_p.stem}.mp4")

    # Step 1: Try the suggested command
    if suggested_cmd:
        cmd_str = suggested_cmd.replace("{input}", input_path).replace("{output}", output_path)
        logger.info("Running suggested ffmpeg: %s", cmd_str[:100])
        success = _run_ffmpeg(cmd_str)
        if success and Path(output_path).exists():
            result["success"] = True
            result["output_path"] = output_path
            result["duration"] = get_video_duration(output_path)
            # Apply branding post-processing
            branded = _apply_branding_pipeline(output_path, dest_account_id)
            if branded:
                result["output_path"] = branded
                result["duration"] = get_video_duration(branded)
            return result
        logger.warning("Suggested ffmpeg failed, trying default.")

    # Step 2: Try default command (trim 0.5s + re-encode)
    cmd_str = scheduler_config.FFMPEG_DEFAULT_CMD.format(
        input=input_path, output=output_path,
    )
    logger.info("Running default ffmpeg: %s", cmd_str[:100])
    success = _run_ffmpeg(cmd_str)
    if success and Path(output_path).exists():
        result["success"] = True
        result["output_path"] = output_path
        result["duration"] = get_video_duration(output_path)
        branded = _apply_branding_pipeline(output_path, dest_account_id)
        if branded:
            result["output_path"] = branded
            result["duration"] = get_video_duration(branded)
        return result

    # Step 3: Try fallback (plain re-encode, no trim)
    cmd_str = scheduler_config.FFMPEG_FALLBACK_CMD.format(
        input=input_path, output=output_path,
    )
    logger.info("Running fallback ffmpeg: %s", cmd_str[:100])
    success = _run_ffmpeg(cmd_str)
    if success and Path(output_path).exists():
        result["success"] = True
        result["output_path"] = output_path
        result["duration"] = get_video_duration(output_path)
        branded = _apply_branding_pipeline(output_path, dest_account_id)
        if branded:
            result["output_path"] = branded
            result["duration"] = get_video_duration(branded)
        return result

    result["error"] = "all ffmpeg attempts failed"
    return result


def _run_ffmpeg(cmd_str: str) -> bool:
    """Run an ffmpeg command string. Returns True on success."""
    try:
        proc = subprocess.run(
            cmd_str, shell=True,
            capture_output=True, text=True, timeout=300,
        )
        if proc.returncode != 0:
            logger.warning("ffmpeg exit %d: %s", proc.returncode, proc.stderr[:200])
            return False
        return True
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg timed out")
        return False
    except Exception as e:
        logger.error("ffmpeg error: %s", e)
        return False


# ── Branding Pipeline (#3) ───────────────────────────────────────

def _apply_branding_pipeline(video_path: str, dest_account_id: str = "") -> str | None:
    """
    Apply branding post-processing steps in order:
    1. Crop variation (random 2-8px)
    2. Watermark overlay (per-destination)
    3. Intro/outro concat (if configured)
    Returns new file path, or None if no branding applied.
    """
    current = video_path
    changed = False

    # Step 1: Crop variation
    if scheduler_config.CROP_VARIATION_ENABLED:
        cropped = apply_crop_variation(current)
        if cropped:
            if current != video_path:
                Path(current).unlink(missing_ok=True)
            current = cropped
            changed = True

    # Step 2: Watermark
    watermark_path = _find_watermark(dest_account_id)
    if watermark_path:
        watermarked = apply_watermark(current, watermark_path)
        if watermarked:
            if current != video_path:
                Path(current).unlink(missing_ok=True)
            current = watermarked
            changed = True

    # Step 3: Intro/outro
    intro_path = scheduler_config.BRANDING_INTRO
    outro_path = scheduler_config.BRANDING_OUTRO
    if intro_path or outro_path:
        concatted = apply_intro_outro(current, intro_path, outro_path)
        if concatted:
            if current != video_path:
                Path(current).unlink(missing_ok=True)
            current = concatted
            changed = True

    return current if changed else None


def _find_watermark(dest_account_id: str) -> str | None:
    """Find watermark PNG for a destination. Falls back to default.png."""
    if not dest_account_id:
        return None
    specific = scheduler_config.WATERMARK_DIR / f"{dest_account_id}.png"
    if specific.exists():
        return str(specific)
    default = scheduler_config.WATERMARK_DIR / "default.png"
    if default.exists():
        return str(default)
    return None


def apply_watermark(video_path: str, watermark_path: str, opacity: float = 0.10) -> str | None:
    """Overlay a semi-transparent watermark at bottom-right."""
    output = str(Path(video_path).parent / f"wm_{Path(video_path).name}")
    cmd = (
        f'/usr/bin/ffmpeg -y -i "{video_path}" -i "{watermark_path}" '
        f'-threads 1 '
        f'-filter_complex "[1:v]format=rgba,colorchannelmixer=aa={opacity}[wm];'
        f'[0:v][wm]overlay=W-w-10:H-h-10" '
        f'-c:a copy "{output}"'
    )
    if _run_ffmpeg(cmd) and Path(output).exists():
        logger.info("Watermark applied: %s", Path(output).name)
        return output
    return None


def apply_crop_variation(video_path: str) -> str | None:
    """Random 2-8px crop from edges to create unique frames."""
    max_px = scheduler_config.CROP_MAX_PX
    top = random.randint(2, max_px)
    bottom = random.randint(2, max_px)
    left = random.randint(2, max_px)
    right = random.randint(2, max_px)
    output = str(Path(video_path).parent / f"crop_{Path(video_path).name}")
    cmd = (
        f'/usr/bin/ffmpeg -y -i "{video_path}" '
        f'-threads 1 '
        f'-vf "crop=iw-{left+right}:ih-{top+bottom}:{left}:{top}" '
        f'-c:a copy "{output}"'
    )
    if _run_ffmpeg(cmd) and Path(output).exists():
        logger.info("Crop variation applied: top=%d bot=%d left=%d right=%d", top, bottom, left, right)
        return output
    return None


def apply_intro_outro(video_path: str, intro_path: str = "", outro_path: str = "") -> str | None:
    """Concatenate optional intro and/or outro clips."""
    if not intro_path and not outro_path:
        return None
    parts = []
    if intro_path and Path(intro_path).exists():
        parts.append(intro_path)
    parts.append(video_path)
    if outro_path and Path(outro_path).exists():
        parts.append(outro_path)
    if len(parts) < 2:
        return None  # Nothing to concat
    # Write concat list
    concat_file = str(Path(video_path).parent / "concat_list.txt")
    with open(concat_file, "w") as f:
        for p in parts:
            f.write(f"file '{p}'\n")
    output = str(Path(video_path).parent / f"branded_{Path(video_path).name}")
    cmd = f'ffmpeg -y -f concat -safe 0 -i "{concat_file}" -c copy "{output}"'
    if _run_ffmpeg(cmd) and Path(output).exists():
        Path(concat_file).unlink(missing_ok=True)
        logger.info("Intro/outro concat applied (%d parts)", len(parts))
        return output
    Path(concat_file).unlink(missing_ok=True)
    return None


# ── Validation ────────────────────────────────────────────────────

def validate_for_upload(path: str, platform: str = "youtube") -> dict:
    """
    Validate video meets platform requirements.
    Returns dict with: valid, warnings (list), error.
    """
    result = {"valid": True, "warnings": [], "error": None}

    if not Path(path).exists():
        result["valid"] = False
        result["error"] = "file does not exist"
        return result

    duration = get_video_duration(path)
    width, height = get_video_dimensions(path)
    file_size = Path(path).stat().st_size / (1024 * 1024)

    # Duration checks
    if platform == "youtube" and duration > scheduler_config.MAX_SHORTS_DURATION:
        result["valid"] = False
        result["error"] = f"duration {duration:.1f}s exceeds YouTube Shorts limit ({scheduler_config.MAX_SHORTS_DURATION}s)"
        return result

    if platform == "instagram" and duration > 90:
        result["valid"] = False
        result["error"] = f"duration {duration:.1f}s exceeds Instagram Reels limit (90s)"
        return result

    # Aspect ratio check (should be vertical: height > width)
    if width > 0 and height > 0:
        if width > height:
            result["warnings"].append(f"horizontal video ({width}x{height}), may not perform as Short/Reel")

    # File size check
    if file_size > 256:
        result["warnings"].append(f"large file ({file_size:.1f}MB), upload may take long")

    if duration == 0:
        result["warnings"].append("could not determine duration")

    return result
