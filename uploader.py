"""
uploader.py — Platform upload adapters for YouTube Shorts and Instagram Reels.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

import scheduler_config
import oauth_helper

logger = logging.getLogger(__name__)


class UploadResult:
    """Standardized upload result."""

    # S5: Error classification types
    # AUTH_ERROR, RATE_LIMIT, QUOTA_EXCEEDED, NETWORK_ERROR,
    # VALIDATION_FAIL, PLATFORM_REJECT, TOKEN_EXPIRED, UNKNOWN

    def __init__(
        self,
        success: bool = False,
        uploaded_url: str = "",
        platform_id: str = "",
        error: str = "",
        error_type: str = "UNKNOWN",
        retryable: bool = True,
    ):
        self.success = success
        self.uploaded_url = uploaded_url
        self.platform_id = platform_id
        self.error = error
        self.error_type = error_type if not success else ""
        self.retryable = retryable

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "uploaded_url": self.uploaded_url,
            "platform_id": self.platform_id,
            "error": self.error,
            "error_type": self.error_type,
            "retryable": self.retryable,
        }


# ── YouTube Shorts Uploader ──────────────────────────────────────

class YouTubeUploader:
    """Upload videos to YouTube as Shorts via Data API v3 resumable upload."""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.account = oauth_helper.get_account(account_id)

    def upload(
        self,
        video_path: str,
        title: str,
        description: str,
        tags: list[str] | None = None,
        hashtags: list[str] | None = None,
        category_id: str = "22",
    ) -> UploadResult:
        """Upload a video to YouTube as a Short."""
        import requests

        if not self.account or not self.account.get("token_valid"):
            return UploadResult(error="invalid_credentials", error_type="AUTH_ERROR", retryable=False)

        token = oauth_helper.get_access_token(self.account_id)
        if not token:
            return UploadResult(error="no_valid_token", error_type="TOKEN_EXPIRED", retryable=False)

        # Build description with hashtags
        full_desc = description
        if hashtags:
            full_desc += "\n\n" + " ".join(f"#{h.strip('#')}" for h in hashtags[:15])

        # Step 1: Start resumable upload
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=UTF-8",
            "X-Upload-Content-Type": "video/*",
            "X-Upload-Content-Length": str(Path(video_path).stat().st_size),
        }

        metadata = {
            "snippet": {
                "title": title[:100],
                "description": full_desc[:5000],
                "tags": (tags or [])[:500],
                "categoryId": category_id or "22",
            },
            "status": {
                "privacyStatus": "public",
                "selfDeclaredMadeForKids": False,
                "madeForKids": False,
            },
        }

        init_url = (
            "https://www.googleapis.com/upload/youtube/v3/videos"
            "?uploadType=resumable&part=snippet,status"
        )

        for attempt in range(1, scheduler_config.MAX_UPLOAD_ATTEMPTS + 1):
            try:
                resp = requests.post(
                    init_url, headers=headers,
                    data=json.dumps(metadata), timeout=30,
                )

                if resp.status_code == 401:
                    # Try token refresh
                    if oauth_helper.refresh_youtube_token(self.account_id):
                        token = oauth_helper.get_access_token(self.account_id)
                        headers["Authorization"] = f"Bearer {token}"
                        continue
                    return UploadResult(error="auth_failed_401", error_type="AUTH_ERROR", retryable=False)

                if resp.status_code == 403:
                    return UploadResult(
                        error=f"permission_denied: {resp.text[:200]}",
                        error_type="PLATFORM_REJECT",
                        retryable=False,
                    )

                if resp.status_code >= 500:
                    wait = scheduler_config.RETRY_BACKOFF_BASE * (3 ** (attempt - 1))
                    logger.warning("YouTube 5xx (attempt %d), backing off %.1fs", attempt, wait)
                    time.sleep(wait)
                    continue

                if resp.status_code == 429:
                    retry_after = int(resp.headers.get("Retry-After", 30))
                    logger.warning("YouTube 429, waiting %ds", retry_after)
                    time.sleep(retry_after)
                    continue

                resp.raise_for_status()
                upload_url = resp.headers.get("Location")
                if not upload_url:
                    return UploadResult(error="no_upload_url_in_response", error_type="PLATFORM_REJECT")

                break
            except requests.RequestException as e:
                if attempt == scheduler_config.MAX_UPLOAD_ATTEMPTS:
                    return UploadResult(error=f"init_failed: {e}", error_type="NETWORK_ERROR")
                wait = scheduler_config.RETRY_BACKOFF_BASE * (3 ** (attempt - 1))
                time.sleep(wait)
        else:
            return UploadResult(error="max_init_retries_exceeded", error_type="RATE_LIMIT")

        # Step 2: Upload the video file
        try:
            with open(video_path, "rb") as f:
                upload_headers = {
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "video/*",
                    "Content-Length": str(Path(video_path).stat().st_size),
                }
                resp = requests.put(
                    upload_url, headers=upload_headers,
                    data=f, timeout=600,
                )

            if resp.status_code in (200, 201):
                data = resp.json()
                video_id = data.get("id", "")
                url = f"https://youtube.com/shorts/{video_id}"
                logger.info("YouTube upload success: %s", url)
                return UploadResult(
                    success=True, uploaded_url=url, platform_id=video_id,
                )
            elif resp.status_code == 401:
                return UploadResult(error="auth_expired_during_upload", error_type="TOKEN_EXPIRED", retryable=False)
            else:
                return UploadResult(error=f"upload_failed_{resp.status_code}: {resp.text[:200]}", error_type="PLATFORM_REJECT")

        except requests.RequestException as e:
            return UploadResult(error=f"upload_exception: {e}", error_type="NETWORK_ERROR")


# ── Instagram Reels Uploader ─────────────────────────────────────

class InstagramUploader:
    """Upload videos to Instagram as Reels via Graph API."""

    def __init__(self, account_id: str):
        self.account_id = account_id
        self.account = oauth_helper.get_account(account_id)

    def _check_ig_app_mode(self, token: str) -> dict:
        """
        Validate Instagram app is in Live Mode via debug_token.
        Returns {"ok": True} or {"ok": False, "reason": "..."}.
        """
        import requests
        app_token = f"{scheduler_config.INSTAGRAM_APP_ID}|{scheduler_config.INSTAGRAM_APP_SECRET}"
        if not app_token or app_token == "|":
            return {"ok": False, "reason": "ig_app_credentials_not_configured"}
        try:
            resp = requests.get(
                "https://graph.facebook.com/debug_token",
                params={"input_token": token, "access_token": app_token},
                timeout=10,
            )
            if resp.status_code != 200:
                return {"ok": False, "reason": f"debug_token_http_{resp.status_code}"}
            data = resp.json().get("data", {})
            if not data.get("is_valid", False):
                return {"ok": False, "reason": "ig_token_invalid"}
            app_type = data.get("type", "")
            if app_type == "USER" and data.get("application", "") == "":
                return {"ok": False, "reason": "ig_app_in_dev_mode"}
            return {"ok": True}
        except requests.RequestException as e:
            logger.warning("IG debug_token check failed: %s", e)
            return {"ok": True}  # Don't block on network errors

    def upload(
        self,
        video_path: str,
        title: str,
        description: str,
        tags: list[str] | None = None,
        hashtags: list[str] | None = None,
        category_id: str | None = None,
    ) -> UploadResult:
        """Upload a video to Instagram as a Reel."""
        import requests

        if not self.account or not self.account.get("token_valid"):
            return UploadResult(error="invalid_credentials", error_type="AUTH_ERROR", retryable=False)

        token = oauth_helper.get_access_token(self.account_id)
        ig_user_id = self.account.get("ig_user_id")
        if not token or not ig_user_id:
            return UploadResult(error="missing_ig_credentials", error_type="AUTH_ERROR", retryable=False)

        # Gap #1: Check if app is in Live Mode before uploading
        mode_check = self._check_ig_app_mode(token)
        if not mode_check["ok"]:
            logger.error("Instagram app mode check failed: %s", mode_check["reason"])
            return UploadResult(error=mode_check["reason"], error_type="PLATFORM_REJECT", retryable=False)

        # Build caption with hashtags
        caption = description
        if hashtags:
            caption += "\n\n" + " ".join(f"#{h.strip('#')}" for h in hashtags[:30])

        # Step 1: Create media container
        # Note: Instagram Graph API requires video to be accessible via URL.
        # For server-to-server, you typically need to host the video at a public URL
        # or use the resumable upload API. Here we attempt hosted URL approach.
        # In production, you would upload to a temp S3/GCS bucket first.
        container_url = f"https://graph.instagram.com/v17.0/{ig_user_id}/media"

        for attempt in range(1, scheduler_config.MAX_UPLOAD_ATTEMPTS + 1):
            try:
                resp = requests.post(container_url, data={
                    "media_type": "REELS",
                    "caption": caption[:2200],
                    "share_to_feed": "true",
                    "access_token": token,
                    # In production: "video_url": public_url_of_video
                }, timeout=30)

                if resp.status_code == 401 or resp.status_code == 190:
                    # Token expired
                    if oauth_helper.refresh_instagram_token(self.account_id):
                        token = oauth_helper.get_access_token(self.account_id)
                        continue
                    return UploadResult(error="ig_auth_failed", error_type="AUTH_ERROR", retryable=False)

                if resp.status_code == 429:
                    wait = 30 * attempt
                    logger.warning("Instagram 429, waiting %ds", wait)
                    time.sleep(wait)
                    continue

                if resp.status_code >= 500:
                    wait = scheduler_config.RETRY_BACKOFF_BASE * (3 ** (attempt - 1))
                    time.sleep(wait)
                    continue

                if resp.status_code != 200:
                    error_data = resp.json() if resp.text else {}
                    error_msg = error_data.get("error", {}).get("message", resp.text[:200])
                    return UploadResult(error=f"container_failed: {error_msg}", error_type="PLATFORM_REJECT")

                container_id = resp.json().get("id")
                if not container_id:
                    return UploadResult(error="no_container_id", error_type="PLATFORM_REJECT")

                break
            except requests.RequestException as e:
                if attempt == scheduler_config.MAX_UPLOAD_ATTEMPTS:
                    return UploadResult(error=f"container_exception: {e}", error_type="NETWORK_ERROR")
                time.sleep(scheduler_config.RETRY_BACKOFF_BASE * (3 ** (attempt - 1)))
        else:
            return UploadResult(error="max_container_retries_exceeded", error_type="RATE_LIMIT")

        # Step 2: Wait for container to be ready (poll status)
        # S3: 5s intervals × 24 polls = 120s max wait
        status_url = f"https://graph.instagram.com/v17.0/{container_id}"
        for poll in range(1, 25):
            time.sleep(5)
            try:
                status_resp = requests.get(status_url, params={
                    "fields": "status_code",
                    "access_token": token,
                }, timeout=15)
                if status_resp.status_code == 200:
                    status = status_resp.json().get("status_code")
                    logger.debug("IG container poll %d/24: status=%s", poll, status)
                    if status == "FINISHED":
                        break
                    if status == "ERROR":
                        return UploadResult(
                            error="ig_container_processing_error",
                            error_type="PLATFORM_REJECT",
                        )
            except requests.RequestException:
                logger.debug("IG container poll %d/24: request error, retrying", poll)
                continue
        else:
            return UploadResult(error="ig_container_timeout_120s", error_type="NETWORK_ERROR")

        # Step 3: Publish
        publish_url = f"https://graph.instagram.com/v17.0/{ig_user_id}/media_publish"
        try:
            resp = requests.post(publish_url, data={
                "creation_id": container_id,
                "access_token": token,
            }, timeout=30)

            if resp.status_code == 200:
                media_id = resp.json().get("id", "")
                url = f"https://www.instagram.com/reel/{media_id}/"
                logger.info("Instagram upload success: %s", url)
                return UploadResult(
                    success=True, uploaded_url=url, platform_id=media_id,
                )
            else:
                error_data = resp.json() if resp.text else {}
                error_msg = error_data.get("error", {}).get("message", resp.text[:200])
                return UploadResult(error=f"publish_failed: {error_msg}", error_type="PLATFORM_REJECT")
        except requests.RequestException as e:
            return UploadResult(error=f"publish_exception: {e}", error_type="NETWORK_ERROR")


# ── Factory ───────────────────────────────────────────────────────

def get_uploader(account_id: str):
    """Get the appropriate uploader for an account."""
    account = oauth_helper.get_account(account_id)
    if not account:
        return None
    platform = account.get("platform", "")
    if platform == "youtube":
        return YouTubeUploader(account_id)
    elif platform == "instagram":
        return InstagramUploader(account_id)
    return None
