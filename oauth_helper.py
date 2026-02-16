"""
oauth_helper.py — OAuth handshake and token management for destination accounts.
Handles YouTube and Instagram OAuth flows, token refresh, and encrypted storage.
"""

import json
import logging
import os
import time
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import scheduler_config

logger = logging.getLogger(__name__)


# ── Credential storage (simple encrypted-at-rest JSON file) ──────

import fcntl

def _load_credentials() -> dict:
    """Load stored credentials from JSON file. ASSUMES LOCK IS HELD if used internally."""
    cred_path = scheduler_config.CREDENTIALS_FILE
    if not cred_path.exists():
        return {"accounts": {}, "oauth_states": {}}
    try:
        with open(cred_path, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"accounts": {}, "oauth_states": {}}


def _save_credentials(data: dict):
    """Save credentials to JSON file. ASSUMES LOCK IS HELD if used internally."""
    cred_path = scheduler_config.CREDENTIALS_FILE
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cred_path, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(str(cred_path), 0o600)


def _update_creds_transactional(update_fn):
    """Utility to perform thread-safe and process-safe updates to credentials file."""
    cred_path = scheduler_config.CREDENTIALS_FILE
    cred_path.parent.mkdir(parents=True, exist_ok=True)
    
    # We use a helper lock file to manage flock, to avoid issues with 'w' mode truncating before lock
    lock_path = cred_path.with_suffix(".lock")
    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            creds = _load_credentials()
            updated_creds = update_fn(creds)
            _save_credentials(updated_creds)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)


# ── Account management ───────────────────────────────────────────

def get_all_accounts() -> list[dict]:
    """Return all registered destination accounts (without sensitive tokens)."""
    creds = _load_credentials() # Read-only, fine for single op
    accounts = []
    for account_id, info in creds.get("accounts", {}).items():
        accounts.append({
            "account_id": account_id,
            "platform": info.get("platform", "unknown"),
            "account_name": info.get("account_name", account_id),
            "status": info.get("status", "unknown"),
            "token_valid": info.get("token_valid", False),
            "connected_at": info.get("connected_at", ""),
            "last_refresh": info.get("last_refresh", ""),
        })
    return accounts


def get_account(account_id: str) -> dict | None:
    """Get full account info including tokens (for internal use)."""
    creds = _load_credentials()
    return creds.get("accounts", {}).get(account_id)


def save_account(account_id: str, account_data: dict):
    """Save or update an account's credentials."""
    def _update(creds):
        creds.setdefault("accounts", {})[account_id] = account_data
        return creds
    
    _update_creds_transactional(_update)
    logger.info("Saved account: %s (%s)", account_id, account_data.get("platform"))


def mark_account_invalid(account_id: str, reason: str = ""):
    """Mark an account's token as invalid."""
    def _update(creds):
        if account_id in creds.get("accounts", {}):
            creds["accounts"][account_id]["token_valid"] = False
            creds["accounts"][account_id]["status"] = "token_invalid"
            creds["accounts"][account_id]["invalid_reason"] = reason
        return creds
    
    _update_creds_transactional(_update)
    logger.warning("Marked account %s as invalid: %s", account_id, reason)


def remove_account(account_id: str) -> bool:
    """Remove an account entirely."""
    removed = [False]
    def _update(creds):
        if account_id in creds.get("accounts", {}):
            del creds["accounts"][account_id]
            removed[0] = True
        return creds
    
    _update_creds_transactional(_update)
    return removed[0]


# ── YouTube OAuth ─────────────────────────────────────────────────

def generate_youtube_oauth_url() -> tuple[str, str]:
    """
    Generate a YouTube OAuth2 authorization URL.
    Returns (url, state_token).
    """
    # Gap #9: Warn if redirect URI is HTTP while HTTPS is preferred
    redirect = scheduler_config.YOUTUBE_REDIRECT_URI
    if scheduler_config.OAUTH_REQUIRE_HTTPS and redirect.startswith("http://"):
        logger.warning(
            "SECURITY: OAuth redirect URI uses HTTP (%s). "
            "Set OAUTH_REQUIRE_HTTPS=false or update YOUTUBE_REDIRECT_URI to HTTPS.",
            redirect,
        )

    state = secrets.token_urlsafe(32)
    
    def _update(creds):
        creds.setdefault("oauth_states", {})[state] = {
            "platform": "youtube",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return creds
    
    _update_creds_transactional(_update)

    import urllib.parse
    params = {
        "client_id": scheduler_config.YOUTUBE_CLIENT_ID,
        "redirect_uri": redirect,
        "response_type": "code",
        "scope": "https://www.googleapis.com/auth/youtube.upload https://www.googleapis.com/auth/youtube",
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
    return url, state


def exchange_youtube_code(code: str, state: str) -> dict:
    """
    Exchange an authorization code for access + refresh tokens.
    Returns account info dict on success.
    """
    import requests

    # Validate state
    creds = _load_credentials()
    if state not in creds.get("oauth_states", {}):
        return {"error": "invalid_state"}

    # Exchange code
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "code": code,
        "client_id": scheduler_config.YOUTUBE_CLIENT_ID,
        "client_secret": scheduler_config.YOUTUBE_CLIENT_SECRET,
        "redirect_uri": scheduler_config.YOUTUBE_REDIRECT_URI,
        "grant_type": "authorization_code",
    }, timeout=15)

    if resp.status_code != 200:
        return {"error": f"token_exchange_failed: {resp.text[:200]}"}

    tokens = resp.json()

    # Get channel info
    headers = {"Authorization": f"Bearer {tokens['access_token']}"}
    ch_resp = requests.get(
        "https://www.googleapis.com/youtube/v3/channels",
        params={"part": "snippet", "mine": "true"},
        headers=headers, timeout=15,
    )

    channel_name = "unknown"
    channel_id = "unknown"
    if ch_resp.status_code == 200:
        items = ch_resp.json().get("items", [])
        if items:
            channel_name = items[0]["snippet"]["title"]
            channel_id = items[0]["id"]

    account_id = f"yt_{channel_id}"
    now = datetime.now(timezone.utc).isoformat()

    account_data = {
        "platform": "youtube",
        "account_name": channel_name,
        "channel_id": channel_id,
        "access_token": tokens["access_token"],
        "refresh_token": tokens.get("refresh_token", ""),
        "token_expiry": now,  # Will be refreshed before use
        "expires_in": tokens.get("expires_in", 3600),
        "token_valid": True,
        "status": "active",
        "connected_at": now,
        "last_refresh": now,
    }

    def _update_final(creds):
        creds.setdefault("accounts", {})[account_id] = account_data
        if state in creds.get("oauth_states", {}):
            del creds["oauth_states"][state]
        return creds
    
    _update_creds_transactional(_update_final)
    logger.info("Saved and cleaned up account: %s", account_id)

    return {"account_id": account_id, "account_name": channel_name, "platform": "youtube"}


def refresh_youtube_token(account_id: str) -> bool:
    """Refresh the access token for a YouTube account."""
    import requests

    account = get_account(account_id)
    if not account or account.get("platform") != "youtube":
        return False

    refresh_token = account.get("refresh_token")
    if not refresh_token:
        mark_account_invalid(account_id, "no_refresh_token")
        return False

    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": scheduler_config.YOUTUBE_CLIENT_ID,
        "client_secret": scheduler_config.YOUTUBE_CLIENT_SECRET,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token",
    }, timeout=15)

    if resp.status_code != 200:
        mark_account_invalid(account_id, f"refresh_failed: {resp.status_code}")
        return False

    tokens = resp.json()
    account["access_token"] = tokens["access_token"]
    account["expires_in"] = tokens.get("expires_in", 3600)
    account["last_refresh"] = datetime.now(timezone.utc).isoformat()
    account["token_valid"] = True
    account["status"] = "active"
    save_account(account_id, account)
    logger.info("Refreshed YouTube token for %s", account_id)
    return True


# ── Instagram OAuth ───────────────────────────────────────────────

def generate_instagram_oauth_url() -> tuple[str, str]:
    """Generate an Instagram OAuth authorization URL."""
    state = secrets.token_urlsafe(32)
    def _update(creds):
        creds.setdefault("oauth_states", {})[state] = {
            "platform": "instagram",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        return creds
    
    _update_creds_transactional(_update)

    params = {
        "client_id": scheduler_config.INSTAGRAM_APP_ID,
        "redirect_uri": scheduler_config.YOUTUBE_REDIRECT_URI,  # Reuse callback
        "scope": "instagram_basic,instagram_content_publish",
        "response_type": "code",
        "state": state,
    }
    url = "https://api.instagram.com/oauth/authorize?" + "&".join(
        f"{k}={v}" for k, v in params.items()
    )
    return url, state


def exchange_instagram_code(code: str, state: str) -> dict:
    """Exchange code for Instagram long-lived token."""
    import requests

    creds = _load_credentials()
    if state not in creds.get("oauth_states", {}):
        return {"error": "invalid_state"}

    # Step 1: Exchange for short-lived token
    resp = requests.post("https://api.instagram.com/oauth/access_token", data={
        "client_id": scheduler_config.INSTAGRAM_APP_ID,
        "client_secret": scheduler_config.INSTAGRAM_APP_SECRET,
        "grant_type": "authorization_code",
        "redirect_uri": scheduler_config.YOUTUBE_REDIRECT_URI,
        "code": code,
    }, timeout=15)

    if resp.status_code != 200:
        return {"error": f"token_exchange_failed: {resp.text[:200]}"}

    short_data = resp.json()
    short_token = short_data.get("access_token")
    user_id = str(short_data.get("user_id", ""))

    # Step 2: Exchange for long-lived token
    resp2 = requests.get("https://graph.instagram.com/access_token", params={
        "grant_type": "ig_exchange_token",
        "client_secret": scheduler_config.INSTAGRAM_APP_SECRET,
        "access_token": short_token,
    }, timeout=15)

    if resp2.status_code == 200:
        long_data = resp2.json()
        access_token = long_data.get("access_token", short_token)
        expires_in = long_data.get("expires_in", 5184000)
    else:
        access_token = short_token
        expires_in = 3600

    # Get username
    me_resp = requests.get(f"https://graph.instagram.com/{user_id}", params={
        "fields": "id,username",
        "access_token": access_token,
    }, timeout=15)
    username = "unknown"
    if me_resp.status_code == 200:
        username = me_resp.json().get("username", "unknown")

    account_id = f"ig_{user_id}"
    now = datetime.now(timezone.utc).isoformat()

    account_data = {
        "platform": "instagram",
        "account_name": username,
        "ig_user_id": user_id,
        "access_token": access_token,
        "expires_in": expires_in,
        "token_valid": True,
        "status": "active",
        "connected_at": now,
        "last_refresh": now,
    }

    def _update_final(creds):
        creds.setdefault("accounts", {})[account_id] = account_data
        if state in creds.get("oauth_states", {}):
            del creds["oauth_states"][state]
        return creds
    
    _update_creds_transactional(_update_final)
    logger.info("Saved and cleaned up account: %s", account_id)

    return {"account_id": account_id, "account_name": username, "platform": "instagram"}


def refresh_instagram_token(account_id: str) -> bool:
    """Refresh a long-lived Instagram token."""
    import requests

    account = get_account(account_id)
    if not account or account.get("platform") != "instagram":
        return False

    access_token = account.get("access_token")
    if not access_token:
        mark_account_invalid(account_id, "no_access_token")
        return False

    resp = requests.get("https://graph.instagram.com/refresh_access_token", params={
        "grant_type": "ig_refresh_token",
        "access_token": access_token,
    }, timeout=15)

    if resp.status_code != 200:
        mark_account_invalid(account_id, f"refresh_failed: {resp.status_code}")
        return False

    data = resp.json()
    account["access_token"] = data.get("access_token", access_token)
    account["expires_in"] = data.get("expires_in", 5184000)
    account["last_refresh"] = datetime.now(timezone.utc).isoformat()
    account["token_valid"] = True
    save_account(account_id, account)
    logger.info("Refreshed Instagram token for %s", account_id)
    return True


# ── Token refresh scheduler ──────────────────────────────────────

def refresh_all_tokens():
    """Refresh all tokens that are nearing expiry."""
    accounts = _load_credentials().get("accounts", {})
    for account_id, info in accounts.items():
        if not info.get("token_valid"):
            continue
        platform = info.get("platform")
        try:
            if platform == "youtube":
                refresh_youtube_token(account_id)
            elif platform == "instagram":
                refresh_instagram_token(account_id)
        except Exception as e:
            logger.error("Failed to refresh token for %s: %s", account_id, e)
            mark_account_invalid(account_id, str(e))


def get_access_token(account_id: str) -> str | None:
    """Get a valid access token for an account, refreshing if needed."""
    account = get_account(account_id)
    if not account:
        return None
    if not account.get("token_valid"):
        return None

    # Try to use existing token (we refresh proactively in scheduler)
    return account.get("access_token")
