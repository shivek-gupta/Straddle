"""
Automated Headless Kotak Neo Authentication & Session Manager via TOTP.
Provides:
- Automatic TOTP generation using pyotp (Zero manual OTP entry).
- Headless login via NeoAPI: totp_login -> totp_validate(mpin).
- Session caching in data/kotak_session.json.
- Morning automated scheduler daemon (08:45 AM IST).
- Telegram alert notifications on login success/failure.
"""

import os
import sys
import time
import json
import logging
import threading
import datetime
from pathlib import Path
from typing import Dict, Any, Optional

import requests
import pyotp
from neo_api_client import NeoAPI

import config

logger = logging.getLogger("kotak_auth")
logger.setLevel(logging.INFO)

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))


def now_ist() -> datetime.datetime:
    return datetime.datetime.now(IST)


def today_ist_str() -> str:
    return now_ist().strftime("%Y-%m-%d")


_AUTH_LOCK = threading.Lock()
_ACTIVE_CLIENT: Optional[NeoAPI] = None
_LAST_LOGIN_ATTEMPT: float = 0.0
_COOLDOWN_SECONDS: float = 30.0


def send_telegram_notification(message: str) -> bool:
    """Send alert to user Telegram."""
    token = getattr(config, "TELEGRAM_BOT_TOKEN", "")
    chat_id = getattr(config, "TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        res = requests.post(url, json=payload, timeout=8)
        return res.status_code == 200
    except Exception as e:
        logger.warning(f"Failed to send Telegram alert: {e}")
        return False


def get_cached_session() -> Dict[str, Any]:
    """Read saved Kotak Neo session state."""
    if config.KOTAK_SESSION_FILE.exists():
        try:
            with open(config.KOTAK_SESSION_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_cached_session(data: Dict[str, Any]):
    """Save session state to disk."""
    try:
        with open(config.KOTAK_SESSION_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save session state: {e}")


def is_session_active_for_today() -> bool:
    """Check if we already have an active verified session for today."""
    session = get_cached_session()
    if not session.get("is_authenticated"):
        return False
    session_date = session.get("date")
    if session_date != today_ist_str():
        return False
    global _ACTIVE_CLIENT
    if _ACTIVE_CLIENT is not None:
        return True
    return False


def get_authenticated_client(force: bool = False) -> Optional[NeoAPI]:
    """
    Retrieve authenticated NeoAPI client instance.
    If session is missing or expired, performs automated TOTP login.
    """
    global _ACTIVE_CLIENT, _LAST_LOGIN_ATTEMPT

    with _AUTH_LOCK:
        if not force and _ACTIVE_CLIENT is not None:
            return _ACTIVE_CLIENT

        creds = config.get_kotak_credentials()
        consumer_key = creds.get("consumer_key", "").strip()
        consumer_secret = creds.get("consumer_secret", "").strip()
        mobile = creds.get("mobile_number", "").strip()
        ucc = creds.get("ucc", "").strip()
        mpin = creds.get("mpin", "").strip()
        totp_key = creds.get("totp_key", "").strip()
        env = creds.get("environment", "prod").strip()

        if not consumer_key or not consumer_secret:
            logger.warning("[Kotak Auth] Consumer Key or Consumer Secret not configured.")
            return None

        if not mobile or not mpin or not totp_key:
            logger.warning("[Kotak Auth] Mobile, MPIN, or TOTP Key missing. Required for auto-login.")
            return None

        # Check cooldown to prevent hammering on repeated failures
        now_ts = time.time()
        if (now_ts - _LAST_LOGIN_ATTEMPT) < _COOLDOWN_SECONDS and not force:
            logger.info("[Kotak Auth] In cooldown period. Skipping login attempt.")
            return _ACTIVE_CLIENT

        _LAST_LOGIN_ATTEMPT = now_ts

        print(f"[{now_ist().strftime('%H:%M:%S')}] [KOTAK-AUTH] Initiating automated TOTP login for UCC: {ucc or mobile}...")

        try:
            # 1. Initialize NeoAPI
            client = NeoAPI(
                consumer_key=consumer_key,
                consumer_secret=consumer_secret,
                environment=env
            )

            # 2. Generate live TOTP code via pyotp
            totp_clean = totp_key.replace(" ", "").upper()
            totp_code = pyotp.TOTP(totp_clean).now()

            # 3. Call totp_login
            login_res = client.totp_login(
                mobile_number=mobile,
                ucc=ucc if ucc else None,
                totp=totp_code
            )
            logger.info(f"[Kotak Auth] Step 1 totp_login response: {login_res}")

            # 4. Call totp_validate with MPIN
            val_res = client.totp_validate(mpin=mpin)
            logger.info(f"[Kotak Auth] Step 2 totp_validate response: {val_res}")

            _ACTIVE_CLIENT = client

            session_data = {
                "is_authenticated": True,
                "date": today_ist_str(),
                "time": now_ist().strftime("%H:%M:%S"),
                "ucc": ucc,
                "mobile": mobile[-4:] if len(mobile) >= 4 else mobile,
                "environment": env,
                "last_success_ts": time.time()
            }
            save_cached_session(session_data)

            msg = "Kotak Neo Auto-Login Successful\nUCC: " + str(ucc) + "\nTime: " + now_ist().strftime("%d-%b-%Y %H:%M:%S IST")
            send_telegram_notification(msg)
            print(f"[{now_ist().strftime('%H:%M:%S')}] [KOTAK-AUTH] Login successful! Session established.")
            return _ACTIVE_CLIENT

        except Exception as e:
            logger.error(f"[Kotak Auth] Auto-login failed: {e}")
            err_msg = "Kotak Neo Auto-Login Failed\nError: " + str(e) + "\nTime: " + now_ist().strftime("%d-%b-%Y %H:%M:%S IST")
            send_telegram_notification(err_msg)
            return None


def get_auth_status() -> Dict[str, Any]:
    """Return comprehensive status for Web UI and health monitoring."""
    creds = config.get_kotak_credentials()
    session = get_cached_session()
    has_creds = bool(creds.get("consumer_key") and creds.get("mobile_number") and creds.get("totp_key"))
    is_active = (_ACTIVE_CLIENT is not None) or (session.get("is_authenticated") and session.get("date") == today_ist_str())

    return {
        "configured": has_creds,
        "is_authenticated": is_active,
        "date": session.get("date", "Not logged in today"),
        "time": session.get("time", "--"),
        "ucc": creds.get("ucc", session.get("ucc", "Not set")),
        "masked_mobile": (creds.get("mobile_number", "")[:2] + "****" + creds.get("mobile_number", "")[-4:]) if len(creds.get("mobile_number", "")) >= 6 else "Not set",
        "has_totp_key": bool(creds.get("totp_key")),
        "has_mpin": bool(creds.get("mpin")),
        "environment": creds.get("environment", "prod")
    }


def start_morning_scheduler():
    """Background daemon thread for daily 08:45 AM token refresh."""
    def _loop():
        logger.info("[Scheduler] Kotak Neo 08:45 AM morning auth scheduler started.")
        while True:
            try:
                now = now_ist()
                if now.weekday() < 5:
                    target_time = now.replace(hour=8, minute=45, second=0, microsecond=0)
                    if now < target_time:
                        sleep_seconds = (target_time - now).total_seconds()
                    else:
                        if not is_session_active_for_today():
                            logger.info("[Scheduler] Session missing for today. Triggering catch-up login...")
                            get_authenticated_client(force=True)
                        tomorrow = now + datetime.timedelta(days=1)
                        target_tomorrow = tomorrow.replace(hour=8, minute=45, second=0, microsecond=0)
                        sleep_seconds = (target_tomorrow - now).total_seconds()
                    
                    time.sleep(min(sleep_seconds, 300))
                else:
                    time.sleep(3600)
            except Exception as e:
                logger.error(f"[Scheduler] Exception in morning scheduler: {e}")
                time.sleep(60)

    t = threading.Thread(target=_loop, daemon=True, name="KotakMorningScheduler")
    t.start()


if __name__ == "__main__":
    print("=" * 65)
    print("KOTAK NEO AUTOMATED TOTP LOGIN CLI")
    print("=" * 65)
    cli_client = get_authenticated_client(force=True)
    if cli_client:
        print("\nSUCCESS: Authenticated successfully with Kotak Neo!")
        try:
            limits = cli_client.limits()
            print("Broker Limits / Funds Response:", limits)
        except Exception as ex:
            print("Note on limits check:", ex)
    else:
        print("\nNOTICE: Could not authenticate. Please verify credentials in config or UI.")
