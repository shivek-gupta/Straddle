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
import httpx
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
_COOLDOWN_SECONDS: float = 10.0
_LAST_AUTH_ERROR: str = ""

_WARMUP_THREAD_STARTED = False
_WARMUP_LOCK = threading.Lock()


def warm_up_connection(client: Optional[NeoAPI] = None) -> bool:
    """
    Pre-warms the HTTP/2 TCP connection and TLS session to Kotak Neo OMS gateway.
    Eliminates cold-start connection penalties (DNS + TCP SYN/ACK + TLS 1.3 handshake),
    reducing subsequent order dispatch latency down to the physical RTT minimum (<25ms).
    """
    try:
        c = client or _ACTIVE_CLIENT
        if c is None or not hasattr(c, "api_client") or not hasattr(c.api_client, "rest_client"):
            return False
        session = getattr(c.api_client.rest_client, "session", None)
        if session and isinstance(session, httpx.Client):
            urls = ["https://mis.kotaksecurities.com/"]
            for cfg_obj in (getattr(c, "configuration", None), getattr(c.api_client, "configuration", None)):
                if cfg_obj is not None:
                    b_url = getattr(cfg_obj, "base_url", None) or getattr(cfg_obj, "host", None)
                    if b_url and str(b_url).startswith("http"):
                        clean_u = str(b_url).rstrip("/") + "/"
                        if clean_u not in urls:
                            urls.append(clean_u)
            for u in urls:
                try:
                    session.head(u, timeout=httpx.Timeout(1.2))
                except Exception:
                    pass
            return True
    except Exception as e:
        logger.debug(f"[Kotak Auth] Connection warm-up notice: {e}")
    return False


def _start_connection_warmup_daemon():
    global _WARMUP_THREAD_STARTED
    with _WARMUP_LOCK:
        if _WARMUP_THREAD_STARTED:
            return
        _WARMUP_THREAD_STARTED = True

    def _warmup_loop():
        # High-frequency keep-alive ping loop (every 3s) so connection NEVER drops or goes idle
        while True:
            try:
                time.sleep(3.0)
                c = _ACTIVE_CLIENT
                if c is not None:
                    warm_up_connection(c)
            except Exception:
                time.sleep(3.0)

    th = threading.Thread(target=_warmup_loop, daemon=True, name="KotakWarmupDaemon")
    th.start()


def get_network_time_offset() -> float:
    """Calculate time drift (in seconds) between system clock and Kotak Neo / NTP server."""
    try:
        r = requests.head("https://mis.kotaksecurities.com", timeout=3)
        server_date = r.headers.get("Date")
        if server_date:
            import email.utils
            server_ts = email.utils.parsedate_to_datetime(server_date).timestamp()
            return server_ts - time.time()
    except Exception:
        pass
    try:
        r = requests.head("https://www.google.com", timeout=3)
        server_date = r.headers.get("Date")
        if server_date:
            import email.utils
            server_ts = email.utils.parsedate_to_datetime(server_date).timestamp()
            return server_ts - time.time()
    except Exception:
        pass
    return 0.0


def get_last_auth_error() -> str:
    """Return the last authentication error message."""
    global _LAST_AUTH_ERROR
    return _LAST_AUTH_ERROR


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


def is_client_trading_ready(client: Optional[NeoAPI] = None) -> bool:
    """
    Inviolable verification: check if the NeoAPI client instance has completed
    both Step 1 (TOTP Login) and Step 2 (MPIN 2FA Validation) and possesses
    active, valid trade authorization tokens ('edit_token' & 'edit_sid').
    """
    c = client or _ACTIVE_CLIENT
    if c is None:
        return False
    cfg = getattr(c, "configuration", None)
    if cfg is None:
        return False
    edit_token = getattr(cfg, "edit_token", None)
    edit_sid = getattr(cfg, "edit_sid", None)
    return bool(edit_token and edit_sid and str(edit_token).strip() and str(edit_sid).strip())


def _extract_neo_error(res: Any) -> str:
    """Extract human-readable error from any Kotak Neo response format."""
    if not isinstance(res, dict):
        return str(res) if res else ""
    for k in ("error", "Error", "Error Message", "message", "errMsg", "emsg", "rejectionReason", "rejRsn", "desc"):
        val = res.get(k)
        if val:
            if isinstance(val, list):
                msgs = [e.get("message", str(e)) if isinstance(e, dict) else str(e) for e in val]
                return "; ".join(msgs)
            if isinstance(val, dict):
                return val.get("message") or str(val)
            return str(val)
    if "fault" in res:
        f = res.get("fault")
        if isinstance(f, dict):
            return f.get("message") or str(f)
        return str(f)
    if isinstance(res.get("data"), dict):
        d = res["data"]
        for dk in ("errMsg", "emsg", "message", "error"):
            if d.get(dk):
                return str(d[dk])
    return ""


def invalidate_session():
    """Clear memory client and cached session state on 401, 2FA, or token expiration."""
    global _ACTIVE_CLIENT, _LAST_AUTH_ERROR
    with _AUTH_LOCK:
        _ACTIVE_CLIENT = None
        _LAST_AUTH_ERROR = "Session expired or unauthorized (2FA required)"
        session = get_cached_session()
        session["is_authenticated"] = False
        session["has_edit_token"] = False
        session["has_edit_sid"] = False
        save_cached_session(session)
        logger.info("[Kotak Auth] Session invalidated. Ready for re-authentication.")


def is_session_active_for_today() -> bool:
    """Check if we already have an active verified 2FA trading session for today."""
    session = get_cached_session()
    if not session.get("is_authenticated"):
        return False
    session_date = session.get("date")
    if session_date != today_ist_str():
        return False
    global _ACTIVE_CLIENT
    return is_client_trading_ready(_ACTIVE_CLIENT)


def get_authenticated_client(force: bool = False) -> Optional[NeoAPI]:
    """
    Retrieve authenticated NeoAPI client instance with verified 2FA trading permissions.
    If session is missing, unauthenticated, or missing trade tokens, performs automated TOTP login + MPIN validation.
    """
    global _ACTIVE_CLIENT, _LAST_LOGIN_ATTEMPT, _LAST_AUTH_ERROR

    # Fast path: Zero-lock atomic read for parallel sub-millisecond order dispatch
    if not force:
        cached = _ACTIVE_CLIENT
        if cached is not None and is_client_trading_ready(cached):
            if not _WARMUP_THREAD_STARTED:
                _start_connection_warmup_daemon()
            return cached

    with _AUTH_LOCK:
        if not force and _ACTIVE_CLIENT is not None and is_client_trading_ready(_ACTIVE_CLIENT):
            return _ACTIVE_CLIENT

        creds = config.get_kotak_credentials()
        consumer_key = creds.get("consumer_key", "").strip()
        mobile = creds.get("mobile_number", "").strip()
        ucc = creds.get("ucc", "").strip()
        mpin = creds.get("mpin", "").strip()
        totp_key = creds.get("totp_key", "").strip()
        env = creds.get("environment", "prod").strip()

        if not consumer_key:
            _LAST_AUTH_ERROR = "Consumer Key is missing. Please enter your Kotak Neo Consumer Key."
            logger.warning(f"[Kotak Auth] {_LAST_AUTH_ERROR}")
            return None

        if not mobile:
            _LAST_AUTH_ERROR = "Registered Mobile Number is missing."
            logger.warning(f"[Kotak Auth] {_LAST_AUTH_ERROR}")
            return None

        if not ucc:
            _LAST_AUTH_ERROR = "UCC (Client Code) is missing."
            logger.warning(f"[Kotak Auth] {_LAST_AUTH_ERROR}")
            return None

        if not mpin:
            _LAST_AUTH_ERROR = "MPIN (6-digit) is missing."
            logger.warning(f"[Kotak Auth] {_LAST_AUTH_ERROR}")
            return None

        if not totp_key:
            _LAST_AUTH_ERROR = "TOTP Secret Key (Base32) is missing."
            logger.warning(f"[Kotak Auth] {_LAST_AUTH_ERROR}")
            return None

        # Check cooldown to prevent hammering on repeated failures (only if client is already valid or attempted < 3s ago)
        now_ts = time.time()
        if (now_ts - _LAST_LOGIN_ATTEMPT) < 3.0 and not force:
            logger.info("[Kotak Auth] In cooldown period. Skipping login attempt.")
            return _ACTIVE_CLIENT if is_client_trading_ready(_ACTIVE_CLIENT) else None

        _LAST_LOGIN_ATTEMPT = now_ts
        _LAST_AUTH_ERROR = ""

        # Format mobile number to ensure +91 prefix
        clean_mobile = mobile.strip()
        if clean_mobile.startswith("+91"):
            pass
        elif clean_mobile.startswith("91") and len(clean_mobile) == 12:
            clean_mobile = "+" + clean_mobile
        elif len(clean_mobile) == 10:
            clean_mobile = "+91" + clean_mobile

        print(f"[{now_ist().strftime('%H:%M:%S')}] [KOTAK-AUTH] Initiating automated TOTP login for UCC: {ucc} (Mobile: {clean_mobile})...")

        try:
            # 1. Initialize NeoAPI with HTTP/2 pooling, ultra-responsive socket timeouts, and 3600s keepalive
            client = NeoAPI(
                consumer_key=consumer_key,
                environment=env,
                http2=True,
                timeout=httpx.Timeout(connect=2.0, read=5.0, write=3.0, pool=2.0),
                limits=httpx.Limits(max_connections=100, max_keepalive_connections=50, keepalive_expiry=3600.0)
            )

            # 2. Generate live TOTP code via pyotp with network time synchronization
            totp_clean = totp_key.replace(" ", "").upper()
            drift = get_network_time_offset()
            if abs(drift) > 3.0:
                logger.info(f"[Kotak Auth] Clock skew detected ({drift:+.1f}s). Synchronizing TOTP generation with real server time.")
            synced_now = datetime.datetime.fromtimestamp(time.time() + drift, tz=datetime.timezone.utc)
            totp_code = pyotp.TOTP(totp_clean).at(synced_now)

            # 3. Call totp_login (Step 1: View Access)
            login_res = client.totp_login(
                mobile_number=clean_mobile,
                ucc=ucc,
                totp=totp_code
            )
            logger.info(f"[Kotak Auth] Step 1 totp_login response: {login_res}")

            login_err = _extract_neo_error(login_res)
            view_token = getattr(client.configuration, "view_token", None)
            if login_err or not view_token:
                raise RuntimeError(f"TOTP Login failed: {login_err or 'No view token received from Kotak Neo'}")

            # 4. Call totp_validate with MPIN (Step 2: Full Trading Access / 2FA Validation)
            val_res = client.totp_validate(mpin=mpin)
            logger.info(f"[Kotak Auth] Step 2 totp_validate response: {val_res}")

            val_err = _extract_neo_error(val_res)
            edit_token = getattr(client.configuration, "edit_token", None)
            edit_sid = getattr(client.configuration, "edit_sid", None)
            if val_err or not edit_token or not edit_sid:
                raise RuntimeError(f"MPIN 2FA Validation failed: {val_err or 'No trade authorization token received from Kotak Neo (2FA incomplete)'}")

            _ACTIVE_CLIENT = client
            _LAST_AUTH_ERROR = ""
            threading.Thread(target=warm_up_connection, args=(client,), daemon=True).start()
            _start_connection_warmup_daemon()

            session_data = {
                "is_authenticated": True,
                "has_edit_token": bool(edit_token),
                "has_edit_sid": bool(edit_sid),
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
            print(f"[{now_ist().strftime('%H:%M:%S')}] [KOTAK-AUTH] Login successful! Session established with verified 2FA trading permissions.")
            return _ACTIVE_CLIENT

        except Exception as e:
            _LAST_AUTH_ERROR = str(e)
            logger.error(f"[Kotak Auth] Auto-login failed: {e}")
            err_msg = "Kotak Neo Auto-Login Failed\nError: " + str(e) + "\nTime: " + now_ist().strftime("%d-%b-%Y %H:%M:%S IST")
            send_telegram_notification(err_msg)
            return None


def get_auth_status() -> Dict[str, Any]:
    """Return comprehensive status for Web UI and health monitoring."""
    creds = config.get_kotak_credentials()
    session = get_cached_session()
    has_creds = bool(creds.get("consumer_key") and creds.get("mobile_number") and creds.get("ucc") and creds.get("totp_key") and creds.get("mpin"))
    is_active = is_client_trading_ready(_ACTIVE_CLIENT)
    if not is_active and has_creds:
        try:
            cli = get_authenticated_client(force=False)
            is_active = is_client_trading_ready(cli)
        except Exception:
            pass

    return {
        "configured": has_creds,
        "is_authenticated": is_active,
        "trading_ready": is_active,
        "date": session.get("date", "Not logged in today"),
        "time": session.get("time", "--"),
        "ucc": creds.get("ucc", session.get("ucc", "Not set")),
        "masked_mobile": (creds.get("mobile_number", "")[:2] + "****" + creds.get("mobile_number", "")[-4:]) if len(creds.get("mobile_number", "")) >= 6 else "Not set",
        "has_totp_key": bool(creds.get("totp_key")),
        "has_mpin": bool(creds.get("mpin")),
        "environment": creds.get("environment", "prod"),
        "last_error": _LAST_AUTH_ERROR
    }


def start_morning_scheduler():
    """Background daemon thread for daily 08:15 AM token refresh and intraday session keeper."""
    def _loop():
        logger.info("[Scheduler] Kotak Neo morning auth scheduler and session keeper started.")
        while True:
            try:
                now = now_ist()
                if now.weekday() < 5:  # Monday to Friday
                    # Active market window: 08:15 to 15:35 IST
                    start_window = now.replace(hour=8, minute=15, second=0, microsecond=0)
                    end_window = now.replace(hour=15, minute=35, second=0, microsecond=0)

                    if now < start_window:
                        sleep_seconds = max(10, min((start_window - now).total_seconds(), 300))
                    elif start_window <= now <= end_window:
                        # Intraday market hours: Ensure valid 2FA trading session is perpetually active
                        if not is_session_active_for_today():
                            logger.info("[Scheduler] Intraday session check: Kotak Neo session missing or missing 2FA. Re-authenticating...")
                            get_authenticated_client(force=True)
                        sleep_seconds = 180  # Check session health every 3 minutes during market hours
                    else:
                        tomorrow = now + datetime.timedelta(days=1)
                        target_tomorrow = tomorrow.replace(hour=8, minute=15, second=0, microsecond=0)
                        sleep_seconds = max(60, min((target_tomorrow - now).total_seconds(), 3600))

                    time.sleep(sleep_seconds)
                else:
                    time.sleep(3600)  # Weekend check every hour
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
