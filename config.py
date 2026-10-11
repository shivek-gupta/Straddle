"""
Configuration for Standalone Straddle & Strangle Algorithmic Trading Platform.
Supports Kotak Neo API and Paper Trading Engine.
Runs on Port 5001 by default to operate in parallel with Swing Scanner.
"""

import os
import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"

DATA_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Web Server Settings
WEB_HOST = os.environ.get("WEB_HOST", "0.0.0.0")
WEB_PORT = int(os.environ.get("PORT", os.environ.get("WEB_PORT", "5001")))
SECRET_KEY = os.environ.get("SECRET_KEY", "kotak-straddle-secret-key-2026")

# Straddle File Paths
STRADDLE_DIR = DATA_DIR
STRADDLE_STRATEGIES_FILE = STRADDLE_DIR / "straddle_strategies.json"
STRADDLE_STATE_FILE = STRADDLE_DIR / "straddle_state.json"
STRADDLE_LOGS_FILE = STRADDLE_DIR / "straddle_logs.json"
STRADDLE_PNL_HISTORY_FILE = STRADDLE_DIR / "straddle_pnl_history.json"
STRADDLE_ORDERBOOK_FILE = STRADDLE_DIR / "straddle_orderbook.json"
STRADDLE_BACKUPS_DIR = STRADDLE_DIR / "straddle_backups"
STRADDLE_BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

# Kotak Neo Credentials & Session Files
KOTAK_CREDENTIALS_FILE = DATA_DIR / "kotak_credentials.json"
KOTAK_SESSION_FILE = DATA_DIR / "kotak_session.json"

# Default strike steps per underlying
STRIKE_STEPS = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
    "SENSEX": 100,
    "BANKEX": 100,
}

# Default lot sizes per underlying
DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
    "BANKNIFTY": 15,
    "FINNIFTY": 65,
    "MIDCPNIFTY": 120,
    "SENSEX": 10,
}

# Underlying spot symbols on Kotak Neo / NSE
UNDERLYING_SYMBOLS = {
    "NIFTY": "Nifty 50",
    "BANKNIFTY": "Nifty Bank",
    "FINNIFTY": "Nifty Fin Service",
    "MIDCPNIFTY": "NIFTY MID SELECT",
    "SENSEX": "SENSEX",
}

# Telegram Alerts Configuration
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8742400395:AAGiPqmr0EqGFr2LSoYrL2d5b_Gh6GQJrOM")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "595111539")


def get_kotak_credentials() -> dict:
    """Retrieve Kotak Neo credentials from file or environment variables."""
    creds = {
        "consumer_key": os.environ.get("KOTAK_CONSUMER_KEY", ""),
        "consumer_secret": os.environ.get("KOTAK_CONSUMER_SECRET", ""),
        "mobile_number": os.environ.get("KOTAK_MOBILE_NUMBER", ""),
        "ucc": os.environ.get("KOTAK_UCC", ""),
        "mpin": os.environ.get("KOTAK_MPIN", ""),
        "totp_key": os.environ.get("KOTAK_TOTP_KEY", ""),
        "environment": os.environ.get("KOTAK_ENV", "prod")
    }
    if KOTAK_CREDENTIALS_FILE.exists():
        try:
            with open(KOTAK_CREDENTIALS_FILE, "r", encoding="utf-8") as f:
                saved = json.load(f)
                if isinstance(saved, dict):
                    for k, v in saved.items():
                        if v and not creds.get(k):
                            creds[k] = v
                        elif v:
                            creds[k] = v
        except Exception:
            pass
    return creds


def save_kotak_credentials(creds: dict) -> bool:
    """Persist updated Kotak Neo credentials."""
    try:
        current = get_kotak_credentials()
        current.update(creds)
        with open(KOTAK_CREDENTIALS_FILE, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2)
        return True
    except Exception:
        return False
