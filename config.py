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

# Ultra-Low Latency Execution Cycle Interval (seconds)
# Tuned down from 0.3s (300ms) to 0.025s (25ms) for 12x faster trigger evaluation and exit execution
EXECUTION_CYCLE_INTERVAL_SEC = float(os.environ.get("EXECUTION_CYCLE_INTERVAL_SEC", "0.025"))

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

# Default strike steps per underlying (Equities + MCX Commodities)
DEFAULT_HEDGE_CONFIG = {
    "enabled": True,
    "initial_otm": 20,              # Initial strike distance in OTM steps (e.g. OTM 20 = 1000 pts for Nifty)
    "max_distance_threshold": 22,   # Max allowed distance before shifting hedge (e.g. 22 strikes = 1100 pts)
    "reset_otm": 20,                # Target OTM distance to shift hedge when threshold exceeded
    "exit_with_parent": True        # Automatically square off hedge when parent short leg has no re-entries left
}

STRIKE_STEPS = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
    "MIDCPNIFTY": 25,
    "SENSEX": 100,
    "BANKEX": 100,
    "CRUDEOIL": 50,
    "CRUDEOILM": 50,
    "NATURALGAS": 5,
    "NATGASMINI": 5,
    "GOLD": 500,
    "GOLDM": 500,
    "SILVER": 1000,
    "SILVERM": 1000,
    "COPPER": 10,
}

# Default lot sizes per underlying
DEFAULT_LOT_SIZES = {
    "NIFTY": 65,
    "BANKNIFTY": 15,
    "FINNIFTY": 65,
    "MIDCPNIFTY": 120,
    "SENSEX": 10,
    "BANKEX": 15,
    "CRUDEOIL": 100,
    "CRUDEOILM": 10,
    "NATURALGAS": 1250,
    "NATGASMINI": 250,
    "GOLD": 1,
    "GOLDM": 100,
    "SILVER": 30,
    "SILVERM": 5,
    "COPPER": 2500,
}

# Exchange Freeze Limits in Lots
FREEZE_LIMIT_LOTS = {
    "NIFTY": 27,        # NSE NIFTY Freeze: 27 lots (1,755 qty <= 1,800 freeze limit @ 65 lot size)
    "BANKNIFTY": 60,    # NSE BANKNIFTY Freeze: 60 lots (900 qty @ 15 lot size)
    "FINNIFTY": 27,     # NSE FINNIFTY Freeze: 27 lots (1,755 qty <= 1,800 freeze limit @ 65 lot size)
    "MIDCPNIFTY": 23,   # NSE MIDCPNIFTY Freeze: 23 lots (2,760 qty <= 2,800 freeze limit @ 120 lot size)
    "SENSEX": 100,      # BSE SENSEX Freeze: 100 lots (1,000 qty @ 10 lot size)
    "BANKEX": 60,       # BSE BANKEX Freeze: 60 lots (900 qty @ 15 lot size)
    "CRUDEOIL": 100,    # MCX CRUDEOIL: 100 lots (10,000 qty @ 100)
    "CRUDEOILM": 1000,  # MCX CRUDEOILM: 1000 lots (10,000 qty @ 10)
    "NATURALGAS": 40,   # MCX NATURALGAS: 40 lots (50,000 qty @ 1250)
    "NATGASMINI": 200,  # MCX NATGASMINI: 200 lots (50,000 qty @ 250)
    "GOLD": 100,        # MCX GOLD: 100 lots (100 qty @ 1)
    "GOLDM": 100,       # MCX GOLDM: 100 lots (10,000 qty @ 100)
    "SILVER": 20,       # MCX SILVER: 20 lots (600 qty @ 30)
    "SILVERM": 120,     # MCX SILVERM: 120 lots (600 qty @ 5)
    "COPPER": 20,       # MCX COPPER: 20 lots (50,000 qty @ 2500)
}

# Quantity Freeze Limits for Order Slicing (Quantity = Freeze Lots * Lot Size)
FREEZE_LIMITS = {
    "NIFTY": 1800,       # Max order limit: 1800 qty (27 lots * 65 = 1755)
    "BANKNIFTY": 900,    # 60 lots * 15
    "FINNIFTY": 1800,    # Max order limit: 1800 qty (27 lots * 65 = 1755)
    "MIDCPNIFTY": 2800,  # Max order limit: 2800 qty (23 lots * 120 = 2760)
    "SENSEX": 1000,      # 100 lots * 10
    "BANKEX": 900,       # 60 lots * 15
    "CRUDEOIL": 10000,   # 100 lots * 100
    "CRUDEOILM": 10000,  # 1000 lots * 10
    "NATURALGAS": 50000, # 40 lots * 1250
    "NATGASMINI": 50000, # 200 lots * 250
    "GOLD": 100,         # 100 lots * 1
    "GOLDM": 10000,      # 100 lots * 100
    "SILVER": 600,       # 20 lots * 30
    "SILVERM": 600,      # 120 lots * 5
    "COPPER": 50000,     # 20 lots * 2500
}

# Minimum Price Tick Size per Underlying
TICK_SIZES = {
    "NIFTY": 0.05,
    "BANKNIFTY": 0.05,
    "FINNIFTY": 0.05,
    "MIDCPNIFTY": 0.05,
    "SENSEX": 0.05,
    "BANKEX": 0.05,
    "CRUDEOIL": 1.00,
    "CRUDEOILM": 1.00,
    "NATURALGAS": 0.10,
    "NATGASMINI": 0.10,
    "GOLD": 1.00,
    "GOLDM": 1.00,
    "SILVER": 1.00,
    "SILVERM": 1.00,
    "COPPER": 0.05,
}

# Approximate Margin per Short Lot (SPAN + Exposure) for Baskets & Leverage Multipliers
DEFAULT_MARGIN_PER_LOT = {
    "NIFTY": 124746.08,
    "BANKNIFTY": 117500.0,
    "FINNIFTY": 108000.0,
    "MIDCPNIFTY": 102000.0,
    "SENSEX": 122000.0,
    "BANKEX": 114000.0,
    "CRUDEOIL": 210000.0,
    "CRUDEOILM": 21000.0,
    "NATURALGAS": 140000.0,
    "NATGASMINI": 28000.0,
    "GOLD": 1815574.25,
    "GOLDM": 181557.42,
    "SILVER": 625000.0,
    "SILVERM": 104500.0,
    "COPPER": 285000.0,
}

# Accurate Exchange SPAN Margin per Short Lot for Equity Indices & MCX Commodities
BASE_SPAN_PER_LOT = {
    "NIFTY": 104000.0,
    "BANKNIFTY": 98000.0,
    "FINNIFTY": 90000.0,
    "MIDCPNIFTY": 85000.0,
    "SENSEX": 102000.0,
    "BANKEX": 95000.0,
    "CRUDEOIL": 165000.0,
    "CRUDEOILM": 16500.0,
    "NATURALGAS": 110000.0,
    "NATGASMINI": 22000.0,
    "GOLD": 1450000.0,
    "GOLDM": 145000.0,
    "SILVER": 520000.0,
    "SILVERM": 87000.0,
    "COPPER": 220000.0,
}

# Accurate Exchange Exposure Margin per Short Lot for Equity Indices & MCX Commodities
BASE_EXP_PER_LOT = {
    "NIFTY": 41492.16,
    "BANKNIFTY": 39000.0,
    "FINNIFTY": 36000.0,
    "MIDCPNIFTY": 34000.0,
    "SENSEX": 40000.0,
    "BANKEX": 38000.0,
    "CRUDEOIL": 90000.0,
    "CRUDEOILM": 9000.0,
    "NATURALGAS": 60000.0,
    "NATGASMINI": 12000.0,
    "GOLD": 731148.50,
    "GOLDM": 73114.85,
    "SILVER": 210000.0,
    "SILVERM": 35000.0,
    "COPPER": 130000.0,
}

# Comprehensive Underlying / Commodity Metadata (Order, Labels, Color Codes, Segments)
ALL_UNDERLYINGS_LIST = [
    {
        "id": "NIFTY",
        "symbol": "NIFTY",
        "label": "Nifty",
        "display_name": "Nifty 50",
        "segment": "NSE_FO",
        "exchange": "NSE",
        "is_commodity": False,
        "color": "#3b82f6",
        "bg_color": "rgba(59, 130, 246, 0.15)",
        "border_color": "#3b82f6",
        "lot_size": 65,
        "strike_step": 50,
        "tick_size": 0.05,
        "freeze_limit": 1800,
        "market_hours": "09:15 - 15:30",
        "quote_unit": "₹"
    },
    {
        "id": "CRUDEOIL",
        "symbol": "CRUDEOIL",
        "label": "Crude Oil",
        "display_name": "Crude Oil",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#f59e0b",
        "bg_color": "rgba(245, 158, 11, 0.15)",
        "border_color": "#f59e0b",
        "lot_size": 100,
        "strike_step": 50,
        "tick_size": 1.00,
        "freeze_limit": 10000,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/bbl"
    },
    {
        "id": "NATURALGAS",
        "symbol": "NATURALGAS",
        "label": "Natural Gas",
        "display_name": "Natural Gas",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#8b5cf6",
        "bg_color": "rgba(139, 92, 246, 0.15)",
        "border_color": "#8b5cf6",
        "lot_size": 1250,
        "strike_step": 5,
        "tick_size": 0.10,
        "freeze_limit": 50000,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/mmBtu"
    },
    {
        "id": "GOLD",
        "symbol": "GOLD",
        "label": "Gold",
        "display_name": "Gold (1kg)",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#eab308",
        "bg_color": "rgba(234, 179, 8, 0.15)",
        "border_color": "#eab308",
        "lot_size": 1,
        "strike_step": 500,
        "tick_size": 1.00,
        "freeze_limit": 100,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/10g"
    },
    {
        "id": "SILVER",
        "symbol": "SILVER",
        "label": "Silver",
        "display_name": "Silver (30kg)",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#94a3b8",
        "bg_color": "rgba(148, 163, 184, 0.15)",
        "border_color": "#94a3b8",
        "lot_size": 30,
        "strike_step": 1000,
        "tick_size": 1.00,
        "freeze_limit": 600,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/kg"
    },
    {
        "id": "CRUDEOILM",
        "symbol": "CRUDEOILM",
        "label": "Crude Oil Mini",
        "display_name": "Crude Oil Mini",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#f97316",
        "bg_color": "rgba(249, 115, 22, 0.15)",
        "border_color": "#f97316",
        "lot_size": 10,
        "strike_step": 50,
        "tick_size": 1.00,
        "freeze_limit": 10000,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/bbl"
    },
    {
        "id": "NATGASMINI",
        "symbol": "NATGASMINI",
        "label": "Natural Gas Mini",
        "display_name": "Natural Gas Mini",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#d946ef",
        "bg_color": "rgba(217, 70, 239, 0.15)",
        "border_color": "#d946ef",
        "lot_size": 250,
        "strike_step": 5,
        "tick_size": 0.10,
        "freeze_limit": 50000,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/mmBtu"
    },
    {
        "id": "GOLDM",
        "symbol": "GOLDM",
        "label": "Gold Mini",
        "display_name": "Gold Mini (100g)",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#facc15",
        "bg_color": "rgba(250, 204, 21, 0.15)",
        "border_color": "#facc15",
        "lot_size": 100,
        "strike_step": 500,
        "tick_size": 1.00,
        "freeze_limit": 10000,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/10g"
    },
    {
        "id": "SILVERM",
        "symbol": "SILVERM",
        "label": "Silver Mini",
        "display_name": "Silver Mini (5kg)",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#38bdf8",
        "bg_color": "rgba(56, 189, 248, 0.15)",
        "border_color": "#38bdf8",
        "lot_size": 5,
        "strike_step": 1000,
        "tick_size": 1.00,
        "freeze_limit": 600,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/kg"
    },
    {
        "id": "COPPER",
        "symbol": "COPPER",
        "label": "Copper",
        "display_name": "Copper",
        "segment": "MCX_FO",
        "exchange": "MCX",
        "is_commodity": True,
        "color": "#ea580c",
        "bg_color": "rgba(234, 88, 12, 0.15)",
        "border_color": "#ea580c",
        "lot_size": 2500,
        "strike_step": 10,
        "tick_size": 0.05,
        "freeze_limit": 50000,
        "market_hours": "09:00 - 23:30",
        "quote_unit": "₹/kg"
    }
]

# Fast map by ID / Symbol
COMMODITY_METADATA = {item["id"]: item for item in ALL_UNDERLYINGS_LIST}

# Underlying spot / future symbols on Kotak Neo / NSE / MCX
UNDERLYING_SYMBOLS = {
    "NIFTY": "Nifty 50",
    "BANKNIFTY": "Nifty Bank",
    "FINNIFTY": "Nifty Fin Service",
    "MIDCPNIFTY": "NIFTY MID SELECT",
    "SENSEX": "SENSEX",
    "CRUDEOIL": "CRUDEOIL",
    "CRUDEOILM": "CRUDEOILM",
    "NATURALGAS": "NATURALGAS",
    "NATGASMINI": "NATGASMINI",
    "GOLD": "GOLD",
    "GOLDM": "GOLDM",
    "SILVER": "SILVER",
    "SILVERM": "SILVERM",
    "COPPER": "COPPER",
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
