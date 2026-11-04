"""
Instrument Sync Engine for Straddle Pro
Synchronizes daily F&O options and futures contracts directly from Kotak Neo Scrip Master
for all 15 active underlyings (Indices & Commodities).
"""

import os
import io
import re
import gzip
import json
import time
import logging
import urllib.request
import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import pandas as pd

logger = logging.getLogger("instrument_sync")

TARGET_UNDERLYINGS = {
    "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX",
    "CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM",
    "SILVER", "SILVERM", "COPPER"
}

MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12
}

DEFAULT_LOTS = {
    "NIFTY": 65, "BANKNIFTY": 30, "FINNIFTY": 65, "MIDCPNIFTY": 120, "SENSEX": 20, "BANKEX": 30,
    "CRUDEOIL": 100, "CRUDEOILM": 10, "NATURALGAS": 1250, "NATGASMINI": 250,
    "GOLD": 1, "GOLDM": 100, "SILVER": 30, "SILVERM": 5, "COPPER": 2500
}


def _get_kotak_scripmaster_urls() -> List[tuple]:
    """Retrieve the direct URLs for today's transformed CSV files from Kotak Neo."""
    urls = []
    # 1. Try Kotak API client if configured
    try:
        from kotak_broker_adapter import get_kotak_adapter
        adapter = get_kotak_adapter()
        client = adapter.client if adapter else None
        if client and hasattr(client, "scrip_master"):
            report = client.scrip_master()
            if isinstance(report, dict) and "filesPaths" in report:
                for path in report["filesPaths"]:
                    p_lower = path.lower()
                    if "nse_fo" in p_lower:
                        urls.append(("NSE_FO", path))
                    elif "bse_fo" in p_lower:
                        urls.append(("BSE_FO", path))
                    elif "mcx_fo" in p_lower:
                        urls.append(("MCX_FO", path))
                if urls:
                    return urls
    except Exception as e:
        logger.debug(f"[InstrumentSync] client.scrip_master() fallback: {e}")

    # 2. Date-based fallback URL generation
    today = datetime.datetime.now(datetime.timezone.utc).date()
    for delta in (0, -1, -2):
        d_str = (today + datetime.timedelta(days=delta)).strftime("%Y-%m-%d")
        candidate_urls = [
            ("NSE_FO", f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{d_str}/transformed/nse_fo.csv"),
            ("BSE_FO", f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{d_str}/transformed/bse_fo.csv"),
            ("MCX_FO", f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{d_str}/transformed/mcx_fo.csv"),
        ]
        # Quick probe of NSE_FO
        try:
            req = urllib.request.Request(candidate_urls[0][1], headers={"User-Agent": "Mozilla/5.0"}, method="HEAD")
            with urllib.request.urlopen(req, timeout=4) as resp:
                if resp.status == 200:
                    return candidate_urls
        except Exception:
            continue

    # Fallback default
    d_today = today.strftime("%Y-%m-%d")
    return [
        ("NSE_FO", f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{d_today}/transformed/nse_fo.csv"),
        ("BSE_FO", f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{d_today}/transformed/bse_fo.csv"),
        ("MCX_FO", f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{d_today}/transformed/mcx_fo.csv"),
    ]


def parse_date_from_scrip(ref: str, tsym: str) -> Optional[datetime.datetime]:
    """Extract expiry datetime from Kotak pScripRefKey or pTrdSymbol."""
    # 1. Match ddMMMyy in ref (e.g. NIFTY01DEC2623200.00CE -> 01DEC26)
    m = re.search(r"(\d{2}[A-Z]{3}\d{2})", str(ref or ""))
    if m:
        try:
            return datetime.datetime.strptime(m.group(1), "%d%b%y")
        except Exception:
            pass

    # 2. Match ddMMMyy in tsym (e.g. CRUDEOIL17NOV2611750CE -> 17NOV26)
    m = re.search(r"(\d{2}[A-Z]{3}\d{2})", str(tsym or ""))
    if m:
        try:
            return datetime.datetime.strptime(m.group(1), "%d%b%y")
        except Exception:
            pass

    return None


def sync_instruments_from_kotak(force: bool = False, target_file: Optional[Path] = None) -> Dict[str, Any]:
    """
    Downloads the live scrip master from Kotak Neo, parses all active F&O contracts for target
    underlyings, and merges them into data/instruments.json.gz.
    """
    if target_file is None:
        target_file = Path("data/instruments.json.gz")

    now = time.time()
    # Check if target file is already fresh (< 12 hours) and force is False
    if not force and target_file.exists():
        age_hours = (now - target_file.stat().st_mtime) / 3600.0
        if age_hours < 12.0:
            logger.info(f"[InstrumentSync] instruments.json.gz is {age_hours:.1f}h fresh. Skipping sync.")
            return {"success": True, "status": "already_fresh", "age_hours": age_hours}

    logger.info("[InstrumentSync] Starting Kotak Neo F&O instruments synchronization...")
    urls = _get_kotak_scripmaster_urls()
    parsed_instruments = []
    underlying_counts = {}

    for seg, url in urls:
        try:
            logger.info(f"[InstrumentSync] Downloading {seg} master from {url}...")
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = resp.read()

            df = pd.read_csv(io.BytesIO(data), low_memory=False)
            df.columns = [c.strip().rstrip(";") for c in df.columns]

            # Filter only target underlyings
            sub = df[df["pSymbolName"].astype(str).str.upper().isin(TARGET_UNDERLYINGS)]
            count_seg = 0

            for _, r in sub.iterrows():
                tsym = str(r.get("pTrdSymbol", "")).strip()
                name = str(r.get("pSymbolName", "")).strip().upper()
                ref = str(r.get("pScripRefKey", "")).strip()
                tok = str(r.get("pSymbol", "")).strip()
                opt_type = str(r.get("pOptionType", "")).strip().upper()
                inst_type = str(r.get("pInstType", "")).strip().upper()

                # Determine if CE, PE, or FUT
                itype = None
                if opt_type in ("CE", "PE"):
                    itype = opt_type
                elif tsym.endswith("CE"):
                    itype = "CE"
                elif tsym.endswith("PE"):
                    itype = "PE"
                elif "FUT" in tsym or "FUT" in inst_type:
                    itype = "FUT"

                if not itype or not tok:
                    continue

                dt = parse_date_from_scrip(ref, tsym)
                if not dt:
                    continue

                # Set expiry time to 15:30 IST (10:00 UTC)
                dt = dt.replace(hour=10, minute=0, second=0, tzinfo=datetime.timezone.utc)
                exp_ms = int(dt.timestamp() * 1000)

                strike_raw = float(r.get("dStrikePrice", 0.0) or 0.0)
                strike = round(strike_raw / 100.0, 2) if itype in ("CE", "PE") else 0.0

                lot = int(r.get("lLotSize") or r.get("iLotSize") or DEFAULT_LOTS.get(name, 50))
                freeze = float(r.get("lFreezeQty", 0) or 0)
                formatted_date = dt.strftime("%d %b %Y").upper()

                human_sym = f"{name} {int(strike) if strike==int(strike) else strike} {itype} {formatted_date}" if itype in ("CE", "PE") else f"{name} FUT {formatted_date}"

                parsed_instruments.append({
                    "weekly": True,
                    "segment": seg,
                    "name": name,
                    "exchange": seg.split("_")[0],
                    "expiry": exp_ms,
                    "instrument_type": itype,
                    "asset_symbol": name,
                    "underlying_symbol": name,
                    "instrument_key": f"{seg}|{tok}",
                    "lot_size": lot,
                    "freeze_quantity": freeze,
                    "exchange_token": tok,
                    "minimum_lot": lot,
                    "tick_size": 0.05,
                    "asset_type": "INDEX" if "FO" in seg and seg != "MCX_FO" else "COMMODITY",
                    "underlying_type": "INDEX" if "FO" in seg and seg != "MCX_FO" else "COMMODITY",
                    "trading_symbol": human_sym,
                    "strike_price": strike,
                    "qty_multiplier": 1.0,
                    "kotak_trading_symbol": tsym
                })
                count_seg += 1
                underlying_counts[name] = underlying_counts.get(name, 0) + 1

            logger.info(f"[InstrumentSync] Parsed {count_seg} active contracts from {seg}.")
        except Exception as e_seg:
            logger.error(f"[InstrumentSync] Error parsing {seg}: {e_seg}")

    if not parsed_instruments:
        logger.error("[InstrumentSync] No instruments parsed from Kotak master.")
        return {"success": False, "error": "No instruments parsed from Kotak master"}

    # Load existing instruments and merge
    merged_instruments = []
    if target_file.exists():
        try:
            with gzip.open(target_file, "rt", encoding="utf-8") as f:
                existing = json.load(f)
            # Keep all existing non-target records
            kept = [
                r for r in existing
                if not (
                    r.get("segment") in ("NSE_FO", "BSE_FO", "MCX_FO") and
                    str(r.get("name", "")).upper().replace(" ", "") in TARGET_UNDERLYINGS
                )
            ]
            merged_instruments.extend(kept)
        except Exception as e_read:
            logger.warning(f"[InstrumentSync] Error reading existing target file: {e_read}")

    merged_instruments.extend(parsed_instruments)

    # Save atomically to target_file
    tmp_path = target_file.with_suffix(".tmp.gz")
    target_file.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(tmp_path, "wt", encoding="utf-8") as f:
        json.dump(merged_instruments, f)

    if target_file.exists():
        target_file.unlink()
    tmp_path.rename(target_file)

    logger.info(f"[InstrumentSync] Successfully wrote {len(merged_instruments)} instruments to {target_file}.")
    return {
        "success": True,
        "total_instruments": len(merged_instruments),
        "synced_active_contracts": len(parsed_instruments),
        "underlying_counts": underlying_counts
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    res = sync_instruments_from_kotak(force=True)
    print("Sync Result:", res)

