"""
Unified Broker Adapter & Execution Engine for Kotak Neo API & Paper Trading.
Implements:
- Direct high-speed order placement, modification, and cancellation via Kotak Neo.
- Standardized orderbook, positions, margin, and tradebook polling.
- Real-time option chain & expiries via Kotak Neo SDK.
- High-fidelity Paper Trading engine for risk-free strategy verification.
"""

import time
import json
import logging
import threading
import io
import os
import gzip
import re
import uuid
import math
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path

import config
import kotak_auth

logger = logging.getLogger("kotak_broker_adapter")
logger.setLevel(logging.INFO)

MONTH_CODE_MAP = {
    "01": "1", "JAN": "1",
    "02": "2", "FEB": "2",
    "03": "3", "MAR": "3",
    "04": "4", "APR": "4",
    "05": "5", "MAY": "5",
    "06": "6", "JUN": "6",
    "07": "7", "JUL": "7",
    "08": "8", "AUG": "8",
    "09": "9", "SEP": "9",
    "10": "O", "OCT": "O",
    "11": "N", "NOV": "N",
    "12": "D", "DEC": "D",
}

_KOTAK_TOKEN_MAP: Dict[str, str] = {}
_KOTAK_DISPLAY_MAP: Dict[str, str] = {}
_MAP_LOCK = threading.Lock()
_MAP_LOADED = False


def _init_kotak_symbol_maps():
    """Initializes high-speed token and symbol resolution cache."""
    global _KOTAK_TOKEN_MAP, _KOTAK_DISPLAY_MAP, _MAP_LOADED
    with _MAP_LOCK:
        if _MAP_LOADED and len(_KOTAK_TOKEN_MAP) > 1000:
            return

        # 1. Populate from local instruments.json.gz (Immediate availability)
        try:
            inst_path = getattr(config, "DATA_DIR", Path("data")) / "instruments.json.gz"
            if not inst_path.exists():
                inst_path = Path("data/instruments.json.gz")
            if inst_path.exists():
                with gzip.open(inst_path, "rt", encoding="utf-8") as f:
                    data = json.load(f)
                    for r in data:
                        seg = r.get("segment")
                        if seg in ("NSE_FO", "BSE_FO", "MCX_FO") and r.get("exchange_token"):
                            tok = str(r["exchange_token"]).strip()
                            tsym = str(r.get("trading_symbol", "")).strip()
                            if r.get("kotak_trading_symbol"):
                                canon = str(r["kotak_trading_symbol"]).strip()
                                _KOTAK_TOKEN_MAP[tok] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper()] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper().replace(" ", "")] = canon
                                _KOTAK_DISPLAY_MAP[canon.upper()] = canon
                                continue
                            parts = tsym.split()
                            if len(parts) >= 6:
                                und = parts[0].upper()
                                strike = parts[1]
                                opt_type = parts[2].upper()
                                day = parts[3].zfill(2)
                                month = parts[4].upper()
                                year = parts[5][-2:]
                                if seg == "MCX_FO" or any(c in und for c in ("CRUDE", "NATURAL", "NATGAS", "GOLD", "SILVER", "COPPER")):
                                    canon = f"{und}{day}{month}{year}{strike}{opt_type}"
                                else:
                                    is_w = bool(r.get("weekly", True))
                                    if is_w:
                                        canon = f"{und}{year}{MONTH_CODE_MAP.get(month, month[:1])}{day}{strike}{opt_type}"
                                    else:
                                        canon = f"{und}{year}{month}{strike}{opt_type}"
                                _KOTAK_TOKEN_MAP[tok] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper()] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper().replace(" ", "")] = canon
                            elif len(parts) >= 4 and parts[1].upper() == "FUT":
                                # Futures contract e.g. CRUDEOIL FUT 19 NOV 26
                                und = parts[0].upper()
                                day = parts[2].zfill(2) if len(parts) >= 5 else ""
                                month = parts[3].upper() if len(parts) >= 5 else parts[2].upper()
                                year = parts[4][-2:] if len(parts) >= 5 else parts[3][-2:]
                                if seg == "MCX_FO" or any(c in und for c in ("CRUDE", "NATURAL", "NATGAS", "GOLD", "SILVER", "COPPER")):
                                    canon = f"{und}{day}{month}{year}FUT"
                                else:
                                    canon = f"{und}{year}{month}FUT"
                                _KOTAK_TOKEN_MAP[tok] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper()] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper().replace(" ", "")] = canon
        except Exception as e:
            logger.warning(f"[Kotak Adapter] Note indexing instruments.json.gz: {e}")

        # 2. Enrich from Kotak Neo Scrip Master CSV if cached (NSE, BSE, MCX)
        try:
            import neo_api_client.utils.scrip_cache as sc
            import pandas as pd
            for seg in ("nse_fo", "bse_fo", "mcx_fo"):
                try:
                    csv_bytes = sc.read_csv(seg)
                    if csv_bytes:
                        df = pd.read_csv(io.BytesIO(csv_bytes), usecols=["pSymbol", "pTrdSymbol"])
                        for _, row in df.iterrows():
                            tok = str(row["pSymbol"]).strip()
                            trd = str(row["pTrdSymbol"]).strip()
                            _KOTAK_TOKEN_MAP[tok] = trd
                            _KOTAK_DISPLAY_MAP[trd.upper()] = trd
                            _KOTAK_DISPLAY_MAP[trd.upper().replace(" ", "")] = trd
                except Exception as e_seg:
                    logger.debug(f"[Kotak Adapter] Note reading {seg} cache: {e_seg}")
        except Exception as e:
            logger.debug(f"[Kotak Adapter] Note loading scrip cache: {e}")

        _MAP_LOADED = True
        logger.info(f"[Kotak Adapter] Initialized symbol resolution cache with {len(_KOTAK_TOKEN_MAP)} tokens.")


def _resolve_kotak_trading_symbol(symbol: str, instrument_key: str = "") -> str:
    """
    Translates any input symbol (human display string, token, or Upstox format)
    into Kotak Neo's exact canonical exchange trading symbol format (e.g. NIFTY26O1922600CE).
    """
    if not _MAP_LOADED:
        _init_kotak_symbol_maps()

    # Priority 1: Match by token in instrument_key
    tok = str(instrument_key or "").split("|")[-1].strip()
    if tok and tok in _KOTAK_TOKEN_MAP:
        return _KOTAK_TOKEN_MAP[tok]

    # Priority 2: Direct lookup in display map
    clean_sym = str(symbol or "").strip().upper()
    if clean_sym in _KOTAK_DISPLAY_MAP:
        return _KOTAK_DISPLAY_MAP[clean_sym]

    # Priority 3: Check if already in canonical format (no spaces, e.g. NIFTY26O1922600CE)
    no_space = clean_sym.replace(" ", "")
    if no_space in _KOTAK_DISPLAY_MAP:
        return _KOTAK_DISPLAY_MAP[no_space]
    if bool(re.match(r"^[A-Z]+[0-9]{2}[A-Z0-9]+$", no_space)):
        return no_space

    # Priority 4: Algorithmic transformation of space-separated display string
    # e.g. "NIFTY 22600 CE 19 OCT 26" -> "NIFTY26O1922600CE"
    # e.g. "GOLD 138500 CE 27 NOV 26" -> "GOLD27NOV26138500CE"
    parts = clean_sym.split()
    if len(parts) >= 6:
        und = parts[0].upper()
        if parts[1].replace(".", "", 1).isdigit() and parts[2].upper() in ("CE", "PE"):
            strike = parts[1]
            opt_type = parts[2].upper()
            day = parts[3].zfill(2)
            month = parts[4].upper()
            year = parts[5][-2:]
        elif parts[-1].upper() in ("CE", "PE"):
            day = parts[1].zfill(2)
            month = parts[2].upper()
            year = parts[3][-2:]
            strike = parts[4]
            opt_type = parts[5].upper()
        else:
            strike = parts[1]
            opt_type = parts[2].upper()
            day = parts[3].zfill(2)
            month = parts[4].upper()
            year = parts[5][-2:]

        is_comm = any(c in und for c in ("CRUDE", "NATURAL", "NATGAS", "GOLD", "SILVER", "COPPER"))
        if is_comm or "MCX" in str(instrument_key).upper():
            return f"{und}{day}{month}{year}{strike}{opt_type}"
        else:
            m_code = MONTH_CODE_MAP.get(month, month[:1])
            return f"{und}{year}{m_code}{day}{strike}{opt_type}"

    return no_space


def get_tick_size_for_symbol(symbol: str = "", instrument_key: str = "") -> float:
    s = f"{symbol} {instrument_key}".upper()
    for comm, ts in sorted(getattr(config, "TICK_SIZES", {}).items(), key=lambda x: len(x[0]), reverse=True):
        if comm in s:
            return ts
    return 0.05


def get_lot_size_for_symbol(symbol: str = "", instrument_key: str = "") -> int:
    s = f"{symbol} {instrument_key}".upper()
    for und, ls in sorted(getattr(config, "DEFAULT_LOT_SIZES", {}).items(), key=lambda x: len(x[0]), reverse=True):
        if und in s:
            return ls
    return 65 if "NIFTY" in s else 15


def get_freeze_limit_lots_for_symbol(symbol: str = "", instrument_key: str = "") -> int:
    s = f"{symbol} {instrument_key}".upper()
    for und, fl_lots in sorted(getattr(config, "FREEZE_LIMIT_LOTS", {}).items(), key=lambda x: len(x[0]), reverse=True):
        if und in s:
            return fl_lots
    return 27 if "NIFTY" in s else 60


def get_freeze_limit_for_symbol(symbol: str = "", instrument_key: str = "") -> int:
    fl_lots = get_freeze_limit_lots_for_symbol(symbol, instrument_key)
    lot_size = get_lot_size_for_symbol(symbol, instrument_key)
    return fl_lots * lot_size


def get_exchange_segment_for_symbol(symbol: str = "", instrument_key: str = "") -> str:
    s = f"{symbol} {instrument_key}".upper()
    if any(m in s for m in ("MCX", "CRUDEOIL", "NATURALGAS", "NATGAS", "GOLD", "SILVER", "COPPER")):
        return "mcx_fo"
    if any(b in s for b in ("BSE", "SENSEX", "BANKEX")):
        return "bse_fo"
    return "nse_fo"


def round_to_tick(price: float, tick_size: float = 0.05, symbol: str = "", instrument_key: str = "") -> float:
    """Enforce exchange-mandated price tick multiples (0.05 for NSE/BSE, 0.10/1.00 for MCX)."""
    if not price or float(price) <= 0:
        return 0.0
    ts = tick_size
    if symbol or instrument_key:
        ts = get_tick_size_for_symbol(symbol, instrument_key)
    return round(round(float(price) / ts) * ts, 2)


class BaseBrokerAdapter:
    """Abstract interface for broker order execution and market depth."""

    def __init__(self, name: str = "BaseBroker"):
        self.name = name

    def get_status(self) -> Dict[str, Any]:
        return {"name": self.name, "connected": True, "mode": "BASE"}

    def place_order(
        self,
        symbol: str,
        instrument_key: str,
        transaction_type: str,
        quantity: int,
        product: str = "MIS",
        order_type: str = "LIMIT",
        price: float = 0.0,
        trigger_price: float = 0.0,
        tag: str = "straddle_algo",
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def modify_order(
        self,
        order_id: str,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        raise NotImplementedError

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        return {"status": "UNKNOWN", "order_id": order_id}

    def get_order_book(self) -> List[Dict[str, Any]]:
        return []

    def get_trades(self) -> List[Dict[str, Any]]:
        return []

    def get_positions(self, force: bool = False) -> List[Dict[str, Any]]:
        return []

    def get_funds(self) -> Dict[str, Any]:
        return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}

    def get_live_quotes(self, instrument_tokens: List[str]) -> Dict[str, Any]:
        quotes = {}
        now_ts = time.time()
        import straddle_engine
        for tok in instrument_tokens:
            raw_tok = str(tok).strip()
            if not raw_tok:
                continue
            if raw_tok in _LIVE_ADAPTER._quotes_cache:
                quotes[raw_tok] = _LIVE_ADAPTER._quotes_cache[raw_tok]
                continue
            clean_sym = raw_tok.split("|")[-1].strip().upper()
            if clean_sym in straddle_engine._SPOT_CACHE and straddle_engine._SPOT_CACHE[clean_sym] > 0:
                p = straddle_engine._SPOT_CACHE[clean_sym]
                quotes[raw_tok] = {"ltp": p, "bid": p, "ask": p, "oi": 0, "volume": 0}
                continue
            if clean_sym in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"):
                try:
                    p = straddle_engine._fetch_live_index_spot_network(clean_sym)
                    if p > 0:
                        quotes[raw_tok] = {"ltp": p, "bid": p, "ask": p, "oi": 0, "volume": 0}
                except Exception:
                    pass
        return quotes

    def wait_for_order_fill(
        self,
        order_id: str,
        timeout_sec: float = 15.0,
        poll_interval_sec: float = 0.1,
    ) -> Dict[str, Any]:
        """Wait until an order is confirmed filled by broker exchange."""
        return {"filled": True, "status": "COMPLETE", "order_id": str(order_id)}


class KotakNeoBrokerAdapter(BaseBrokerAdapter):
    """
    Direct High-Speed Kotak Neo F&O Broker Adapter.
    Translates algorithmic orders into Kotak Securities Trade API calls.
    """

    def __init__(self):
        super().__init__(name="Kotak Neo")
        self._lock = threading.Lock()
        self._quotes_cache: Dict[str, Dict[str, Any]] = {}
        self._quotes_cache_ts: Dict[str, float] = {}
        self._funds_cache: Dict[str, Any] = {}
        self._funds_cache_ts: float = 0.0
        self._positions_cache: List[Dict[str, Any]] = []
        self._positions_cache_ts: float = 0.0
        self._last_429_ts: float = 0.0
        _init_kotak_symbol_maps()

    def _get_client(self):
        c = kotak_auth.get_authenticated_client(force=False)
        if c is None:
            c = kotak_auth.get_authenticated_client(force=True)
        return c

    def get_status(self) -> Dict[str, Any]:
        client = self._get_client()
        status_info = kotak_auth.get_auth_status()
        return {
            "name": "Kotak Neo",
            "connected": client is not None,
            "mode": "LIVE_BROKER",
            "ucc": status_info.get("ucc", "--"),
            "authenticated": status_info.get("is_authenticated", False)
        }

    def _place_single_order(
        self,
        client,
        canonical_symbol: str,
        transaction_type: str,
        quantity: int,
        product: str = "MIS",
        order_type: str = "LIMIT",
        price: float = 0.0,
        trigger_price: float = 0.0,
        tag: str = "straddle_algo",
        segment: str = "nse_fo",
        tick_size: float = 0.05
    ) -> Dict[str, Any]:
        """Places a single slice order with Kotak Neo API and confirms RMS status."""
        clean_tt = "B" if transaction_type.upper() in ("BUY", "B") else "S"
        clean_ot = "L" if order_type.upper() in ("LIMIT", "LMT", "L") else ("MKT" if order_type.upper() in ("MARKET", "MKT") else ("SL-M" if "M" in order_type.upper() else "SL"))
        clean_prod = "MIS" if product.upper() in ("MIS", "I", "INTRADAY") else "NRML"
        clean_price = str(round_to_tick(price, tick_size=tick_size)) if clean_ot in ("L", "SL") else "0"
        clean_trig = str(round_to_tick(trigger_price, tick_size=tick_size)) if trigger_price and trigger_price > 0 else "0"
        clean_qty = str(int(quantity))

        tag_prefix = re.sub(r'[^a-zA-Z0-9]', '', str(tag or 'std'))[:6] or 'std'
        unique_tag = f"{tag_prefix}_{int(time.time()*1000)%1000000}_{uuid.uuid4().hex[:4]}"

        try:
            res = client.place_order(
                exchange_segment=segment,
                product=clean_prod,
                price=clean_price,
                order_type=clean_ot,
                quantity=clean_qty,
                validity="DAY",
                trading_symbol=canonical_symbol,
                transaction_type=clean_tt,
                trigger_price=clean_trig,
                tag=unique_tag
            )
            logger.info(f"[Kotak Neo] Order slice ({clean_qty} qty) response: {res}")

            if isinstance(res, dict):
                st_code = res.get("stCode")
                if st_code and int(st_code) != 200:
                    err_msg = res.get("errMsg") or res.get("message") or f"Broker rejected with code {st_code}"
                    logger.warning(f"[Kotak Neo] Order slice rejected: {err_msg} (res: {res})")
                    return {"success": False, "error": err_msg, "data": res}

            order_id = None
            if isinstance(res, dict):
                order_id = res.get("nOrdNo") or res.get("order_id") or (res.get("data") or {}).get("nOrdNo")

            if order_id:
                # Fast parallel RMS status verification (40ms initial micro-wait, 1 retry if pending)
                confirmed_status = "OPEN"
                for attempt in range(2):
                    if attempt > 0:
                        time.sleep(0.10)
                    else:
                        time.sleep(0.04)
                    try:
                        hist = client.order_history(order_id=str(order_id))
                        d = hist.get("data", {})
                        items = d.get("data", []) if isinstance(d, dict) else (hist if isinstance(hist, list) else [])
                        if items and isinstance(items, list):
                            latest = items[0]
                            st = str(latest.get("ordSt", "")).lower()
                            if st in ("rejected", "cancelled"):
                                rej_msg = latest.get("rejRsn") or latest.get("rejLongDesc") or latest.get("rejShortDesc") or "Order rejected by broker RMS"
                                logger.warning(f"[Kotak Neo] Order slice {order_id} rejected by RMS: {rej_msg}")
                                if "adapter has been logged off" in rej_msg.lower() or "adapter is logged off" in rej_msg.lower():
                                    rej_msg = "Adapter has been Logged off (Market Closed: Exchange Gateway is offline outside trading hours / on weekends. Orders will execute when market opens)."
                                return {
                                    "success": False,
                                    "status": "REJECTED",
                                    "order_id": str(order_id),
                                    "error": rej_msg,
                                    "status_message": rej_msg,
                                    "data": latest
                                }
                            elif st in ("traded", "complete", "open", "trigger pending"):
                                confirmed_status = st.upper()
                                break
                    except Exception as e_chk:
                        logger.debug(f"[Kotak Neo] RMS check notice: {e_chk}")

                return {
                    "success": True,
                    "status": confirmed_status,
                    "order_id": str(order_id),
                    "data": res,
                    "quantity": int(quantity)
                }

            err_msg = str(res.get("errMsg") or res.get("message") or res.get("error") or res)
            return {"success": False, "error": err_msg, "data": res}
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception placing order slice: {e}")
            return {"success": False, "error": str(e)}

    def place_order(
        self,
        symbol: str,
        instrument_key: str,
        transaction_type: str,
        quantity: int,
        product: str = "MIS",
        order_type: str = "LIMIT",
        price: float = 0.0,
        trigger_price: float = 0.0,
        tag: str = "straddle_algo",
    ) -> Dict[str, Any]:
        client = self._get_client()
        if client is None:
            return {
                "success": False,
                "error": "Kotak Neo not authenticated. Please check credentials in settings.",
                "status_code": 401
            }

        canonical_symbol = _resolve_kotak_trading_symbol(symbol, instrument_key)
        segment = get_exchange_segment_for_symbol(canonical_symbol, instrument_key)
        tick_size = get_tick_size_for_symbol(canonical_symbol, instrument_key)
        freeze_limit_qty = get_freeze_limit_for_symbol(canonical_symbol, instrument_key)
        lot_size = get_lot_size_for_symbol(canonical_symbol, instrument_key)
        freeze_limit_lots = get_freeze_limit_lots_for_symbol(canonical_symbol, instrument_key)

        logger.info(f"[Kotak Neo] Order request: sym='{symbol}' -> canon='{canonical_symbol}', seg='{segment}', qty={quantity}, freeze_qty={freeze_limit_qty} ({freeze_limit_lots} lots @ {lot_size}/lot)")

        total_qty = int(quantity)
        if total_qty <= 0:
            return {"success": False, "error": "Invalid quantity <= 0"}

        # Calculate slices strictly in multiples of lot size and exchange freeze limit lots (e.g. 27 lots for NIFTY)
        slices = []
        if total_qty <= freeze_limit_qty:
            slices = [total_qty]
        else:
            total_lots = max(1, total_qty // lot_size)
            rem_lots = total_lots
            while rem_lots > freeze_limit_lots:
                slices.append(freeze_limit_lots * lot_size)
                rem_lots -= freeze_limit_lots
            if rem_lots > 0:
                slices.append(rem_lots * lot_size)
            # Ensure no residual remainder lost
            residual = total_qty - sum(slices)
            if residual > 0:
                slices[-1] += residual

        if len(slices) > 1:
            logger.info(f"[Kotak Neo] Order Slicing triggered: {total_qty} qty sliced into {len(slices)} orders: {slices}")

        def _dispatch_slice(item):
            s_idx, s_qty = item
            slice_tag = f"{tag}_{s_idx+1}" if len(slices) > 1 else tag
            return self._place_single_order(
                client=client,
                canonical_symbol=canonical_symbol,
                transaction_type=transaction_type,
                quantity=s_qty,
                product=product,
                order_type=order_type,
                price=price,
                trigger_price=trigger_price,
                tag=slice_tag,
                segment=segment,
                tick_size=tick_size
            )

        # Execute multiple slices concurrently at sub-millisecond speed
        if len(slices) > 1:
            with concurrent.futures.ThreadPoolExecutor(max_workers=min(12, len(slices))) as pool:
                slice_results = list(pool.map(_dispatch_slice, enumerate(slices)))
        else:
            slice_results = [_dispatch_slice((0, slices[0]))]

        order_ids = []
        filled_qty = 0
        failed_qty = 0
        failed_error = ""

        for s_idx, res in enumerate(slice_results):
            s_qty = slices[s_idx]
            if res.get("success"):
                order_ids.append(str(res.get("order_id")))
                filled_qty += s_qty
            else:
                failed_qty += s_qty
                if not failed_error:
                    failed_error = res.get("error", "Slice order failed")
                logger.error(f"[Kotak Neo] Slice {s_idx+1}/{len(slices)} failed: {failed_error}")

        all_succeeded = (len(order_ids) == len(slices)) and (failed_qty == 0)

        if order_ids:
            primary_id = order_ids[0] if len(order_ids) == 1 else ",".join(order_ids)
            return {
                "success": all_succeeded,
                "order_id": primary_id,
                "child_order_ids": order_ids,
                "status": "COMPLETE" if all_succeeded else "PARTIAL",
                "slices": slice_results,
                "sliced": len(slices) > 1,
                "filled_qty": filled_qty,
                "remaining_qty": failed_qty,
                "error": failed_error if not all_succeeded else None,
                "data": slice_results[0].get("data") if slice_results else {}
            }

        return {"success": False, "error": failed_error or "All order slices rejected"}

    def modify_order(
        self,
        order_id: str,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        client = self._get_client()
        if client is None:
            return {"success": False, "error": "Kotak Neo client not authenticated."}

        try:
            ot_raw = (order_type or "").upper()
            if "SL-M" in ot_raw or "SLM" in ot_raw:
                clean_ot = "SL-M"
            elif "SL" in ot_raw:
                clean_ot = "SL"
            elif "MKT" in ot_raw or "MARKET" in ot_raw:
                clean_ot = "MKT"
            else:
                clean_ot = "L"

            clean_price = str(round_to_tick(price)) if (price is not None and clean_ot != "MKT") else "0"
            clean_trig = str(round_to_tick(trigger_price)) if (trigger_price is not None and clean_ot in ("SL", "SL-M")) else "0"

            # If quantity not provided, fetch existing quantity from order history so Kotak RMS doesn't reject with Invalid Quantity
            if not quantity or int(quantity) <= 0:
                try:
                    hist = client.order_history(order_id=str(order_id))
                    items = hist.get("data") if isinstance(hist, dict) else hist
                    if isinstance(items, dict):
                        items = items.get("data")
                    if isinstance(items, list) and items:
                        quantity = int(items[0].get("qty") or items[0].get("fldQty") or 65)
                except Exception:
                    pass

            clean_qty = str(int(quantity or 65))

            res = client.modify_order(
                order_id=str(order_id),
                price=clean_price,
                order_type=clean_ot,
                quantity=clean_qty,
                validity="DAY",
                trigger_price=clean_trig
            )
            return {"success": True, "order_id": str(order_id), "data": res}
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception modifying order {order_id}: {e}")
            return {"success": False, "error": str(e)}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        client = self._get_client()
        if client is None:
            return {"success": False, "error": "Kotak Neo client not authenticated."}

        try:
            res = client.cancel_order(order_id=str(order_id))
            return {"success": True, "order_id": str(order_id), "data": res}
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception cancelling order {order_id}: {e}")
            return {"success": False, "error": str(e)}

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        """Directly query Kotak Neo OMS/RMS for status of a specific order."""
        client = self._get_client()
        oid_str = str(order_id).strip()
        if not client or not oid_str:
            return {"status": "UNKNOWN", "order_id": oid_str}

        # 1. Quick check against order_report by ID
        try:
            res = client.order_report(order_id=oid_str)
            d = res.get("data") if isinstance(res, dict) else res
            items = d.get("data") if isinstance(d, dict) else (d if isinstance(d, list) else [d] if isinstance(d, dict) else [])
            if items and isinstance(items, list):
                item = items[0]
                st = str(item.get("ordSt") or item.get("status") or "").upper()
                if any(w in st for w in ("TRAD", "COMP", "FILL")):
                    return {
                        "filled": True,
                        "status": "COMPLETE",
                        "price": float(item.get("avgPrc") or item.get("prc") or 0.0),
                        "order_id": oid_str
                    }
                if any(w in st for w in ("CANC", "CAN")):
                    return {"filled": False, "status": "CANCELLED", "order_id": oid_str}
                if any(w in st for w in ("REJ")):
                    return {"filled": False, "status": "REJECTED", "order_id": oid_str, "error": item.get("rejRsn")}
                if any(w in st for w in ("TRIG")):
                    return {"filled": False, "status": "TRIGGER PENDING", "order_id": oid_str}
                if any(w in st for w in ("OPEN")):
                    return {"filled": False, "status": "OPEN", "order_id": oid_str}
        except Exception as e_or:
            logger.debug(f"[Kotak Neo] order_report by id {oid_str} notice: {e_or}")

        # 2. Check via order history
        try:
            hist = client.order_history(order_id=oid_str)
            d = hist.get("data") if isinstance(hist, dict) else hist
            items = d.get("data") if isinstance(d, dict) else (d if isinstance(d, list) else [])
            if items and isinstance(items, list):
                item = items[0]
                st = str(item.get("ordSt") or item.get("status") or "").upper()
                if any(w in st for w in ("TRAD", "COMP", "FILL")):
                    return {
                        "filled": True,
                        "status": "COMPLETE",
                        "price": float(item.get("avgPrc") or item.get("prc") or 0.0),
                        "order_id": oid_str
                    }
                if any(w in st for w in ("CANC", "CAN")):
                    return {"filled": False, "status": "CANCELLED", "order_id": oid_str}
                if any(w in st for w in ("REJ")):
                    return {"filled": False, "status": "REJECTED", "order_id": oid_str, "error": item.get("rejRsn")}
                if any(w in st for w in ("TRIG")):
                    return {"filled": False, "status": "TRIGGER PENDING", "order_id": oid_str}
                if any(w in st for w in ("OPEN")):
                    return {"filled": False, "status": "OPEN", "order_id": oid_str}
        except Exception as e_h:
            logger.debug(f"[Kotak Neo] order_history for {oid_str} notice: {e_h}")

        return {"status": "UNKNOWN", "order_id": oid_str}

    def wait_for_order_fill(
        self,
        order_id: str,
        timeout_sec: float = 15.0,
        poll_interval_sec: float = 0.05,
    ) -> Dict[str, Any]:
        """
        Polls Kotak Neo OMS/RMS order history until the order status is confirmed
        'TRADED' / 'COMPLETE', or fails if 'REJECTED' / 'CANCELLED'.
        Guarantees that exit orders are completely filled before re-entries fire,
        eliminating concurrent position margin spikes.
        Default poll interval is 5ms (0.005s) for ultra-low latency execution.
        """
        client = self._get_client()
        if client is None:
            return {"filled": False, "status": "NO_CLIENT", "error": "Kotak Neo client not authenticated."}

        oid_str = str(order_id).strip()
        start_t = time.time()
        logger.debug(f"[Kotak Neo] Polling order {oid_str} until FILLED (timeout={timeout_sec}s, poll={poll_interval_sec}s)...")

        while time.time() - start_t < timeout_sec:
            try:
                hist = client.order_history(order_id=oid_str)
                items = []
                if isinstance(hist, dict):
                    d = hist.get("data")
                    if isinstance(d, list):
                        items = d
                    elif isinstance(d, dict):
                        items = d.get("data", []) if isinstance(d.get("data"), list) else [d]
                elif isinstance(hist, list):
                    items = hist

                if items and isinstance(items, list):
                    latest = items[0]
                    st = str(latest.get("ordSt", "")).lower()
                    if st in ("traded", "complete"):
                        avg_p = float(latest.get("avgPrc", latest.get("prc", 0.0)) or 0.0)
                        f_qty = int(latest.get("fldQty", latest.get("qty", 0)) or 0)
                        logger.info(f"[Kotak Neo] Order {oid_str} confirmed FILLED: status='{st}', avg_price={avg_p}, qty={f_qty}")
                        return {
                            "filled": True,
                            "status": "COMPLETE",
                            "order_id": oid_str,
                            "price": avg_p,
                            "filled_quantity": f_qty,
                            "data": latest,
                        }
                    elif st in ("rejected", "cancelled"):
                        rej_msg = latest.get("rejRsn") or latest.get("rejLongDesc") or latest.get("rejShortDesc") or f"Order {st}"
                        logger.warning(f"[Kotak Neo] Order {oid_str} not filled: status='{st}', reason='{rej_msg}'")
                        if "adapter has been logged off" in rej_msg.lower() or "adapter is logged off" in rej_msg.lower():
                            rej_msg = "Adapter has been Logged off (Market Closed: Exchange Gateway is offline outside trading hours / on weekends. Orders will execute when market opens)."
                        return {
                            "filled": False,
                            "status": st.upper(),
                            "order_id": oid_str,
                            "error": rej_msg,
                            "data": latest,
                        }
            except Exception as e:
                logger.debug(f"[Kotak Neo] Exception checking order {oid_str} history: {e}")

            time.sleep(poll_interval_sec)

        # Fallback check against active broker order report
        try:
            ob = self.get_order_book()
            for o in ob:
                if str(o.get("order_id")) == oid_str:
                    st = str(o.get("status", "")).upper()
                    if "COMP" in st or "TRAD" in st:
                        return {
                            "filled": True,
                            "status": "COMPLETE",
                            "order_id": oid_str,
                            "price": float(o.get("average_price") or o.get("price") or 0.0),
                            "filled_quantity": int(o.get("filled_quantity") or o.get("quantity") or 0),
                        }
                    elif "REJ" in st or "CAN" in st:
                        return {
                            "filled": False,
                            "status": st,
                            "order_id": oid_str,
                            "error": o.get("status_message") or "Order rejected/cancelled",
                        }
        except Exception:
            pass

        logger.warning(f"[Kotak Neo] Order {oid_str} wait_for_order_fill timed out after {timeout_sec}s")
        return {"filled": False, "status": "TIMEOUT", "order_id": oid_str, "error": f"Timed out after {timeout_sec}s"}

    def get_order_book(self) -> List[Dict[str, Any]]:
        client = self._get_client()
        if client is None:
            return []

        try:
            res = client.order_report()
            data = res.get("data") if isinstance(res, dict) else res
            if not isinstance(data, list):
                return []

            standardized = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                # Map Kotak Neo order fields to unified schema
                oid = str(item.get("nOrdNo") or item.get("order_id") or "")
                tsym = item.get("trdSym") or item.get("trading_symbol") or ""
                tt = "BUY" if str(item.get("trnsTp", "")).upper() in ("B", "BUY") else "SELL"
                status_raw = str(item.get("ordSt", item.get("status", "OPEN"))).upper()

                if "TRAD" in status_raw or "FILL" in status_raw or "COMPLETE" in status_raw:
                    status = "COMPLETE"
                elif "REJ" in status_raw:
                    status = "REJECTED"
                elif "CAN" in status_raw:
                    status = "CANCELLED"
                elif "TRIG" in status_raw:
                    status = "TRIGGER PENDING"
                else:
                    status = "OPEN"

                qty = int(item.get("qty", item.get("quantity", 0)) or 0)
                f_qty = int(item.get("fldQty", item.get("filled_quantity", 0)) or 0)
                prc = float(item.get("prc", item.get("price", 0.0)) or 0.0)
                avg_prc = float(item.get("avgPrc", item.get("average_price", prc)) or prc)

                standardized.append({
                    "order_id": oid,
                    "trading_symbol": tsym,
                    "transaction_type": tt,
                    "status": status,
                    "quantity": qty,
                    "filled_quantity": f_qty,
                    "price": prc,
                    "average_price": avg_prc,
                    "order_timestamp": item.get("ordDtTm") or item.get("order_timestamp") or time.strftime("%H:%M:%S"),
                    "status_message": item.get("rejRsn") or item.get("status_message") or "",
                    "product": item.get("prod", "MIS"),
                    "client_id": item.get("actId") or "Kotak Neo",
                    "tag": str(item.get("guiOrdId") or item.get("tag") or item.get("remarks") or item.get("tok") or ""),
                })
            return standardized

        except Exception as e:
            logger.error(f"[Kotak Neo] Exception reading order report: {e}")
            return []

    def get_positions(self, force: bool = False) -> List[Dict[str, Any]]:
        now_ts = time.time()
        if not force and self._positions_cache and (now_ts - self._positions_cache_ts) < 2.0:
            return list(self._positions_cache)

        client = self._get_client()
        if client is None:
            return list(self._positions_cache) if self._positions_cache else []

        try:
            res = client.positions()
            data = res.get("data") if isinstance(res, dict) else res
            if not isinstance(data, list):
                return []

            standardized = []
            for item in data:
                if not isinstance(item, dict):
                    continue

                def _safe_float(val, d=0.0):
                    try:
                        if val is None or val == "":
                            return d
                        f = float(val)
                        return f if not math.isnan(f) else d
                    except (ValueError, TypeError):
                        return d

                def _safe_int(val, d=0):
                    try:
                        if val is None or val == "":
                            return d
                        return int(float(val))
                    except (ValueError, TypeError):
                        return d

                # Kotak Neo positions provide:
                # 'flBuyQty' & 'flSellQty' (day filled) and 'cfBuyQty' & 'cfSellQty' (carry forward)
                fl_buy = _safe_int(item.get("flBuyQty"))
                fl_sell = _safe_int(item.get("flSellQty"))
                cf_buy = _safe_int(item.get("cfBuyQty"))
                cf_sell = _safe_int(item.get("cfSellQty"))

                tot_buy_qty = (fl_buy + cf_buy) if (fl_buy + cf_buy) > 0 else (_safe_int(item.get("buyQty")) or _safe_int(item.get("buy_quantity")))
                tot_sell_qty = (fl_sell + cf_sell) if (fl_sell + cf_sell) > 0 else (_safe_int(item.get("sellQty")) or _safe_int(item.get("sell_quantity")))

                if item.get("netQty") is not None:
                    net_qty = _safe_int(item.get("netQty"))
                elif item.get("quantity") is not None:
                    net_qty = _safe_int(item.get("quantity"))
                else:
                    net_qty = tot_buy_qty - tot_sell_qty

                tsym = str(item.get("trdSym", item.get("trading_symbol", "")) or "")

                fl_buy_amt = _safe_float(item.get("buyAmt"))
                cf_buy_amt = _safe_float(item.get("cfBuyAmt"))
                fl_sell_amt = _safe_float(item.get("sellAmt"))
                cf_sell_amt = _safe_float(item.get("cfSellAmt"))

                tot_buy_amt = (fl_buy_amt + cf_buy_amt) if (fl_buy_amt + cf_buy_amt) > 0 else _safe_float(item.get("buy_amt"))
                tot_sell_amt = (fl_sell_amt + cf_sell_amt) if (fl_sell_amt + cf_sell_amt) > 0 else _safe_float(item.get("sell_amt"))

                buy_avg = (tot_buy_amt / tot_buy_qty) if tot_buy_qty > 0 else (_safe_float(item.get("buyAvgPrc")) or _safe_float(item.get("buy_avg")))
                sell_avg = (tot_sell_amt / tot_sell_qty) if tot_sell_qty > 0 else (_safe_float(item.get("sellAvgPrc")) or _safe_float(item.get("sell_avg")))

                ltp = _safe_float(item.get("ltp") or item.get("last_price"))
                pnl = _safe_float(item.get("urmtom") or item.get("urMtom") or item.get("pnl"))
                rpnl = _safe_float(item.get("rpnl") or item.get("realizedPnl") or item.get("rPnl"))
                ikey = str(item.get("tok", item.get("instrument_token", item.get("instrument_key", ""))) or "")
                lot_size = _safe_int(item.get("lotSz") or item.get("brdLtQty") or 65)
                if lot_size <= 0:
                    lot_size = 65

                avg_price = buy_avg if net_qty > 0 else (sell_avg if net_qty < 0 else (((buy_avg + sell_avg) / 2.0) if (buy_avg > 0 and sell_avg > 0) else (buy_avg or sell_avg)))

                disp_sym = _KOTAK_TOKEN_MAP.get(ikey, tsym) or tsym

                standardized.append({
                    "symbol": disp_sym,
                    "trading_symbol": tsym,
                    "instrument_key": ikey,
                    "token": ikey,
                    "quantity": net_qty,
                    "net_qty": net_qty,
                    "lots": max(1, abs(net_qty) // lot_size) if abs(net_qty) > 0 else 0,
                    "lot_size": lot_size,
                    "buy_quantity": tot_buy_qty,
                    "sell_quantity": tot_sell_qty,
                    "buy_qty": tot_buy_qty,
                    "sell_qty": tot_sell_qty,
                    "buy_avg": round(buy_avg, 2),
                    "sell_avg": round(sell_avg, 2),
                    "average_price": round(avg_price, 2),
                    "avg_price": round(avg_price, 2),
                    "last_price": round(ltp, 2),
                    "ltp": round(ltp, 2),
                    "pnl": round(pnl, 2),
                    "realised_pnl": round(rpnl, 2),
                    "strike_price": _safe_float(item.get("stkPrc")),
                    "option_type": str(item.get("optTp") or "").upper(),
                    "expiry_date": str(item.get("expDt") or ""),
                    "product": item.get("prod", "NRML"),
                    "exchange_segment": item.get("exSeg", "nse_fo"),
                    "client_id": item.get("actId") or "Kotak Neo Live"
                })

            # Backfill live LTP and Unrealized PnL if broker positions payload lacks real-time ticks
            open_tokens = [p["token"] for p in standardized if p["net_qty"] != 0 and p.get("token")]
            if open_tokens:
                try:
                    quotes = self.get_live_quotes(open_tokens)
                    for p in standardized:
                        tok = p.get("token")
                        tsym_key = p.get("trading_symbol")
                        q = quotes.get(tok) or quotes.get(f"NSE_FO|{tok}") or quotes.get(tsym_key)
                        if q and _safe_float(q.get("ltp")) > 0:
                            live_p = _safe_float(q["ltp"])
                            p["ltp"] = round(live_p, 2)
                            p["last_price"] = round(live_p, 2)
                            if p["net_qty"] > 0 and p["avg_price"] > 0:
                                p["pnl"] = round((live_p - p["avg_price"]) * p["net_qty"], 2)
                            elif p["net_qty"] < 0 and p["avg_price"] > 0:
                                p["pnl"] = round((p["avg_price"] - live_p) * abs(p["net_qty"]), 2)
                except Exception as q_err:
                    logger.debug(f"[Kotak Neo] Backfill live quotes note: {q_err}")

            self._positions_cache = standardized
            self._positions_cache_ts = time.time()
            return standardized
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception fetching positions: {e}")
            return list(self._positions_cache) if self._positions_cache else []

    def get_funds(self) -> Dict[str, Any]:
        now_ts = time.time()
        if self._funds_cache and (now_ts - self._funds_cache_ts) < 2.5:
            return self._funds_cache

        client = self._get_client()
        if client is None:
            return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}

        try:
            res = client.limits()
            data = res.get("data") if (isinstance(res, dict) and "data" in res and isinstance(res["data"], dict)) else res
            if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                data = data[0]

            if isinstance(data, dict):
                avail = float(data.get("Net") or data.get("net") or data.get("availableMargin") or data.get("available_margin") or 0.0)
                used = float(data.get("MarginUsed") or data.get("MarginUsedPrsnt") or data.get("utilizedMargin") or data.get("marginUsed") or 0.0)
                collateral = float(data.get("CollateralValue") or data.get("Collateral") or data.get("collateral") or 0.0)
                span = float(data.get("SpanMarginPrsnt") or data.get("FoSpanrgnNrmlPrsnt") or 0.0)
                exposure = float(data.get("ExposureMarginPrsnt") or data.get("FoExpMrgnNrmlPrsnt") or 0.0)
                payin = float(data.get("RmsPayInAmt") or data.get("payin") or 0.0)
                unavailable = float(data.get("DeliveryMarginPresent") or data.get("CncSellcrdPresent") or 0.0)
                total = round(avail + used, 2)

                res_dict = {
                    "available_margin": avail,
                    "available_cash": avail,
                    "used_margin": used,
                    "collateral_margin": collateral,
                    "span_margin": span,
                    "exposure_margin": exposure,
                    "payin_amount": payin,
                    "unavailable_to_trade": unavailable,
                    "net_cash": total,
                    "total_margin": total,
                    "raw": data
                }
                self._funds_cache = res_dict
                self._funds_cache_ts = now_ts
                return res_dict
            return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception fetching funds: {e}")
            return self._funds_cache or {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}

    def get_live_quotes(self, instrument_tokens: List[str]) -> Dict[str, Any]:
        client = self._get_client()
        if client is None or not instrument_tokens:
            return {}

        try:
            import straddle_engine
            now_ts = time.time()

            # If Kotak recently returned 429 Too Many Requests, serve from cache to prevent API flooding
            if (now_ts - getattr(self, "_last_429_ts", 0.0)) < 2.0:
                cached_res = {}
                for tok in instrument_tokens:
                    raw_tok = str(tok).strip()
                    if raw_tok in self._quotes_cache:
                        cached_res[raw_tok] = self._quotes_cache[raw_tok]
                return cached_res

            quotes_data = {}
            req_tokens = []
            tokens_to_fetch = []

            # Mapping for index symbols to Kotak Neo cm tokens
            index_neo_tokens = {
                "NIFTY": ("Nifty 50", "nse_cm"),
                "BANKNIFTY": ("Nifty Bank", "nse_cm"),
                "FINNIFTY": ("Nifty Fin Service", "nse_cm"),
                "MIDCPNIFTY": ("NIFTY MID SELECT", "nse_cm"),
                "SENSEX": ("SENSEX", "bse_cm"),
                "BANKEX": ("BANKEX", "bse_cm"),
                "NSE_INDEX|NIFTY 50": ("Nifty 50", "nse_cm"),
                "NSE_INDEX|NIFTY BANK": ("Nifty Bank", "nse_cm"),
                "NSE_INDEX|NIFTY FIN SERVICE": ("Nifty Fin Service", "nse_cm"),
                "NSE_INDEX|NIFTY MID SELECT": ("NIFTY MID SELECT", "nse_cm"),
                "BSE_INDEX|SENSEX": ("SENSEX", "bse_cm"),
                "BSE_INDEX|BANKEX": ("BANKEX", "bse_cm"),
            }

            comm_fut_map = {
                "GOLD": ("495213", "mcx_fo"),
                "GOLDM": ("571445", "mcx_fo"),
                "SILVER": ("495214", "mcx_fo"),
                "SILVERM": ("483080", "mcx_fo"),
                "CRUDEOIL": ("573422", "mcx_fo"),
                "CRUDEOILM": ("573423", "mcx_fo"),
                "NATURALGAS": ("574319", "mcx_fo"),
                "NATGASMINI": ("574320", "mcx_fo"),
                "COPPER": ("578634", "mcx_fo"),
            }

            for tok in instrument_tokens:
                raw_tok = str(tok).strip()
                if not raw_tok:
                    continue

                # Check recent memory cache (1.5s TTL for smooth real-time ticking)
                if raw_tok in self._quotes_cache and (now_ts - self._quotes_cache_ts.get(raw_tok, 0)) < 1.5:
                    quotes_data[raw_tok] = self._quotes_cache[raw_tok]
                    continue

                clean_sym = raw_tok.split("|")[-1].strip().upper()
                if raw_tok.upper() in index_neo_tokens or clean_sym in index_neo_tokens:
                    idx_pair = index_neo_tokens.get(raw_tok.upper()) or index_neo_tokens.get(clean_sym)
                    if idx_pair:
                        req_tokens.append({"instrument_token": idx_pair[0], "exchange_segment": idx_pair[1]})
                        tokens_to_fetch.append(clean_sym)
                elif clean_sym in comm_fut_map or raw_tok in comm_fut_map:
                    tok_id, comm_seg = comm_fut_map.get(clean_sym) or comm_fut_map.get(raw_tok)
                    dyn_key = getattr(straddle_engine, "UNDERLYING_KEYS", {}).get(clean_sym)
                    if dyn_key and "|" in dyn_key:
                        tok_id = dyn_key.split("|")[-1]
                    if tok_id:
                        req_tokens.append({"instrument_token": tok_id, "exchange_segment": comm_seg})
                        tokens_to_fetch.append(clean_sym)
                else:
                    token_id = raw_tok.split("|")[-1].strip()
                    if not token_id.isdigit():
                        continue
                    seg = get_exchange_segment_for_symbol(raw_tok, raw_tok)
                    req_tokens.append({"instrument_token": token_id, "exchange_segment": seg})
                    tokens_to_fetch.append(raw_tok)

            if req_tokens:
                # De-duplicate tokens by (instrument_token, exchange_segment) to avoid Kotak 400 Bad Request
                deduped_tokens = []
                seen_tok_keys = set()
                for rt in req_tokens:
                    t_key = (str(rt.get("instrument_token")), str(rt.get("exchange_segment")).lower())
                    if t_key not in seen_tok_keys:
                        seen_tok_keys.add(t_key)
                        deduped_tokens.append(rt)
                req_tokens = deduped_tokens

                reverse_index = {
                    "Nifty 50": "NIFTY",
                    "Nifty Bank": "BANKNIFTY",
                    "Nifty Fin Service": "FINNIFTY",
                    "NIFTY MID SELECT": "MIDCPNIFTY",
                    "SENSEX": "SENSEX",
                    "BANKEX": "BANKEX",
                }

                reverse_comm = {
                    "495213": "GOLD", "571445": "GOLDM",
                    "495214": "SILVER", "483080": "SILVERM",
                    "573422": "CRUDEOIL", "573423": "CRUDEOILM",
                    "574319": "NATURALGAS", "574320": "NATGASMINI",
                    "578634": "COPPER"
                }
                # Also include dynamically resolved futures tokens
                for c_k, c_ik in getattr(straddle_engine, "UNDERLYING_KEYS", {}).items():
                    if "|" in c_ik:
                        reverse_comm[c_ik.split("|")[-1]] = c_k

                def _fetch_single_batch(b_tokens):
                    try:
                        # Enforce 150ms minimum spacing between Kotak REST calls
                        with self._lock:
                            elapsed = time.time() - getattr(self, "_last_api_request_ts", 0.0)
                            if elapsed < 0.15:
                                time.sleep(0.15 - elapsed)
                            self._last_api_request_ts = time.time()
                        return client.quotes(instrument_tokens=b_tokens, quote_type="all")
                    except Exception as eb:
                        eb_str = str(eb).lower()
                        if "429" in eb_str or "too many request" in eb_str:
                            self._last_429_ts = time.time()
                            logger.debug("[Kotak Neo] Rate limited (429) during quotes. Throttling.")
                        elif "401" in eb_str or "unauthorized" in eb_str:
                            logger.warning("[Kotak Neo] 401 Unauthorized during quotes. Triggering session refresh...")
                            try:
                                kotak_auth.invalidate_session()
                                kotak_auth.get_authenticated_client(force=True)
                            except Exception:
                                pass
                        logger.debug(f"[Kotak Neo] Batch quotes error: {eb}")
                        return None

                batches = [req_tokens[i:i + 40] for i in range(0, len(req_tokens), 40)]
                batch_results = []
                for b in batches:
                    res = _fetch_single_batch(b)
                    if res:
                        batch_results.append(res)
                    if len(batches) > 1:
                        time.sleep(0.1)

                for res in batch_results:
                    if not res:
                        continue
                    if isinstance(res, dict) and res.get("status_code") == 429:
                        self._last_429_ts = time.time()
                    try:
                        raw_list = res.get("data") if isinstance(res, dict) else res
                        if isinstance(raw_list, list):
                            for q in raw_list:
                                if not isinstance(q, dict):
                                    continue
                                tok = str(q.get("exchange_token") or q.get("instrument_token") or q.get("tok", ""))
                                best_bid = float(q.get("bid", 0.0) or (q.get("depth", {}).get("buy", [{}])[0].get("price", 0.0) if isinstance(q.get("depth"), dict) and q.get("depth", {}).get("buy") else 0.0))
                                best_ask = float(q.get("ask", 0.0) or (q.get("depth", {}).get("sell", [{}])[0].get("price", 0.0) if isinstance(q.get("depth"), dict) and q.get("depth", {}).get("sell") else 0.0))
                                entry = {
                                    "ltp": float(q.get("ltp", 0.0) or 0.0),
                                    "bid": best_bid,
                                    "ask": best_ask,
                                    "oi": int(q.get("open_int", q.get("oi", 0)) or 0),
                                    "volume": int(q.get("last_volume", q.get("vol", 0)) or 0)
                                }
                                if tok:
                                    quotes_data[tok] = entry
                                    quotes_data[f"NSE_FO|{tok}"] = entry
                                    quotes_data[f"BSE_FO|{tok}"] = entry
                                    quotes_data[f"MCX_FO|{tok}"] = entry
                                    self._quotes_cache[tok] = entry
                                    self._quotes_cache[f"NSE_FO|{tok}"] = entry
                                    self._quotes_cache[f"BSE_FO|{tok}"] = entry
                                    self._quotes_cache[f"MCX_FO|{tok}"] = entry
                                    self._quotes_cache_ts[tok] = now_ts
                                    self._quotes_cache_ts[f"NSE_FO|{tok}"] = now_ts
                                    self._quotes_cache_ts[f"BSE_FO|{tok}"] = now_ts
                                    self._quotes_cache_ts[f"MCX_FO|{tok}"] = now_ts

                                    if tok in reverse_index:
                                        idx_sym = reverse_index[tok]
                                        quotes_data[idx_sym] = entry
                                        quotes_data[f"NSE_INDEX|{tok}"] = entry
                                        quotes_data[f"BSE_INDEX|{tok}"] = entry
                                        self._quotes_cache[idx_sym] = entry
                                        self._quotes_cache[f"NSE_INDEX|{tok}"] = entry
                                        self._quotes_cache[f"BSE_INDEX|{tok}"] = entry
                                        self._quotes_cache_ts[idx_sym] = now_ts
                                        if entry["ltp"] > 0:
                                            with straddle_engine._SPOT_FETCH_LOCK:
                                                straddle_engine._SPOT_CACHE[idx_sym] = entry["ltp"]
                                                straddle_engine._LAST_SPOT_FETCH_TS[idx_sym] = now_ts

                                    if tok in reverse_comm:
                                        c_sym = reverse_comm[tok]
                                        quotes_data[c_sym] = entry
                                        quotes_data[f"MCX|{c_sym}"] = entry
                                        self._quotes_cache[c_sym] = entry
                                        self._quotes_cache[f"MCX|{c_sym}"] = entry
                                        self._quotes_cache_ts[c_sym] = now_ts
                                        self._quotes_cache_ts[f"MCX|{c_sym}"] = now_ts
                                        if entry["ltp"] > 0:
                                            with straddle_engine._SPOT_FETCH_LOCK:
                                                straddle_engine._SPOT_CACHE[c_sym] = entry["ltp"]
                                                straddle_engine._LAST_SPOT_FETCH_TS[c_sym] = now_ts

                                sym = q.get("display_symbol")
                                if sym:
                                    quotes_data[sym] = entry
                                    self._quotes_cache[sym] = entry
                                    self._quotes_cache_ts[sym] = now_ts
                                tsym = q.get("trdSym") or q.get("trading_symbol") or q.get("symbol")
                                if tsym:
                                    quotes_data[tsym] = entry
                                    self._quotes_cache[tsym] = entry
                                    self._quotes_cache_ts[tsym] = now_ts
                    except Exception as ex_b:
                        logger.debug(f"[Kotak Neo] Batch quotes note: {ex_b}")

            # Fallback for equity indices if broker quote didn't arrive or returned zero
            for idx_cand in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"):
                if any(idx_cand in str(t).upper() for t in instrument_tokens) and (idx_cand not in quotes_data or float(quotes_data[idx_cand].get("ltp", 0.0)) <= 0):
                    try:
                        spot_p = straddle_engine._fetch_live_index_spot_network(idx_cand)
                        if spot_p > 0:
                            entry = {"ltp": spot_p, "bid": spot_p, "ask": spot_p, "oi": 0, "volume": 0}
                            quotes_data[idx_cand] = entry
                            quotes_data[f"NSE_INDEX|{idx_cand}"] = entry
                            self._quotes_cache[idx_cand] = entry
                            self._quotes_cache_ts[idx_cand] = now_ts
                            with straddle_engine._SPOT_FETCH_LOCK:
                                straddle_engine._SPOT_CACHE[idx_cand] = spot_p
                                straddle_engine._LAST_SPOT_FETCH_TS[idx_cand] = now_ts
                    except Exception:
                        pass
            return quotes_data
        except Exception as e:
            logger.warning(f"[Kotak Neo] Exception fetching quotes: {e}")
            return {}


class PaperBrokerAdapter(BaseBrokerAdapter):
    """
    High-Fidelity Simulated Paper Trading Engine.
    Executes trades with zero monetary risk, maintains paper positions and order history.
    Fully supports Equities (NIFTY/BANKNIFTY) and all MCX Commodities with order slicing.
    """

    def __init__(self):
        super().__init__(name="Paper Trading")
        self._positions_file = config.DATA_DIR / "straddle_paper_positions.json"
        self._orderbook_file = config.DATA_DIR / "straddle_paper_orderbook.json"
        self._lock = threading.Lock()

    def get_status(self) -> Dict[str, Any]:
        return {
            "name": "Paper Trading",
            "connected": True,
            "mode": "PAPER",
            "ucc": "PAPER-SIMULATOR",
            "authenticated": True
        }

    def _read_json(self, path: Path) -> List[Dict[str, Any]]:
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        return data
            except Exception:
                pass
        return []

    def _write_json(self, path: Path, data: Any):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception:
            pass

    def place_order(
        self,
        symbol: str,
        instrument_key: str,
        transaction_type: str,
        quantity: int,
        product: str = "MIS",
        order_type: str = "LIMIT",
        price: float = 0.0,
        trigger_price: float = 0.0,
        tag: str = "straddle_algo",
    ) -> Dict[str, Any]:
        with self._lock:
            canonical_symbol = _resolve_kotak_trading_symbol(symbol, instrument_key)
            tick_size = get_tick_size_for_symbol(canonical_symbol, instrument_key)
            freeze_limit_qty = get_freeze_limit_for_symbol(canonical_symbol, instrument_key)
            lot_size = get_lot_size_for_symbol(canonical_symbol, instrument_key)
            freeze_limit_lots = get_freeze_limit_lots_for_symbol(canonical_symbol, instrument_key)
            total_qty = int(quantity)

            # Order slicing in simulation strictly in multiples of lot size and freeze limit lots
            slices = []
            if total_qty <= freeze_limit_qty:
                slices = [total_qty]
            else:
                total_lots = max(1, total_qty // lot_size)
                rem_lots = total_lots
                while rem_lots > freeze_limit_lots:
                    slices.append(freeze_limit_lots * lot_size)
                    rem_lots -= freeze_limit_lots
                if rem_lots > 0:
                    slices.append(rem_lots * lot_size)
                residual = total_qty - sum(slices)
                if residual > 0:
                    slices[-1] += residual

            exec_price = price if price and price > 0 else 100.0
            exec_price = round_to_tick(exec_price, tick_size=tick_size)

            order_ids = []
            orders = self._read_json(self._orderbook_file)
            now_str = time.strftime("%H:%M:%S")

            is_adv = tag in ("adv_tgt", "adv_sl") or ("SL" in order_type.upper())
            init_status = ("TRIGGER PENDING" if "SL" in order_type.upper() else "OPEN") if is_adv else "COMPLETE"

            for s_idx, slice_qty in enumerate(slices):
                order_id = f"PAPER_{int(time.time()*1000)}_{s_idx+1}"
                order_ids.append(order_id)
                slice_label = f" [Slice {s_idx+1}/{len(slices)}]" if len(slices) > 1 else ""
                init_msg = f"Resting on Paper Orderbook ({slice_qty} qty{slice_label})" if is_adv else f"Filled in Paper Simulation ({slice_qty} qty{slice_label})"
                order_entry = {
                    "order_id": order_id,
                    "trading_symbol": symbol,
                    "canonical_symbol": canonical_symbol,
                    "instrument_key": instrument_key,
                    "transaction_type": transaction_type.upper(),
                    "quantity": int(slice_qty),
                    "filled_quantity": 0 if is_adv else int(slice_qty),
                    "price": exec_price,
                    "trigger_price": trigger_price or 0.0,
                    "average_price": 0.0 if is_adv else exec_price,
                    "status": init_status,
                    "order_timestamp": now_str,
                    "product": product,
                    "tag": f"{tag}_{s_idx+1}" if len(slices) > 1 else tag,
                    "client_id": "PAPER TRADING",
                    "status_message": init_msg
                }
                orders.insert(0, order_entry)

            self._write_json(self._orderbook_file, orders[:500])

            # Update paper positions ONLY for non-advance orders (entry/exit)
            if not is_adv:
                positions = self._read_json(self._positions_file)
                found = False
                for p in positions:
                    if p.get("trading_symbol") == symbol:
                        curr_qty = int(p.get("quantity", 0))
                        change_qty = total_qty if transaction_type.upper() == "BUY" else -total_qty
                        new_qty = curr_qty + change_qty
                        p["quantity"] = new_qty
                        p["last_price"] = exec_price
                        found = True
                        break

                if not found:
                    positions.append({
                        "trading_symbol": symbol,
                        "quantity": total_qty if transaction_type.upper() == "BUY" else -total_qty,
                        "average_price": exec_price,
                        "last_price": exec_price,
                        "pnl": 0.0,
                        "product": product,
                        "client_id": "PAPER TRADING"
                    })

                self._write_json(self._positions_file, positions)

            return {
                "success": True,
                "order_id": order_ids[0] if len(order_ids) == 1 else ",".join(order_ids),
                "sliced": len(slices) > 1,
                "status": init_status,
                "data": {"order_id": order_ids[0], "filled_quantity": 0 if is_adv else total_qty, "price": exec_price}
            }

    def modify_order(
        self,
        order_id: str,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._lock:
            orders = self._read_json(self._orderbook_file)
            for o in orders:
                if str(o.get("order_id")) == str(order_id):
                    if price is not None:
                        o["price"] = float(price)
                    if trigger_price is not None:
                        o["trigger_price"] = float(trigger_price)
                    if quantity is not None and int(quantity) > 0:
                        o["quantity"] = int(quantity)
                    break
            self._write_json(self._orderbook_file, orders)
            return {"success": True, "order_id": order_id, "message": "Paper order updated"}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        with self._lock:
            orders = self._read_json(self._orderbook_file)
            for o in orders:
                if str(o.get("order_id")) == str(order_id):
                    o["status"] = "CANCELLED"
                    o["status_message"] = "Cancelled by client/algo in Paper simulation"
                    break
            self._write_json(self._orderbook_file, orders)
            return {"success": True, "order_id": order_id, "message": "Paper order cancelled"}

    def get_order_status(self, order_id: str) -> Dict[str, Any]:
        with self._lock:
            orders = self._read_json(self._orderbook_file)
            for o in orders:
                if str(o.get("order_id")) == str(order_id):
                    st = str(o.get("status", "OPEN")).upper()
                    return {
                        "status": st,
                        "filled": "COMP" in st or "TRAD" in st,
                        "price": float(o.get("average_price") or o.get("price") or 0.0),
                        "trigger_price": float(o.get("trigger_price") or 0.0),
                        "quantity": int(o.get("quantity") or 0),
                        "order_id": str(order_id)
                    }
            return {"status": "UNKNOWN", "order_id": str(order_id)}

    def get_order_book(self) -> List[Dict[str, Any]]:
        return self._read_json(self._orderbook_file)

    def get_positions(self, force: bool = False) -> List[Dict[str, Any]]:
        return self._read_json(self._positions_file)

    def get_funds(self) -> Dict[str, Any]:
        return {
            "available_margin": 1000000.0,
            "used_margin": 150000.0,
            "net_cash": 1150000.0
        }

    def get_live_quotes(self, instrument_tokens: List[str]) -> Dict[str, Any]:
        quotes = {}
        now_ts = time.time()
        import straddle_engine
        for tok in instrument_tokens:
            raw_tok = str(tok).strip()
            if not raw_tok:
                continue
            if raw_tok in _LIVE_ADAPTER._quotes_cache:
                quotes[raw_tok] = _LIVE_ADAPTER._quotes_cache[raw_tok]
                continue
            clean_sym = raw_tok.split("|")[-1].strip().upper()
            if clean_sym in straddle_engine._SPOT_CACHE and straddle_engine._SPOT_CACHE[clean_sym] > 0:
                p = straddle_engine._SPOT_CACHE[clean_sym]
                quotes[raw_tok] = {"ltp": p, "bid": p, "ask": p, "oi": 0, "volume": 0}
                continue
            if clean_sym in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"):
                try:
                    p = straddle_engine._fetch_live_index_spot_network(clean_sym)
                    if p > 0:
                        quotes[raw_tok] = {"ltp": p, "bid": p, "ask": p, "oi": 0, "volume": 0}
                except Exception:
                    pass
        return quotes

    def wait_for_order_fill(
        self,
        order_id: str,
        timeout_sec: float = 15.0,
        poll_interval_sec: float = 0.1,
    ) -> Dict[str, Any]:
        return {"filled": True, "status": "COMPLETE", "order_id": str(order_id), "price": 0.0}


_LIVE_ADAPTER = KotakNeoBrokerAdapter()
_PAPER_ADAPTER = PaperBrokerAdapter()


def get_broker_adapter(mode: str = "AUTO") -> BaseBrokerAdapter:
    """Return appropriate adapter based on requested mode or active engine settings."""
    m = (mode or "").strip().upper()
    if m in ("PAPER", "PAPER_TRADING", "SIMULATION", "VIRTUAL"):
        return _PAPER_ADAPTER
    elif m in ("LIVE", "LIVE_BROKER", "KOTAK", "KOTAK_NEO"):
        return _LIVE_ADAPTER

    # Auto mode: check state or session
    auth_status = kotak_auth.get_auth_status()
    if auth_status.get("is_authenticated"):
        return _LIVE_ADAPTER
    return _PAPER_ADAPTER
