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
                        if r.get("segment") in ("NSE_FO", "BSE_FO") and r.get("exchange_token"):
                            tok = str(r["exchange_token"]).strip()
                            tsym = str(r.get("trading_symbol", "")).strip()
                            is_w = bool(r.get("weekly", True))
                            parts = tsym.split()
                            if len(parts) >= 6:
                                und = parts[0].upper()
                                strike = parts[1]
                                opt_type = parts[2].upper()
                                day = parts[3].zfill(2)
                                month = parts[4].upper()
                                year = parts[5][-2:]
                                if is_w:
                                    canon = f"{und}{year}{MONTH_CODE_MAP.get(month, month[:1])}{day}{strike}{opt_type}"
                                else:
                                    canon = f"{und}{year}{month}{strike}{opt_type}"
                                _KOTAK_TOKEN_MAP[tok] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper()] = canon
                                _KOTAK_DISPLAY_MAP[tsym.upper().replace(" ", "")] = canon
        except Exception as e:
            logger.warning(f"[Kotak Adapter] Note indexing instruments.json.gz: {e}")

        # 2. Enrich from Kotak Neo Scrip Master CSV if cached
        try:
            import neo_api_client.utils.scrip_cache as sc
            import pandas as pd
            for seg in ("nse_fo", "bse_fo"):
                csv_bytes = sc.read_csv(seg)
                if csv_bytes:
                    df = pd.read_csv(io.BytesIO(csv_bytes), usecols=["pSymbol", "pTrdSymbol"])
                    for _, row in df.iterrows():
                        tok = str(row["pSymbol"]).strip()
                        trd = str(row["pTrdSymbol"]).strip()
                        _KOTAK_TOKEN_MAP[tok] = trd
                        _KOTAK_DISPLAY_MAP[trd.upper()] = trd
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
    parts = clean_sym.split()
    if len(parts) >= 6:
        und = parts[0].upper()
        strike = parts[1]
        opt_type = parts[2].upper()
        day = parts[3].zfill(2)
        month = parts[4].upper()
        year = parts[5][-2:]
        m_code = MONTH_CODE_MAP.get(month, month[:1])
        return f"{und}{year}{m_code}{day}{strike}{opt_type}"

    return no_space


def round_to_tick(price: float, tick_size: float = 0.05) -> float:
    """Enforce exchange-mandated price tick multiples (0.05 for NSE/BSE)."""
    if not price or float(price) <= 0:
        return 0.0
    return round(round(float(price) / tick_size) * tick_size, 2)


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

    def get_order_book(self) -> List[Dict[str, Any]]:
        return []

    def get_trades(self) -> List[Dict[str, Any]]:
        return []

    def get_positions(self) -> List[Dict[str, Any]]:
        return []

    def get_funds(self) -> Dict[str, Any]:
        return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}

    def get_live_quotes(self, instrument_tokens: List[str]) -> Dict[str, Any]:
        return {}


class KotakNeoBrokerAdapter(BaseBrokerAdapter):
    """
    Direct High-Speed Kotak Neo F&O Broker Adapter.
    Translates algorithmic orders into Kotak Securities Trade API calls.
    """

    def __init__(self):
        super().__init__(name="Kotak Neo")
        self._lock = threading.Lock()
        _init_kotak_symbol_maps()

    def _get_client(self):
        return kotak_auth.get_authenticated_client(force=False)

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

        # Resolve exact canonical trading symbol required by Kotak Neo
        canonical_symbol = _resolve_kotak_trading_symbol(symbol, instrument_key)
        logger.info(f"[Kotak Neo] Placing order: symbol='{symbol}', key='{instrument_key}' -> canonical='{canonical_symbol}'")

        # Normalize parameters for Kotak Neo (Must adhere strictly to allowed canonical values)
        clean_tt = "B" if transaction_type.upper() in ("BUY", "B") else "S"
        clean_ot = "L" if order_type.upper() in ("LIMIT", "LMT", "L") else ("MKT" if order_type.upper() in ("MARKET", "MKT") else ("SL-M" if "M" in order_type.upper() else "SL"))
        clean_prod = "MIS" if product.upper() in ("MIS", "I", "INTRADAY") else "NRML"
        clean_price = str(round_to_tick(price)) if clean_ot in ("L", "SL") else "0"
        clean_trig = str(round_to_tick(trigger_price)) if trigger_price and trigger_price > 0 else "0"
        clean_qty = str(int(quantity))

        # Detect segment (Kotak Neo canonical values: nse_fo, bse_fo, nse_cm, bse_cm)
        segment = "bse_fo" if any(b in canonical_symbol.upper() for b in ("SENSEX", "BANKEX")) else "nse_fo"

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
                tag=tag or "straddle"
            )
            logger.info(f"[Kotak Neo] Order response: {res}")

            # Check for error status code from Kotak Neo
            if isinstance(res, dict):
                st_code = res.get("stCode")
                if st_code and int(st_code) != 200:
                    err_msg = res.get("errMsg") or res.get("message") or f"Broker rejected with code {st_code}"
                    logger.warning(f"[Kotak Neo] Order placement rejected: {err_msg} (response: {res})")
                    return {"success": False, "error": err_msg, "data": res}
            
            # Extract Order ID
            order_id = None
            if isinstance(res, dict):
                order_id = res.get("nOrdNo") or res.get("order_id") or (res.get("data") or {}).get("nOrdNo")
            
            if order_id:
                return {"success": True, "order_id": str(order_id), "data": res}
            
            err_msg = str(res.get("errMsg") or res.get("message") or res.get("error") or res.get("Error") or res)
            return {"success": False, "error": err_msg, "data": res}

        except Exception as e:
            logger.error(f"[Kotak Neo] Exception placing order: {e}")
            return {"success": False, "error": str(e)}

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
            clean_ot = "L" if order_type and order_type.upper() in ("LIMIT", "LMT", "L") else ("MKT" if order_type and order_type.upper() in ("MARKET", "MKT") else "L")
            clean_price = str(round_to_tick(price)) if price is not None else "0"
            clean_qty = str(int(quantity)) if quantity is not None else "0"
            clean_trig = str(round_to_tick(trigger_price)) if trigger_price is not None else "0"

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
                    "client_id": item.get("actId") or "Kotak Neo"
                })
            return standardized

        except Exception as e:
            logger.error(f"[Kotak Neo] Exception reading order report: {e}")
            return []

    def get_positions(self) -> List[Dict[str, Any]]:
        client = self._get_client()
        if client is None:
            return []

        try:
            res = client.positions()
            data = res.get("data") if isinstance(res, dict) else res
            if not isinstance(data, list):
                return []

            standardized = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                net_qty = int(item.get("netQty", item.get("quantity", 0)) or 0)
                tsym = item.get("trdSym", item.get("trading_symbol", ""))
                buy_avg = float(item.get("buyAvgPrc", 0.0) or 0.0)
                sell_avg = float(item.get("sellAvgPrc", 0.0) or 0.0)
                ltp = float(item.get("ltp", 0.0) or 0.0)
                pnl = float(item.get("urmtom", item.get("pnl", 0.0)) or 0.0)

                standardized.append({
                    "trading_symbol": tsym,
                    "quantity": net_qty,
                    "buy_quantity": int(item.get("buyQty", 0) or 0),
                    "sell_quantity": int(item.get("sellQty", 0) or 0),
                    "average_price": buy_avg if net_qty > 0 else (sell_avg if net_qty < 0 else 0.0),
                    "last_price": ltp,
                    "pnl": pnl,
                    "product": item.get("prod", "MIS"),
                    "client_id": item.get("actId") or "Kotak Neo"
                })
            return standardized
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception fetching positions: {e}")
            return []

    def get_funds(self) -> Dict[str, Any]:
        client = self._get_client()
        if client is None:
            return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}

        try:
            res = client.limits()
            data = res.get("data") if isinstance(res, dict) else res
            if isinstance(data, dict):
                avail = float(data.get("availableMargin", data.get("net", 0.0)) or 0.0)
                used = float(data.get("utilizedMargin", data.get("marginUsed", 0.0)) or 0.0)
                return {
                    "available_margin": avail,
                    "used_margin": used,
                    "net_cash": avail + used
                }
            return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}
        except Exception as e:
            logger.error(f"[Kotak Neo] Exception fetching funds: {e}")
            return {"available_margin": 0.0, "used_margin": 0.0, "net_cash": 0.0}

    def get_live_quotes(self, instrument_tokens: List[str]) -> Dict[str, Any]:
        client = self._get_client()
        if client is None or not instrument_tokens:
            return {}

        try:
            import straddle_engine
            req_tokens = []
            quotes_data = {}
            for tok in instrument_tokens:
                raw_tok = str(tok).strip()
                if not raw_tok:
                    continue
                clean_sym = raw_tok.split("|")[-1].strip().upper()
                if clean_sym in ("NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"):
                    try:
                        spot_p = straddle_engine._fetch_live_index_spot_network(clean_sym)
                        if spot_p > 0:
                            quotes_data[clean_sym] = {"ltp": spot_p, "bid": spot_p, "ask": spot_p, "oi": 0, "volume": 0}
                    except Exception:
                        pass
                else:
                    token_id = raw_tok.split("|")[-1].strip()
                    seg = "bse_fo" if any(b in raw_tok.upper() for b in ("SENSEX", "BANKEX")) else "nse_fo"
                    req_tokens.append({"instrument_token": token_id, "exchange_segment": seg})
            
            if req_tokens:
                res = client.quotes(instrument_tokens=req_tokens, quote_type="all")
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
                        sym = q.get("display_symbol")
                        if sym:
                            quotes_data[sym] = entry
            return quotes_data
        except Exception as e:
            logger.warning(f"[Kotak Neo] Exception fetching quotes: {e}")
            return {}


class PaperBrokerAdapter(BaseBrokerAdapter):
    """
    High-Fidelity Simulated Paper Trading Engine.
    Executes trades with zero monetary risk, maintains paper positions and order history.
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
            order_id = f"PAPER_{int(time.time()*1000)}"
            exec_price = price if price and price > 0 else 100.0

            order_entry = {
                "order_id": order_id,
                "trading_symbol": symbol,
                "instrument_key": instrument_key,
                "transaction_type": transaction_type.upper(),
                "quantity": int(quantity),
                "filled_quantity": int(quantity),
                "price": round_to_tick(exec_price),
                "average_price": round_to_tick(exec_price),
                "status": "COMPLETE",
                "order_timestamp": time.strftime("%H:%M:%S"),
                "product": product,
                "tag": tag,
                "client_id": "PAPER TRADING",
                "status_message": "Filled in Paper Simulation"
            }

            orders = self._read_json(self._orderbook_file)
            orders.insert(0, order_entry)
            self._write_json(self._orderbook_file, orders[:500])

            # Update paper positions
            positions = self._read_json(self._positions_file)
            found = False
            for p in positions:
                if p.get("trading_symbol") == symbol:
                    curr_qty = int(p.get("quantity", 0))
                    change_qty = int(quantity) if transaction_type.upper() == "BUY" else -int(quantity)
                    new_qty = curr_qty + change_qty
                    p["quantity"] = new_qty
                    p["last_price"] = exec_price
                    found = True
                    break

            if not found:
                positions.append({
                    "trading_symbol": symbol,
                    "quantity": int(quantity) if transaction_type.upper() == "BUY" else -int(quantity),
                    "average_price": exec_price,
                    "last_price": exec_price,
                    "pnl": 0.0,
                    "product": product,
                    "client_id": "PAPER TRADING"
                })

            self._write_json(self._positions_file, positions)
            return {"success": True, "order_id": order_id, "data": order_entry}

    def modify_order(
        self,
        order_id: str,
        quantity: Optional[int] = None,
        price: Optional[float] = None,
        trigger_price: Optional[float] = None,
        order_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {"success": True, "order_id": order_id, "message": "Paper order updated"}

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        return {"success": True, "order_id": order_id, "message": "Paper order cancelled"}

    def get_order_book(self) -> List[Dict[str, Any]]:
        return self._read_json(self._orderbook_file)

    def get_positions(self) -> List[Dict[str, Any]]:
        return self._read_json(self._positions_file)

    def get_funds(self) -> Dict[str, Any]:
        return {
            "available_margin": 1000000.0,
            "used_margin": 150000.0,
            "net_cash": 1150000.0
        }

    def get_live_quotes(self, instrument_tokens: List[str]) -> Dict[str, Any]:
        return {}


_LIVE_ADAPTER = KotakNeoBrokerAdapter()
_PAPER_ADAPTER = PaperBrokerAdapter()


def get_broker_adapter(mode: str = "AUTO") -> BaseBrokerAdapter:
    """Return appropriate adapter based on requested mode or active engine settings."""
    if mode.upper() == "PAPER":
        return _PAPER_ADAPTER
    elif mode.upper() in ("LIVE", "LIVE_BROKER", "KOTAK"):
        return _LIVE_ADAPTER

    # Auto mode: check state or session
    auth_status = kotak_auth.get_auth_status()
    if auth_status.get("is_authenticated"):
        return _LIVE_ADAPTER
    return _PAPER_ADAPTER
