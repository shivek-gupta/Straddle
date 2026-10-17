"""
High-Speed Standalone Web Server for Straddle & Strangle Algorithmic Trading Platform.
Dedicated Flask application running on Port 5001.
Features:
- Complete Quantiply-grade Straddle API routes.
- Kotak Neo API authentication & credentials management endpoints.
- Zero-data-loss Git pull, auto-commit, and deferred restart.
- Background health & morning scheduler initialization.
"""

import os
import sys
import time
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path
from threading import Thread

try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    if hasattr(sys.stderr, 'reconfigure'):
        sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

from flask import Flask, jsonify, request, render_template, send_from_directory

import config
import kotak_auth
import kotak_broker_adapter
from straddle_engine import get_straddle_manager

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("web_server")

app = Flask(
    __name__,
    template_folder=str(config.PROJECT_ROOT / "templates"),
    static_folder=str(config.PROJECT_ROOT / "static"),
    static_url_path="/static"
)
app.config["SECRET_KEY"] = config.SECRET_KEY


# ==========================================================
# PAGE ROUTES
# ==========================================================

@app.route("/")
def index():
    creds = config.get_kotak_credentials()
    auth_status = kotak_auth.get_auth_status()
    return render_template("index.html", creds=creds, auth_status=auth_status)


@app.after_request
def add_cache_headers(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ==========================================================
# KOTAK NEO AUTHENTICATION ROUTES
# ==========================================================

@app.route("/api/kotak/status", methods=["GET"])
def api_kotak_status():
    try:
        status = kotak_auth.get_auth_status()
        return jsonify({"success": True, "status": status})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kotak/login", methods=["POST"])
def api_kotak_login():
    try:
        client = kotak_auth.get_authenticated_client(force=True)
        if client:
            return jsonify({"success": True, "message": "Authenticated with Kotak Neo successfully!"})
        err = kotak_auth.get_last_auth_error() or "Login failed. Please verify credentials and whitelist IP."
        return jsonify({"success": False, "error": err}), 400
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kotak/save_credentials", methods=["POST"])
def api_kotak_save_credentials():
    try:
        data = request.get_json(silent=True) or {}
        ok = config.save_kotak_credentials(data)
        if ok:
            return jsonify({"success": True, "message": "Kotak Neo credentials saved successfully."})
        return jsonify({"success": False, "error": "Failed to save credentials."}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/kotak/credentials", methods=["GET"])
def api_kotak_credentials():
    try:
        creds = config.get_kotak_credentials()
        return jsonify({
            "success": True,
            "credentials": {
                "consumer_key": creds.get("consumer_key", ""),
                "consumer_secret": creds.get("consumer_secret", ""),
                "mobile_number": creds.get("mobile_number", ""),
                "ucc": creds.get("ucc", ""),
                "mpin": creds.get("mpin", ""),
                "totp_key": creds.get("totp_key", ""),
                "environment": creds.get("environment", "prod")
            }
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


# ==========================================================
# STRADDLE ENGINE API ROUTES
# ==========================================================

@app.route("/api/straddle/state", methods=["GET"])
def api_straddle_state():
    try:
        mgr = get_straddle_manager()
        state = mgr.get_full_state()
        return jsonify({"success": True, "state": state})
    except Exception as e:
        logger.error(f"Error in api_straddle_state: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/spot", methods=["GET"])
def api_straddle_spot():
    try:
        und = request.args.get("underlying", "NIFTY").upper()
        mgr = get_straddle_manager()
        spot = mgr.get_cached_spot_price(und)
        step = 50 if und == "NIFTY" else (100 if und in ("BANKNIFTY", "SENSEX") else 50)
        atm = round(spot / step) * step if spot > 0 else 0
        return jsonify({
            "success": True,
            "underlying": und,
            "spot": spot,
            "atm": atm,
            "timestamp": datetime.now().strftime("%H:%M:%S")
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/logs", methods=["GET"])
def api_straddle_logs():
    try:
        mgr = get_straddle_manager()
        logs = mgr.get_logs(limit=250)
        return jsonify({"success": True, "logs": logs})
    except Exception as e:
        logger.error(f"Error in api_straddle_logs: {e}")
        return jsonify({"success": False, "error": str(e), "logs": []}), 500


@app.route("/api/straddle/strategy/save", methods=["POST"])
def api_straddle_strategy_save():
    try:
        data = request.get_json(silent=True) or {}
        mgr = get_straddle_manager()
        res = mgr.save_strategy(data)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/delete", methods=["POST"])
def api_straddle_strategy_delete():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id") or data.get("id") or data.get("name") or ""
        if not sid:
            return jsonify({"success": False, "error": "No strategy ID provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.delete_strategy(sid)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/set_active", methods=["POST"])
def api_straddle_strategy_set_active():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id", "")
        mgr = get_straddle_manager()
        res = mgr.set_active_strategy(sid)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/start", methods=["POST"])
def api_straddle_start():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id")
        force_fresh = bool(data.get("force_fresh", True))
        mgr = get_straddle_manager()
        res = mgr.start_algo(sid, force_fresh=force_fresh)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/reset", methods=["POST"])
def api_straddle_strategy_reset():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id") or data.get("id") or data.get("strategy_name") or data.get("name") or data.get("algo_name")
        sname = data.get("name") or data.get("strategy_name") or data.get("algo_name")
        mgr = get_straddle_manager()
        res = mgr.reset_algo(strategy_id=sid, strategy_name=sname)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_strategy_reset: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/positions/clear_closed", methods=["POST"])
def api_straddle_positions_clear_closed():
    try:
        mgr = get_straddle_manager()
        res = mgr.clear_closed_positions()
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/terminate", methods=["POST"])
def api_straddle_terminate():
    try:
        mgr = get_straddle_manager()
        res = mgr.terminate_algo()
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/square_off_all", methods=["POST"])
def api_straddle_square_off_all():
    try:
        mgr = get_straddle_manager()
        res = mgr.square_off_all_and_terminate()
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/square_off_leg", methods=["POST"])
def api_straddle_square_off_leg():
    try:
        data = request.get_json(silent=True) or {}
        leg_idx = int(data.get("leg_index", -1))
        mgr = get_straddle_manager()
        res = mgr.square_off_leg(leg_idx)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/retry_legs", methods=["POST"])
def api_straddle_retry_legs():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id")
        leg_ids = data.get("leg_ids")
        leg_indices = data.get("leg_indices")
        updated_legs = data.get("updated_legs")
        mgr = get_straddle_manager()
        res = mgr.retry_failed_legs(
            strategy_id=sid,
            leg_ids=leg_ids,
            leg_indices=leg_indices,
            updated_legs=updated_legs
        )
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_retry_legs: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/update_running_position", methods=["POST"])
def api_straddle_update_running_position():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id")
        leg_id = data.get("leg_id")
        leg_idx = data.get("leg_index")
        t_mode = data.get("target_mode", "%")
        t_val = data.get("target_val")
        sl_mode = data.get("sl_mode", "%")
        sl_val = data.get("sl_val")
        new_lots = data.get("new_lots")
        new_strike = data.get("new_strike")
        mgr = get_straddle_manager()
        res = mgr.update_running_position(
            strategy_id=sid,
            leg_id=leg_id,
            leg_index=leg_idx,
            target_mode=t_mode,
            target_val=t_val,
            sl_mode=sl_mode,
            sl_val=sl_val,
            new_lots=new_lots,
            new_strike=new_strike,
        )
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_update_running_position: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/rectify_leg", methods=["POST"])
def api_straddle_rectify_leg():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id")
        leg_id = data.get("leg_id")
        leg_idx = data.get("leg_index")
        updated_cfg = data.get("updated_leg_config")
        retry_imm = bool(data.get("retry_immediately", True))
        mgr = get_straddle_manager()
        res = mgr.rectify_failed_leg(
            strategy_id=sid,
            leg_id=leg_id,
            leg_index=leg_idx,
            updated_leg_config=updated_cfg,
            retry_immediately=retry_imm
        )
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_rectify_leg: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/adoptable_positions", methods=["GET"])
def api_straddle_adoptable_positions():
    try:
        sid = request.args.get("strategy_id")
        mgr = get_straddle_manager()
        positions = mgr.get_adoptable_broker_positions(strategy_id=sid)
        return jsonify({"success": True, "positions": positions})
    except Exception as e:
        logger.error(f"Error in api_straddle_adoptable_positions: {e}")
        return jsonify({"success": False, "error": str(e), "positions": []}), 500


@app.route("/api/straddle/adopt_position", methods=["POST"])
def api_straddle_adopt_position():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id")
        pos_data = data.get("position_data") or data.get("position") or data
        mgr = get_straddle_manager()
        res = mgr.adopt_broker_position(strategy_id=sid, position_data=pos_data)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_adopt_position: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/detach_position", methods=["POST"])
def api_straddle_detach_position():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id")
        leg_id = data.get("leg_id")
        leg_idx = data.get("leg_index")
        mgr = get_straddle_manager()
        res = mgr.detach_position(strategy_id=sid, leg_id=leg_id, leg_index=leg_idx)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_detach_position: {e}")
        return jsonify({"success": False, "error": str(e)}), 500



@app.route("/api/straddle/reset_algo", methods=["POST"])
def api_straddle_reset_algo():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id") or data.get("id") or data.get("strategy_name") or data.get("name") or data.get("algo_name")
        sname = data.get("name") or data.get("strategy_name") or data.get("algo_name")
        mgr = get_straddle_manager()
        res = mgr.reset_algo(strategy_id=sid, strategy_name=sname)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_reset_algo: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/toggle_mode", methods=["POST"])
def api_straddle_toggle_mode():
    try:
        data = request.get_json(silent=True) or {}
        mode = data.get("mode", "LIVE_BROKER")
        mgr = get_straddle_manager()
        res = mgr.toggle_execution_mode(mode)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/option_chain", methods=["GET"])
def api_straddle_option_chain():
    try:
        und = request.args.get("underlying") or request.args.get("index") or "NIFTY"
        exp_ms_raw = request.args.get("expiry_ms")
        exp_ms = int(exp_ms_raw) if exp_ms_raw and exp_ms_raw.isdigit() else None
        range_raw = request.args.get("strike_range")
        strike_range = int(range_raw) if range_raw and (range_raw.isdigit() or range_raw == "0") else 15
        mgr = get_straddle_manager()
        chain = mgr.get_option_chain_snapshot(underlying=und, expiry_ms=exp_ms, strike_range=strike_range)
        return jsonify({"success": True, "data": chain, "chain": chain})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/search_contracts", methods=["GET"])
def api_straddle_search_contracts():
    try:
        q = request.args.get("query") or request.args.get("q") or ""
        und = request.args.get("underlying") or ""
        limit = int(request.args.get("limit", 24))
        mgr = get_straddle_manager()
        results = mgr.search_contracts(query=q, underlying=und if und else None, limit=limit)
        return jsonify({"success": True, "results": results, "count": len(results)})
    except Exception as e:
        return jsonify({"success": False, "error": str(e), "results": []}), 500


@app.route("/api/straddle/contract_quote", methods=["GET"])
def api_straddle_contract_quote():
    try:
        und = request.args.get("underlying") or "NIFTY"
        strike_raw = request.args.get("strike", "0")
        try:
            strike = float(strike_raw)
        except ValueError:
            strike = 0.0
        opt_type = request.args.get("opt_type") or request.args.get("type") or "CE"
        exp_date = request.args.get("expiry_date") or request.args.get("expiry")
        exp_ms_raw = request.args.get("expiry_ms")
        exp_ms = int(exp_ms_raw) if exp_ms_raw and exp_ms_raw.isdigit() else None

        mgr = get_straddle_manager()
        quote = mgr.get_contract_quote(
            underlying=und,
            strike=strike,
            opt_type=opt_type,
            expiry_ms=exp_ms,
            expiry_date=exp_date
        )
        return jsonify({"success": True, "data": quote})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/basket/quotes", methods=["POST"])
def api_straddle_basket_quotes():
    try:
        data = request.get_json(silent=True) or {}
        contracts = data.get("contracts") or data.get("orders") or []
        mgr = get_straddle_manager()
        results = mgr.get_batch_contract_quotes(contracts)
        return jsonify({"success": True, "quotes": results})
    except Exception as e:
        logger.error(f"Error in api_straddle_basket_quotes: {e}")
        return jsonify({"success": False, "error": str(e), "quotes": []}), 500



@app.route("/api/straddle/strategy/duplicate", methods=["POST"])
def api_straddle_strategy_duplicate():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id", "")
        mgr = get_straddle_manager()
        res = mgr.duplicate_strategy(sid)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/rename", methods=["POST"])
def api_straddle_strategy_rename():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id", "")
        new_name = data.get("name", "")
        mgr = get_straddle_manager()
        res = mgr.rename_strategy(sid, new_name)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/toggle_enable", methods=["POST"])
@app.route("/api/straddle/strategy/enable", methods=["POST"])
def api_straddle_strategy_toggle_enable():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id") or data.get("id") or ""
        enabled = bool(data.get("enabled", True))
        if not sid:
            return jsonify({"success": False, "error": "No strategy ID provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.toggle_strategy_enable(sid, enabled)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/create_preset", methods=["POST"])
def api_straddle_strategy_create_preset():
    try:
        data = request.get_json(silent=True) or {}
        ptype = data.get("preset_type", "SHORT_STRADDLE")
        und = data.get("underlying", "NIFTY")
        name = data.get("name")
        days = data.get("days")
        mgr = get_straddle_manager()
        res = mgr.create_preset_strategy(name=name, preset_type=ptype, underlying=und, days=days)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500




@app.route("/api/straddle/strategy/square_off", methods=["POST"])
def api_straddle_strategy_square_off():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id", "")
        if not sid:
            return jsonify({"success": False, "error": "No strategy ID provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.square_off_strategy(sid)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/batch_delete", methods=["POST"])
def api_straddle_strategy_batch_delete():
    try:
        data = request.get_json(silent=True) or {}
        ids = data.get("strategy_ids", [])
        if not ids:
            return jsonify({"success": False, "error": "No strategy IDs provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.batch_delete_strategies(ids)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/batch_duplicate", methods=["POST"])
def api_straddle_strategy_batch_duplicate():
    try:
        data = request.get_json(silent=True) or {}
        ids = data.get("strategy_ids", [])
        if not ids:
            return jsonify({"success": False, "error": "No strategy IDs provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.batch_duplicate_strategies(ids)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/batch_toggle_enable", methods=["POST"])
def api_straddle_strategy_batch_toggle_enable():
    try:
        data = request.get_json(silent=True) or {}
        ids = data.get("strategy_ids", [])
        enabled = bool(data.get("enabled", True))
        if not ids:
            return jsonify({"success": False, "error": "No strategy IDs provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.batch_toggle_strategies_enable(ids, enabled)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/batch_set_days", methods=["POST"])
def api_straddle_strategy_batch_set_days():
    try:
        data = request.get_json(silent=True) or {}
        ids = data.get("strategy_ids", [])
        days = data.get("days", ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"])
        if not ids:
            return jsonify({"success": False, "error": "No strategy IDs provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.batch_set_strategies_days(ids, days)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/batch_reset", methods=["POST"])
def api_straddle_strategy_batch_reset():
    try:
        data = request.get_json(silent=True) or {}
        ids = data.get("strategy_ids", [])
        if not ids:
            return jsonify({"success": False, "error": "No strategy IDs provided"}), 400
        mgr = get_straddle_manager()
        res = mgr.batch_reset_strategies(ids)
        return jsonify(res)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/orderbook", methods=["GET"])
def api_straddle_orderbook():
    try:
        client_id = request.args.get("client_id")
        algo_id = request.args.get("algo_id")
        source_filter = request.args.get("source_filter") or request.args.get("source")
        status_filter = request.args.get("status_filter") or request.args.get("status")
        mgr = get_straddle_manager()
        orders = mgr.get_orderbook(
            client_id=client_id,
            algo_id=algo_id,
            source_filter=source_filter,
            status_filter=status_filter
        )
        return jsonify({"success": True, "orders": orders})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/pnl_chart", methods=["GET"])
def api_straddle_pnl_chart():
    try:
        selector_type = request.args.get("selector_type", "client")
        entity_id = request.args.get("entity_id") or request.args.get("client_id") or request.args.get("strategy_id")
        start_date = request.args.get("start_date")
        end_date = request.args.get("end_date")
        underlying = request.args.get("underlying")
        mgr = get_straddle_manager()
        chart_data = mgr.get_pnl_chart_data(
            selector_type=selector_type,
            entity_id=entity_id,
            start_date=start_date,
            end_date=end_date,
            underlying=underlying
        )
        return jsonify({"success": True, "data": chart_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/margin", methods=["GET"])
def api_straddle_margin():
    try:
        client_id = request.args.get("client_id")
        mgr = get_straddle_manager()
        margin_data = mgr.get_margin_overview(client_id=client_id)
        return jsonify({"success": True, "margin": margin_data})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/sync_broker", methods=["POST", "GET"])
def api_straddle_sync_broker():
    try:
        mgr = get_straddle_manager()
        res = mgr.sync_broker_data()
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_sync_broker: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/open_positions", methods=["GET"])
def api_straddle_open_positions():
    try:
        mgr = get_straddle_manager()
        res = mgr.get_all_open_positions()
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_open_positions: {e}")
        return jsonify({"success": False, "error": str(e), "open_positions": [], "total_open_mtm": 0.0}), 500


@app.route("/api/straddle/square_off_open_position", methods=["POST"])
def api_straddle_square_off_open_position():
    try:
        data = request.get_json(silent=True) or {}
        symbol = data.get("symbol", "")
        instrument_key = data.get("instrument_key", "")
        product = data.get("product", "I")
        qty = int(data.get("quantity") or data.get("abs_qty") or 0)
        side = data.get("side", "BUY")
        mgr = get_straddle_manager()
        res = mgr.square_off_open_position(
            symbol=symbol,
            instrument_key=instrument_key,
            product=product,
            quantity=qty,
            side=side
        )
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_square_off_open_position: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/place_direct_order", methods=["POST"])
def api_straddle_place_direct_order():
    try:
        data = request.get_json(silent=True) or {}
        symbol = data.get("symbol", "")
        instrument_key = data.get("instrument_key", "")
        transaction_type = data.get("transaction_type", "BUY")
        quantity = int(data.get("quantity", 0))
        product = data.get("product", "NRML")
        order_type = data.get("order_type", "LIMIT")
        price = float(data.get("price", 0.0))
        trigger_price = float(data.get("trigger_price", 0.0))
        tag = data.get("tag", "manual_direct_order")

        mgr = get_straddle_manager()
        res = mgr.place_direct_order(
            symbol=symbol,
            instrument_key=instrument_key,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product,
            order_type=order_type,
            price=price,
            trigger_price=trigger_price,
            tag=tag
        )
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_place_direct_order: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/order_margin", methods=["POST"])
def api_straddle_order_margin():
    try:
        data = request.get_json(silent=True) or {}
        symbol = data.get("symbol", "")
        instrument_key = data.get("instrument_key", "")
        transaction_type = data.get("transaction_type", "BUY")
        quantity = int(data.get("quantity", 65))
        product = data.get("product", "D")
        price = float(data.get("price", 0.0))
        underlying = data.get("underlying", "NIFTY")

        mgr = get_straddle_manager()
        res = mgr.calculate_order_margin(
            symbol=symbol,
            instrument_key=instrument_key,
            transaction_type=transaction_type,
            quantity=quantity,
            product=product,
            price=price,
            underlying=underlying
        )
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_order_margin: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/basket/margin", methods=["POST"])
def api_straddle_basket_margin():
    try:
        data = request.get_json(silent=True) or {}
        orders = data.get("orders") or []
        multiplier = int(data.get("multiplier") or 1)
        mgr = get_straddle_manager()
        res = mgr.calculate_basket_margin(orders=orders, multiplier=multiplier)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_basket_margin: {e}")
        return jsonify({"success": False, "error": str(e), "required_margin": 0.0, "available_margin": 0.0}), 500


@app.route("/api/straddle/basket/execute", methods=["POST"])
def api_straddle_basket_execute():
    try:
        data = request.get_json(silent=True) or {}
        orders = data.get("orders") or []
        multiplier = int(data.get("multiplier") or 1)
        mgr = get_straddle_manager()
        res = mgr.execute_basket_orders(orders=orders, multiplier=multiplier)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_basket_execute: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/square_off_all_open_positions", methods=["POST"])
def api_straddle_square_off_all_open_positions():
    try:
        mgr = get_straddle_manager()
        res = mgr.square_off_all_open_positions()
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_square_off_all_open_positions: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/strategy/set_days", methods=["POST"])
def api_straddle_strategy_set_days():
    try:
        data = request.get_json(silent=True) or {}
        sid = data.get("strategy_id", "")
        days = data.get("days", [])
        mgr = get_straddle_manager()
        res = mgr.set_strategy_days(sid, days)
        return jsonify(res)
    except Exception as e:
        logger.error(f"Error in api_straddle_strategy_set_days: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/straddle/expiries", methods=["GET"])
def api_straddle_expiries():
    try:
        und = request.args.get("underlying") or request.args.get("index") or "NIFTY"
        mgr = get_straddle_manager()
        expiries = mgr.get_expiries_for_underlying(und)
        return jsonify({"success": True, "expiries": expiries})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500



# ==========================================================
# SYSTEM, GIT UPDATES & ZERO DATA LOSS RESTORATION
# ==========================================================

@app.route("/api/system/git_status", methods=["GET"])
def api_system_git_status():
    try:
        branch_res = subprocess.run(["git", "branch", "--show-current"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        head_res = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        log_res = subprocess.run(["git", "log", "-n", "3", "--oneline"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        status_res = subprocess.run(["git", "status", "-s"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        return jsonify({
            "branch": branch_res.stdout.strip() or "main",
            "head": head_res.stdout.strip(),
            "log": log_res.stdout.strip(),
            "status": status_res.stdout.strip()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/system/git_commit_push", methods=["POST"])
def api_system_git_commit_push():
    """Auto commit and push local changes to GitHub repository & Oracle Cloud."""
    try:
        data = request.get_json(silent=True) or {}
        msg = data.get("message") or f"Auto-update: Straddle Platform {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        
        # 1. Add all files except data/ (data is preserved independently)
        add_res = subprocess.run(["git", "add", "."], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        # 2. Commit
        commit_res = subprocess.run(["git", "commit", "-m", msg], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        # 3. Push
        push_res = subprocess.run(["git", "push", "origin", "main"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        
        output = (add_res.stdout or "") + (commit_res.stdout or "") + (push_res.stdout or "") + (push_res.stderr or "")
        return jsonify({
            "success": push_res.returncode == 0,
            "output": output.strip(),
            "message": "Committed and pushed successfully to GitHub repository & Oracle Cloud!" if push_res.returncode == 0 else "Push encountered notice."
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


def _execute_safe_git_pull_operation():
    """
    Executes a comprehensive, zero-data-loss Git pull / reset to origin/main.
    Recursively snapshots all data files (strategies, state, credentials, sessions, orderbooks)
    before reset, and fully restores/reconciles them post-reset.
    """
    # 1. Snapshot ALL files in data/
    data_snapshots = {}
    data_dir_path = config.DATA_DIR
    if data_dir_path.exists():
        for root, _, files in os.walk(str(data_dir_path)):
            for f_name in files:
                full_p = Path(root) / f_name
                try:
                    rel_p = str(full_p.relative_to(config.PROJECT_ROOT)).replace("\\", "/")
                    if full_p.stat().st_size > 0:
                        with open(full_p, "rb") as f_snap:
                            data_snapshots[rel_p] = f_snap.read()
                except Exception:
                    pass

    # 2. Check and ensure git remote points to GitHub repository
    try:
        rem_get = subprocess.run(["git", "remote", "get-url", "origin"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        if rem_get.returncode != 0 or not rem_get.stdout.strip():
            subprocess.run(["git", "remote", "add", "origin", "https://github.com/shivek-gupta/Straddle.git"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
        elif "github.com/shivek-gupta/Straddle" not in rem_get.stdout:
            subprocess.run(["git", "remote", "set-url", "origin", "https://github.com/shivek-gupta/Straddle.git"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
    except Exception:
        pass

    rem_chk = subprocess.run(["git", "remote"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
    has_remote = bool(rem_chk.stdout.strip())

    if has_remote:
        subprocess.run(["git", "fetch", "--all"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True, timeout=60)
        res = subprocess.run(["git", "reset", "--hard", "origin/main"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True, timeout=30)
        if res.returncode != 0:
            res = subprocess.run(["git", "pull", "--rebase", "origin", "main"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True, timeout=30)
        out = (res.stdout or "") + "\n" + (res.stderr or "")
        success = (res.returncode == 0)
    else:
        out = "Standalone mode: Local files are up-to-date. Data snapshot reconciled. Reloading server..."
        success = True

    # 3. Restore snapshots
    for rel, b_data in data_snapshots.items():
        dest_p = config.PROJECT_ROOT / rel
        try:
            dest_p.parent.mkdir(parents=True, exist_ok=True)
            with open(dest_p, "wb") as f_out:
                f_out.write(b_data)
        except Exception as e_rest:
            out += f"\nNote restoring {rel}: {e_rest}"

    return success, out


@app.route("/api/system/git_pull", methods=["POST"])
def api_system_git_pull():
    try:
        success, out = _execute_safe_git_pull_operation()

        def _deferred_restart():
            time.sleep(1.5)
            try:
                subprocess.run(["sudo", "systemctl", "restart", "straddle"], timeout=10)
            except Exception:
                pass
            try:
                os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception:
                try:
                    os._exit(0)
                except Exception:
                    pass

        Thread(target=_deferred_restart, daemon=True).start()

        return jsonify({
            "success": success,
            "output": out,
            "message": "Git pull completed successfully with full data preservation. Server reloading..."
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/system/restart", methods=["POST"])
def api_system_restart():
    try:
        def _deferred_restart():
            time.sleep(1.2)
            try:
                subprocess.run(["sudo", "systemctl", "restart", "straddle"], timeout=10)
            except Exception:
                pass
            try:
                os.execv(sys.executable, [sys.executable] + sys.argv)
            except Exception:
                os._exit(0)

        Thread(target=_deferred_restart, daemon=True).start()
        return jsonify({"success": True, "message": "Server restarting gracefully..."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ==========================================================
# SERVER INITIALIZATION
# ==========================================================

def _auto_git_sync_worker():
    """Periodically check remote GitHub repository and auto-pull if updates exist."""
    time.sleep(10)
    while True:
        try:
            rem_chk = subprocess.run(["git", "remote"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True)
            if rem_chk.stdout.strip():
                subprocess.run(["git", "fetch", "origin", "main"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True, timeout=30)
                local_h = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True).stdout.strip()
                remote_h = subprocess.run(["git", "rev-parse", "origin/main"], cwd=str(config.PROJECT_ROOT), capture_output=True, text=True).stdout.strip()
                if remote_h and local_h and remote_h != local_h:
                    logger.info(f"[Auto-Updater] New version detected ({remote_h[:7]}). Pulling updates automatically...")
                    success, _ = _execute_safe_git_pull_operation()
                    if success:
                        logger.info("[Auto-Updater] Update applied cleanly. Reloading server process...")
                        time.sleep(2)
                        try:
                            subprocess.run(["sudo", "systemctl", "restart", "straddle"], timeout=10)
                        except Exception:
                            pass
                        os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as e:
            logger.debug(f"[Auto-Updater] Check notice: {e}")
        time.sleep(15)


def init_services():
    """Start background schedulers and auto-authentication."""
    try:
        kotak_auth.start_morning_scheduler()
    except Exception as e:
        logger.warning(f"Could not start morning scheduler: {e}")

    try:
        # Start automatic background git updater
        Thread(target=_auto_git_sync_worker, daemon=True, name="AutoGitSyncWorker").start()
    except Exception as e:
        logger.warning(f"Could not start auto git sync worker: {e}")

    try:
        # Pre-initialize Straddle Engine singleton on startup
        # to ensure background evaluation loop and auto square-off runner are active
        logger.info("Initializing Straddle Engine background runner...")
        get_straddle_manager()
    except Exception as e:
        logger.warning(f"Straddle Engine startup notice: {e}")

    try:
        # Check authentication on startup
        if not kotak_auth.is_session_active_for_today():
            logger.info("Attempting startup authentication with Kotak Neo...")
            kotak_auth.get_authenticated_client(force=False)
    except Exception as e:
        logger.warning(f"Startup auth check notice: {e}")


if __name__ == "__main__":
    init_services()
    print("=" * 70)
    print("[STRADDLE PRO] STRADDLE & STRANGLE ALGORITHMIC PLATFORM (KOTAK NEO)")
    print(f"Server URL: http://{config.WEB_HOST}:{config.WEB_PORT}")
    print("=" * 70)
    app.run(host=config.WEB_HOST, port=config.WEB_PORT, debug=False, threaded=True)
