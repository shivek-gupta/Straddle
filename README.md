# ⚡ STRADDLE PRO — Kotak Neo Algorithmic Trading Platform

High-Speed, Quantiply-Grade Straddle & Strangle Algorithmic Trading Engine.
Decoupled and optimized for ultra-low latency execution via Kotak Securities Neo API (Zero Brokerage F&O).

---

## 🚀 Key Features
- **Independent Execution Engine:** Dedicated process isolated from equity swing scanners. Zero GIL contention or CPU stalling.
- **Sub-Second Tick Processing:** Real-time multi-leg evaluation, Greeks calculation (Black-Scholes fallback), and dynamic trailing stop-loss.
- **Kotak Neo API Integration:**
  - Automated headless authentication using TOTP (`pyotp`). Zero manual OTP intervention.
  - Daily morning auto-scheduler daemon running at 08:45 AM IST before market open.
  - Full support for both **Live Broker Mode** and simulated **Paper Trading Mode**.
- **Unified Trading Terminal (Port 5001):**
  - Interactive Option Chain with spot tickers, ATM indicators, and 1-Click order placement.
  - Strategy Matrix with presets: Short Straddle, Short Strangle, Iron Condor, Iron Fly, Wait & Trade.
  - Live MTM Equity Curve Tracker (real-time canvas line/candle tracker).
  - Unified Orderbook, open/closed positions table, and emergency kill switches.
- **Zero-Data-Loss Git Updates & Cloud Sync:**
  - Auto-commit & push to GitHub repository & Oracle Cloud with one click.
  - Safe Git pull that recursively snapshots all strategies, active multipliers, and credentials before update and restores them post-reset.

---

## 🛠️ Quick Start (Windows)
1. Run `start_straddle.bat` (automatically installs requirements and starts web server on port 5001).
2. Open `http://localhost:5001` in your browser.
3. Click **⚙️ Kotak API** in the top navigation and enter:
   - Consumer Key & Consumer Secret
   - Mobile Number & UCC (Client Code)
   - 6-Digit MPIN & TOTP Secret Key (from Google Authenticator setup)
4. Click **⚡ Login / Test Connection** to verify live session.

---

## ☁️ Running on Oracle Cloud (Alongside Swing Scanner)
You can run this platform on the same Oracle Cloud ARM instance:
- **Swing Scanner:** Runs on Port `5000` via `swingscanner.service`.
- **Straddle Platform:** Runs on Port `5001` via `straddle.service`.

### Systemd Setup on Oracle Cloud:
```bash
sudo cp straddle.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable straddle
sudo systemctl start straddle
sudo systemctl status straddle
```

### Oracle Cloud VCN Firewall:
Add an Ingress Rule in your Oracle Cloud Virtual Cloud Network (VCN) Security List:
- **Source CIDR:** `0.0.0.0/0`
- **Protocol:** `TCP`
- **Destination Port:** `5001`
