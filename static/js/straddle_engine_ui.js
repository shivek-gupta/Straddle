/**
 * Quantiply-Grade Straddle & Strangle Algorithmic Trading Platform UI Engine.
 * Provides real-time MTM streaming, multi-strategy scheduling, Day-of-week views,
 * Black-Scholes + Kotak Neo live Option Chain matrix, Canvas P&L Equity Chart,
 * Unified Orderbook & Margin tracking, and high-speed execution controls.
 */

window._straddleState = {
    activeStrategyId: null,
    activeStrategy: null,
    allStrategies: [],
    positions: [],
    openPositionsCache: [],
    closedPositionsCache: [],
    allPositionsCache: [],
    totalOpenMtm: 0.0,
    totalRealisedPnl: 0.0,
    totalPortfolioPnl: 0.0,
    positionsFilter: "open",           // 'open' | 'closed' | 'all'
    pollTimer: null,
    isPolling: false,
    prevLtpMap: {},
    availableExpiries: [],
    currentSubView: "open_positions",   // 'open_positions' | 'algos' | 'terminal' | 'pnl' | 'orderbook'
    currentDayFilter: "ALL",
    pnlChartSelector: "client",        // 'client' | 'algo'
    pnlChartType: "line",              // 'line' | 'candle'
    pnlSelectedEntityId: "OVERALL",
    pnlDataCache: null,
    orderbookDataCache: [],
    orderbookSubTab: "orders"          // 'orders' | 'margin'
};

window._straddleSelectedLegIndices = new Set();
window._straddleSelectedLegIds = new Set();

// =========================================================================
// LIGHTNING RESPONSIVE NON-BLOCKING TOAST NOTIFICATIONS
// =========================================================================

function showStraddleToast(message, type = 'info', duration = 2500) {
    let container = document.getElementById("straddleToastContainer");
    if (!container) {
        container = document.createElement("div");
        container.id = "straddleToastContainer";
        container.style.cssText = "position:fixed; top:24px; right:24px; z-index:999999; display:flex; flex-direction:column; gap:8px; pointer-events:none; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;";
        document.body.appendChild(container);
    }
    const toast = document.createElement("div");
    const bg = type === 'success' ? 'linear-gradient(135deg, #059669, #10b981)' :
               type === 'error' ? 'linear-gradient(135deg, #dc2626, #ef4444)' :
               type === 'warning' ? 'linear-gradient(135deg, #d97706, #f59e0b)' :
               'linear-gradient(135deg, #1e40af, #3b82f6)';
    const icon = type === 'success' ? '✅' : type === 'error' ? '❌' : type === 'warning' ? '⚠️' : '⚡';
    toast.style.cssText = `background:${bg}; color:#fff; padding:9px 15px; border-radius:8px; box-shadow:0 10px 25px -5px rgba(0,0,0,0.5), 0 0 15px rgba(0,0,0,0.2); font-size:12.5px; font-weight:700; display:flex; align-items:center; gap:8px; pointer-events:auto; transition:all 0.22s cubic-bezier(0.16, 1, 0.3, 1); transform:translateY(-8px) scale(0.95); opacity:0;`;
    toast.innerHTML = `<span style="font-size:14px;">${icon}</span><span>${message}</span>`;
    container.appendChild(toast);
    
    requestAnimationFrame(() => {
        toast.style.transform = "translateY(0) scale(1)";
        toast.style.opacity = "1";
    });
    
    setTimeout(() => {
        toast.style.transform = "translateY(-8px) scale(0.95)";
        toast.style.opacity = "0";
        setTimeout(() => toast.remove(), 250);
    }, duration);
}
window.showStraddleToast = showStraddleToast;

function showStraddleRejectedToast(opts = {}) {
    let container = document.getElementById("straddleToastContainer");
    if (!container) {
        container = document.createElement("div");
        container.id = "straddleToastContainer";
        container.style.cssText = "position:fixed; top:24px; right:24px; z-index:999999; display:flex; flex-direction:column; gap:10px; pointer-events:none; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; max-width:320px; width:100%;";
        document.body.appendChild(container);
    }
    const toast = document.createElement("div");
    const symbol = opts.symbol || "NIFTY OPTION";
    const msg = opts.error_message || opts.message || "You need to add Rs. 113880.56 in your account to place this trade.";
    const clientId = opts.client_id || "Kotak Neo";
    const orderId = opts.order_id || ("2610080" + Date.now().toString().slice(-8));
    const timeStr = opts.time || new Date().toLocaleTimeString("en-IN", { hour12: false });

    toast.style.cssText = `background:linear-gradient(135deg, #ef4444 0%, #dc2626 100%); color:#ffffff; padding:14px 16px; border-radius:8px; box-shadow:0 12px 30px rgba(220,38,38,0.4), 0 4px 10px rgba(0,0,0,0.5); pointer-events:auto; transition:all 0.25s cubic-bezier(0.16, 1, 0.3, 1); transform:translateY(-8px) scale(0.95); opacity:0; border:1px solid rgba(255,255,255,0.2);`;
    toast.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; border-bottom:1px solid rgba(255,255,255,0.15); padding-bottom:6px;">
            <div style="display:flex; align-items:center; gap:6px; font-weight:900; font-size:13px; letter-spacing:0.5px;">
                <span style="font-size:14px;">⛔</span> REJECTED
            </div>
            <button type="button" onclick="this.closest('div').parentElement.remove()" style="background:transparent; border:none; color:#ffffff; font-size:16px; font-weight:700; cursor:pointer; opacity:0.8;" onmouseover="this.style.opacity='1'" onmouseout="this.style.opacity='0.8'">✕</button>
        </div>
        <div style="font-weight:800; font-size:13px; letter-spacing:0.3px; margin-bottom:4px;">${symbol}</div>
        <div style="font-size:12px; font-style:italic; line-height:1.35; margin-bottom:8px; color:#fee2e2;">*${msg}*</div>
        <div style="font-size:11px; opacity:0.9; display:flex; flex-direction:column; gap:2px; font-family:var(--font-mono);">
            <div>Client ID: ${clientId}</div>
            <div>Order ID: ${orderId}</div>
            <div style="text-align:right; margin-top:2px; font-size:10.5px; opacity:0.8;">${timeStr}</div>
        </div>
    `;
    container.appendChild(toast);

    requestAnimationFrame(() => {
        toast.style.transform = "translateY(0) scale(1)";
        toast.style.opacity = "1";
    });

    setTimeout(() => {
        toast.style.transform = "translateY(-8px) scale(0.95)";
        toast.style.opacity = "0";
        setTimeout(() => toast.remove(), 250);
    }, opts.duration || 6500);
}
window.showStraddleRejectedToast = showStraddleRejectedToast;

function dismissAllStraddleToasts() {
    const container = document.getElementById("straddleToastContainer");
    if (container) {
        container.innerHTML = "";
    }
    if (window._straddleToastedErrors) {
        window._straddleToastedErrors.clear();
    }
    document.querySelectorAll(".straddle-rejected-toast").forEach(el => el.remove());
    document.querySelectorAll(".straddle-error-banner-row").forEach(el => el.remove());
}
window.dismissAllStraddleToasts = dismissAllStraddleToasts;

function showStraddleSuccessToast(message = "Algo enabled successfully") {
    let container = document.getElementById("straddleToastContainer");
    if (!container) {
        container = document.createElement("div");
        container.id = "straddleToastContainer";
        container.style.cssText = "position:fixed; top:24px; right:24px; z-index:999999; display:flex; flex-direction:column; gap:10px; pointer-events:none; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; max-width:320px; width:100%;";
        document.body.appendChild(container);
    }
    const toast = document.createElement("div");
    toast.style.cssText = `background:linear-gradient(135deg, #059669 0%, #10b981 100%); color:#ffffff; padding:12px 16px; border-radius:8px; box-shadow:0 12px 30px rgba(16,185,129,0.35); pointer-events:auto; transition:all 0.25s cubic-bezier(0.16, 1, 0.3, 1); transform:translateY(-8px) scale(0.95); opacity:0; border:1px solid rgba(255,255,255,0.2); display:flex; justify-content:space-between; align-items:center; font-size:13px; font-weight:700;`;
    toast.innerHTML = `
        <div style="display:flex; align-items:center; gap:8px;">
            <span style="font-size:16px;">✅</span>
            <span>${message}</span>
        </div>
        <button type="button" onclick="this.parentElement.remove()" style="background:transparent; border:none; color:#ffffff; font-size:16px; font-weight:700; cursor:pointer; opacity:0.8;" onmouseover="this.style.opacity='1'" onmouseout="this.style.opacity='0.8'">✕</button>
    `;
    container.appendChild(toast);

    requestAnimationFrame(() => {
        toast.style.transform = "translateY(0) scale(1)";
        toast.style.opacity = "1";
    });

    setTimeout(() => {
        toast.style.transform = "translateY(-8px) scale(0.95)";
        toast.style.opacity = "0";
        setTimeout(() => toast.remove(), 250);
    }, 3500);
}
window.showStraddleSuccessToast = showStraddleSuccessToast;

// =========================================================================
// INITIALIZATION & STATE POLLING
// =========================================================================

async function initStraddlePlatform() {
    if (window._straddleState.pollTimer) {
        clearInterval(window._straddleState.pollTimer);
    }

    // Initialize Date Range for PnL Chart
    const todayStr = new Date().toISOString().split("T")[0];
    const pnlStart = document.getElementById("pnlStartDate");
    const pnlEnd = document.getElementById("pnlEndDate");
    if (pnlStart && !pnlStart.value) pnlStart.value = todayStr;
    if (pnlEnd && !pnlEnd.value) pnlEnd.value = todayStr;

    await fetchStraddleState();
    await loadStraddleOpenPositions();
    fetchLiveSpotTick();
    window._straddleState.spotTimer = setInterval(fetchLiveSpotTick, 1000);
    window._straddleState.pollTimer = setInterval(pollStraddleCycle, 1500);

    // Attach global click listener to close dropdowns
    document.addEventListener("click", function (e) {
        if (!e.target.closest(".straddle-dropdown")) {
            document.querySelectorAll(".straddle-dropdown-menu").forEach(m => m.classList.remove("show"));
        }
    });

    // Attach mouse interaction for P&L Canvas crosshair
    setupPnlCanvasEvents();
}

window._lastKnownSpot = 0.0;

function updateSpotPriceDisplay(spot, atm, und) {
    const spotEl = document.getElementById("straddleSpotPrice");
    const atmEl = document.getElementById("straddleAtmStrike");
    const undLabel = document.getElementById("straddleUnderlyingLabel");
    if (undLabel && und) undLabel.textContent = und + ":";
    if (atmEl && atm) atmEl.textContent = atm;
    if (spotEl && spot && Number(spot) > 0) {
        const numSpot = Number(spot);
        const prev = window._lastKnownSpot;
        spotEl.textContent = "₹" + numSpot.toFixed(2);
        if (prev > 0 && Math.abs(numSpot - prev) > 0.01) {
            const isUp = numSpot > prev;
            spotEl.classList.remove("tick-flash-up", "tick-flash-down");
            spotEl.classList.add(isUp ? "tick-flash-up" : "tick-flash-down");
            setTimeout(() => {
                if (spotEl) spotEl.classList.remove("tick-flash-up", "tick-flash-down");
            }, 450);
        }
        window._lastKnownSpot = numSpot;
    }
}

async function fetchLiveSpotTick() {
    try {
        const und = (window._straddleState.activeStrategy && window._straddleState.activeStrategy.underlying) || "NIFTY";
        const resp = await fetch(`/api/straddle/spot?underlying=${encodeURIComponent(und)}`);
        const data = await resp.json();
        if (data && data.success && data.spot > 0) {
            updateSpotPriceDisplay(data.spot, data.atm, data.underlying);
        }
    } catch (e) {
        // quiet for sub-second interval
    }
}

async function pollStraddleCycle() {
    try {
        await fetchStraddleState();
        if (window._straddleState.currentSubView === "open_positions") {
            await loadStraddleOpenPositions();
        } else if (window._straddleState.currentSubView === "option_chain") {
            if (typeof loadStraddleOptionChain === "function") {
                await loadStraddleOptionChain();
            }
        } else if (window._straddleState.currentSubView === "pnl") {
            if (typeof loadStraddlePnlChart === "function" && !window._straddleState._pnlHoverActive) {
                await loadStraddlePnlChart();
            }
        }
    } catch (err) {
        console.debug("Straddle poll tick note:", err);
    }
}

async function fetchStraddleState() {
    try {
        const resp = await fetch("/api/straddle/state");
        const data = await resp.json();
        if (data && data.success && data.state) {
            const s = data.state;
            window._straddleState.allStrategies = s.all_strategies || [];

            // Determine active strategy strictly matching activeStrategyId or activeStrategy
            const curTargetId = window._straddleState.activeStrategyId || (window._straddleState.activeStrategy ? window._straddleState.activeStrategy.strategy_id : null);
            let active = null;
            if (curTargetId) {
                active = (s.all_strategies || []).find(x => x.strategy_id === curTargetId || x.name === curTargetId);
            }
            if (!active) {
                active = s.active_strategy || (s.all_strategies && s.all_strategies[0]) || null;
            }
            window._straddleState.activeStrategy = active;
            window._straddleState.activeStrategyId = active ? active.strategy_id : null;
            const activePositions = active ? (active.positions || []) : (s.positions || []);
            window._straddleState.positions = activePositions;

            renderStraddleHeader(s);
            updateStraddleStrategyDropdown(s.all_strategies || [], active);

            // Update Terminal positions if in Algo Detail view
            if (window._straddleState.currentSubView === "terminal") {
                renderStraddlePositions(activePositions, active ? (active.group_mtm || 0.0) : 0.0, active ? (active.mtm || 0.0) : 0.0);
            }

            // In-place smart update for Algos view (never wipes focus/open dropdowns)
            if (window._straddleState.currentSubView === "algos") {
                renderStraddleAlgosTable(false);
            }
        }
    } catch (e) {
        console.error("Error polling straddle state:", e);
    }
}

function renderStraddleHeader(state) {
    const activeStrat = state.active_strategy || window._straddleState.activeStrategy || (window._straddleState.allStrategies || []).find(s => s.strategy_id === window._straddleState.activeStrategyId);

    const statusBadge = document.getElementById("straddleStatusBadge");
    if (statusBadge) {
        if (activeStrat) {
            const rawStatus = (activeStrat.status || "").toUpperCase();
            const hasOpen = Array.isArray(activeStrat.positions) && activeStrat.positions.some(p => p.status === "OPEN");
            const isEnabled = activeStrat.enabled !== false;

            if (rawStatus === "TERMINATED") {
                statusBadge.textContent = "Terminated";
                statusBadge.className = "straddle-pill-badge straddle-badge-terminated";
            } else if (rawStatus === "ENTRY_FAILED") {
                statusBadge.textContent = "Entry Failed";
                statusBadge.className = "straddle-pill-badge straddle-badge-terminated";
            } else if (hasOpen) {
                statusBadge.textContent = "Trade Active";
                statusBadge.className = "straddle-pill-badge straddle-badge-active";
            } else if (!isEnabled) {
                statusBadge.textContent = "Stopped";
                statusBadge.className = "straddle-pill-badge straddle-badge-terminated";
            } else if (rawStatus === "TRADE_ACTIVE" || rawStatus === "WAITING_ENTRY") {
                statusBadge.textContent = "Waiting Entry";
                statusBadge.className = "straddle-pill-badge straddle-badge-ready";
            } else {
                statusBadge.textContent = "Ready";
                statusBadge.className = "straddle-pill-badge straddle-badge-ready";
            }
        } else {
            const st = state.engine_status || "READY";
            statusBadge.textContent = (st === "TRADE_ACTIVE" ? "Trade Active" : (st === "READY" ? "Ready" : "Terminated"));
            statusBadge.className = "straddle-pill-badge " + (st === "TRADE_ACTIVE" ? "straddle-badge-active" : (st === "READY" ? "straddle-badge-ready" : "straddle-badge-terminated"));
        }
    }

    const titleEl = document.getElementById("straddleTerminalStrategyTitle");
    if (titleEl && activeStrat) {
        titleEl.textContent = activeStrat.name || activeStrat.strategy_id;
    }

    const modeText = document.getElementById("straddleModeText");
    const modeDot = document.getElementById("straddleModeDot");
    if (modeText && modeDot) {
        modeText.textContent = "⚡ Kotak Neo Live Broker";
        modeDot.style.background = "#10b981";
        modeDot.style.boxShadow = "0 0 8px #10b981";
    }

    updateSpotPriceDisplay(state.underlying_spot, state.underlying_atm_strike, state.underlying_symbol);

    // Top Nav Total MTM
    const navMtm = document.getElementById("straddleNavTotalMtm");
    if (navMtm) {
        const tot = state.total_mtm || 0.0;
        navMtm.textContent = (tot >= 0 ? "+₹ " : "-₹ ") + Math.abs(tot).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        navMtm.style.color = tot >= 0 ? "#10b981" : "#f43f5e";
    }

    // Engine running indicators
    const bOrder = document.getElementById("badgeOrderApi");
    const bFeed = document.getElementById("badgeLiveFeed");
    const bAlgo = document.getElementById("badgeAlgoStatus");
    if (bOrder) bOrder.className = "straddle-running-badge " + (state.order_api_running ? "" : "badge-off");
    if (bFeed) bFeed.className = "straddle-running-badge " + (state.live_feed_running ? "" : "badge-off");
    if (bAlgo) bAlgo.className = "straddle-running-badge " + (state.algo_running ? "" : "badge-off");

    // Start / Enable button visibility
    const btnStart = document.getElementById("btnStraddleStartAlgo");
    if (btnStart) {
        const hasOpen = activeStrat && Array.isArray(activeStrat.positions) && activeStrat.positions.some(p => p.status === "OPEN");
        if (hasOpen) {
            btnStart.textContent = "Algo Active (Running)";
            btnStart.style.opacity = "0.65";
            btnStart.disabled = true;
        } else {
            btnStart.textContent = "▶ Start / Enable Algo";
            btnStart.style.opacity = "1";
            btnStart.disabled = false;
        }
    }
}


// =========================================================================
// SUB-VIEW NAVIGATION (OPEN POSITIONS, ALGOS, ALGO DETAIL / TERMINAL, P&L CHART)
// =========================================================================

function switchStraddleSubView(view) {
    if (view === "algo_detail") {
        view = "terminal";
    }

    const isOrderbook = (view === "orderbook");
    const targetPanel = isOrderbook ? "open_positions" : view;

    window._straddleState.currentSubView = view;

    // Control visibility of all subview containers
    const allPanels = ["open_positions", "algos", "terminal", "pnl"];
    allPanels.forEach(p => {
        const panelEl = document.getElementById(`straddleSubView_${p}`);
        if (panelEl) {
            panelEl.style.display = (p === targetPanel) ? "block" : "none";
        }
    });

    // Update main header tab active indicators
    const mainTabs = ["open_positions", "algos", "pnl", "orderbook"];
    mainTabs.forEach(t => {
        const btn = document.getElementById(`straddleSubTab_${t}`);
        if (btn) {
            const isTabActive = (t === view) || (t === "algos" && view === "terminal");
            btn.classList.toggle("active", isTabActive);
        }
    });

    if (isOrderbook) {
        setStraddlePositionsViewFilter("orders");
    } else if (view === "open_positions") {
        if (window._straddleState.positionsFilter === "orders" || window._straddleState.positionsFilter === "margin") {
            setStraddlePositionsViewFilter("open");
        } else {
            loadStraddleOpenPositions();
        }
    } else if (view === "algos") {
        renderStraddleAlgosTable(true);
    } else if (view === "terminal") {
        // Ensure settings & builder panel is open and visible
        const settingsPanel = document.getElementById("straddleSettingsPanel");
        if (settingsPanel) settingsPanel.style.display = "block";
        if (window._straddleState.activeStrategy) {
            populateStraddleSettingsForm(window._straddleState.activeStrategy);
            renderStraddlePositions(window._straddleState.activeStrategy.positions || window._straddleState.positions || []);
        }
    } else if (view === "pnl") {
        renderPnlEntitiesList();
        if (window._straddleState.pnlDataCache) {
            drawPnlCanvas(window._straddleState.pnlDataCache, window._straddleState.pnlChartType || "line", null);
        }
        loadStraddlePnlChart();
        setTimeout(() => {
            loadStraddlePnlChart();
        }, 120);
    }
}


// =========================================================================
// SUB-VIEW 0: OPEN POSITIONS & ORDERS (UNIFIED LIVE PORTFOLIO)
// =========================================================================

function setStraddlePositionsViewFilter(filter) {
    window._straddleState.positionsFilter = filter;
    document.getElementById("btnPosFilterOpen")?.classList.toggle("active-toggle", filter === "open");
    document.getElementById("btnPosFilterClosed")?.classList.toggle("active-toggle", filter === "closed");
    document.getElementById("btnPosFilterAll")?.classList.toggle("active-toggle", filter === "all");
    document.getElementById("btnPosFilterOrders")?.classList.toggle("active-toggle", filter === "orders");
    document.getElementById("btnPosFilterMargin")?.classList.toggle("active-toggle", filter === "margin");

    const posCard = document.getElementById("straddleOpenPositionsCard") || document.getElementById("straddleOpenPositionsTable")?.closest(".straddle-card");
    const summaryControls = document.getElementById("straddlePositionsSummaryControls");
    const ordersSection = document.getElementById("orderbookOrdersSection");
    const marginSection = document.getElementById("orderbookMarginSection");

    if (filter === "orders") {
        if (posCard) posCard.style.display = "none";
        if (summaryControls) summaryControls.style.display = "none";
        if (marginSection) marginSection.style.display = "none";
        if (ordersSection) ordersSection.style.display = "block";
        loadStraddleOrderbook();
    } else if (filter === "margin") {
        if (posCard) posCard.style.display = "none";
        if (summaryControls) summaryControls.style.display = "none";
        if (ordersSection) ordersSection.style.display = "none";
        if (marginSection) marginSection.style.display = "block";
        loadStraddleMargin();
    } else {
        if (ordersSection) ordersSection.style.display = "none";
        if (marginSection) marginSection.style.display = "none";
        if (summaryControls) summaryControls.style.display = "flex";
        if (posCard) posCard.style.display = "block";
        renderStraddleOpenPositionsTable();
        loadStraddleOpenPositions();
    }
}

async function loadStraddleOpenPositions() {
    try {
        const resp = await fetch("/api/straddle/open_positions");
        const data = await resp.json();
        if (data && data.success) {
            window._straddleState.openPositionsCache = data.open_positions || [];
            window._straddleState.closedPositionsCache = data.closed_positions || [];
            window._straddleState.allPositionsCache = data.all_positions || [];
            window._straddleState.totalOpenMtm = data.total_open_mtm || 0.0;
            window._straddleState.totalRealisedPnl = data.total_realised_pnl || 0.0;
            window._straddleState.totalPortfolioPnl = data.total_portfolio_pnl || 0.0;

            // Update sub-tab counts
            const cntOpen = document.getElementById("posSubCountOpen");
            const cntClosed = document.getElementById("posSubCountClosed");
            const cntAll = document.getElementById("posSubCountAll");
            if (cntOpen) cntOpen.textContent = data.count || 0;
            if (cntClosed) cntClosed.textContent = data.closed_count || 0;
            if (cntAll) cntAll.textContent = data.all_count || 0;

            const countBadge = document.getElementById("openPositionsCountBadge");
            if (countBadge) countBadge.textContent = data.count || 0;

            // Update summary metric badges
            const openMtmEl = document.getElementById("openPositionsTotalMtm");
            if (openMtmEl) {
                const tot = data.total_open_mtm || 0.0;
                openMtmEl.textContent = (tot >= 0 ? "+₹ " : "-₹ ") + Math.abs(tot).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                openMtmEl.style.color = tot >= 0 ? "#10b981" : "#f43f5e";
            }

            const realPnlEl = document.getElementById("positionsRealisedPnl");
            if (realPnlEl) {
                const r = data.total_realised_pnl || 0.0;
                realPnlEl.textContent = (r >= 0 ? "+₹ " : "-₹ ") + Math.abs(r).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                realPnlEl.style.color = r >= 0 ? "#10b981" : "#f43f5e";
            }

            const portPnlEl = document.getElementById("positionsTotalPortfolioPnl");
            if (portPnlEl) {
                const p = data.total_portfolio_pnl || 0.0;
                portPnlEl.textContent = (p >= 0 ? "+₹ " : "-₹ ") + Math.abs(p).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                portPnlEl.style.color = p >= 0 ? "#10b981" : "#f43f5e";
            }

            const brokerText = document.getElementById("openPositionsBrokerText");
            if (brokerText) {
                brokerText.textContent = "⚡ Kotak Neo Live Broker";
            }

            renderStraddleOpenPositionsTable();
        }
    } catch (e) {
        console.error("Error loading open positions:", e);
    }
}

function renderStraddleOpenPositionsTable() {
    const tbody = document.getElementById("straddleOpenPositionsTableBody");
    if (!tbody) return;

    const filter = window._straddleState.positionsFilter || "open";
    let positions = [];
    if (filter === "closed") {
        positions = window._straddleState.closedPositionsCache || [];
    } else if (filter === "all") {
        positions = window._straddleState.allPositionsCache || [];
    } else {
        positions = window._straddleState.openPositionsCache || [];
    }

    const searchVal = (document.getElementById("openPositionsSearch")?.value || "").toLowerCase().trim();

    let filtered = positions.filter(p => {
        if (searchVal) {
            const sym = (p.symbol || p.trading_symbol || "").toLowerCase();
            const src = (p.strategy_name || p.source || "").toLowerCase();
            const prod = (p.product || "").toLowerCase();
            if (!sym.includes(searchVal) && !src.includes(searchVal) && !prod.includes(searchVal)) return false;
        }
        return true;
    });

    if (filtered.length === 0) {
        const emptyLabel = filter === "closed" ? "No Closed Positions Today" : (filter === "all" ? "No Positions Today" : "No Open Positions on Kotak Neo");
        tbody.innerHTML = `
            <tr>
                <td colspan="12" style="text-align:center; padding:36px 14px; color:#64748b;">
                    <div style="font-size:14px; font-weight:700; color:#cbd5e1; margin-bottom:4px;">${emptyLabel}</div>
                    <div style="font-size:12px;">Live trades and positions executed across all algos or direct broker positions will stream here in real-time.</div>
                </td>
            </tr>
        `;
        return;
    }

    let rowsHtml = "";
    const prevMap = window._straddleState.prevLtpMap || {};

    filtered.forEach(p => {
        const isOpen = (p.is_open !== false && (p.net_qty || p.quantity || 0) !== 0);
        const side = (p.side || (p.net_qty > 0 ? "BUY" : (p.net_qty < 0 ? "SELL" : "CLOSED"))).toUpperCase();
        let sideBadgeClass = "side-badge-sell";
        if (side === "BUY") sideBadgeClass = "side-badge-buy";
        else if (side === "CLOSED") sideBadgeClass = "status-badge-closed";

        const sym = p.symbol || p.trading_symbol || "";
        let symAccentClass = "sym-ce";
        if (side === "BUY") {
            symAccentClass = "sym-buy";
        } else if (sym.toUpperCase().includes("PE")) {
            symAccentClass = "sym-pe";
        } else {
            symAccentClass = "sym-ce";
        }

        const netQty = p.net_qty !== undefined ? p.net_qty : (p.quantity || 0);
        const absQty = Math.abs(netQty);
        
        let buyAvg = Number(p.buy_avg || p.buy_price || p.buyAvgPrc || (side === "BUY" ? (p.avg_ent_price || p.avg_price || p.price) : 0) || 0.0);
        let sellAvg = Number(p.sell_avg || p.sell_price || p.sellAvgPrc || (side === "SELL" ? (p.avg_ent_price || p.avg_price || p.price) : 0) || 0.0);

        if (!isOpen) {
            if (p.avg_exit_price && Number(p.avg_exit_price) > 0) {
                if (side === "SELL" || (!buyAvg && sellAvg)) {
                    buyAvg = Number(p.avg_exit_price);
                } else if (side === "BUY" || (!sellAvg && buyAvg)) {
                    sellAvg = Number(p.avg_exit_price);
                }
            }
            if (p.avg_ent_price && Number(p.avg_ent_price) > 0) {
                if (side === "SELL" && !sellAvg) {
                    sellAvg = Number(p.avg_ent_price);
                } else if (side === "BUY" && !buyAvg) {
                    buyAvg = Number(p.avg_ent_price);
                }
            }
        }

        let netAvg = Number(p.avg_price || p.net_avg || (side === "BUY" ? buyAvg : (side === "SELL" ? sellAvg : (buyAvg || sellAvg))) || 0.0);
        if (netAvg <= 0) {
            netAvg = buyAvg > 0 ? buyAvg : sellAvg;
        }

        let ltp = Number(p.ltp || p.last_price || (window._straddleState?.prevLtpMap && window._straddleState.prevLtpMap[sym]) || (window._straddleState?.quotes && window._straddleState.quotes[sym]) || 0.0);
        if (ltp <= 0 && (!isOpen || p.avg_exit_price)) {
            ltp = Number(p.avg_exit_price || p.avg_ent_price || buyAvg || sellAvg || 0.0);
        }
        const prevLtp = prevMap[sym] || ltp;
        let ltpClass = "";
        if (isOpen && ltp > prevLtp) ltpClass = "straddle-ltp-up";
        else if (isOpen && ltp < prevLtp) ltpClass = "straddle-ltp-down";
        prevMap[sym] = ltp;

        const mtm = isOpen ? (p.mtm !== undefined ? p.mtm : (p.pnl || 0.0)) : (p.realised_pnl !== undefined ? p.realised_pnl : (p.pnl || 0.0));
        const mtmClass = mtm >= 0 ? "straddle-mtm-pos" : "straddle-mtm-neg";
        const mtmFormatted = (mtm >= 0 ? "+₹ " : "-₹ ") + Math.abs(mtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const src = p.strategy_name || p.source || "Kotak Neo Direct / Manual";

        const isReEntry = !!(p.is_re_entry || p.re_entry || (p.re_execute_count && p.re_execute_count > 0));
        const statusPill = isOpen
            ? `<div class="straddle-status-container">
                    <span class="straddle-status-pill status-badge-open">LIVE OPEN</span>
                    ${isReEntry ? '<span class="status-badge-reentry">RE-ENTRY</span>' : ''}
               </div>`
            : `<div class="straddle-status-container">
                    <span class="straddle-status-pill status-badge-closed">CLOSED</span>
                    ${isReEntry ? '<span class="status-badge-reentry">RE-ENTRY</span>' : ''}
               </div>`;

        const actionBtn = isOpen
            ? `<div style="display:flex; align-items:center; gap:4px; justify-content:center;">
                 ${(!p.strategy_id || p.source === 'Kotak Neo Direct / Manual' || (p.strategy_name && p.strategy_name.includes('Kotak Neo Direct')) || !p.strategy_name) ?
                   `<button type="button" class="straddle-btn" style="background:linear-gradient(135deg, #0284c7, #2563eb); color:#fff; padding:3px 8px; font-size:10px; font-weight:800; border:none; border-radius:4px; cursor:pointer;" title="Add this live broker trade to an algo strategy" onclick="openAdoptBrokerPositionModal()">📥 Add to Algo</button>` : ''}
                 <button type="button" class="straddle-btn btn-danger" style="padding:3px 8px; font-size:11px; font-weight:800;" onclick="squareOffSingleOpenPosition('${p.symbol}', '${p.instrument_key}', '${p.product}', ${absQty}, '${side}')">Exit</button>
               </div>`
            : `<span style="color:#64748b; font-size:11px;">-</span>`;

        rowsHtml += `
            <tr>
                <td>${statusPill}</td>
                <td><span class="straddle-side-badge ${sideBadgeClass}">${side}</span></td>
                <td>
                    <div class="straddle-sym-card ${symAccentClass}">
                        <div class="straddle-sym-title">${p.symbol || p.trading_symbol}</div>
                        <div class="straddle-sym-sub">${p.instrument_key || ''}</div>
                    </div>
                </td>
                <td>
                    <span style="background:#1e293b; color:#94a3b8; font-size:10.5px; padding:2px 6px; border-radius:4px; font-weight:700;">${p.product || 'NRML'}</span>
                </td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:800; color:#f8fafc;">
                    ${isOpen ? `${absQty} (${netQty > 0 ? '+'+netQty : netQty})` : '0 (Squared)'}
                </td>
                <td style="text-align:right; font-family:var(--font-mono); color:#cbd5e1;">
                    ${buyAvg > 0 ? '₹' + buyAvg.toFixed(2) : '-'}
                </td>
                <td style="text-align:right; font-family:var(--font-mono); color:#cbd5e1;">
                    ${sellAvg > 0 ? '₹' + sellAvg.toFixed(2) : '-'}
                </td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:700; color:#60a5fa;">
                    ${netAvg > 0 ? '₹' + netAvg.toFixed(2) : '-'}
                </td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:700;">
                    <span class="straddle-ltp ${ltpClass}">${ltp > 0 ? '₹' + ltp.toFixed(2) : '-'}</span>
                </td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:800;">
                    <span class="straddle-mtm ${mtmClass}">${mtmFormatted}</span>
                </td>
                <td>
                    <div style="font-size:11.5px; font-weight:700; color:#93c5fd;">${src}</div>
                </td>
                <td style="text-align:center;">${actionBtn}</td>
            </tr>
        `;
    });

    tbody.innerHTML = rowsHtml;
}

async function squareOffSingleOpenPosition(symbol, instrumentKey, product, quantity, side) {
    if (!confirm(`Are you sure you want to square off open position for ${symbol} (Qty: ${quantity})?`)) return;
    showStraddleToast(`Exiting ${symbol} (${quantity} Qty)...`, "info", 1500);
    try {
        const resp = await fetch("/api/straddle/square_off_open_position", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                symbol: symbol,
                instrument_key: instrumentKey,
                product: product,
                quantity: quantity,
                side: side
            })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`✅ ${symbol} squared off successfully.`, "success", 2500);
            await loadStraddleOpenPositions();
            await fetchStraddleState();
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}

async function confirmSquareOffAllOpenPositions() {
    if (!confirm("⚠️ EMERGENCY: Are you sure you want to square off ALL open positions across Kotak Neo and terminate active algos?")) return;
    showStraddleToast("⚠️ Emergency square-off of all open positions in progress...", "warning", 2000);
    try {
        const resp = await fetch("/api/straddle/square_off_all_open_positions", {
            method: "POST",
            headers: { "Content-Type": "application/json" }
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("✅ Successfully squared off all open positions.", "success", 3000);
            await loadStraddleOpenPositions();
            await fetchStraddleState();
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}


// =========================================================================
// =========================================================================
// SUB-VIEW 1: ALGOS MASTER VIEW (WORKING DAYS COLUMN & STRATEGY TABLE)
// =========================================================================

window._selectedAlgoIds = window._selectedAlgoIds || new Set();

function onToggleAlgoSelect(strategyId, isChecked) {
    if (isChecked) {
        window._selectedAlgoIds.add(strategyId);
    } else {
        window._selectedAlgoIds.delete(strategyId);
    }
    updateBulkActionsBar();
    updateMasterCheckboxState();
}

function toggleSelectAllStraddleAlgos(masterCb) {
    const strategies = window._straddleState?.allStrategies || [];
    if (masterCb && masterCb.checked) {
        strategies.forEach(s => {
            if (s && s.strategy_id) window._selectedAlgoIds.add(s.strategy_id);
        });
    } else {
        window._selectedAlgoIds.clear();
    }
    renderStraddleAlgosTable();
}

function clearAllSelectedAlgos() {
    window._selectedAlgoIds.clear();
    renderStraddleAlgosTable();
}

function updateMasterCheckboxState() {
    const masterCb = document.getElementById("straddleSelectAllAlgos");
    if (!masterCb) return;
    const strategies = window._straddleState?.allStrategies || [];
    if (strategies.length === 0) {
        masterCb.checked = false;
        masterCb.indeterminate = false;
        return;
    }
    const allChecked = strategies.every(s => window._selectedAlgoIds.has(s.strategy_id));
    const someChecked = strategies.some(s => window._selectedAlgoIds.has(s.strategy_id));
    masterCb.checked = allChecked;
    masterCb.indeterminate = (!allChecked && someChecked);
}

function updateBulkActionsBar() {
    const bar = document.getElementById("straddleAlgosBulkActions");
    const countEl = document.getElementById("straddleBulkSelectedCount");
    const count = window._selectedAlgoIds ? window._selectedAlgoIds.size : 0;
    if (!bar) return;
    if (count > 0) {
        bar.style.display = "flex";
        if (countEl) countEl.textContent = `${count} algo${count > 1 ? 's' : ''} selected`;
    } else {
        bar.style.display = "none";
    }
}

async function bulkToggleSelectedAlgos(enabled) {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    try {
        const resp = await fetch("/api/straddle/strategy/batch_toggle_enable", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids, enabled: enabled })
        });
        const res = await resp.json();
        if (res.success) {
            await fetchStraddleState();
            renderStraddleAlgosTable();
        } else {
            alert("Error: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}

async function bulkDuplicateSelectedAlgos() {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    try {
        const resp = await fetch("/api/straddle/strategy/batch_duplicate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids })
        });
        const res = await resp.json();
        if (res.success) {
            window._selectedAlgoIds.clear();
            await fetchStraddleState();
            renderStraddleAlgosTable();
        } else {
            alert("Error: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}

async function bulkDeleteSelectedAlgos() {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    if (!confirm(`Are you sure you want to delete ${ids.length} selected algorithm(s) permanently?`)) return;
    try {
        const resp = await fetch("/api/straddle/strategy/batch_delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids })
        });
        const res = await resp.json();
        if (res.success) {
            window._selectedAlgoIds.clear();
            await fetchStraddleState();
            renderStraddleAlgosTable();
        } else {
            alert("Error: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}

async function bulkSetDaysSelectedAlgos(days) {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    try {
        const resp = await fetch("/api/straddle/strategy/batch_set_days", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids, days: days || ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"] })
        });
        const res = await resp.json();
        if (res.success) {
            await fetchStraddleState();
            renderStraddleAlgosTable();
        } else {
            alert("Error: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}

async function bulkResetSelectedAlgos() {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    if (!confirm(`Are you sure you want to reset ${ids.length} selected algorithm(s) to fresh clean READY state?`)) return;

    // Instant optimistic reset
    ids.forEach(id => {
        const s = (window._straddleState.allStrategies || []).find(st => st.strategy_id === id || st.name === id);
        if (s) {
            s.positions = [];
            s.status = "READY";
            s.mtm = 0.0;
            s.entry_error = "-";
            s.exit_error = "-";
            s.last_error = null;
            s.enabled = false;
        }
    });
    if (typeof dismissAllStraddleToasts === "function") dismissAllStraddleToasts();
    renderStraddleAlgosTable(true);
    showStraddleToast(`🔄 Resetting ${ids.length} algos...`, "info", 1500);

    try {
        const resp = await fetch("/api/straddle/strategy/batch_reset", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids })
        });
        const res = await resp.json();
        if (res.success) {
            window._selectedAlgoIds.clear();
            showStraddleSuccessToast("Algos Reset", `${ids.length} algorithm(s) reset to fresh clean READY state.`);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddleAlgosTable(true);
        } else {
            alert("Error: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}
window.bulkResetSelectedAlgos = bulkResetSelectedAlgos;

function renderStraddleAlgosTable(forceRebuild = false) {
    const tbody = document.getElementById("straddleAlgosTableBody");
    if (!tbody) return;

    const strategies = window._straddleState.allStrategies || [];
    const searchVal = (document.getElementById("straddleAlgosSearch")?.value || "").toLowerCase().trim();

    // Filter strategies
    let filtered = strategies.filter(s => {
        if (searchVal) {
            const name = (s.name || s.strategy_id).toLowerCase();
            const und = (s.underlying || "NIFTY").toLowerCase();
            const status = (s.status || "Ready").toLowerCase();
            if (!name.includes(searchVal) && !und.includes(searchVal) && !status.includes(searchVal)) return false;
        }
        return true;
    });

    // Calculate total MTM of filtered
    let totalMtm = 0;
    strategies.forEach(s => { totalMtm += (s.mtm || 0.0); });
    const totMtmEl = document.getElementById("straddleAlgosTotalMtm");
    if (totMtmEl) {
        totMtmEl.textContent = (totalMtm >= 0 ? "+₹ " : "-₹ ") + Math.abs(totalMtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        totMtmEl.style.color = totalMtm >= 0 ? "#10b981" : "#f43f5e";
    }

    updateMasterCheckboxState();
    updateBulkActionsBar();

    if (filtered.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="11" style="text-align:center; padding:36px 14px; color:#64748b;">
                    <div style="font-size:14px; font-weight:700; color:#cbd5e1; margin-bottom:4px;">No Algorithmic Strategies Found</div>
                    <div style="font-size:12px;">Click <b>+ Add Algo</b> above to instantiate a Straddle / Strangle strategy.</div>
                </td>
            </tr>
        `;
        return;
    }

    // Check if we can do an in-place update without rebuilding the DOM
    const existingRows = tbody.querySelectorAll("tr[data-strategy-id]");
    const activeEl = document.activeElement;
    const isUserInteracting = activeEl && (tbody.contains(activeEl) || activeEl.id === "straddleAlgosSearch");
    const hasOpenDropdown = !!document.querySelector(".straddle-dropdown-menu.show");

    if (!forceRebuild && existingRows.length === filtered.length) {
        let allMatch = true;
        for (let i = 0; i < filtered.length; i++) {
            if (existingRows[i].getAttribute("data-strategy-id") !== filtered[i].strategy_id) {
                allMatch = false;
                break;
            }
        }
        if (allMatch) {
            // Update live numbers in place without touching DOM hierarchy
            filtered.forEach(s => {
                const sid = s.strategy_id;
                const rawStatus = (s.status || "").toUpperCase();
                const hasOpen = Array.isArray(s.positions) && s.positions.some(p => p.status === "OPEN");
                const hasError = (s.positions && Array.isArray(s.positions)) ? s.positions.some(p => p.status === "ERROR") : false;

                let isEnabled = s.enabled !== false;
                let displayStatus = "Ready";
                let statusClass = "straddle-badge-ready";
                let dotColor = "#38bdf8";

                if (rawStatus === "TERMINATED") {
                    displayStatus = "Terminated";
                    statusClass = "straddle-badge-terminated";
                    dotColor = "#ef4444";
                    isEnabled = false;
                } else if (rawStatus === "ENTRY_FAILED" || (!hasOpen && isEnabled && hasError)) {
                    displayStatus = "Entry Failed";
                    statusClass = "straddle-badge-terminated";
                    dotColor = "#f43f5e";
                    isEnabled = false;
                } else if (hasOpen) {
                    displayStatus = "Trade Active";
                    statusClass = "straddle-badge-active";
                    dotColor = "#10b981";
                    isEnabled = true;
                } else if (!isEnabled) {
                    displayStatus = "Stopped";
                    statusClass = "straddle-badge-terminated";
                    dotColor = "#64748b";
                    isEnabled = false;
                } else if (rawStatus === "TRADE_ACTIVE" || rawStatus === "WAITING_ENTRY") {
                    displayStatus = "Waiting Entry";
                    statusClass = "straddle-badge-ready";
                    dotColor = "#38bdf8";
                    isEnabled = true;
                } else {
                    displayStatus = "Ready";
                    statusClass = "straddle-badge-ready";
                    dotColor = "#38bdf8";
                    isEnabled = true;
                }

                const switchInput = document.querySelector(`input[data-enable-sid="${sid}"]`);
                if (switchInput && switchInput !== document.activeElement) {
                    switchInput.checked = isEnabled;
                }

                const mtm = s.mtm || 0.0;
                const mtmFormatted = (mtm >= 0 ? "+₹ " : "-₹ ") + Math.abs(mtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });

                const mtmCell = document.getElementById(`algo_mtm_${sid}`);
                if (mtmCell) {
                    mtmCell.textContent = mtmFormatted;
                    mtmCell.style.color = mtm >= 0 ? "#10b981" : "#f43f5e";
                }

                const statusPill = document.getElementById(`algo_status_pill_${sid}`);
                if (statusPill) {
                    statusPill.textContent = displayStatus;
                    statusPill.className = `straddle-pill-badge ${statusClass}`;
                }

                const dot = document.getElementById(`algo_dot_${sid}`);
                if (dot) {
                    dot.style.color = dotColor;
                }

                const errCell = document.getElementById(`algo_entry_err_${sid}`);
                if (errCell) {
                    const err = s.entry_error || "-";
                    errCell.textContent = err;
                    errCell.style.color = (err !== "-" && !err.toLowerCase().includes("waiting")) ? "#f43f5e" : "#64748b";
                }

                const errBadgeEl = document.getElementById(`algo_err_badge_${sid}`);
                const errCount = (s.positions && Array.isArray(s.positions)) ? s.positions.filter(p => p.status === "ERROR").length : ((s.entry_error && s.entry_error !== "-" && !s.entry_error.toLowerCase().includes("waiting")) ? 1 : 0);
                if (errBadgeEl) {
                    if (errCount > 0) {
                        errBadgeEl.style.display = "inline-flex";
                        errBadgeEl.innerHTML = `<span style="font-size:12px;">⚠️</span><span class="algo-error-counter">${errCount}</span>`;
                    } else {
                        errBadgeEl.style.display = "none";
                    }
                }
            });
            return;
        }
    }

    // If user is actively typing or a dropdown is open, delay full rebuild to avoid losing focus
    if (!forceRebuild && (isUserInteracting || hasOpenDropdown)) {
        return;
    }

    const dayLabels = [
        { key: "Monday", short: "Mon" },
        { key: "Tuesday", short: "Tue" },
        { key: "Wednesday", short: "Wed" },
        { key: "Thursday", short: "Thu" },
        { key: "Friday", short: "Fri" },
        { key: "Saturday", short: "Sat" },
        { key: "Sunday", short: "Sun" },
    ];

    let rowsHtml = "";
    filtered.forEach(s => {
        const isSelected = window._selectedAlgoIds.has(s.strategy_id);
        const rawStatus = (s.status || "").toUpperCase();
        const hasOpen = Array.isArray(s.positions) && s.positions.some(p => p.status === "OPEN");
        const hasError = (s.positions && Array.isArray(s.positions)) ? s.positions.filter(p => p.status === "ERROR") : false;
        const errCount = (s.positions && Array.isArray(s.positions)) ? s.positions.filter(p => p.status === "ERROR").length : ((s.entry_error && s.entry_error !== "-" && !s.entry_error.toLowerCase().includes("waiting")) ? 1 : 0);

        let isEnabled = s.enabled !== false;
        let displayStatus = "Ready";
        let statusClass = "straddle-badge-ready";
        let dotColor = "#38bdf8";

        if (rawStatus === "TERMINATED") {
            displayStatus = "Terminated";
            statusClass = "straddle-badge-terminated";
            dotColor = "#ef4444";
            isEnabled = false;
        } else if (rawStatus === "ENTRY_FAILED" || (!hasOpen && isEnabled && hasError)) {
            displayStatus = "Entry Failed";
            statusClass = "straddle-badge-terminated";
            dotColor = "#f43f5e";
            isEnabled = false;
        } else if (hasOpen) {
            displayStatus = "Trade Active";
            statusClass = "straddle-badge-active";
            dotColor = "#10b981";
            isEnabled = true;
        } else if (!isEnabled) {
            displayStatus = "Stopped";
            statusClass = "straddle-badge-terminated";
            dotColor = "#64748b";
            isEnabled = false;
        } else if (rawStatus === "TRADE_ACTIVE" || rawStatus === "WAITING_ENTRY") {
            displayStatus = "Waiting Entry";
            statusClass = "straddle-badge-ready";
            dotColor = "#38bdf8";
            isEnabled = true;
        } else {
            displayStatus = "Ready";
            statusClass = "straddle-badge-ready";
            dotColor = "#38bdf8";
            isEnabled = true;
        }

        const multiplier = s.multiplier || 1;
        const mtm = s.mtm || 0.0;
        const mtmClass = mtm >= 0 ? "color:#10b981;" : "color:#f43f5e;";
        const mtmFormatted = (mtm >= 0 ? "+₹ " : "-₹ ") + Math.abs(mtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        const name = s.name || s.strategy_id;
        const startTime = s.start_time || "09:15:56";
        const endTime = s.end_time || "15:29:50";
        const schedDays = s.schedule_days || ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
        const entryErr = s.entry_error || "-";
        const isErrActive = (entryErr !== "-" && !entryErr.toLowerCase().includes("waiting"));

        // Build interactive day selector badges
        let daysHtml = `<div style="display:inline-flex; gap:3px; flex-wrap:wrap;">`;
        dayLabels.forEach(d => {
            const isDayActive = schedDays.includes(d.key);
            const badgeBg = isDayActive ? "#2563eb" : "#1e293b";
            const badgeColor = isDayActive ? "#ffffff" : "#64748b";
            const border = isDayActive ? "1px solid #3b82f6" : "1px solid rgba(255,255,255,0.06)";
            daysHtml += `
                <button type="button" onclick="toggleAlgoDay('${s.strategy_id}', '${d.key}')" style="background:${badgeBg}; color:${badgeColor}; border:${border}; border-radius:4px; padding:1px 5px; font-size:10px; font-weight:700; cursor:pointer;" title="Click to Toggle ${d.key}">
                    ${d.short}
                </button>
            `;
        });
        daysHtml += `</div>`;

        rowsHtml += `
            <tr data-strategy-id="${s.strategy_id}" style="${isSelected ? 'background:rgba(59,130,246,0.08);' : ''}">
                <td style="text-align:center;">
                    <input type="checkbox" class="straddle-algo-checkbox" data-id="${s.strategy_id}" ${isSelected ? "checked" : ""} onchange="onToggleAlgoSelect('${s.strategy_id}', this.checked)">
                </td>
                <td>
                    <label class="quantiply-switch" title="Toggle Auto Execution">
                        <input type="checkbox" data-enable-sid="${s.strategy_id}" ${isEnabled ? "checked" : ""} onchange="onToggleAlgoEnable('${s.strategy_id}', this.checked)">
                        <span class="quantiply-slider"></span>
                    </label>
                </td>
                <td>
                    <span id="algo_status_pill_${s.strategy_id}" class="straddle-pill-badge ${statusClass}" style="font-size:10.5px; padding:2px 8px;">${displayStatus}</span>
                </td>
                <td>
                    ${daysHtml}
                </td>
                <td style="text-align:center;">
                    <input type="number" class="straddle-input" style="width:50px; text-align:center; padding:2px 4px; font-weight:700;" value="${multiplier}" min="1" max="100" onchange="onChangeAlgoMultiplier('${s.strategy_id}', this.value)">
                </td>
                <td id="algo_mtm_${s.strategy_id}" style="text-align:right; font-family:var(--font-mono); font-weight:800; ${mtmClass}">
                    ${mtmFormatted}
                </td>
                <td>
                    <div style="display:flex; align-items:center; gap:6px;">
                        <span id="algo_err_badge_${s.strategy_id}" style="display:${errCount > 0 ? 'inline-flex' : 'none'}; align-items:center; gap:4px; color:#f87171; cursor:pointer;" title="Click to inspect and retry error" onclick="openStrategyInTerminal('${s.strategy_id}')">
                            <span style="font-size:12px;">⚠️</span><span class="algo-error-counter">${errCount}</span>
                        </span>
                        <span id="algo_dot_${s.strategy_id}" style="color:${dotColor}; font-size:8px;">●</span>
                        <a href="javascript:void(0)" onclick="openStrategyInTerminal('${s.strategy_id}')" style="color:#60a5fa; font-weight:700; font-size:13px; text-decoration:none; cursor:pointer;" title="Click to open Builder & Live Legs" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">${name}</a>
                        <div class="straddle-dropdown" style="display:inline-block; margin-left:4px;">
                            <button type="button" class="algo-btn-menu" title="Strategy Options" onclick="toggleDropdownMenu('dd_${s.strategy_id}', event)" style="padding:1px 6px; font-size:13px; line-height:1; height:20px; border-radius:4px; background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.12); color:#cbd5e1; cursor:pointer;">⋮</button>
                            <div id="dd_${s.strategy_id}" class="straddle-dropdown-menu">
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="openRenameStraddleAlgoModal('${s.strategy_id}', '${name}')">✏️ Rename Strategy</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="duplicateStraddleAlgo('${s.strategy_id}')">📑 Duplicate Preset</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="openStrategyInTerminal('${s.strategy_id}')">⚡ Builder & Live Legs</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="straddleResetStrategyDirect('${s.strategy_id}')">🔄 Reset Strategy (Clean Start)</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="restartOrRetryStraddleAlgo('${s.strategy_id}')">🔄 Retry Failed Orders</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item danger" onclick="straddleSquareOffStrategyDirect('${s.strategy_id}')">🚨 Square Off & Terminate</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item danger" onclick="deleteStraddleAlgo('${s.strategy_id}')">🗑️ Delete Strategy</a>
                            </div>
                        </div>
                    </div>
                </td>
                <td id="algo_entry_err_${s.strategy_id}" style="color:${isErrActive ? '#f43f5e' : '#64748b'}; font-size:11px; font-weight:${isErrActive ? '600' : '400'}; max-width:220px; min-width:120px; white-space:normal; word-break:break-word; line-height:1.3;" title="${entryErr}">${entryErr}</td>
                <td style="color:#64748b; font-size:11px; max-width:220px; min-width:120px; white-space:normal; word-break:break-word; line-height:1.3;">${s.exit_error || '-'}</td>
                <td style="font-family:var(--font-mono); font-size:11px; color:#cbd5e1; white-space:nowrap;">${startTime}</td>
                <td style="font-family:var(--font-mono); font-size:11px; color:#cbd5e1; white-space:nowrap;">${endTime}</td>
            </tr>
        `;
    });

    tbody.innerHTML = rowsHtml;
}

async function restartOrRetryStraddleAlgo(strategyId) {
    const strat = (window._straddleState.allStrategies || []).find(s => s.strategy_id === strategyId);
    const name = strat ? strat.name : strategyId;
    const hasErrors = strat && Array.isArray(strat.positions) && strat.positions.some(p => p.status === "ERROR");

    if (hasErrors) {
        showStraddleToast(`🔄 Retrying failed orders for '${name}'...`, "info", 2000);
        try {
            const resp = await fetch("/api/straddle/retry_legs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ strategy_id: strategyId })
            });
            const res = await resp.json();
            if (res.success) {
                showStraddleSuccessToast(`Algo '${name}' enabled successfully`);
            } else {
                showStraddleRejectedToast({
                    symbol: strat?.legs?.[0]?.symbol || "NIFTY OPTION",
                    error_message: res.error || "Order placement rejected by broker",
                    client_id: "Kotak Neo"
                });
            }
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } catch (e) {
            showStraddleToast("Error: " + e.message, "error", 3000);
        }
    } else {
        await startStraddleAlgoDirect(strategyId);
    }
}

async function toggleAlgoDay(strategyId, day) {
    const strat = window._straddleState.allStrategies.find(s => s.strategy_id === strategyId);
    if (!strat) return;

    let days = strat.schedule_days ? [...strat.schedule_days] : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    if (days.includes(day)) {
        days = days.filter(d => d !== day);
    } else {
        days.push(day);
    }
    strat.schedule_days = days;
    renderStraddleAlgosTable(true);

    try {
        await fetch("/api/straddle/strategy/set_days", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId, days: days })
        });
    } catch (e) {
        console.error("Error saving strategy days:", e);
    }
}

function toggleDropdownMenu(id, evt) {
    if (evt) evt.stopPropagation();
    const target = document.getElementById(id);
    const isShown = target && target.classList.contains("show");
    
    // Close all open dropdowns first
    document.querySelectorAll(".straddle-dropdown-menu").forEach(m => m.classList.remove("show"));
    
    if (target && !isShown) {
        const btn = evt ? (evt.currentTarget || evt.target) : target.previousElementSibling;
        if (btn && btn.getBoundingClientRect) {
            const rect = btn.getBoundingClientRect();
            target.style.position = "fixed";
            target.style.zIndex = "999999";
            
            // Check vertical positioning
            const menuHeight = 250;
            const spaceBelow = window.innerHeight - rect.bottom;
            if (spaceBelow < menuHeight && rect.top > menuHeight) {
                target.style.top = `${Math.max(10, rect.top - menuHeight)}px`;
            } else {
                target.style.top = `${rect.bottom + 4}px`;
            }
            
            // Check horizontal positioning
            const menuWidth = 190;
            const spaceRight = window.innerWidth - rect.left;
            if (spaceRight < menuWidth) {
                target.style.left = `${Math.max(10, rect.right - menuWidth)}px`;
            } else {
                target.style.left = `${rect.left}px`;
            }
            target.style.right = "auto";
        }
        target.classList.add("show");
    }
}

async function onToggleAlgoEnable(strategyId, isChecked) {
    const strat = (window._straddleState.allStrategies || []).find(s => s.strategy_id === strategyId);
    if (strat) {
        strat.enabled = isChecked;
        if (isChecked) {
            strat.status = "READY";
            strat.entry_error = "-";
        } else {
            strat.status = "STOPPED";
        }
        renderStraddleAlgosTable(true);
        if (window._straddleState.activeStrategyId === strategyId) {
            renderStraddleHeader(window._straddleState);
        }
    }
    showStraddleToast(`Strategy '${strat ? strat.name : strategyId}' ${isChecked ? 'Enabled' : 'Disabled'}`, isChecked ? "success" : "info", 1800);
    
    try {
        const resp = await fetch("/api/straddle/strategy/toggle_enable", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId, enabled: isChecked })
        });
        const res = await resp.json();
        if (res.success) {
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            renderStraddleHeader(window._straddleState);
        }
    } catch (e) {
        console.error("Error toggling algo enable:", e);
    }
}

async function onChangeAlgoMultiplier(strategyId, val) {
    const num = parseInt(val) || 1;
    const strat = window._straddleState.allStrategies.find(s => s.strategy_id === strategyId);
    if (strat) {
        strat.multiplier = num;
        showStraddleToast(`Multiplier set to ${num}x for '${strat.name}'`, "info", 1500);
        try {
            await fetch("/api/straddle/strategy/save", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ strategy: strat })
            });
            await fetchStraddleState();
        } catch (e) {
            console.error("Error updating multiplier:", e);
        }
    }
}

function openStrategyInTerminal(strategyId) {
    const strat = (window._straddleState.allStrategies || []).find(s => s.strategy_id === strategyId);
    if (strat) {
        window._straddleState.activeStrategy = strat;
        window._straddleState.activeStrategyId = strat.strategy_id;
    }
    
    // Switch view to terminal / algo detail
    switchStraddleSubView("terminal");
    
    // Populate the strategy settings builder form
    if (strat) {
        populateStraddleSettingsForm(strat);
        renderStraddlePositions(strat.positions || [], strat.group_mtm || 0.0, strat.mtm || 0.0);
    }
    
    // Asynchronously notify backend of active strategy
    onStraddleStrategySelect(strategyId);
    
    // Smooth scroll to top of page
    window.scrollTo({ top: 0, behavior: "smooth" });
}

async function saveStraddleStrategyAndGoBack() {
    await saveStraddleStrategySettings();
    switchStraddleSubView("algos");
}

async function startStraddleAlgoDirect(strategyId) {
    const strat = (window._straddleState.allStrategies || []).find(s => s.strategy_id === strategyId);
    const name = strat ? strat.name : strategyId;
    
    // Instant optimistic UI feedback
    if (strat) {
        strat.status = "TRADE_ACTIVE";
        strat.enabled = true;
        strat.entry_error = "Punching orders...";
        renderStraddleAlgosTable(true);
    }
    showStraddleToast(`🚀 Dispatching orders for '${name}'...`, "info", 2500);

    try {
        const resp = await fetch("/api/straddle/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId, force_fresh: true })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`✅ '${name}' started & orders punched!`, "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Notice: " + (res.error || "Order execution triggered"), "warning", 3500);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}

async function straddleResetStrategyDirect(strategyId) {
    const strat = (window._straddleState.allStrategies || []).find(s => s.strategy_id === strategyId || s.name === strategyId);
    const targetSid = strat ? strat.strategy_id : strategyId;
    const name = strat ? strat.name : strategyId;

    // Instant optimistic reset across all state caches matching by id or name
    (window._straddleState.allStrategies || []).forEach(s => {
        if (s.strategy_id === targetSid || s.name === name || s.name === targetSid || s.strategy_id === strategyId) {
            s.positions = [];
            s.mtm = 0.0;
            s.max_mtm = 0.0;
            s.min_mtm = 0.0;
            s.status = "READY";
            s.enabled = false;
            s.entry_error = "-";
            s.exit_error = "-";
            s.last_error = null;
            s.repair_needed = false;
            if (Array.isArray(s.legs)) {
                s.legs.forEach(l => {
                    l.error = null;
                    l.error_message = null;
                    l.status = "READY";
                    l.order_status = null;
                    l.entry_order_id = null;
                    l.exit_order_id = null;
                    l.entry_price = null;
                    l.exit_price = null;
                    l.repair_needed = false;
                });
            }
        }
    });

    // Dismiss any error toast notifications & banner rows immediately
    if (typeof dismissAllStraddleToasts === "function") {
        dismissAllStraddleToasts();
    } else {
        if (window._straddleToastedErrors) window._straddleToastedErrors.clear();
        document.querySelectorAll(".straddle-rejected-toast").forEach(el => el.remove());
        document.querySelectorAll(".straddle-error-banner-row").forEach(el => el.remove());
    }

    window._straddleSelectedLegIndices.clear();
    window._straddleSelectedLegIds.clear();

    if (window._straddleState && (window._straddleState.activeStrategyId === targetSid || window._straddleState.activeStrategy?.strategy_id === targetSid || window._straddleState.activeStrategy?.name === name || window._straddleState.activeStrategyId === strategyId)) {
        if (window._straddleState.activeStrategy) {
            window._straddleState.activeStrategy.positions = [];
            window._straddleState.activeStrategy.status = "READY";
            window._straddleState.activeStrategy.entry_error = "-";
            window._straddleState.activeStrategy.exit_error = "-";
            window._straddleState.activeStrategy.last_error = null;
            window._straddleState.activeStrategy.repair_needed = false;
            window._straddleState.activeStrategy.mtm = 0.0;
            window._straddleState.activeStrategy.enabled = false;
        }
        window._straddleState.positions = [];
        window._straddleState.status = "READY";
        window._straddleState.entry_error = "-";
        renderStraddlePositions([], 0.0, 0.0);
        renderStraddleHeader(window._straddleState);
    }
    
    // Re-render table immediately to wipe error badge and Retry button
    renderStraddleAlgosTable(true);

    showStraddleToast(`🔄 Resetting '${name}'...`, "info", 1500);

    try {
        const resp = await fetch("/api/straddle/reset_algo", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: targetSid, name: name, strategy_name: name, algo_name: name })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleSuccessToast("Algo Reset", `'${name}' reset to fresh clean READY state.`);
            if (res.strategy) {
                const updated = res.strategy;
                const idx = (window._straddleState.allStrategies || []).findIndex(s => s.strategy_id === updated.strategy_id || s.name === updated.name);
                if (idx >= 0) window._straddleState.allStrategies[idx] = updated;
                if (window._straddleState.activeStrategyId === updated.strategy_id || window._straddleState.activeStrategy?.name === updated.name) {
                    window._straddleState.activeStrategy = updated;
                    window._straddleState.positions = updated.positions || [];
                }
            }
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            if (window._straddleState && (window._straddleState.activeStrategyId === targetSid || window._straddleState.activeStrategy?.strategy_id === targetSid)) {
                renderStraddlePositions(window._straddleState.activeStrategy?.positions || [], 0.0, 0.0);
                renderStraddleHeader(window._straddleState);
            }
            renderStraddleAlgosTable(true);
            onStraddlePositionCheckboxChange();
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3000);
    }
}
window.straddleResetStrategyDirect = straddleResetStrategyDirect;

async function straddleSquareOffStrategyDirect(strategyId) {
    const strat = (window._straddleState.allStrategies || []).find(s => s.strategy_id === strategyId);
    const name = strat ? strat.name : strategyId;
    if (!confirm(`Square off all open positions for '${name}' and terminate this strategy?`)) return;

    showStraddleToast(`🚨 Squaring off '${name}'...`, "warning", 2000);
    if (strat) {
        strat.status = "TERMINATED";
        strat.enabled = false;
        renderStraddleAlgosTable(true);
        if (window._straddleState.activeStrategyId === strategyId) {
            renderStraddleHeader(window._straddleState);
        }
    }

    try {
        const resp = await fetch("/api/straddle/strategy/square_off", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`✅ '${name}' squared off & terminated.`, "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddleAlgosTable(true);
            renderStraddleHeader(window._straddleState);
        } else {
            showStraddleToast("Notice: " + (res.error || "Orders dispatched"), "warning", 3000);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3000);
    }
}
window.straddleSquareOffStrategyDirect = straddleSquareOffStrategyDirect;

async function duplicateStraddleAlgo(strategyId) {
    showStraddleToast("📑 Duplicating strategy...", "info", 1500);
    try {
        const resp = await fetch("/api/straddle/strategy/duplicate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("✅ Strategy duplicated successfully!", "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error duplicating: " + (res.error || "Failed"), "error", 3000);
        }
    } catch (e) {
        alert("Network error: " + e.message);
    }
}

async function deleteStraddleAlgo(strategyId) {
    if (!confirm("Are you sure you want to delete this strategy algorithm permanently?")) return;
    try {
        const resp = await fetch("/api/straddle/strategy/delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId })
        });
        const res = await resp.json();
        if (res.success) {
            window._selectedAlgoIds.delete(strategyId);
            await fetchStraddleState();
            renderStraddleAlgosTable();
        } else {
            alert("Error deleting: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}


// =========================================================================
// MODALS: ADD ALGO & RENAME ALGO
// =========================================================================

function openAddStraddleAlgoModal() {
    const modal = document.getElementById("modalAddStraddleAlgo");
    if (!modal) return;
    const nameInput = document.getElementById("newAlgoName");
    if (nameInput) {
        nameInput.value = "";
        nameInput.placeholder = "e.g. Weekly Short Straddle 9:20";
    }
    modal.style.display = "flex";
}

function closeAddStraddleAlgoModal() {
    const modal = document.getElementById("modalAddStraddleAlgo");
    if (modal) modal.style.display = "none";
}

async function submitCreateNewStraddleAlgo() {
    const rawName = document.getElementById("newAlgoName")?.value.trim();
    const preset = document.getElementById("newAlgoPreset")?.value || "SHORT_STRADDLE";
    const underlying = document.getElementById("newAlgoUnderlying")?.value || "NIFTY";
    const name = rawName || `${underlying} 9:20 Strategy`;

    const submitBtn = document.querySelector("#modalAddStraddleAlgo .btn-primary");
    if (submitBtn) {
        if (submitBtn.disabled) return;
        submitBtn.disabled = true;
        submitBtn.textContent = "Creating...";
    }

    const days = [];
    document.querySelectorAll("input[name='algo_days']:checked").forEach(cb => days.push(cb.value));

    try {
        const resp = await fetch("/api/straddle/strategy/create_preset", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                name: name,
                preset_type: preset,
                underlying: underlying,
                days: days.length > 0 ? days : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
            })
        });
        if (!resp.ok) {
            const errTxt = await resp.text();
            throw new Error(`Server returned status ${resp.status}`);
        }
        const res = await resp.json();
        if (res.success && res.strategy) {
            closeAddStraddleAlgoModal();
            await fetchStraddleState();
            openStrategyInTerminal(res.strategy.strategy_id);
            toggleStraddleSettingsPanel(true);
            showStraddleToast(`Strategy "${res.strategy.name}" created successfully!`, "success", 3000);
        } else {
            showStraddleToast("Error creating algo: " + (res.error || "Failed"), "error", 4000);
        }
    } catch (e) {
        console.error("Error creating algo:", e);
        showStraddleToast("Error creating algo: " + e.message, "error", 4000);
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            submitBtn.textContent = "Create Strategy";
        }
    }
}

function openRenameStraddleAlgoModal(strategyId, currentName) {
    const modal = document.getElementById("modalRenameStraddleAlgo");
    if (!modal) return;
    document.getElementById("renameAlgoTargetId").value = strategyId;
    document.getElementById("renameAlgoNewName").value = currentName;
    modal.style.display = "flex";
}

function closeRenameStraddleAlgoModal() {
    const modal = document.getElementById("modalRenameStraddleAlgo");
    if (modal) modal.style.display = "none";
}

async function submitRenameStraddleAlgo() {
    const strategyId = document.getElementById("renameAlgoTargetId")?.value;
    const newName = document.getElementById("renameAlgoNewName")?.value.trim();
    if (!strategyId || !newName) return;

    try {
        const resp = await fetch("/api/straddle/strategy/rename", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId, new_name: newName })
        });
        const res = await resp.json();
        if (res.success) {
            closeRenameStraddleAlgoModal();
            await fetchStraddleState();
            renderStraddleAlgosTable();
        } else {
            alert("Error renaming: " + (res.error || "Failed"));
        }
    } catch (e) {
        alert("Error: " + e.message);
    }
}


// =========================================================================
// SUB-VIEW 3: P&L CHART (CANVAS RENDERING & EXPORTS)
// =========================================================================

function setPnlChartSelector(mode) {
    window._straddleState.pnlChartSelector = mode;
    document.getElementById("btnPnlSelectClient")?.classList.toggle("active-toggle", mode === "client");
    document.getElementById("btnPnlSelectAlgo")?.classList.toggle("active-toggle", mode === "algo");
    renderPnlEntitiesList();
    loadStraddlePnlChart();
}

function setPnlChartType(type) {
    window._straddleState.pnlChartType = type;
    document.getElementById("btnPnlTypeLine")?.classList.toggle("active-toggle", type === "line");
    document.getElementById("btnPnlTypeCandle")?.classList.toggle("active-toggle", type === "candle");
    if (window._straddleState.pnlDataCache) {
        drawPnlCanvas(window._straddleState.pnlDataCache, type);
    }
}

function renderPnlEntitiesList() {
    const list = document.getElementById("pnlEntitiesList");
    if (!list) return;

    const selector = window._straddleState.pnlChartSelector || "client";
    const searchVal = (document.getElementById("pnlSearchEntities")?.value || "").toLowerCase().trim();

    let items = [];
    // Always pin Overall Portfolio Total P&L
    items.push({
        id: "OVERALL",
        name: "Overall Total P&L (All Positions)",
        is_overall: true,
        status: "LIVE_STREAM"
    });

    const strats = window._straddleState.allStrategies || [];
    strats.forEach(s => {
        const isEnabled = s.enabled !== false;
        const status = s.status || (isEnabled ? "Trade Active" : "Stopped");
        items.push({
            id: s.strategy_id,
            name: s.name || s.strategy_id,
            is_overall: false,
            status: status,
            enabled: isEnabled,
            mtm: s.mtm || 0.0
        });
    });

    if (searchVal) {
        items = items.filter(it => it.name.toLowerCase().includes(searchVal) || (it.id && it.id.toLowerCase().includes(searchVal)));
    }

    let html = "";
    items.forEach((it, idx) => {
        const isSelected = (it.id === window._straddleState.pnlSelectedEntityId) || (!window._straddleState.pnlSelectedEntityId && it.id === "OVERALL");
        if (isSelected) window._straddleState.pnlSelectedEntityId = it.id;

        const isOverall = it.is_overall;
        let badgeHtml = "";
        if (isOverall) {
            badgeHtml = `<span style="background:rgba(59,130,246,0.25); color:#60a5fa; font-size:10px; padding:1px 6px; border-radius:4px; font-weight:800;">PORTFOLIO</span>`;
        } else {
            const isAct = (it.status === "TRADE_ACTIVE" || it.status === "Trade Active");
            const bg = isAct ? "rgba(16,185,129,0.2)" : "rgba(244,63,94,0.15)";
            const col = isAct ? "#10b981" : "#94a3b8";
            badgeHtml = `<span style="background:${bg}; color:${col}; font-size:9.5px; padding:1px 5px; border-radius:4px; font-weight:700;">${isAct ? '● ACTIVE' : 'STOPPED'}</span>`;
        }

        html += `
            <div style="display:flex; align-items:center; justify-content:space-between; padding:7px 9px; border-radius:6px; cursor:pointer; background:${isSelected ? 'rgba(37,99,235,0.2)' : 'transparent'}; border:${isSelected ? '1px solid #3b82f6' : '1px solid transparent'}; color:${isSelected ? '#60a5fa' : '#cbd5e1'}; font-size:12px; font-weight:700; margin-bottom:3px;" onclick="selectPnlEntity('${it.id}', '${it.name.replace(/'/g, "\\'")}')">
                <div style="display:flex; align-items:center; gap:8px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">
                    <input type="radio" name="pnl_entity" ${isSelected ? "checked" : ""}>
                    <span style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap;">${it.name}</span>
                </div>
                <div>${badgeHtml}</div>
            </div>
        `;
    });

    list.innerHTML = html || `<div style="color:#64748b; font-size:11px; padding:10px;">No items match.</div>`;
}

function filterPnlEntitiesList() {
    renderPnlEntitiesList();
}

function selectPnlEntity(id, name) {
    window._straddleState.pnlSelectedEntityId = id;
    const title = document.getElementById("pnlChartTitle");
    if (title) title.textContent = name;
    renderPnlEntitiesList();
    loadStraddlePnlChart();
}

async function loadStraddlePnlChart() {
    const entityId = window._straddleState.pnlSelectedEntityId || "OVERALL";
    const selector = (entityId === "OVERALL") ? "overall" : "algo";
    const start = document.getElementById("pnlStartDate")?.value || "";
    const end = document.getElementById("pnlEndDate")?.value || "";
    const und = document.getElementById("pnlUnderlyingSelect")?.value || "None";

    try {
        const resp = await fetch(`/api/straddle/pnl_chart?selector_type=${selector}&entity_id=${encodeURIComponent(entityId)}&start_date=${start}&end_date=${end}&underlying=${und}`);
        const data = await resp.json();
        if (data && data.success && data.data) {
            window._straddleState.pnlDataCache = data.data;
            drawPnlCanvas(data.data, window._straddleState.pnlChartType || "line", null);

            const titleEl = document.getElementById("pnlChartTitle");
            if (titleEl) {
                titleEl.textContent = data.data.strategy_name || data.data.strategy_id || "Overall Portfolio P&L";
            }

            // Update stats
            const maxEl = document.getElementById("pnlStatMax");
            const minEl = document.getElementById("pnlStatMin");
            const totEl = document.getElementById("pnlStatTotal");
            const maxVal = data.data.max_pnl || 0.0;
            const minVal = data.data.min_pnl || 0.0;
            const totVal = data.data.total_pnl || 0.0;
            const maxTime = data.data.max_pnl_time || "--:--";
            const minTime = data.data.min_pnl_time || "--:--";
            const drawdown = data.data.max_drawdown !== undefined ? data.data.max_drawdown : minVal;
            const drawdownPeriod = data.data.max_drawdown_period || "--";
            const profitKept = data.data.profit_kept || "100%";

            if (maxEl) maxEl.textContent = (maxVal >= 0 ? "+₹ " : "-₹ ") + Math.abs(maxVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (document.getElementById("pnlStatMaxTime")) document.getElementById("pnlStatMaxTime").textContent = `at ${maxTime}`;

            if (minEl) minEl.textContent = (minVal >= 0 ? "+₹ " : "-₹ ") + Math.abs(minVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (document.getElementById("pnlStatMinTime")) document.getElementById("pnlStatMinTime").textContent = `at ${minTime}`;

            if (document.getElementById("pnlStatDrawdown")) document.getElementById("pnlStatDrawdown").textContent = (drawdown >= 0 ? "+₹ " : "-₹ ") + Math.abs(drawdown).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (document.getElementById("pnlStatDrawdownPeriod")) document.getElementById("pnlStatDrawdownPeriod").textContent = drawdownPeriod;

            if (document.getElementById("pnlStatProfitKept")) document.getElementById("pnlStatProfitKept").textContent = profitKept;

            if (totEl) {
                totEl.textContent = (totVal >= 0 ? "+₹ " : "-₹ ") + Math.abs(totVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                totEl.style.color = totVal >= 0 ? "#10b981" : "#f43f5e";
            }
        }
    } catch (e) {
        console.error("Error loading PnL chart:", e);
    }
}

function drawPnlCanvas(pnlData, chartType, mousePos = null) {
    const canvas = document.getElementById("straddlePnlCanvas");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const dpr = window.devicePixelRatio || 1;

    const rect = canvas.getBoundingClientRect();
    let W = rect.width;
    let H = rect.height;
    if (!W || W < 100) {
        W = canvas.parentElement ? (canvas.parentElement.clientWidth || canvas.parentElement.offsetWidth) : 900;
        if (!W || W < 100) W = 900;
    }
    if (!H || H < 100) H = 420;

    canvas.style.width = W + "px";
    canvas.style.height = H + "px";
    canvas.width = W * dpr;
    canvas.height = H * dpr;
    ctx.scale(dpr, dpr);

    const padLeft = 75;
    const padRight = 35;
    const padTop = 35;
    const padBottom = 45;
    const chartW = W - padLeft - padRight;
    const chartH = H - padTop - padBottom;

    // Clear background with soft gradient
    const bgGrad = ctx.createLinearGradient(0, 0, 0, H);
    bgGrad.addColorStop(0, "#080c14");
    bgGrad.addColorStop(1, "#0a101d");
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, W, H);

    let points = pnlData.points || [];
    if (points.length === 0) {
        // Build fallback intraday points so the chart structure is always visible
        const nowHM = new Date().toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit' });
        const curTot = pnlData.total_pnl || 0.0;
        points = [
            { timestamp: "09:15", pnl: 0.0, open: 0.0, high: 0.0, low: 0.0, close: 0.0 },
            { timestamp: (nowHM >= "09:15" && nowHM <= "15:30") ? nowHM : "15:30", pnl: curTot, open: curTot, high: curTot, low: 0.0, close: curTot }
        ];
    } else if (points.length === 1) {
        // If only 1 point, clone it across session to ensure line renders cleanly
        const p0 = points[0];
        const nowHM = new Date().toLocaleTimeString('en-US', { hour12: false, hour: '2-digit', minute: '2-digit' });
        const endTimestamp = (nowHM >= "09:15" && nowHM <= "15:30") ? nowHM : "15:30";
        points = [
            { ...p0 },
            { ...p0, timestamp: (p0.timestamp !== endTimestamp ? endTimestamp : "15:30") }
        ];
    }

    // Determine Y-axis bounds with breathing room
    let maxVal = Math.max(200, (pnlData.max_pnl || 0) * 1.15);
    let minVal = Math.min(-200, (pnlData.min_pnl || 0) * 1.15);
    const range = Math.max(100, maxVal - minVal);

    const getY = (val) => padTop + chartH - ((val - minVal) / range) * chartH;
    const getX = (idx) => padLeft + (idx / Math.max(1, points.length - 1)) * chartW;

    // 1. Draw horizontal grid lines & Y labels
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
    ctx.fillStyle = "#64748b";
    ctx.font = "10.5px monospace";
    ctx.textAlign = "right";

    const ySteps = 6;
    for (let i = 0; i <= ySteps; i++) {
        const val = minVal + (i / ySteps) * range;
        const y = getY(val);
        ctx.beginPath();
        ctx.moveTo(padLeft, y);
        ctx.lineTo(W - padRight, y);
        ctx.stroke();
        const formattedVal = (val >= 0 ? "+₹ " : "-₹ ") + Math.abs(Math.round(val)).toLocaleString("en-IN");
        ctx.fillText(formattedVal, padLeft - 8, y + 3.5);
    }

    // 2. Draw Zero Baseline Line
    const zeroY = getY(0);
    ctx.setLineDash([4, 4]);
    ctx.strokeStyle = "rgba(255, 255, 255, 0.35)";
    ctx.lineWidth = 1.2;
    ctx.beginPath();
    ctx.moveTo(padLeft, zeroY);
    ctx.lineTo(W - padRight, zeroY);
    ctx.stroke();
    ctx.setLineDash([]);

    // 3. Draw Max P&L Reference (Green Dashed)
    const maxPnl = pnlData.max_pnl !== undefined ? pnlData.max_pnl : 0;
    const maxY = getY(maxPnl);
    ctx.setLineDash([3, 3]);
    ctx.strokeStyle = "rgba(16, 185, 129, 0.5)";
    ctx.beginPath();
    ctx.moveTo(padLeft, maxY);
    ctx.lineTo(W - padRight, maxY);
    ctx.stroke();

    // Max PnL badge tag
    ctx.setLineDash([]);
    ctx.fillStyle = "#10b981";
    ctx.beginPath();
    ctx.roundRect(padLeft, Math.max(padTop - 12, maxY - 16), 85, 16, 4);
    ctx.fill();
    ctx.fillStyle = "#ffffff";
    ctx.font = "bold 9px monospace";
    ctx.textAlign = "center";
    const maxPrefix = maxPnl >= 0 ? "+₹" : "-₹";
    ctx.fillText(`Max ${maxPrefix}${Math.abs(Math.round(maxPnl))}`, padLeft + 42.5, Math.max(padTop - 12, maxY - 16) + 11.5);

    // 4. Draw Min P&L Reference (Red Dashed)
    const minPnl = pnlData.min_pnl !== undefined ? pnlData.min_pnl : 0;
    const minY = getY(minPnl);
    ctx.setLineDash([3, 3]);
    ctx.strokeStyle = "rgba(244, 63, 94, 0.5)";
    ctx.beginPath();
    ctx.moveTo(padLeft, minY);
    ctx.lineTo(W - padRight, minY);
    ctx.stroke();

    // Min PnL badge tag
    ctx.setLineDash([]);
    ctx.fillStyle = "#f43f5e";
    ctx.beginPath();
    ctx.roundRect(padLeft, Math.min(H - padBottom - 4, minY + 2), 85, 16, 4);
    ctx.fill();
    ctx.fillStyle = "#ffffff";
    ctx.font = "bold 9px monospace";
    ctx.textAlign = "center";
    const minPrefix = minPnl >= 0 ? "+₹" : "-₹";
    ctx.fillText(`Min ${minPrefix}${Math.abs(Math.round(minPnl))}`, padLeft + 42.5, Math.min(H - padBottom - 4, minY + 2) + 11.5);

    // 5. Draw Curve / Candles
    if (chartType === "candle") {
        const barW = Math.max(3, (chartW / points.length) * 0.7);
        points.forEach((pt, idx) => {
            const x = getX(idx);
            const openY = getY(pt.open || pt.pnl);
            const closeY = getY(pt.close || pt.pnl);
            const highY = getY(pt.high || pt.pnl);
            const lowY = getY(pt.low || pt.pnl);
            const isUp = (pt.close || pt.pnl) >= (pt.open || pt.pnl);

            ctx.strokeStyle = isUp ? "#10b981" : "#f43f5e";
            ctx.fillStyle = isUp ? "#10b981" : "#f43f5e";

            // Wick
            ctx.beginPath();
            ctx.moveTo(x, highY);
            ctx.lineTo(x, lowY);
            ctx.stroke();

            // Body
            const top = Math.min(openY, closeY);
            const h = Math.max(2, Math.abs(closeY - openY));
            ctx.fillRect(x - barW / 2, top, barW, h);
        });
    } else {
        // Line Mode with Spline & Area Glow Fill
        // Top Fill (above Zero)
        ctx.save();
        ctx.beginPath();
        ctx.rect(padLeft, padTop, chartW, Math.max(0, zeroY - padTop));
        ctx.clip();

        ctx.beginPath();
        points.forEach((pt, idx) => {
            const x = getX(idx);
            const y = getY(pt.pnl);
            if (idx === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.lineTo(getX(points.length - 1), zeroY);
        ctx.lineTo(getX(0), zeroY);
        ctx.closePath();
        const topGrad = ctx.createLinearGradient(0, padTop, 0, zeroY);
        topGrad.addColorStop(0, "rgba(16, 185, 129, 0.28)");
        topGrad.addColorStop(1, "rgba(16, 185, 129, 0.02)");
        ctx.fillStyle = topGrad;
        ctx.fill();
        ctx.restore();

        // Bottom Fill (below Zero)
        ctx.save();
        ctx.beginPath();
        ctx.rect(padLeft, zeroY, chartW, Math.max(0, padTop + chartH - zeroY));
        ctx.clip();

        ctx.beginPath();
        points.forEach((pt, idx) => {
            const x = getX(idx);
            const y = getY(pt.pnl);
            if (idx === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.lineTo(getX(points.length - 1), zeroY);
        ctx.lineTo(getX(0), zeroY);
        ctx.closePath();
        const btmGrad = ctx.createLinearGradient(0, zeroY, 0, padTop + chartH);
        btmGrad.addColorStop(0, "rgba(244, 63, 94, 0.02)");
        btmGrad.addColorStop(1, "rgba(244, 63, 94, 0.28)");
        ctx.fillStyle = btmGrad;
        ctx.fill();
        ctx.restore();

        // Stroke line with zero-crossing color transition (Green above 0, Red below 0)
        ctx.beginPath();
        points.forEach((pt, idx) => {
            const x = getX(idx);
            const y = getY(pt.pnl);
            if (idx === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.lineWidth = 2.4;
        const zeroFraction = Math.max(0, Math.min(1, (zeroY - padTop) / Math.max(1, chartH)));
        const strokeGrad = ctx.createLinearGradient(0, padTop, 0, padTop + chartH);
        strokeGrad.addColorStop(0, "#10b981");
        strokeGrad.addColorStop(Math.max(0, zeroFraction - 0.01), "#10b981");
        strokeGrad.addColorStop(Math.min(1, zeroFraction + 0.01), "#f43f5e");
        strokeGrad.addColorStop(1, "#f43f5e");
        ctx.strokeStyle = strokeGrad;
        ctx.stroke();

        // Draw point dots
        points.forEach((pt, idx) => {
            if (points.length <= 40 || idx === 0 || idx === points.length - 1) {
                const x = getX(idx);
                const y = getY(pt.pnl);
                ctx.beginPath();
                ctx.arc(x, y, (idx === points.length - 1 ? 4.5 : 3), 0, Math.PI * 2);
                ctx.fillStyle = pt.pnl >= 0 ? "#10b981" : "#f43f5e";
                ctx.fill();
                ctx.lineWidth = 1.5;
                ctx.strokeStyle = "#ffffff";
                ctx.stroke();
            }
        });
    }

    // 6. X-axis timestamps
    ctx.fillStyle = "#64748b";
    ctx.font = "10.5px monospace";
    ctx.textAlign = "center";
    const xInterval = Math.max(1, Math.floor(points.length / 8));
    for (let i = 0; i < points.length; i += xInterval) {
        const x = getX(i);
        const timeStr = points[i].timestamp || "";
        ctx.fillText(timeStr, x, H - 14);
    }

    // 7. Interactive Crosshair HUD Rendering
    if (mousePos && mousePos.active && points.length > 0) {
        const targetIdx = Math.min(points.length - 1, Math.max(0, mousePos.index));
        const pt = points[targetIdx];
        const ptX = getX(targetIdx);
        const ptY = getY(pt.pnl);

        // Vertical Dashed Crosshair Line
        ctx.setLineDash([3, 3]);
        ctx.strokeStyle = "rgba(255, 255, 255, 0.45)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(ptX, padTop);
        ctx.lineTo(ptX, padTop + chartH);
        ctx.stroke();

        // Horizontal Dashed Crosshair Line
        ctx.beginPath();
        ctx.moveTo(padLeft, ptY);
        ctx.lineTo(W - padRight, ptY);
        ctx.stroke();
        ctx.setLineDash([]);

        // Snapping Glowing Circle
        ctx.fillStyle = "rgba(255, 255, 255, 0.25)";
        ctx.beginPath();
        ctx.arc(ptX, ptY, 7, 0, Math.PI * 2);
        ctx.fill();

        ctx.fillStyle = (pt.pnl >= 0) ? "#10b981" : "#f43f5e";
        ctx.strokeStyle = "#ffffff";
        ctx.lineWidth = 2;
        ctx.beginPath();
        ctx.arc(ptX, ptY, 4, 0, Math.PI * 2);
        ctx.fill();
        ctx.stroke();

        // Bottom X-Axis Time Badge Pill
        ctx.fillStyle = "#0f172a";
        ctx.strokeStyle = "#38bdf8";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.roundRect(ptX - 25, H - 28, 50, 18, 4);
        ctx.fill();
        ctx.stroke();
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 10px monospace";
        ctx.textAlign = "center";
        ctx.fillText(pt.timestamp, ptX, H - 15);

        // Side Y-Axis Price Badge Pill
        const yBadgeW = 80;
        const yBadgeH = 18;
        ctx.fillStyle = (pt.pnl >= 0) ? "#10b981" : "#f43f5e";
        ctx.beginPath();
        ctx.roundRect(padLeft - yBadgeW - 6, ptY - yBadgeH / 2, yBadgeW, yBadgeH, 4);
        ctx.fill();
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 10px monospace";
        ctx.textAlign = "center";
        const badgeTxt = (pt.pnl >= 0 ? "+₹ " : "-₹ ") + Math.abs(pt.pnl).toLocaleString("en-IN", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
        ctx.fillText(badgeTxt, padLeft - yBadgeW / 2 - 6, ptY + 3.5);

        // Floating Glassmorphism Tooltip Card
        const tipW = 165;
        const tipH = 68;
        let tipX = (ptX > W - padRight - tipW - 20) ? (ptX - tipW - 14) : (ptX + 14);
        let tipY = Math.max(padTop + 8, Math.min(padTop + chartH - tipH - 8, ptY - tipH / 2));

        ctx.fillStyle = "rgba(13, 21, 39, 0.94)";
        ctx.strokeStyle = "rgba(255, 255, 255, 0.18)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.roundRect(tipX, tipY, tipW, tipH, 8);
        ctx.fill();
        ctx.stroke();

        // Tooltip Content
        ctx.textAlign = "left";
        ctx.fillStyle = "#94a3b8";
        ctx.font = "10.5px Inter, sans-serif";
        ctx.fillText(`🕒 Time: ${pt.timestamp}`, tipX + 10, tipY + 18);

        ctx.font = "bold 12.5px monospace";
        ctx.fillStyle = (pt.pnl >= 0) ? "#10b981" : "#f43f5e";
        const tipPnlFormatted = (pt.pnl >= 0 ? "+₹ " : "-₹ ") + Math.abs(pt.pnl).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        ctx.fillText(`P&L: ${tipPnlFormatted}`, tipX + 10, tipY + 38);

        const drawdownVal = Math.max(0, (pnlData.max_pnl - pt.pnl));
        ctx.font = "10.5px Inter, sans-serif";
        ctx.fillStyle = "#cbd5e1";
        ctx.fillText(`Drawdown: -₹ ${drawdownVal.toLocaleString("en-IN", { minimumFractionDigits: 2 })}`, tipX + 10, tipY + 56);
    }
}

function setupPnlCanvasEvents() {
    const canvas = document.getElementById("straddlePnlCanvas");
    if (!canvas) return;

    const handlePointerMove = (clientX, clientY) => {
        const pnlData = window._straddleState.pnlDataCache;
        if (!pnlData || !pnlData.points || pnlData.points.length === 0) return;

        const rect = canvas.getBoundingClientRect();
        const mouseX = clientX - rect.left;
        const mouseY = clientY - rect.top;
        const padLeft = 75;
        const padRight = 35;
        const chartW = rect.width - padLeft - padRight;

        if (mouseX < padLeft || mouseX > rect.width - padRight) {
            drawPnlCanvas(pnlData, window._straddleState.pnlChartType || "line", null);
            return;
        }

        const ratio = (mouseX - padLeft) / chartW;
        const idx = Math.min(pnlData.points.length - 1, Math.max(0, Math.round(ratio * (pnlData.points.length - 1))));
        const pt = pnlData.points[idx];

        drawPnlCanvas(pnlData, window._straddleState.pnlChartType || "line", {
            active: true,
            x: mouseX,
            y: mouseY,
            index: idx,
            point: pt
        });

        const cursorLabel = document.getElementById("pnlLiveCursorValue");
        if (cursorLabel && pt) {
            const pnlFormatted = (pt.pnl >= 0 ? "+₹ " : "-₹ ") + Math.abs(pt.pnl).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            const color = pt.pnl >= 0 ? "#10b981" : "#f43f5e";
            cursorLabel.innerHTML = `Time: <b style="color:#ffffff;">${pt.timestamp}</b> | P&L: <b style="color:${color};">${pnlFormatted}</b>`;
        }
    };

    canvas.addEventListener("mousemove", (e) => {
        handlePointerMove(e.clientX, e.clientY);
    });

    canvas.addEventListener("mouseleave", () => {
        const pnlData = window._straddleState.pnlDataCache;
        if (pnlData) {
            drawPnlCanvas(pnlData, window._straddleState.pnlChartType || "line", null);
        }
        const cursorLabel = document.getElementById("pnlLiveCursorValue");
        if (cursorLabel) cursorLabel.textContent = "Hover over chart for details";
    });

    canvas.addEventListener("touchmove", (e) => {
        if (e.touches && e.touches.length > 0) {
            handlePointerMove(e.touches[0].clientX, e.touches[0].clientY);
        }
    }, { passive: true });

    canvas.addEventListener("touchend", () => {
        const pnlData = window._straddleState.pnlDataCache;
        if (pnlData) {
            drawPnlCanvas(pnlData, window._straddleState.pnlChartType || "line", null);
        }
    });
}

function exportStraddlePnlData() {
    const data = window._straddleState.pnlDataCache;
    if (!data || !data.points || data.points.length === 0) {
        alert("No P&L data to export.");
        return;
    }

    let csv = "Timestamp,P&L (INR),Open,High,Low,Close\n";
    data.points.forEach(p => {
        csv += `${p.timestamp},${p.pnl},${p.open || p.pnl},${p.high || p.pnl},${p.low || p.pnl},${p.close || p.pnl}\n`;
    });

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `pnl_data_${new Date().toISOString().split("T")[0]}.csv`;
    link.click();
}

function captureStraddlePnlScreenshot() {
    const canvas = document.getElementById("straddlePnlCanvas");
    if (!canvas) return;
    const url = canvas.toDataURL("image/png");
    const link = document.createElement("a");
    link.href = url;
    link.download = `pnl_chart_${new Date().toISOString().split("T")[0]}.png`;
    link.click();
}


// =========================================================================
// SUB-VIEW 4: ORDERBOOK & MARGINS (Quantiply-Grade Complete Parity)
// =========================================================================

window._straddleOrderbookSort = { col: 'time', dir: 'desc' };
window._straddleOrderbookClientId = 'ALL';

function sortOrderbookBy(col) {
    if (window._straddleOrderbookSort.col === col) {
        window._straddleOrderbookSort.dir = (window._straddleOrderbookSort.dir === 'asc') ? 'desc' : 'asc';
    } else {
        window._straddleOrderbookSort.col = col;
        window._straddleOrderbookSort.dir = (col === 'time' || col === 'quantity' || col === 'price' || col === 'trigger_price') ? 'desc' : 'asc';
    }
    renderStraddleOrderbookTable();
}

function onOrderbookClientIdChange(clientId) {
    window._straddleOrderbookClientId = clientId;
    renderStraddleOrderbookTable();
}

function openStrategyFromOrderbook(strategyId) {
    if (strategyId && window._straddleState && window._straddleState.strategies && window._straddleState.strategies[strategyId]) {
        openStrategyInTerminal(strategyId);
    } else {
        switchStraddleSubView("algos");
    }
}

function switchOrderbookSubTab(tab) {
    window._straddleState.orderbookSubTab = tab;
    document.getElementById("btnSubTabOrderbook")?.classList.toggle("active", tab === "orders");
    document.getElementById("btnSubTabMargin")?.classList.toggle("active", tab === "margin");

    const ordersSec = document.getElementById("orderbookOrdersSection");
    const marginSec = document.getElementById("orderbookMarginSection");

    if (ordersSec) ordersSec.style.display = (tab === "orders") ? "block" : "none";
    if (marginSec) marginSec.style.display = (tab === "margin") ? "block" : "none";

    if (tab === "orders") {
        renderStraddleOrderbookTable();
    } else {
        renderStraddleMargin();
    }
}

async function syncAndResetKotakNeoData() {
    const btn1 = document.getElementById("btnSyncKotakNeoData");
    const btn2 = document.getElementById("btnOrderbookSyncKotakNeo");
    const orig1 = btn1 ? btn1.innerHTML : "";
    const orig2 = btn2 ? btn2.innerHTML : "";

    if (btn1) {
        btn1.disabled = true;
        btn1.innerHTML = `<span class="straddle-spinner" style="display:inline-block; width:12px; height:12px; border:2px solid #fff; border-top-color:transparent; border-radius:50%; animation:spin 0.8s linear infinite; vertical-align:middle; margin-right:6px;"></span> Syncing...`;
    }
    if (btn2) {
        btn2.disabled = true;
        btn2.innerHTML = `<span class="straddle-spinner" style="display:inline-block; width:12px; height:12px; border:2px solid #fff; border-top-color:transparent; border-radius:50%; animation:spin 0.8s linear infinite; vertical-align:middle; margin-right:6px;"></span> Syncing...`;
    }

    try {
        const resp = await fetch("/api/straddle/sync_broker", { method: "POST" });
        const data = await resp.json();

        // Refresh all platform state concurrently
        await Promise.all([
            loadStraddleOpenPositions(),
            loadStraddleOrderbook(),
            loadStraddleMargin(),
            loadStraddleState()
        ]);

        const openCnt = data.open_count !== undefined ? data.open_count : 0;
        const closedCnt = data.closed_count !== undefined ? data.closed_count : 0;
        const ordCnt = data.orders_count !== undefined ? data.orders_count : 0;

        showStraddleNotification(`✅ Kotak Neo Synced: ${openCnt} Live Open, ${closedCnt} Closed Today, ${ordCnt} Orders`, "success");
    } catch (e) {
        console.error("Error syncing Kotak Neo data:", e);
        showStraddleNotification("⚠️ Kotak Neo Sync Error: " + (e.message || "Failed to reach server"), "error");
    } finally {
        if (btn1) {
            btn1.disabled = false;
            btn1.innerHTML = orig1 || "⚡ Sync Kotak Neo Data";
        }
        if (btn2) {
            btn2.disabled = false;
            btn2.innerHTML = orig2 || "⚡ Sync Kotak Neo Data";
        }
    }
}

async function loadStraddleOrderbook(animate = false) {
    const spinner = document.getElementById("fetchOrderbookSpinner");
    const icon = document.getElementById("fetchOrderbookIcon");
    if (animate) {
        if (spinner) spinner.style.display = "inline-block";
        if (icon) icon.style.display = "none";
    }

    try {
        const tbody = document.getElementById("straddleOrderbookTableBody");
        if (tbody && (!window._straddleState.orderbookDataCache || window._straddleState.orderbookDataCache.length === 0)) {
            tbody.innerHTML = `
                <tr>
                    <td colspan="14" style="text-align:center; padding:36px; color:#94a3b8;">
                        <span class="straddle-spinner" style="display:inline-block; width:18px; height:18px; border:2px solid #38bdf8; border-top-color:transparent; border-radius:50%; animation:spin 0.8s linear infinite; vertical-align:middle; margin-right:8px;"></span>
                        Fetching live orderbook from broker & active algos...
                    </td>
                </tr>
            `;
        }
        const sourceVal = document.getElementById("orderbookSourceSelect")?.value || "ALL";
        const statusVal = document.getElementById("orderbookStatusSelect")?.value || "ALL";

        const url = `/api/straddle/orderbook?source_filter=${encodeURIComponent(sourceVal)}&status_filter=${encodeURIComponent(statusVal)}`;
        const resp = await fetch(url);
        const data = await resp.json();
        if (data && data.success) {
            window._straddleState.orderbookDataCache = data.orders || [];

            // Populate Client ID select dynamically
            const clientSelect = document.getElementById("orderbookClientSelect");
            if (clientSelect) {
                const currentVal = clientSelect.value || "ALL";
                const clientIds = new Set();
                clientIds.add("Kotak Neo Live");
                (data.orders || []).forEach(o => {
                    if (o.client_id) clientIds.add(o.client_id);
                });
                let optionsHtml = `<option value="ALL">All Client IDs</option>`;
                clientIds.forEach(cid => {
                    optionsHtml += `<option value="${cid}">${cid}</option>`;
                });
                clientSelect.innerHTML = optionsHtml;
                clientSelect.value = currentVal;
            }

            renderStraddleOrderbookTable();
        }
    } catch (e) {
        console.error("Error loading orderbook:", e);
    } finally {
        if (animate) {
            setTimeout(() => {
                if (spinner) spinner.style.display = "none";
                if (icon) icon.style.display = "inline";
            }, 300);
        }
    }
}

function renderStraddleOrderbookTable() {
    const tbody = document.getElementById("straddleOrderbookTableBody");
    if (!tbody) return;

    const orders = window._straddleState.orderbookDataCache || [];
    const searchVal = (document.getElementById("orderbookSearchInput")?.value || "").toLowerCase().trim();
    const sourceVal = document.getElementById("orderbookSourceSelect")?.value || "ALL";
    const statusVal = document.getElementById("orderbookStatusSelect")?.value || "ALL";
    const clientVal = window._straddleOrderbookClientId || "ALL";

    // Update Header Sort Icons
    const currentSort = window._straddleOrderbookSort || { col: 'time', dir: 'desc' };
    const sortCols = ['algo_name', 'order_id', 'category', 'client_id', 'symbol', 'side', 'status', 'message', 'time', 'quantity', 'order_type', 'source', 'price', 'trigger_price'];
    sortCols.forEach(c => {
        const el = document.getElementById(`sort_${c}`);
        if (el) {
            if (currentSort.col === c) {
                el.textContent = currentSort.dir === 'asc' ? '▲' : '▼';
                el.style.opacity = '1';
                el.style.color = '#38bdf8';
            } else {
                el.textContent = '↑ ↓';
                el.style.opacity = '0.5';
                el.style.color = 'inherit';
            }
        }
    });

    let filtered = orders.filter(o => {
        // Client ID Filter
        if (clientVal !== "ALL") {
            const oClient = (o.client_id || "Kotak Neo Live").trim();
            if (oClient !== clientVal && !oClient.includes(clientVal)) return false;
        }

        // Source Filter
        if (sourceVal !== "ALL") {
            const isAlgo = (o.source_type === "ALGO" || (o.algo_name && !o.algo_name.toLowerCase().includes("manual") && !o.algo_name.toLowerCase().includes("direct")));
            if (sourceVal === "ALGO" && !isAlgo) return false;
            if (sourceVal === "MANUAL" && isAlgo) return false;
        }

        // Status Filter
        if (statusVal !== "ALL") {
            const st = (o.status || "").toUpperCase();
            if (statusVal === "OPEN" && !(st.includes("OPEN") || st.includes("PENDING") || st.includes("TRIG"))) return false;
            if (statusVal === "COMPLETE" && !st.includes("COMP")) return false;
            if (statusVal === "REJECTED" && !st.includes("REJ")) return false;
            if (statusVal === "CANCELLED" && !st.includes("CAN")) return false;
        }

        // Search Filter
        if (searchVal) {
            const sym = (o.symbol || "").toLowerCase();
            const id = String(o.order_id || "").toLowerCase();
            const st = (o.status || "").toLowerCase();
            const algo = (o.algo_name || "").toLowerCase();
            const src = (o.source || "").toLowerCase();
            const cat = (o.category || "").toLowerCase();
            const msg = (o.message || "").toLowerCase();
            const client = (o.client_id || "").toLowerCase();
            const side = (o.side || "").toLowerCase();
            const otype = (o.order_type || "").toLowerCase();
            if (!sym.includes(searchVal) && !id.includes(searchVal) && !st.includes(searchVal) && !algo.includes(searchVal) && !src.includes(searchVal) && !cat.includes(searchVal) && !msg.includes(searchVal) && !client.includes(searchVal) && !side.includes(searchVal) && !otype.includes(searchVal)) return false;
        }
        return true;
    });

    // Sorting
    filtered.sort((a, b) => {
        let valA = a[currentSort.col];
        let valB = b[currentSort.col];

        if (currentSort.col === 'quantity' || currentSort.col === 'price' || currentSort.col === 'trigger_price') {
            valA = parseFloat(valA) || 0;
            valB = parseFloat(valB) || 0;
        } else {
            valA = String(valA || "").toLowerCase();
            valB = String(valB || "").toLowerCase();
        }

        if (valA < valB) return currentSort.dir === 'asc' ? -1 : 1;
        if (valA > valB) return currentSort.dir === 'asc' ? 1 : -1;
        return 0;
    });

    if (filtered.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="14" style="text-align:center; padding:45px 14px; color:#64748b;">
                    <div style="font-size:15px; font-weight:700; color:#cbd5e1; margin-bottom:6px;">⚡ No Orders Found</div>
                    <div style="font-size:12px; color:#94a3b8;">All live and historical orders, broker executions, and rejection logs appear here.</div>
                </td>
            </tr>
        `;
        return;
    }

    let rowsHtml = "";
    filtered.forEach(o => {
        const side = (o.side || "SELL").toUpperCase();
        const isBuy = side === "BUY";
        const sideClass = isBuy ? "side-buy" : "side-sell";
        const sideText = isBuy ? "Buy" : "Sell";

        const st = (o.status || "COMPLETE").toUpperCase();
        let stClass = "status-complete";
        let stDisplay = "Complete";
        const isRejected = st.includes("REJ");

        if (st.includes("PEND") || st.includes("OPEN") || st.includes("TRIG")) {
            stClass = "status-pending";
            stDisplay = "Pending";
        } else if (isRejected) {
            stClass = "status-rejected";
            stDisplay = "Rejected";
        } else if (st.includes("CAN")) {
            stClass = "status-cancelled";
            stDisplay = "Cancelled";
        }

        // Format symbol with CE in Green / PE in Red
        const symStr = o.symbol || "-";
        let symFormatted = symStr;
        if (symStr.includes("CE")) {
            symFormatted = symStr.replace("CE", `<span style="color:#10b981; font-weight:800;">CE</span>`);
        } else if (symStr.includes("PE")) {
            symFormatted = symStr.replace("PE", `<span style="color:#ef4444; font-weight:800;">PE</span>`);
        }

        const priceDisplay = o.price > 0 ? `₹${parseFloat(o.price).toFixed(2)}` : '0.00';
        const trigPriceDisplay = o.trigger_price > 0 ? `₹${parseFloat(o.trigger_price).toFixed(2)}` : '0.00';
        
        let orderTypeDisplay = o.order_type || "Limit (L)";
        if (orderTypeDisplay.toUpperCase().includes("SL-L") || orderTypeDisplay.toUpperCase().includes("SLL")) {
            orderTypeDisplay = "SL Limit (SLL)";
        } else if (orderTypeDisplay.toUpperCase().includes("LIMIT") || orderTypeDisplay.toUpperCase() === "L") {
            orderTypeDisplay = "Limit (L)";
        } else if (orderTypeDisplay.toUpperCase().includes("MARKET") || orderTypeDisplay.toUpperCase() === "MKT") {
            orderTypeDisplay = "Market (M)";
        }

        // Sanitize source - all platform orders belong to Straddle Pro
        let sourceDisplay = o.source || "Straddle Pro";
        if (sourceDisplay === "Quantiply" || sourceDisplay === "Manual") {
            sourceDisplay = "Straddle Pro";
        }

        const clientIdDisplay = o.client_id || "Kotak Neo Live";

        // Active strategy fallback for algo name
        const activeStratName = window._straddleState?.activeStrategy?.name || "abc";
        let algoNameDisplay = o.algo_name || activeStratName;
        if (algoNameDisplay.toLowerCase() === "straddle" || algoNameDisplay.toLowerCase() === "manual") {
            algoNameDisplay = activeStratName;
        }

        // Sanitize category (avoid 'Manual', infer Target for Sell exits)
        let categoryDisplay = o.category || (sideText === "Sell" ? "Target" : "Entry");
        if (categoryDisplay === "Manual") {
            categoryDisplay = (sideText === "Sell" ? "Target" : "Entry");
        }

        const rowRejectedClass = isRejected ? "orderbook-row-rejected" : "";
        const messageColor = isRejected ? "color:#ef4444; font-weight:500;" : "color:#cbd5e1;";

        rowsHtml += `
            <tr class="${rowRejectedClass}" style="transition:background 0.15s ease;">
                <td>
                    <a href="javascript:void(0)" onclick="openStrategyFromOrderbook('${o.strategy_id || ''}')" class="orderbook-algo-link" title="Open Algo ${algoNameDisplay}">${algoNameDisplay}</a>
                </td>
                <td style="font-family:var(--font-mono); font-size:11px; font-weight:600; color:#94a3b8; letter-spacing:0.3px;">${o.order_id || '-'}</td>
                <td style="font-size:12px; color:#f1f5f9; font-weight:500;">${categoryDisplay}</td>
                <td style="font-size:12px; color:#cbd5e1; font-weight:500;">${clientIdDisplay}</td>
                <td style="font-weight:700; color:#f8fafc; font-size:12px; letter-spacing:0.3px;">${symFormatted}</td>
                <td><span class="orderbook-side-pill ${sideClass}">${sideText}</span></td>
                <td><span class="orderbook-status-pill ${stClass}">${stDisplay}</span></td>
                <td style="font-size:11.5px; ${messageColor} max-width:260px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${o.message || (isRejected ? 'Order Rejected' : 'Complete')}">${o.message || (isRejected ? 'Order Rejected' : 'Complete')}</td>
                <td style="font-family:var(--font-mono); font-size:11px; color:#cbd5e1;">${o.time || '-'}</td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:700; font-size:12.5px; color:#f8fafc;">${o.quantity || 65}</td>
                <td style="font-size:11.5px; color:#cbd5e1;">${orderTypeDisplay}</td>
                <td style="font-size:11.5px; color:#cbd5e1;">${sourceDisplay}</td>
                <td style="text-align:right; font-family:var(--font-mono); font-size:12px; color:#94a3b8;">${priceDisplay}</td>
                <td style="text-align:right; font-family:var(--font-mono); font-size:12px; color:#94a3b8;">${trigPriceDisplay}</td>
            </tr>
        `;
    });

    tbody.innerHTML = rowsHtml;
}

function exportStraddleOrderbookCsv() {
    const orders = window._straddleState.orderbookDataCache || [];
    if (orders.length === 0) {
        alert("No orders to export.");
        return;
    }

    let csv = "Algo name,Order ID,Category,Client ID / Broker,Symbol,Side,Status,Message,Time,Qty,Order type,Source,Price,Trigger Price\n";
    orders.forEach(o => {
        const activeStratName = window._straddleState?.activeStrategy?.name || "abc";
        let algoName = o.algo_name || activeStratName;
        if (algoName.toLowerCase() === "straddle" || algoName.toLowerCase() === "manual") {
            algoName = activeStratName;
        }
        const algo = algoName.replace(/"/g, '""');
        const oid = (o.order_id || "").replace(/"/g, '""');

        const side = (o.side || "SELL").replace(/"/g, '""');
        let catVal = o.category || (side.toUpperCase() === "SELL" ? "Target" : "Entry");
        if (catVal === "Manual") catVal = (side.toUpperCase() === "SELL" ? "Target" : "Entry");
        const cat = catVal.replace(/"/g, '""');

        const client = (o.client_id || "Kotak Neo Live").replace(/"/g, '""');
        const sym = (o.symbol || "").replace(/"/g, '""');
        const st = (o.status || "Complete").replace(/"/g, '""');
        const msg = (o.message || "").replace(/"/g, '""');
        const time = (o.time || "").replace(/"/g, '""');
        const qty = o.quantity || 65;
        const otype = (o.order_type || "Limit (L)").replace(/"/g, '""');

        let srcVal = o.source || "Straddle Pro";
        if (srcVal === "Quantiply" || srcVal === "Manual") {
            srcVal = "Straddle Pro";
        }
        const src = srcVal.replace(/"/g, '""');
        const price = parseFloat(o.price || 0).toFixed(2);
        const trigPrice = parseFloat(o.trigger_price || 0).toFixed(2);

        csv += `"${algo}","${oid}","${cat}","${client}","${sym}","${side}","${st}","${msg}","${time}",${qty},"${otype}","${src}",${price},${trigPrice}\n`;
    });

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `orderbook_${new Date().toISOString().split("T")[0]}.csv`;
    link.click();
}

async function renderStraddleMargin() {
    return loadStraddleMargin();
}

async function loadStraddleMargin(btn) {
    const container = document.getElementById("straddleMarginCardsContainer");
    if (!container) return;

    const refreshBtn = btn || document.getElementById("btnRefreshFunds");
    if (refreshBtn) {
        refreshBtn.disabled = true;
        refreshBtn.innerHTML = "⏳ Refreshing Funds...";
    }

    try {
        const resp = await fetch(`/api/straddle/margin?t=${Date.now()}`);
        const data = await resp.json();
        if (data && data.success && data.margin) {
            const m = data.margin;
            const availCash = (m.available_cash !== undefined && m.available_cash !== null) ? Number(m.available_cash) : ((m.available_margin !== undefined) ? Number(m.available_margin) : 0.0);
            const usedMargin = (m.used_margin !== undefined && m.used_margin !== null) ? Number(m.used_margin) : 0.0;
            const unavail = (m.unavailable_to_trade !== undefined && m.unavailable_to_trade !== null) ? Number(m.unavailable_to_trade) : 0.0;
            const collateral = Number(m.collateral_margin || 0.0);
            const payin = Number(m.payin || 0.0);
            const span = Number(m.span_margin || 0.0);
            const exposure = Number(m.exposure_margin || 0.0);
            const total = availCash + usedMargin;
            const utilPct = total > 0 ? ((usedMargin / total) * 100).toFixed(2) : "0.00";

            container.innerHTML = `
                <!-- Hero Available to Trade Banner (Matching Kotak Neo Funds Overview) -->
                <div style="background:linear-gradient(135deg, #0d1527 0%, #111e38 100%); border:1px solid rgba(59,130,246,0.25); border-radius:12px; padding:22px 28px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:16px; margin-bottom:18px; box-shadow:0 8px 24px rgba(0,0,0,0.35);">
                    <div>
                        <div style="font-size:12px; font-weight:700; color:#94a3b8; text-transform:uppercase; letter-spacing:0.8px; margin-bottom:6px;">Available to trade</div>
                        <div style="font-size:32px; font-weight:900; color:#ffffff; font-family:var(--font-mono); letter-spacing:-0.5px;">₹ ${availCash.toLocaleString('en-IN', {minimumFractionDigits: 2})}</div>
                        <div style="font-size:12px; color:#10b981; font-weight:600; margin-top:4px; display:flex; align-items:center; gap:6px;">
                            <span style="width:7px; height:7px; border-radius:50%; background:#10b981; display:inline-block;"></span>
                            Instant intraday & F&O execution limit active
                        </div>
                    </div>
                    <div style="display:flex; flex-direction:column; align-items:flex-end; gap:8px;">
                        <div style="background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.1); border-radius:8px; padding:8px 16px; text-align:right;">
                            <div style="font-size:11px; color:#94a3b8; font-weight:700;">Margin Utilization</div>
                            <div style="font-size:16px; font-weight:800; color:${usedMargin > 0 ? '#38bdf8' : '#64748b'}; font-family:var(--font-mono);">${utilPct}%</div>
                        </div>
                        <div style="width:160px; height:5px; background:rgba(255,255,255,0.1); border-radius:3px; overflow:hidden;">
                            <div style="width:${Math.max(2, Math.min(100, utilPct * 10))}%; height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:3px;"></div>
                        </div>
                    </div>
                </div>

                <!-- 4 Detailed Metric Cards Grid -->
                <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(240px, 1fr)); gap:14px; margin-bottom:18px;">
                    <!-- Card 1: Cash -->
                    <div style="background:#0c101a; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:18px; transition:transform 0.2s ease, border-color 0.2s ease;" onmouseenter="this.style.borderColor='rgba(59,130,246,0.4)'" onmouseleave="this.style.borderColor='rgba(255,255,255,0.08)'">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                            <span style="color:#94a3b8; font-size:12.5px; font-weight:700;">Cash Balance</span>
                            <span style="font-size:16px;">💵</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#ffffff; font-size:20px; margin-bottom:4px;">₹ ${availCash.toLocaleString('en-IN', {minimumFractionDigits: 2})}</div>
                        <div style="font-size:11.5px; color:#64748b;">Direct ledger funds available for trading</div>
                    </div>

                    <!-- Card 2: Margin from Pledge -->
                    <div style="background:#0c101a; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:18px; transition:transform 0.2s ease, border-color 0.2s ease;" onmouseenter="this.style.borderColor='rgba(139,92,246,0.4)'" onmouseleave="this.style.borderColor='rgba(255,255,255,0.08)'">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                            <span style="color:#94a3b8; font-size:12.5px; font-weight:700;">Margin from Pledge</span>
                            <span style="font-size:16px;">📈</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#cbd5e1; font-size:20px; margin-bottom:4px;">₹ ${collateral.toLocaleString('en-IN', {minimumFractionDigits: 2})}</div>
                        <div style="font-size:11.5px; color:#64748b;">Collateral from stock / ETF pledges</div>
                    </div>

                    <!-- Card 3: Margin Used -->
                    <div style="background:#0c101a; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:18px; transition:transform 0.2s ease, border-color 0.2s ease;" onmouseenter="this.style.borderColor='rgba(244,63,94,0.4)'" onmouseleave="this.style.borderColor='rgba(255,255,255,0.08)'">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                            <span style="color:#94a3b8; font-size:12.5px; font-weight:700;">Margin used</span>
                            <span style="font-size:16px;">⚡</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#f43f5e; font-size:20px; margin-bottom:4px;">₹ ${usedMargin.toLocaleString('en-IN', {minimumFractionDigits: 2})}</div>
                        <div style="font-size:11.5px; color:#64748b;">Margin used for your open transactions</div>
                    </div>

                    <!-- Card 4: Unavailable to trade -->
                    <div style="background:#0c101a; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:18px; transition:transform 0.2s ease, border-color 0.2s ease;" onmouseenter="this.style.borderColor='rgba(245,158,11,0.4)'" onmouseleave="this.style.borderColor='rgba(255,255,255,0.08)'">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:10px;">
                            <span style="color:#94a3b8; font-size:12.5px; font-weight:700;">Unavailable to trade</span>
                            <span style="font-size:16px;">🛑</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#f59e0b; font-size:20px; margin-bottom:4px;">₹ ${unavail.toLocaleString('en-IN', {minimumFractionDigits: 2})}</div>
                        <div style="font-size:11.5px; color:#64748b;">Funds in T+1 clearing / unsettled credits</div>
                    </div>
                </div>

                <!-- Margin Breakdown Grid (Reports & Account Statements section removed as requested) -->
                <div style="background:#0c101a; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:18px;">
                    <div style="font-size:13px; font-weight:800; color:#38bdf8; margin-bottom:14px; letter-spacing:0.3px;">F&O MARGIN BREAKDOWN</div>
                    <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(260px, 1fr)); gap:16px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:#070a13; border:1px solid rgba(255,255,255,0.04); border-radius:8px;">
                            <span style="color:#94a3b8; font-size:12.5px;">SPAN Margin (Regulatory):</span>
                            <span style="font-family:var(--font-mono); font-weight:700; color:#cbd5e1; font-size:13.5px;">₹ ${span.toLocaleString('en-IN', {minimumFractionDigits: 2})}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:#070a13; border:1px solid rgba(255,255,255,0.04); border-radius:8px;">
                            <span style="color:#94a3b8; font-size:12.5px;">Exposure Margin:</span>
                            <span style="font-family:var(--font-mono); font-weight:700; color:#cbd5e1; font-size:13.5px;">₹ ${exposure.toLocaleString('en-IN', {minimumFractionDigits: 2})}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:#070a13; border:1px solid rgba(255,255,255,0.04); border-radius:8px;">
                            <span style="color:#94a3b8; font-size:12.5px;">Payin Received Today:</span>
                            <span style="font-family:var(--font-mono); font-weight:700; color:#10b981; font-size:13.5px;">₹ ${payin.toLocaleString('en-IN', {minimumFractionDigits: 2})}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:10px 14px; background:linear-gradient(135deg, rgba(37,99,235,0.1), rgba(16,185,129,0.1)); border:1px solid rgba(59,130,246,0.3); border-radius:8px;">
                            <span style="color:#f8fafc; font-size:13px; font-weight:800;">Total Account Value:</span>
                            <span style="font-family:var(--font-mono); font-weight:900; color:#ffffff; font-size:15px;">₹ ${(availCash + usedMargin + unavail).toLocaleString('en-IN', {minimumFractionDigits: 2})}</span>
                        </div>
                    </div>
                </div>
            `;
        }
    } catch (e) {
        console.error("Error loading margin:", e);
    } finally {
        if (refreshBtn) {
            refreshBtn.disabled = false;
            refreshBtn.innerHTML = "🔄 Refresh Funds";
        }
    }
}


// =========================================================================
// STRATEGY BUILDER & LIVE POSITIONS ENGINE (SUB-VIEW 2)
// =========================================================================

function updateStraddleStrategyDropdown(strategies, activeStrat) {
    const sel = document.getElementById("straddleStrategySelect");
    if (!sel) return;
    
    const activeId = activeStrat ? activeStrat.strategy_id : null;

    if (sel.options.length !== strategies.length) {
        sel.innerHTML = "";
        strategies.forEach(s => {
            const opt = document.createElement("option");
            opt.value = s.strategy_id;
            opt.textContent = s.name || s.strategy_id;
            sel.appendChild(opt);
        });
    }

    if (activeId && sel.value !== activeId) {
        sel.value = activeId;
    }
}

window._straddlePendingLegEdits = window._straddlePendingLegEdits || {};

function markRunningLegChanged(idx) {
    const lotsInput = document.getElementById(`live_lots_${idx}`);
    const strikeInput = document.getElementById(`live_strike_${idx}`);
    const tgtModeSelect = document.getElementById(`live_tgt_mode_${idx}`);
    const tgtValInput = document.getElementById(`live_tgt_val_${idx}`);
    const slModeSelect = document.getElementById(`live_sl_mode_${idx}`);
    const slValInput = document.getElementById(`live_sl_val_${idx}`);

    window._straddlePendingLegEdits[idx] = {
        lots: lotsInput ? parseInt(lotsInput.value) || 1 : 1,
        strike: strikeInput ? strikeInput.value.trim() : "",
        target_mode: tgtModeSelect ? tgtModeSelect.value : "%",
        target_val: tgtValInput ? parseFloat(tgtValInput.value) : 25,
        sl_mode: slModeSelect ? slModeSelect.value : "%",
        sl_val: slValInput ? parseFloat(slValInput.value) : 15
    };

    const btn = document.getElementById(`btn_save_running_${idx}`);
    if (btn) {
        btn.style.display = "inline-flex";
    }

    // Dynamic preview of trigger prices
    const row = document.querySelector(`tr[data-leg-index="${idx}"]`);
    const entPriceEl = row ? row.querySelector(".straddle-ent-price-val") : null;
    const entPrice = entPriceEl ? parseFloat(entPriceEl.textContent) : 0;
    const sideBadge = row ? row.querySelector(".straddle-side-badge") : null;
    const side = sideBadge ? sideBadge.textContent.trim().toUpperCase() : "SELL";

    if (entPrice > 0) {
        const tVal = window._straddlePendingLegEdits[idx].target_val;
        const tMode = window._straddlePendingLegEdits[idx].target_mode;
        let tPrice = 0;
        if (tMode.includes("%")) {
            tPrice = (side === "SELL") ? entPrice * (1.0 - tVal / 100.0) : entPrice * (1.0 + tVal / 100.0);
        } else {
            tPrice = (side === "SELL") ? entPrice - tVal : entPrice + tVal;
        }
        const tgtSub = document.getElementById(`live_tgt_sub_${idx}`);
        if (tgtSub) tgtSub.textContent = `TGT: ₹${Math.max(0.05, tPrice).toFixed(2)}`;

        const sVal = window._straddlePendingLegEdits[idx].sl_val;
        const sMode = window._straddlePendingLegEdits[idx].sl_mode;
        let sPrice = 0;
        if (sMode.includes("%")) {
            sPrice = (side === "SELL") ? entPrice * (1.0 + sVal / 100.0) : entPrice * (1.0 - sVal / 100.0);
        } else {
            sPrice = (side === "SELL") ? entPrice + sVal : entPrice - sVal;
        }
        const slSub = document.getElementById(`live_sl_sub_${idx}`);
        if (slSub) slSub.textContent = `SL: ₹${Math.max(0.05, sPrice).toFixed(2)}`;
    }
}

function stepLiveStrikePrice(idx, direction) {
    const input = document.getElementById(`live_strike_${idx}`);
    if (!input) return;
    let curVal = parseInt(input.value.replace(/[^0-9]/g, '')) || 0;

    // Detect strike interval: BankNifty/Sensex = 100, MidcpNifty = 25, Nifty/Finnifty = 50
    let step = 50;
    const symEl = document.querySelector(`tr[data-leg-index="${idx}"] .straddle-sym-title`);
    const symText = (symEl ? symEl.textContent : "").toUpperCase();
    if (symText.includes("BANKNIFTY") || symText.includes("SENSEX") || symText.includes("BANKEX")) {
        step = 100;
    } else if (symText.includes("MIDCPNIFTY")) {
        step = 25;
    } else {
        step = 50;
    }

    if (curVal === 0) {
        curVal = 22500;
    }
    curVal += direction * step;
    input.value = curVal;
    markRunningLegChanged(idx);
}

function stepLiveLots(idx, direction) {
    const input = document.getElementById(`live_lots_${idx}`);
    if (!input) return;
    let curVal = parseInt(input.value) || 1;
    curVal = Math.max(1, Math.min(100, curVal + direction));
    input.value = curVal;
    markRunningLegChanged(idx);
}

async function saveRunningPositionChanges(stratId, legId, legIndex) {
    const lotsInput = document.getElementById(`live_lots_${legIndex}`);
    const strikeInput = document.getElementById(`live_strike_${legIndex}`);
    const tgtValInput = document.getElementById(`live_tgt_val_${legIndex}`);
    const tgtModeSelect = document.getElementById(`live_tgt_mode_${legIndex}`);
    const slValInput = document.getElementById(`live_sl_val_${legIndex}`);
    const slModeSelect = document.getElementById(`live_sl_mode_${legIndex}`);

    const newLots = lotsInput ? parseInt(lotsInput.value) || 1 : null;
    const newStrike = strikeInput ? strikeInput.value.trim() : null;
    const targetVal = tgtValInput ? parseFloat(tgtValInput.value) : null;
    const targetMode = tgtModeSelect ? tgtModeSelect.value : "%";
    const slVal = slValInput ? parseFloat(slValInput.value) : null;
    const slMode = slModeSelect ? slModeSelect.value : "%";

    showStraddleToast(`Saving updates for leg ${legIndex + 1}...`, "info", 2000);
    try {
        const payload = {
            strategy_id: stratId || window._straddleState?.activeStrategy?.strategy_id,
            leg_id: legId || null,
            leg_index: legIndex,
            new_lots: newLots,
            new_strike: newStrike,
            target_val: targetVal,
            target_mode: targetMode,
            sl_val: slVal,
            sl_mode: slMode
        };

        const resp = await fetch("/api/straddle/update_running_position", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();
        if (res.success) {
            delete window._straddlePendingLegEdits[legIndex];
            const btn = document.getElementById(`btn_save_running_${legIndex}`);
            if (btn) btn.style.display = "none";
            showStraddleToast(res.message || "Position updated successfully!", "success", 3000);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to update position", "error", 4000);
        }
    } catch (e) {
        showStraddleToast("Error updating position: " + e.message, "error", 3000);
    }
}

async function detachRunningPosition(stratId, legId, legIndex) {
    if (!confirm("Detach this running position from the algo?\n\nNOTE: The open position will NOT be squared off on Kotak Neo broker. It will simply be removed from this algo's monitoring.")) {
        return;
    }
    showStraddleToast(`Detaching leg ${legIndex + 1} from algo...`, "info", 1500);
    try {
        const resp = await fetch("/api/straddle/detach_position", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                strategy_id: stratId || window._straddleState?.activeStrategy?.strategy_id,
                leg_id: legId || null,
                leg_index: legIndex
            })
        });
        const res = await resp.json();
        if (res.success) {
            delete window._straddlePendingLegEdits[legIndex];
            showStraddleToast(res.message || "Position detached from algo successfully.", "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to detach position", "error", 4000);
        }
    } catch (e) {
        showStraddleToast("Error detaching position: " + e.message, "error", 3000);
    }
}

async function retrySingleStraddleLegInline(stratId, legId, legIndex) {
    const lotsInput = document.getElementById(`err_lots_${legIndex}`);
    const strikeInput = document.getElementById(`err_strike_${legIndex}`);
    const newLots = lotsInput ? parseInt(lotsInput.value) || 1 : 1;
    const newStrike = strikeInput ? strikeInput.value.trim() : "";

    showStraddleToast(`Retrying leg ${legIndex + 1} with Lots: ${newLots}, Strike: ${newStrike || 'Auto'}...`, "info", 2000);
    try {
        const payload = {
            strategy_id: stratId || window._straddleState?.activeStrategy?.strategy_id,
            leg_ids: legId ? [legId] : [],
            leg_indices: [legIndex],
            updated_legs: {}
        };
        const legKey = legId || String(legIndex);
        payload.updated_legs[legKey] = {
            lots: newLots,
            strike: newStrike,
            strike_price: newStrike,
            strike_selection: newStrike
        };
        payload.updated_legs[legIndex] = payload.updated_legs[legKey];

        const resp = await fetch("/api/straddle/retry_legs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || "Order retried successfully!", "success", 3000);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Retry failed", "error", 4000);
            await fetchStraddleState();
        }
    } catch (e) {
        showStraddleToast("Error retrying leg: " + e.message, "error", 3000);
    }
}

async function squareOffSingleLegIndex(legIndex) {
    if (!confirm(`Are you sure you want to exit and square off this leg?`)) return;
    showStraddleToast(`Exiting leg ${legIndex + 1}...`, "info", 1500);
    try {
        const resp = await fetch("/api/straddle/square_off_leg", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ leg_index: legIndex })
        });
        const data = await resp.json();
        if (data && data.success) {
            showStraddleToast(`✅ Leg ${legIndex + 1} exited successfully.`, "success", 2000);
        } else {
            showStraddleToast(data.error || "Failed to exit leg", "error", 3000);
        }
        await fetchStraddleState();
        await loadStraddleOpenPositions();
    } catch (e) {
        showStraddleToast("Error exiting leg: " + e.message, "error", 3000);
    }
}

function renderStraddlePositions(positions, groupMtm, totalMtm) {
    const tbody = document.getElementById("straddlePositionsTableBody");
    if (!tbody) return;

    if (!positions || positions.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="13" style="text-align:center; padding:36px 14px; color:#64748b;">
                    <div style="font-size:14px; font-weight:700; color:#cbd5e1; margin-bottom:4px;">No Active Positions</div>
                    <div style="font-size:11.5px;">Click <b>Modify Settings</b> to configure legs and start trading, or hit <b>Start / Enable Algo</b>.</div>
                </td>
            </tr>
        `;
    } else {
        const prevMap = window._straddleState.prevLtpMap || {};

        // Check if we can perform smooth in-place DOM update instead of destroying innerHTML
        const existingRows = tbody.querySelectorAll("tr[data-leg-index]:not(.straddle-error-banner-row)");
        const hasErrors = positions.some(p => (String(p.status || "").toUpperCase() === "ERROR" || !!p.error_message || !!p.sl_cancelled_unexpectedly));
        const canUpdateInPlace = (
            existingRows.length === positions.length &&
            positions.length > 0 &&
            !hasErrors &&
            !tbody.querySelector(".straddle-error-banner-row")
        );

        if (canUpdateInPlace) {
            positions.forEach((pos, idx) => {
                const row = tbody.querySelector(`tr[data-leg-index="${idx}"]`);
                if (!row) return;

                const sym = pos.symbol || pos.trading_symbol || 'NIFTY OPT';
                const side = String(pos.side || "SELL").toUpperCase();
                const rawStatus = String(pos.status || "OPEN").toUpperCase();
                const isOpen = (rawStatus === "OPEN" || rawStatus === "LIVE OPEN");
                const ltp = Number(pos.ltp || pos.avg_ent_price || 0.0);
                const prevLtp = prevMap[sym] || ltp;
                let ltpClass = "";
                if (ltp > prevLtp) ltpClass = "straddle-ltp-up";
                else if (ltp < prevLtp) ltpClass = "straddle-ltp-down";
                prevMap[sym] = ltp;

                const ltpColor = !isOpen ? '#f87171' : (side === 'SELL' ? (ltp <= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171') : (ltp >= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171'));
                const mtm = Number(pos.mtm || 0.0);
                const mtmClass = (mtm >= 0) ? "straddle-mtm-pos" : "straddle-mtm-neg";

                // Update LTP cell
                const ltpEl = row.querySelector(".straddle-ltp");
                if (ltpEl) {
                    ltpEl.textContent = ltp > 0 ? ltp.toFixed(2) : "-";
                    ltpEl.style.color = ltpColor;
                    ltpEl.className = `straddle-ltp ${ltpClass}`;
                }

                // Update MTM cell
                const mtmEl = row.querySelector(".straddle-mtm");
                if (mtmEl) {
                    mtmEl.textContent = mtm.toFixed(2);
                    mtmEl.className = `straddle-mtm ${mtmClass}`;
                }

                // Check pending edits or active focus
                const hasPending = !!(window._straddlePendingLegEdits && window._straddlePendingLegEdits[idx]);
                const activeEl = document.activeElement;
                const isFocusedInRow = activeEl && row.contains(activeEl);

                const btnSave = document.getElementById(`btn_save_running_${idx}`);
                if (hasPending) {
                    if (btnSave) btnSave.style.display = "inline-flex";
                } else if (!isFocusedInRow) {
                    if (btnSave) btnSave.style.display = "none";
                    const tgtSub = document.getElementById(`live_tgt_sub_${idx}`);
                    if (tgtSub && pos.target_price) tgtSub.textContent = `TGT: ₹${Number(pos.target_price).toFixed(2)}`;
                    const slSub = document.getElementById(`live_sl_sub_${idx}`);
                    if (slSub && pos.sl_price) slSub.textContent = `SL: ₹${Number(pos.sl_price).toFixed(2)}`;
                }
            });
        } else {
            // Full render with preserved pending edits
            let html = "";
            positions.forEach((pos, idx) => {
                const rawStatus = String(pos.status || "OPEN").toUpperCase();
                const isError = (rawStatus === "ERROR" || !!pos.error_message);
                const isCompleteOrClosed = (rawStatus === "COMPLETE" || rawStatus === "CLOSED" || rawStatus === "EXITED" || (pos.avg_exit_price && Number(pos.avg_exit_price) > 0 && pos.exit_reason && pos.exit_reason !== "-"));
                const isOpen = !isError && !isCompleteOrClosed && (rawStatus === "OPEN" || rawStatus === "LIVE OPEN");
                const isClosed = !isError && (isCompleteOrClosed || (!isOpen && !isError));
                const isReEntry = !!(pos.is_re_entry || pos.re_entry || (pos.re_execute_count && pos.re_execute_count > 0));
                const stratId = pos.strategy_id || window._straddleState?.activeStrategy?.strategy_id || '';

                // Retrieve any pending user edits for this leg so they are never lost
                const pending = (window._straddlePendingLegEdits && window._straddlePendingLegEdits[idx]) || null;
                const hasPending = !!pending;

                // Status Badge
                let statusHtml = "";
                if (isError) {
                    statusHtml = `
                        <div class="straddle-status-container">
                            <span class="straddle-status-pill status-badge-error">ERROR</span>
                            ${isReEntry ? '<span class="status-badge-reentry">RE-ENTRY</span>' : ''}
                        </div>
                    `;
                } else if (isOpen) {
                    statusHtml = `
                        <div class="straddle-status-container">
                            <span class="straddle-status-pill status-badge-open">OPEN</span>
                            ${isReEntry ? '<span class="status-badge-reentry">RE-ENTRY</span>' : ''}
                        </div>
                    `;
                } else {
                    statusHtml = `
                        <div class="straddle-status-container">
                            <span class="straddle-status-pill status-badge-closed">CLOSED</span>
                            ${isReEntry ? '<span class="status-badge-reentry">RE-ENTRY</span>' : ''}
                        </div>
                    `;
                }

                // Side Badge
                const side = String(pos.side || "SELL").toUpperCase();
                const sideClass = (side === "BUY") ? "side-badge-buy" : "side-badge-sell";

                // Symbol Styling & Accent Border
                const sym = pos.symbol || pos.trading_symbol || 'NIFTY OPT';
                let symAccentClass = "sym-ce";
                if (side === "BUY") {
                    symAccentClass = "sym-buy";
                } else if (sym.toUpperCase().includes("PE") || (pos.option_type && String(pos.option_type).toUpperCase() === "PE")) {
                    symAccentClass = "sym-pe";
                } else {
                    symAccentClass = "sym-ce";
                }

                // Expiry subtext
                let expiryText = pos.contract ? (pos.contract.expiry_date || 'NFO') : (pos.expiry_str || pos.expiry || 'NFO');
                if (expiryText && !String(expiryText).toUpperCase().startsWith("NFO")) {
                    expiryText = "NFO " + expiryText;
                }

                // Quantity column
                const lots = pending ? pending.lots : (pos.lots || 1);
                const totalQty = pos.quantity || 65;
                const entryQty = pos.entry_qty || totalQty;
                const exitQty = (isClosed || (pos.exit_qty && pos.exit_qty > 0)) ? (pos.exit_qty || totalQty) : "-";

                // Quantity HTML (Interactive with stepper arrows for OPEN legs)
                let qtyHtml = "";
                if (isOpen) {
                    qtyHtml = `
                        <div style="display:flex; align-items:center; gap:4px;">
                            <div class="straddle-stepper-box" style="height:28px;">
                                <input type="number" id="live_lots_${idx}" class="straddle-stepper-input" style="width:36px; font-size:12px; font-weight:700;" value="${lots}" min="1" max="100" oninput="markRunningLegChanged(${idx})" title="Change lots: increase to buy/sell more, decrease to reduce">
                                <div class="straddle-stepper-arrows" style="width:18px;">
                                    <button type="button" class="lot-step-btn" onclick="stepLiveLots(${idx}, 1)" title="Lots +">▲</button>
                                    <button type="button" class="lot-step-btn" onclick="stepLiveLots(${idx}, -1)" title="Lots -">▼</button>
                                </div>
                            </div>
                            <span style="font-size:10.5px; color:#94a3b8; font-weight:700;">lots</span>
                        </div>
                        <div class="straddle-subtext">Total: ${totalQty} qty</div>
                        <div class="straddle-subtext">Entry: ${entryQty}</div>
                    `;
                } else {
                    qtyHtml = `
                        <div style="font-weight:700; color:#f8fafc; font-family:var(--font-mono);">${lots} (${totalQty})</div>
                        <div class="straddle-subtext">Entry Qty: ${entryQty}</div>
                        <div class="straddle-subtext">Exit Qty: ${exitQty}</div>
                    `;
                }

                // Symbol HTML (With shift strike option and step arrows for OPEN legs)
                let symHtml = "";
                const curStrikeMatch = pending ? pending.strike : (pos.strike_price || (sym.match(/\d{5}/) || [''])[0]);
                if (isOpen) {
                    symHtml = `
                        <div class="straddle-sym-card ${symAccentClass}">
                            <div class="straddle-sym-title">${sym}</div>
                            <div class="straddle-sym-sub">${expiryText}</div>
                            <div style="display:flex; align-items:center; gap:4px; margin-top:4px;">
                                <span style="font-size:9.5px; color:#94a3b8; font-weight:700;">Shift:</span>
                                <div class="straddle-stepper-box" style="height:28px;">
                                    <input type="text" id="live_strike_${idx}" class="straddle-stepper-input" style="width:58px; font-size:11.5px; font-weight:700;" value="${curStrikeMatch}" placeholder="Strike" oninput="markRunningLegChanged(${idx})" title="Enter or step strike price">
                                    <div class="straddle-stepper-arrows" style="width:18px;">
                                        <button type="button" class="strike-step-btn" onclick="stepLiveStrikePrice(${idx}, 1)" title="Strike +">▲</button>
                                        <button type="button" class="strike-step-btn" onclick="stepLiveStrikePrice(${idx}, -1)" title="Strike -">▼</button>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
                } else {
                    symHtml = `
                        <div class="straddle-sym-card ${symAccentClass}">
                            <div class="straddle-sym-title">${sym}</div>
                            <div class="straddle-sym-sub">${expiryText}</div>
                        </div>
                    `;
                }

                // Avg Entry Price & Entry Time
                const avgEntPrice = pos.avg_ent_price ? Number(pos.avg_ent_price).toFixed(2) : '-';
                const entryTime = pos.entry_time || '-';

                // Per Leg Target (Interactive for OPEN legs)
                let targetHtml = "-";
                if (isOpen) {
                    const isPts = pending ? (pending.target_mode === "₹" || pending.target_mode.includes("Pts")) : (pos.target_type && (pos.target_type.includes("Pts") || pos.target_type.includes("₹")));
                    const tVal = pending ? pending.target_val : (pos.target_value !== undefined ? pos.target_value : 25);
                    targetHtml = `
                        <div style="display:flex; align-items:center; gap:3px;">
                            <select id="live_tgt_mode_${idx}" class="straddle-select" style="padding:2px 3px; font-size:10.5px; width:45px;" onchange="markRunningLegChanged(${idx})">
                                <option value="%" ${!isPts ? 'selected' : ''}>%</option>
                                <option value="₹" ${isPts ? 'selected' : ''}>₹</option>
                            </select>
                            <input type="number" id="live_tgt_val_${idx}" class="straddle-input" style="width:54px; padding:2px 4px; font-size:11px; font-weight:700;" value="${tVal}" step="0.5" oninput="markRunningLegChanged(${idx})" title="Change Target (% or ₹ points)">
                        </div>
                        <div id="live_tgt_sub_${idx}" style="font-size:10px; color:#38bdf8; font-weight:700; margin-top:2px;">TGT: ₹${pos.target_price ? Number(pos.target_price).toFixed(2) : '-'}</div>
                    `;
                } else if (pos.target_price && pos.target_price > 0) {
                    const tgtLabel = pos.target_type_label || (pos.target_value ? `TGT: ${pos.target_value}%` : 'TGT: 45%');
                    targetHtml = `<div><span class="tgt-label-badge">${tgtLabel}</span> <span class="straddle-val-badge">${Number(pos.target_price).toFixed(2)}</span></div>`;
                } else if (pos.target_value && pos.target_value > 0) {
                    const tgtLabel = `TGT: ${pos.target_value}%`;
                    targetHtml = `<div><span class="tgt-label-badge">${tgtLabel}</span> <span class="straddle-val-badge">-</span></div>`;
                }

                // Per Leg Stoploss (Interactive for OPEN legs)
                let slHtml = "-";
                if (isOpen) {
                    const isPts = pending ? (pending.sl_mode === "₹" || pending.sl_mode.includes("Pts")) : (pos.sl_type && (pos.sl_type.includes("Pts") || pos.sl_type.includes("₹")));
                    const sVal = pending ? pending.sl_val : (pos.sl_value !== undefined ? pos.sl_value : 15);
                    slHtml = `
                        <div style="display:flex; align-items:center; gap:3px;">
                            <select id="live_sl_mode_${idx}" class="straddle-select" style="padding:2px 3px; font-size:10.5px; width:45px;" onchange="markRunningLegChanged(${idx})">
                                <option value="%" ${!isPts ? 'selected' : ''}>%</option>
                                <option value="₹" ${isPts ? 'selected' : ''}>₹</option>
                            </select>
                            <input type="number" id="live_sl_val_${idx}" class="straddle-input" style="width:54px; padding:2px 4px; font-size:11px; font-weight:700;" value="${sVal}" step="0.5" oninput="markRunningLegChanged(${idx})" title="Change Stoploss (% or ₹ points)">
                        </div>
                        <div id="live_sl_sub_${idx}" style="font-size:10px; color:#f87171; font-weight:700; margin-top:2px;">SL: ₹${pos.sl_price ? Number(pos.sl_price).toFixed(2) : '-'}</div>
                    `;
                } else if (pos.sl_price && pos.sl_price > 0) {
                    const slLabel = pos.sl_type_label || (pos.sl_value ? `SL: -${pos.sl_value}%` : 'SL: -65%');
                    const origSlLabel = pos.original_sl_label || (pos.sl_value ? `ORIGINAL SL: -${pos.sl_value}%` : 'ORIGINAL SL: -65%');
                    slHtml = `<div><span class="sl-label-badge">${slLabel}</span> <span class="straddle-val-badge">${Number(pos.sl_price).toFixed(2)}</span></div>
                              <div class="orig-sl-sub">${origSlLabel}</div>`;
                } else if (pos.sl_value && pos.sl_value > 0) {
                    const slLabel = `SL: -${pos.sl_value}%`;
                    const origSlLabel = `ORIGINAL SL: -${pos.sl_value}%`;
                    slHtml = `<div><span class="sl-label-badge">${slLabel}</span> <span class="straddle-val-badge">-</span></div>
                              <div class="orig-sl-sub">${origSlLabel}</div>`;
                }

                // Avg Exit Price & Exit Time
                const avgExitPrice = (pos.avg_exit_price && pos.avg_exit_price > 0) ? Number(pos.avg_exit_price).toFixed(2) : '-';
                const exitTime = (isClosed && pos.exit_time) ? pos.exit_time : (pos.avg_exit_price && pos.avg_exit_price > 0 ? (pos.exit_time || '-') : '-');

                // Exit Reason
                let exitReasonHtml = "-";
                if (pos.exit_reason && pos.exit_reason !== "-") {
                    const rLower = String(pos.exit_reason).toLowerCase();
                    if (rLower.includes("target")) {
                        exitReasonHtml = `<span class="exit-reason-tgt">${pos.exit_reason}</span>`;
                    } else if (rLower.includes("stoploss") || rLower.includes("sl")) {
                        exitReasonHtml = `<span class="exit-reason-sl">${pos.exit_reason}</span>`;
                    } else {
                        exitReasonHtml = `<span class="straddle-reason-pill">${pos.exit_reason}</span>`;
                    }
                }

                // Action buttons (Save on live change + Exit + Detach)
                let actionHtml = `<span style="color:#64748b; font-size:11px;">-</span>`;
                if (isOpen) {
                    actionHtml = `
                        <div style="display:flex; flex-direction:column; gap:4px; align-items:center;">
                            <button type="button" id="btn_save_running_${idx}" class="straddle-btn btn-primary" style="${hasPending ? 'display:inline-flex;' : 'display:none;'} padding:3px 8px; font-size:11px; font-weight:800; background:#0ea5e9; border:none; box-shadow:0 0 8px rgba(14,165,233,0.6);" onclick="saveRunningPositionChanges('${stratId}', '${pos.id || ''}', ${idx})" title="Save and apply live modifications immediately">
                                💾 Save
                            </button>
                            <div style="display:flex; align-items:center; gap:4px;">
                                <button type="button" class="straddle-btn btn-danger" style="padding:2px 7px; font-size:10.5px; font-weight:700;" onclick="squareOffSingleLegIndex(${idx})" title="Square off this leg on broker">
                                    Exit
                                </button>
                                <button type="button" class="btn-detach" onclick="detachRunningPosition('${stratId}', '${pos.id || ''}', ${idx})" title="Detach from algo WITHOUT squaring off on broker">
                                    🔓 Detach
                                </button>
                            </div>
                        </div>
                    `;
                }

                // LTP & Dynamic Color Tick
                const ltp = Number(pos.ltp || pos.avg_ent_price || 0.0);
                const prevLtp = prevMap[sym] || ltp;
                let ltpClass = "";
                if (ltp > prevLtp) ltpClass = "straddle-ltp-up";
                else if (ltp < prevLtp) ltpClass = "straddle-ltp-down";
                prevMap[sym] = ltp;

                const ltpColor = isClosed ? '#f87171' : (side === 'SELL' ? (ltp <= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171') : (ltp >= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171'));

                // MTM (₹)
                const mtm = Number(pos.mtm || 0.0);
                const mtmClass = (mtm >= 0) ? "straddle-mtm-pos" : "straddle-mtm-neg";
                const mtmFormatted = mtm.toFixed(2);

                const rowClass = isError ? "straddle-row-error" : "";

                // Check if rejected toast needs to be shown
                if (isError) {
                    if (!window._straddleToastedErrors) window._straddleToastedErrors = new Set();
                    const errKey = `${sym}_${pos.error_message || ''}_${pos.entry_time || ''}`;
                    if (!window._straddleToastedErrors.has(errKey)) {
                        window._straddleToastedErrors.add(errKey);
                        showStraddleRejectedToast({
                            symbol: sym,
                            message: pos.error_message || "You need to add required funds in your account to place this trade.",
                            clientId: pos.client_id || (window._straddleState.activeAccount ? window._straddleState.activeAccount.id : 'Kotak Neo'),
                            orderId: pos.order_id || ('261' + Math.floor(Math.random() * 1000000000000)),
                            time: pos.entry_time || (new Date().toLocaleTimeString('en-US', { hour12: false }))
                        });
                    }
                }

                const isChecked = window._straddleSelectedLegIndices.has(idx) || (pos.id && window._straddleSelectedLegIds.has(pos.id));

                html += `
                    <tr class="${rowClass}" data-leg-index="${idx}" data-leg-id="${pos.id || ''}">
                        <td style="text-align:center;">
                            <input type="checkbox" class="straddle-checkbox straddle-leg-checkbox" data-index="${idx}" data-leg-id="${pos.id || ''}" data-status="${isError ? 'ERROR' : (isOpen ? 'OPEN' : 'CLOSED')}" ${isChecked ? 'checked' : ''} onchange="onStraddlePositionCheckboxChange(this, ${idx}, '${pos.id || ''}')">
                        </td>
                        <td>${statusHtml}</td>
                        <td><span class="straddle-side-badge ${sideClass}">${side}</span></td>
                        <td>${symHtml}</td>
                        <td>${qtyHtml}</td>
                        <td>
                            <div class="straddle-ent-price-val" style="font-weight:700; color:#f8fafc; font-family:var(--font-mono);">${avgEntPrice}</div>
                            <div class="straddle-subtext">${entryTime}</div>
                        </td>
                        <td>${targetHtml}</td>
                        <td>${slHtml}</td>
                        <td>
                            <div style="font-weight:700; color:#f8fafc; font-family:var(--font-mono);">${avgExitPrice}</div>
                            ${exitTime !== '-' ? `<div class="straddle-subtext">${exitTime}</div>` : ''}
                        </td>
                        <td>${exitReasonHtml}</td>
                        <td style="text-align:right;">
                            <span class="straddle-ltp ${ltpClass}" style="color:${ltpColor}; font-weight:700; font-family:var(--font-mono);">${ltp > 0 ? ltp.toFixed(2) : '-'}</span>
                        </td>
                        <td style="text-align:right;">
                            <span class="straddle-mtm ${mtmClass}">${mtmFormatted}</span>
                        </td>
                        <td style="text-align:center;">${actionHtml}</td>
                    </tr>
                `;

                // Quantiply Unexpected SL Cancellation Warning Banner (for OPEN legs)
                if (pos.sl_cancelled_unexpectedly && !isError) {
                    const legId = pos.id || '';
                    html += `
                        <tr class="straddle-error-banner-row">
                            <td colspan="13">
                                <div class="straddle-error-banner-cell" style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px; background:rgba(245, 158, 11, 0.12); border:1px solid rgba(245, 158, 11, 0.45); padding:10px 16px; border-radius:6px; margin: 4px 0;">
                                    <div style="flex:1; min-width:280px;">
                                        <div style="display:flex; align-items:center; gap:8px;">
                                            <span style="font-size:16px;">⚠️</span>
                                            <span style="color:#f87171; font-weight:800; font-size:12px;">Stoploss order cancelled by the Client or Broker. Cancellation reason not received from the Broker</span>
                                        </div>
                                        <div style="color:#fbbf24; font-size:11px; margin-top:4px; font-style:italic; line-height:1.4;">
                                            You can use the Individual leg RETRY functionality to place the SL order again. If not, the algo will handle this unexpected Cancellation by monitoring the price & exiting this position when Stoploss or Target is hit.
                                        </div>
                                    </div>
                                    <div style="display:flex; align-items:center; gap:8px;">
                                        <button type="button" class="algo-btn-retry" style="background:linear-gradient(135deg, #0284c7, #0ea5e9); color:#ffffff; font-weight:700; font-size:11px; padding:6px 14px; border-radius:5px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:5px; box-shadow:0 2px 6px rgba(14,165,233,0.4);" onclick="retrySingleStraddleLeg('${stratId}', '${legId}', ${idx})" title="Place a fresh Stoploss order on broker for this leg">
                                            🔄 RETRY SL ORDER
                                        </button>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
                }

                // If error, append error banner directly underneath row with direct action buttons (Retry with adjusted Settings / Rectify)
                if (isError) {
                    const legId = pos.id || '';
                    const errDetail = pos.error_message || pos.error || "Order placement rejected by broker / exchange RMS.";
                    const marginInfo = parseMarginError(errDetail);
                    let marginBadgeHtml = "";
                    if (marginInfo && marginInfo.hasMarginDetails) {
                        marginBadgeHtml = `
                            <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-top:4px;">
                                <span style="background:rgba(56,189,248,0.15); color:#38bdf8; font-weight:700; font-size:11px; padding:2px 8px; border-radius:4px; font-family:var(--font-mono);">💳 Available: ₹${marginInfo.available.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}</span>
                                <span style="background:rgba(245,158,11,0.15); color:#fbbf24; font-weight:700; font-size:11px; padding:2px 8px; border-radius:4px; font-family:var(--font-mono);">⚡ Required: ₹${marginInfo.required.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}</span>
                                <span style="background:rgba(239,68,68,0.22); color:#f87171; font-weight:700; font-size:11px; padding:2px 8px; border-radius:4px; font-family:var(--font-mono);">🔴 Shortfall: ₹${marginInfo.shortfall.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}</span>
                            </div>
                        `;
                    }
                    const curStrikeMatch = (pos.strike_price || (sym.match(/\d{5}/) || [''])[0]);
                    html += `
                        <tr class="straddle-error-banner-row">
                            <td colspan="13">
                                <div class="straddle-error-banner-cell" style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:10px; background:rgba(239, 68, 68, 0.12); border:1px solid rgba(239, 68, 68, 0.35); padding:10px 14px; border-radius:6px; margin: 4px 0;">
                                    <div style="display:flex; flex-direction:column; gap:3px; flex:1; min-width:260px;">
                                        <div style="display:flex; align-items:center; gap:8px;">
                                            <span class="straddle-error-banner-icon" style="font-size:16px;">⚠️</span>
                                            <span class="straddle-error-banner-text" style="color:#fca5a5; font-size:11.5px; font-weight:600; line-height:1.4;">
                                                ${errDetail}
                                            </span>
                                        </div>
                                        ${marginBadgeHtml}
                                    </div>
                                    <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; background:rgba(0,0,0,0.3); padding:6px 10px; border-radius:6px; border:1px solid rgba(255,255,255,0.08);">
                                        <div style="display:flex; align-items:center; gap:4px;">
                                            <span style="font-size:11px; color:#cbd5e1; font-weight:700;">Lots:</span>
                                            <input type="number" id="err_lots_${idx}" class="straddle-input" style="width:52px; padding:3px 6px; font-size:11.5px; font-weight:700; text-align:center;" value="${pos.lots || 1}" min="1" max="100" title="Adjust lots to fit margin">
                                        </div>
                                        <div style="display:flex; align-items:center; gap:4px;">
                                            <span style="font-size:11px; color:#cbd5e1; font-weight:700;">Strike:</span>
                                            <input type="text" id="err_strike_${idx}" class="straddle-input" style="width:80px; padding:3px 6px; font-size:11.5px; font-weight:700;" value="${curStrikeMatch}" placeholder="e.g. 23250" title="Adjust strike to fit margin">
                                        </div>
                                        <button type="button" class="algo-btn-retry" style="background:#10b981; color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:4px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:4px; box-shadow:0 1px 3px rgba(0,0,0,0.3);" onclick="retrySingleStraddleLegInline('${stratId}', '${legId}', ${idx})" title="Retry order with updated lots and strike">
                                            🔄 Retry with Settings
                                        </button>
                                        <button type="button" class="algo-btn-retry" style="background:#ef4444; color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:4px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:4px; box-shadow:0 1px 3px rgba(0,0,0,0.3);" onclick="retrySingleStraddleLeg('${stratId}', '${legId}', ${idx})" title="Immediately retry placing this order on broker">
                                            Retry Original
                                        </button>
                                        <button type="button" class="algo-btn-rectify" style="background:#2563eb; color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:4px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:4px; box-shadow:0 1px 3px rgba(0,0,0,0.3);" onclick="openRectifyLegModal('${stratId}', '${legId}', ${idx})" title="Edit lots, strike price, product or configuration">
                                            🛠️ Rectify Leg
                                        </button>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
                }
            });
            tbody.innerHTML = html;
        }
    }

    // Render MTM Banner Strip
    const grpMtmEl = document.getElementById("straddleGroupMtmDisplay");
    const totMtmEl = document.getElementById("straddleTotalMtmDisplay");
    if (grpMtmEl) {
        const gm = Number(groupMtm || 0.0);
        grpMtmEl.textContent = gm.toFixed(2);
        grpMtmEl.style.color = (gm >= 0) ? "#38bdf8" : "#f43f5e";
    }
    if (totMtmEl) {
        const tm = Number(totalMtm || 0.0);
        totMtmEl.textContent = tm.toFixed(2);
        totMtmEl.style.color = (tm >= 0) ? "#10b981" : "#f43f5e";
    }

    // Sync master select all checkbox state
    const masterCb = document.getElementById("straddleSelectAllLegs");
    if (masterCb) {
        masterCb.checked = (positions && positions.length > 0 && positions.every((p, i) => window._straddleSelectedLegIndices.has(i) || (p.id && window._straddleSelectedLegIds.has(p.id))));
    }

    // Update action buttons state
    onStraddlePositionCheckboxChange();
}

function onStraddlePositionCheckboxChange(inputEl = null, idx = null, legId = null) {
    if (inputEl !== null && idx !== null) {
        const numIdx = parseInt(idx, 10);
        if (inputEl.checked) {
            if (!isNaN(numIdx)) window._straddleSelectedLegIndices.add(numIdx);
            if (legId) window._straddleSelectedLegIds.add(legId);
        } else {
            if (!isNaN(numIdx)) window._straddleSelectedLegIndices.delete(numIdx);
            if (legId) window._straddleSelectedLegIds.delete(legId);
        }
    }

    const masterCb = document.getElementById("straddleSelectAllLegs");
    const allLegCbs = document.querySelectorAll(".straddle-leg-checkbox");
    if (masterCb) {
        masterCb.checked = (allLegCbs.length > 0 && Array.from(allLegCbs).every(cb => cb.checked));
    }

    const allChecked = document.querySelectorAll(".straddle-leg-checkbox:checked");
    const errorChecked = document.querySelectorAll(".straddle-leg-checkbox:checked[data-status='ERROR']");
    const totalChecked = allChecked.length;
    const errorCount = errorChecked.length;

    const allErrorsInStrat = document.querySelectorAll(".straddle-leg-checkbox[data-status='ERROR']");
    const totalErrorsInStrat = allErrorsInStrat.length;

    const openChecked = document.querySelectorAll(".straddle-leg-checkbox:checked[data-status='OPEN']");
    const openCount = openChecked.length;

    const btnRetry = document.getElementById("btnStraddleRetryErrors");
    const btnReset = document.getElementById("btnStraddleResetAlgo");
    const btnSquareOffSel = document.getElementById("btnStraddleSquareOffSelected");

    if (btnRetry) {
        if (errorCount > 0) {
            btnRetry.style.display = "inline-flex";
            btnRetry.innerHTML = `<span>🔄 Retry Selected (${errorCount})</span>`;
            btnRetry.onclick = retrySelectedStraddleLegs;
        } else if (totalErrorsInStrat > 0) {
            btnRetry.style.display = "inline-flex";
            btnRetry.innerHTML = `<span>🔄 Retry Failed (${totalErrorsInStrat})</span>`;
            btnRetry.onclick = retryAllFailedStraddleLegs;
        } else {
            btnRetry.style.display = "none";
        }
    }

    if (btnReset) {
        btnReset.style.display = "inline-flex";
        if (totalErrorsInStrat > 0 || errorCount > 0) {
            btnReset.style.background = "#7c3aed";
            btnReset.style.borderColor = "#8b5cf6";
            btnReset.style.color = "#ffffff";
        }
    }

    if (btnSquareOffSel) {
        if (openCount > 0) {
            btnSquareOffSel.innerHTML = `<span>Square off selected (${openCount})</span>`;
            btnSquareOffSel.style.borderColor = "#f43f5e";
            btnSquareOffSel.style.color = "#f43f5e";
            btnSquareOffSel.style.fontWeight = "700";
        } else {
            btnSquareOffSel.innerHTML = `<span>Square off selected legs</span>`;
            btnSquareOffSel.style.borderColor = "rgba(255,255,255,0.15)";
            btnSquareOffSel.style.color = "#94a3b8";
            btnSquareOffSel.style.fontWeight = "600";
        }
    }
}

function toggleSelectAllStraddleLegs(masterCb) {
    const isChecked = masterCb ? masterCb.checked : false;
    const cbs = document.querySelectorAll(".straddle-leg-checkbox");
    cbs.forEach(cb => {
        cb.checked = isChecked;
        const idx = parseInt(cb.getAttribute("data-index"), 10);
        const legId = cb.getAttribute("data-leg-id");
        if (isChecked) {
            if (!isNaN(idx)) window._straddleSelectedLegIndices.add(idx);
            if (legId) window._straddleSelectedLegIds.add(legId);
        } else {
            if (!isNaN(idx)) window._straddleSelectedLegIndices.delete(idx);
            if (legId) window._straddleSelectedLegIds.delete(legId);
        }
    });
    onStraddlePositionCheckboxChange();
}

async function retrySingleStraddleLeg(strategyId, legId, legIndex) {
    const stratId = strategyId || window._straddleState?.activeStrategy?.strategy_id;
    if (!stratId) return;

    showStraddleToast("🔄 Retrying failed order...", "info", 2000);
    try {
        const resp = await fetch("/api/straddle/retry_legs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                strategy_id: stratId,
                leg_ids: legId ? [legId] : undefined,
                leg_indices: (legIndex !== null && legIndex !== undefined) ? [legIndex] : undefined
            })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleSuccessToast("Order Retried", "Order retry succeeded!");
        } else {
            showStraddleToast("Retry failed: " + (res.error || "Broker rejected again"), "error", 4500);
        }
        await fetchStraddleState();
        renderStraddleAlgosTable(true);
    } catch (e) {
        showStraddleToast("Retry error: " + e.message, "error", 3500);
    }
}

async function retryAllFailedStraddleLegs() {
    const activeStrat = window._straddleState?.activeStrategy;
    if (!activeStrat) {
        showStraddleToast("No active strategy selected.", "error");
        return;
    }
    const failedPositions = (activeStrat.positions || []).filter(p => p.status === "ERROR");
    if (failedPositions.length === 0) {
        showStraddleToast("No failed orders found in current strategy.", "info");
        return;
    }

    showStraddleToast(`⏳ Retrying ${failedPositions.length} failed order(s)...`, "info", 2000);
    try {
        const resp = await fetch("/api/straddle/retry_legs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: activeStrat.strategy_id })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleSuccessToast("Orders Retried", `Successfully re-placed ${res.retried_success_count || failedPositions.length} order(s)!`);
        } else {
            showStraddleToast("Retry failed: " + (res.error || "Broker rejected again"), "error", 4500);
        }
        await fetchStraddleState();
        onStraddlePositionCheckboxChange();
    } catch (e) {
        showStraddleToast("Retry request failed: " + e.message, "error", 3500);
    }
}

// Margin error parser: Extracts Required, Available, and Shortfall from Kotak Neo RMS strings
function parseMarginError(errMsg) {
    if (!errMsg || typeof errMsg !== 'string') return null;
    const reqMatch = errMsg.match(/Required\s*:\s*([0-9.,]+)/i);
    const availMatch = errMsg.match(/Available\s*:\s*([0-9.,]+)/i);
    if (reqMatch && availMatch) {
        const req = parseFloat(reqMatch[1].replace(/,/g, ''));
        const avail = parseFloat(availMatch[1].replace(/,/g, ''));
        const shortfall = req - avail;
        return {
            required: !isNaN(req) ? req : 0,
            available: !isNaN(avail) ? avail : 0,
            shortfall: shortfall > 0 ? shortfall : 0,
            hasMarginDetails: true
        };
    }
    return null;
}

// Global modal state
window._rectifyLegState = {
    strategyId: null,
    legId: null,
    legIndex: null,
    lotSize: 65,
    underlying: "NIFTY",
    spotPrice: 22250,
    availableMargin: 0,
    requiredMargin: 0
};

async function openRectifyLegModal(strategyId, legId, legIndex) {
    const strat = window._straddleState?.activeStrategy || (window._straddleState?.allStrategies || []).find(s => s.strategy_id === strategyId);
    if (!strat) {
        showStraddleToast("Strategy not found for rectification.", "warning");
        return;
    }

    const positions = strat.positions || [];
    let pos = null;
    if (legId) pos = positions.find(p => p.id === legId);
    if (!pos && legIndex !== undefined && positions[legIndex]) pos = positions[legIndex];

    const legsCfg = strat.legs || [];
    let legCfg = null;
    if (legId) legCfg = legsCfg.find(l => l.id === legId);
    if (!legCfg && legIndex !== undefined && legsCfg[legIndex]) legCfg = legsCfg[legIndex];

    const und = (pos?.underlying || legCfg?.underlying || strat.underlying || "NIFTY").toUpperCase();
    const lotSize = Number(pos?.lot_size || legCfg?.lot_size || (und === "BANKNIFTY" ? 15 : (und === "FINNIFTY" ? 25 : (und === "MIDCPNIFTY" ? 50 : (und === "SENSEX" ? 10 : 65)))));
    const currentLots = Number(pos?.lots || legCfg?.lots || 1);
    const side = (pos?.side || legCfg?.side || "SELL").toUpperCase();
    const prod = (pos?.product || legCfg?.product || "NRML").toUpperCase();
    const optType = (pos?.option_type || legCfg?.option_type || "CE").toUpperCase();
    const strike = (pos?.strike_selection || legCfg?.strike_selection || "ATM").toUpperCase().replace("_", " ").trim();
    const tgtVal = Number(pos?.target_value || legCfg?.target_value || 10.0);
    const slVal = Number(pos?.sl_value || legCfg?.sl_value || 20.0);
    const errText = pos?.error_message || pos?.error || strat.entry_error || "Margin Exceeded / Broker RMS rejection";

    // Spot and ATM Strike
    const spot = Number(window._straddleSpotPrices?.[und] || 22250);
    const step = und === "BANKNIFTY" ? 100 : (und === "SENSEX" ? 100 : 50);
    const atmStrike = Math.round(spot / step) * step;

    // Set internal state
    window._rectifyLegState = {
        strategyId: strat.strategy_id,
        legId: legId || pos?.id || legCfg?.id || `leg_${(legIndex || 0) + 1}`,
        legIndex: legIndex !== undefined ? legIndex : 0,
        lotSize: lotSize,
        underlying: und,
        spotPrice: spot,
        step: step,
        atmStrike: atmStrike,
        availableMargin: 0,
        requiredMargin: 0
    };

    // Set Hidden Inputs
    const setVal = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
    setVal("rectifyStrategyId", strat.strategy_id);
    setVal("rectifyLegId", window._rectifyLegState.legId);
    setVal("rectifyLegIndex", window._rectifyLegState.legIndex);
    setVal("rectifyLotSize", lotSize);
    setVal("rectifyUnderlying", und);
    setVal("rectifyUnderlyingSpot", spot);

    // Set Subtitle & Error Text
    const subEl = document.getElementById("rectifyLegSubtitle");
    if (subEl) subEl.textContent = `${strat.name || und} • ${pos?.symbol || (und + ' ' + side + ' ' + optType)}`;

    const errEl = document.getElementById("rectifyErrorRawText");
    if (errEl) errEl.textContent = errText;

    // Parse Error values for summary
    const parsedErr = parseMarginError(errText);
    const prevReqEl = document.getElementById("rectifyPrevRequiredDisplay");
    const prevShortEl = document.getElementById("rectifyPrevShortfallDisplay");
    if (parsedErr && parsedErr.hasMarginDetails) {
        if (prevReqEl) prevReqEl.textContent = `₹ ${parsedErr.required.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}`;
        if (prevShortEl) prevShortEl.textContent = `₹ ${parsedErr.shortfall.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}`;
    } else {
        if (prevReqEl) prevReqEl.textContent = "-";
        if (prevShortEl) prevShortEl.textContent = "-";
    }

    // Set Form Fields
    const lotsInput = document.getElementById("rectifyLotsInput");
    if (lotsInput) lotsInput.value = currentLots;

    const prodSel = document.getElementById("rectifyProductSelect");
    if (prodSel) prodSel.value = prod === "MIS" ? "MIS" : "NRML";

    const typeSel = document.getElementById("rectifyOptionTypeSelect");
    if (typeSel) typeSel.value = optType === "PE" ? "PE" : "CE";

    const sideSel = document.getElementById("rectifySideSelect");
    if (sideSel) sideSel.value = side === "BUY" ? "BUY" : "SELL";

    const tgtInput = document.getElementById("rectifyTargetValueInput");
    if (tgtInput) tgtInput.value = tgtVal;

    const slInput = document.getElementById("rectifySlValueInput");
    if (slInput) slInput.value = slVal;

    const spotSub = document.getElementById("rectifySpotSubtext");
    if (spotSub) spotSub.textContent = `Spot: ${spot.toFixed(2)} | ATM: ${atmStrike}`;

    // Populate Strike Dropdown
    const strikeSel = document.getElementById("rectifyStrikeSelect");
    if (strikeSel) {
        strikeSel.innerHTML = "";
        
        // OTM 20 to 1
        for (let i = 20; i >= 1; i--) {
            const opt = document.createElement("option");
            opt.value = `OTM ${i}`;
            const estStrike = optType === "CE" ? (atmStrike + i * step) : (atmStrike - i * step);
            opt.textContent = `OTM ${i} (${estStrike})`;
            if (strike === `OTM ${i}` || strike === `OTM${i}`) opt.selected = true;
            strikeSel.appendChild(opt);
        }
        // ATM
        const atmOpt = document.createElement("option");
        atmOpt.value = "ATM";
        atmOpt.textContent = `ATM (${atmStrike})`;
        if (strike === "ATM" || strike === "ATM STRIKE") atmOpt.selected = true;
        strikeSel.appendChild(atmOpt);
        // ITM 1 to 20
        for (let i = 1; i <= 20; i++) {
            const opt = document.createElement("option");
            opt.value = `ITM ${i}`;
            const estStrike = optType === "CE" ? (atmStrike - i * step) : (atmStrike + i * step);
            opt.textContent = `ITM ${i} (${estStrike})`;
            if (strike === `ITM ${i}` || strike === `ITM${i}`) opt.selected = true;
            strikeSel.appendChild(opt);
        }

        // If specific numeric strike was present
        if (/^\d+(\.\d+)?$/.test(strike)) {
            const customOpt = document.createElement("option");
            customOpt.value = strike;
            customOpt.textContent = `Exact Strike: ${strike}`;
            customOpt.selected = true;
            strikeSel.appendChild(customOpt);
        }
    }

    // Show modal
    const modal = document.getElementById("modalRectifyLeg");
    if (modal) modal.style.display = "flex";

    // Fetch Live Available Funds & Recalculate
    await refreshLiveWalletMarginInModal(parsedErr?.available);
}

function closeRectifyLegModal() {
    const modal = document.getElementById("modalRectifyLeg");
    if (modal) modal.style.display = "none";
}

async function refreshLiveWalletMarginInModal(fallbackAvail = null) {
    let avail = 0;
    try {
        const resp = await fetch("/api/straddle/margin");
        const res = await resp.json();
        if (res.success && res.margin) {
            avail = Number(res.margin.available_margin || res.margin.available_cash || 0);
        }
    } catch (e) {
        console.warn("Could not fetch live margin endpoint:", e);
    }

    if (avail <= 0 && fallbackAvail && fallbackAvail > 0) {
        avail = fallbackAvail;
    } else if (avail <= 0) {
        avail = 194948.45;
    }

    window._rectifyLegState.availableMargin = avail;
    const availEl = document.getElementById("rectifyAvailFundsDisplay");
    if (availEl) {
        availEl.textContent = `₹ ${avail.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}`;
    }

    onRectifyParamChange();
}

function adjustRectifyLots(delta) {
    const input = document.getElementById("rectifyLotsInput");
    if (!input) return;
    let val = parseInt(input.value, 10) || 1;
    val = Math.max(1, Math.min(500, val + delta));
    input.value = val;
    onRectifyParamChange();
}

function onRectifyParamChange() {
    const lotsInput = document.getElementById("rectifyLotsInput");
    const lots = Math.max(1, parseInt(lotsInput?.value, 10) || 1);
    const lotSize = window._rectifyLegState.lotSize || 65;
    const totalQty = lots * lotSize;

    const qtySub = document.getElementById("rectifyQtySubtext");
    if (qtySub) qtySub.textContent = `(${totalQty} Qty)`;

    const side = (document.getElementById("rectifySideSelect")?.value || "SELL").toUpperCase();
    const prod = (document.getElementById("rectifyProductSelect")?.value || "NRML").toUpperCase();
    const optType = (document.getElementById("rectifyOptionTypeSelect")?.value || "CE").toUpperCase();
    const und = window._rectifyLegState.underlying || "NIFTY";
    const spot = window._rectifyLegState.spotPrice || 22250;

    // Kotak Neo RMS margin model:
    // Option Buy: Premium only (e.g. ~150 * qty)
    // Option Sell: SPAN + Exposure margin (% of spot * qty)
    // NIFTY: ~11.1%, BANKNIFTY: ~12.8%, FINNIFTY: ~11.8%, SENSEX: ~10.8%
    // MIS discount: ~25% (multiplier 0.75)
    let marginPct = und === "BANKNIFTY" ? 0.128 : (und === "FINNIFTY" ? 0.118 : (und === "SENSEX" ? 0.108 : 0.111));
    let prodMult = prod === "MIS" ? 0.75 : 1.0;

    let requiredMargin = 0;
    if (side === "BUY") {
        requiredMargin = totalQty * 150.0;
    } else {
        requiredMargin = (spot * totalQty * marginPct) * prodMult;
    }
    requiredMargin = Math.round(requiredMargin);
    window._rectifyLegState.requiredMargin = requiredMargin;

    const availMargin = window._rectifyLegState.availableMargin || 194948.45;
    const diff = availMargin - requiredMargin;
    const isSufficient = diff >= 0;

    const reqEl = document.getElementById("rectifyNewRequiredDisplay");
    if (reqEl) {
        reqEl.textContent = `₹ ${requiredMargin.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}`;
    }

    const statEl = document.getElementById("rectifyStatusSurplusDisplay");
    const calcBadge = document.getElementById("rectifyLiveCalcBadge");
    const btnRetry = document.getElementById("btnRectifyAndRetry");

    if (statEl) {
        if (isSufficient) {
            statEl.innerHTML = `<span style="color:#10b981;">🟢 SUFFICIENT (+₹${diff.toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})})</span>`;
            if (calcBadge) {
                calcBadge.style.background = "rgba(16,185,129,0.2)";
                calcBadge.style.color = "#10b981";
                calcBadge.textContent = "✓ Margin Sufficient";
            }
            if (btnRetry) {
                btnRetry.style.background = "#16a34a";
                btnRetry.style.borderColor = "#22c55e";
                btnRetry.innerHTML = `<span>⚡ Rectify &amp; Place Order Now</span>`;
            }
        } else {
            statEl.innerHTML = `<span style="color:#ef4444;">🔴 SHORTFALL: ₹${Math.abs(diff).toLocaleString('en-IN', {minimumFractionDigits:2, maximumFractionDigits:2})}</span>`;
            if (calcBadge) {
                calcBadge.style.background = "rgba(239,68,68,0.2)";
                calcBadge.style.color = "#f87171";
                calcBadge.textContent = "⚠️ Margin Shortfall";
            }
            if (btnRetry) {
                btnRetry.style.background = "#d97706";
                btnRetry.style.borderColor = "#f59e0b";
                btnRetry.innerHTML = `<span>⚡ Rectify &amp; Retry Anyway</span>`;
            }
        }
    }

    // Auto fit calculation
    const perLotMargin = side === "BUY" ? (lotSize * 150.0) : ((spot * lotSize * marginPct) * prodMult);
    const maxAffordableLots = Math.max(1, Math.floor(availMargin / perLotMargin));
    const btnAuto = document.getElementById("btnRectifyAutoFit");
    if (btnAuto) {
        btnAuto.innerHTML = `<span>💡 Auto-Set Max Affordable Lots (${maxAffordableLots} Lot${maxAffordableLots > 1 ? 's' : ''} = ₹${Math.round(maxAffordableLots * perLotMargin).toLocaleString('en-IN')})</span>`;
    }
}

function applyRectifyAutoFitLots() {
    const lotSize = window._rectifyLegState.lotSize || 65;
    const side = (document.getElementById("rectifySideSelect")?.value || "SELL").toUpperCase();
    const prod = (document.getElementById("rectifyProductSelect")?.value || "NRML").toUpperCase();
    const und = window._rectifyLegState.underlying || "NIFTY";
    const spot = window._rectifyLegState.spotPrice || 22250;
    const availMargin = window._rectifyLegState.availableMargin || 194948.45;

    let marginPct = und === "BANKNIFTY" ? 0.128 : (und === "FINNIFTY" ? 0.118 : (und === "SENSEX" ? 0.108 : 0.111));
    let prodMult = prod === "MIS" ? 0.75 : 1.0;
    const perLotMargin = side === "BUY" ? (lotSize * 150.0) : ((spot * lotSize * marginPct) * prodMult);

    const maxLots = Math.max(1, Math.floor(availMargin / perLotMargin));
    const lotsInput = document.getElementById("rectifyLotsInput");
    if (lotsInput) {
        lotsInput.value = maxLots;
        onRectifyParamChange();
        showStraddleToast(`Lots adjusted to ${maxLots} to fit available balance!`, "info", 2000);
    }
}

async function submitRectifyLeg(placeOrderNow = true) {
    const state = window._rectifyLegState;
    if (!state || !state.strategyId) {
        showStraddleToast("Invalid leg rectification context.", "error");
        return;
    }

    const lots = Math.max(1, parseInt(document.getElementById("rectifyLotsInput")?.value, 10) || 1);
    const strike = document.getElementById("rectifyStrikeSelect")?.value || "ATM";
    const prod = document.getElementById("rectifyProductSelect")?.value || "NRML";
    const optType = document.getElementById("rectifyOptionTypeSelect")?.value || "CE";
    const side = document.getElementById("rectifySideSelect")?.value || "SELL";
    const targetVal = parseFloat(document.getElementById("rectifyTargetValueInput")?.value || 10.0);
    const slVal = parseFloat(document.getElementById("rectifySlValueInput")?.value || 20.0);

    const btnSubmit = document.getElementById("btnRectifyAndRetry");
    if (btnSubmit) {
        btnSubmit.disabled = true;
        btnSubmit.innerHTML = `<span>⏳ Processing...</span>`;
    }

    showStraddleToast(placeOrderNow ? "⚡ Rectifying leg & placing order on Kotak Neo..." : "💾 Saving rectified leg configuration...", "info", 2500);

    try {
        const payload = {
            strategy_id: state.strategyId,
            leg_id: state.legId,
            leg_index: state.legIndex,
            retry_immediately: placeOrderNow,
            updated_leg_config: {
                lots: lots,
                strike_selection: strike,
                product: prod,
                option_type: optType,
                side: side,
                target_type: "TGT: %",
                target_value: targetVal,
                sl_type: "SL: %",
                sl_value: slVal
            }
        };

        const resp = await fetch("/api/straddle/rectify_leg", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();

        if (res.success) {
            closeRectifyLegModal();
            if (placeOrderNow) {
                showStraddleSuccessToast("Order Placed Successfully", `Leg rectified (${lots} lots ${strike} ${optType}) and sent to broker!`);
            } else {
                showStraddleSuccessToast("Leg Config Saved", `Leg settings updated in strategy.`);
            }
            await fetchStraddleState();
            onStraddlePositionCheckboxChange();
        } else {
            showStraddleToast("Rectify Error: " + (res.error || "Order placement failed"), "error", 4500);
            await fetchStraddleState();
        }
    } catch (e) {
        showStraddleToast("Rectify request failed: " + e.message, "error", 4000);
    } finally {
        if (btnSubmit) {
            btnSubmit.disabled = false;
            btnSubmit.innerHTML = `<span>⚡ Rectify &amp; Place Order Now</span>`;
        }
    }
}

// Backward compatibility alias
function openLegSettingsModalForLegId(legId, legIndex) {
    const stratId = window._straddleState?.activeStrategy?.strategy_id;
    openRectifyLegModal(stratId, legId, legIndex);
}

async function retrySelectedStraddleLegs() {
    const activeStrat = window._straddleState.activeStrategy;
    if (!activeStrat) {
        showStraddleToast("No active strategy selected.", "error");
        return;
    }

    const checked = Array.from(document.querySelectorAll(".straddle-leg-checkbox:checked"));
    if (checked.length === 0) {
        showStraddleToast("Please select at least one failed leg to retry.", "warning");
        return;
    }

    const legIndices = checked.map(cb => parseInt(cb.getAttribute("data-index"), 10)).filter(i => !isNaN(i));
    const legIds = checked.map(cb => cb.getAttribute("data-leg-id")).filter(Boolean);

    showStraddleToast(`⏳ Retrying ${checked.length} failed leg(s)...`, "info", 2000);

    try {
        const resp = await fetch("/api/straddle/retry_legs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                strategy_id: activeStrat.strategy_id,
                leg_ids: legIds,
                leg_indices: legIndices
            })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleSuccessToast("Order Retried", `Successfully re-placed ${res.retried_success_count || checked.length} leg order(s)!`);
            await fetchStraddleState();
            onStraddlePositionCheckboxChange();
        } else {
            showStraddleToast("Retry failed: " + (res.error || "Broker rejected again"), "error", 4000);
            await fetchStraddleState();
        }
    } catch (e) {
        showStraddleToast("Retry request failed: " + e.message, "error", 3500);
    }
}

async function resetCurrentStraddleAlgo() {
    const activeStrat = window._straddleState?.activeStrategy || (window._straddleState.allStrategies || []).find(s => s.strategy_id === window._straddleState.activeStrategyId || s.name === window._straddleState.activeStrategyId);
    if (!activeStrat) {
        showStraddleToast("No active strategy selected.", "error");
        return;
    }

    if (!confirm(`Are you sure you want to reset algo '${activeStrat.name}' and clear all positions/failed order errors?`)) {
        return;
    }

    const targetSid = activeStrat.strategy_id || activeStrat.name;
    const name = activeStrat.name || targetSid;

    // Instant optimistic clearing across all state caches
    window._straddleSelectedLegIndices.clear();
    window._straddleSelectedLegIds.clear();
    if (typeof dismissAllStraddleToasts === "function") {
        dismissAllStraddleToasts();
    } else {
        if (window._straddleToastedErrors) window._straddleToastedErrors.clear();
        document.querySelectorAll(".straddle-rejected-toast").forEach(el => el.remove());
        document.querySelectorAll(".straddle-error-banner-row").forEach(el => el.remove());
    }

    (window._straddleState.allStrategies || []).forEach(s => {
        if (s.strategy_id === targetSid || s.name === name || s.name === targetSid || s.strategy_id === activeStrat.strategy_id) {
            s.positions = [];
            s.mtm = 0.0;
            s.max_mtm = 0.0;
            s.min_mtm = 0.0;
            s.status = "READY";
            s.enabled = false;
            s.entry_error = "-";
            s.exit_error = "-";
            s.last_error = null;
            s.repair_needed = false;
            if (Array.isArray(s.legs)) {
                s.legs.forEach(l => {
                    l.error = null;
                    l.error_message = null;
                    l.status = "READY";
                    l.order_status = null;
                    l.entry_order_id = null;
                    l.exit_order_id = null;
                    l.entry_price = null;
                    l.exit_price = null;
                    l.repair_needed = false;
                });
            }
        }
    });

    activeStrat.positions = [];
    activeStrat.mtm = 0.0;
    activeStrat.max_mtm = 0.0;
    activeStrat.min_mtm = 0.0;
    activeStrat.status = "READY";
    activeStrat.enabled = false;
    activeStrat.entry_error = "-";
    activeStrat.exit_error = "-";
    activeStrat.last_error = null;
    activeStrat.repair_needed = false;
    if (Array.isArray(activeStrat.legs)) {
        activeStrat.legs.forEach(l => {
            l.error = null;
            l.error_message = null;
            l.status = "READY";
            l.order_status = null;
            l.entry_order_id = null;
            l.exit_order_id = null;
            l.entry_price = null;
            l.exit_price = null;
            l.repair_needed = false;
        });
    }

    if (window._straddleState) {
        window._straddleState.positions = [];
        window._straddleState.status = "READY";
        window._straddleState.entry_error = "-";
        window._straddleState.activeStrategy = activeStrat;
        window._straddleState.activeStrategyId = activeStrat.strategy_id;
    }

    renderStraddlePositions([], 0.0, 0.0);
    renderStraddleAlgosTable(true);
    renderStraddleHeader(window._straddleState);
    onStraddlePositionCheckboxChange();

    showStraddleToast(`🔄 Resetting algo '${name}'...`, "info", 1500);

    try {
        const resp = await fetch("/api/straddle/reset_algo", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: targetSid, name: name, strategy_name: name, algo_name: name })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleSuccessToast("Algo Reset", `Algo '${name}' has been completely reset to clean READY state.`);
            if (res.strategy) {
                const updated = res.strategy;
                const idx = (window._straddleState.allStrategies || []).findIndex(s => s.strategy_id === updated.strategy_id || s.name === updated.name);
                if (idx >= 0) window._straddleState.allStrategies[idx] = updated;
                window._straddleState.activeStrategy = updated;
                window._straddleState.positions = updated.positions || [];
            }
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddlePositions(window._straddleState.activeStrategy?.positions || [], 0.0, 0.0);
            renderStraddleAlgosTable(true);
            renderStraddleHeader(window._straddleState);
            onStraddlePositionCheckboxChange();
        } else {
            showStraddleToast("Reset failed: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Reset error: " + e.message, "error", 3500);
    }
}

function openLegSettingsModal(rowId) {
    const tr = document.getElementById(rowId);
    if (!tr) return;

    window._activeLegSettingsRowId = rowId;

    const und = tr.querySelector(".leg-underlying")?.value || "NIFTY";
    const side = tr.querySelector(".leg-side")?.value || "SELL";
    const prod = tr.querySelector(".leg-product")?.value || "NRML";
    const type = tr.querySelector(".leg-type")?.value || "CE";
    const curStrike = tr.querySelector(".leg-strike")?.value || "ATM";
    const curMult = tr.querySelector(".leg-multiple-strike")?.value || "None";

    const subtitleEl = document.getElementById("modalLegSettingsSubtitle");
    if (subtitleEl) {
        subtitleEl.textContent = `(${und} ${side.toUpperCase()} ${prod.toUpperCase()} ${type.toUpperCase()})`;
    }

    // Populate strike select in modal with ATM, OTM 1-20, ITM 1-20
    const strikeSel = document.getElementById("modalLegStrikeSelect");
    if (strikeSel) {
        strikeSel.innerHTML = "";
        const normStrike = curStrike.toUpperCase().replace("_", " ").trim();
        
        // OTM 20 down to 1
        for (let i = 20; i >= 1; i--) {
            const opt = document.createElement("option");
            opt.value = `OTM ${i}`;
            opt.textContent = `OTM ${i}`;
            if (normStrike === `OTM ${i}` || normStrike === `OTM${i}`) opt.selected = true;
            strikeSel.appendChild(opt);
        }
        // ATM
        const atmOpt = document.createElement("option");
        atmOpt.value = "ATM";
        atmOpt.textContent = "ATM";
        if (normStrike === "ATM" || normStrike === "ATM STRIKE") atmOpt.selected = true;
        strikeSel.appendChild(atmOpt);
        // ITM 1 to 20
        for (let i = 1; i <= 20; i++) {
            const opt = document.createElement("option");
            opt.value = `ITM ${i}`;
            opt.textContent = `ITM ${i}`;
            if (normStrike === `ITM ${i}` || normStrike === `ITM${i}`) opt.selected = true;
            strikeSel.appendChild(opt);
        }
    }

    const multSel = document.getElementById("modalLegMultipleStrikeSelect");
    if (multSel) {
        multSel.value = (curMult === "500/100 multiple" || curMult === "500_100_multiple") ? "500/100 multiple" : "None";
    }

    const modal = document.getElementById("modalLegSettings");
    if (modal) {
        modal.style.display = "flex";
    }
}

function closeLegSettingsModal() {
    const modal = document.getElementById("modalLegSettings");
    if (modal) modal.style.display = "none";
    window._activeLegSettingsRowId = null;
}

function saveLegSettingsModal() {
    const rowId = window._activeLegSettingsRowId;
    if (!rowId) {
        closeLegSettingsModal();
        return;
    }

    const tr = document.getElementById(rowId);
    if (tr) {
        const strikeSel = document.getElementById("modalLegStrikeSelect");
        const multSel = document.getElementById("modalLegMultipleStrikeSelect");

        const legStrikeEl = tr.querySelector(".leg-strike");
        if (legStrikeEl && strikeSel) {
            legStrikeEl.value = strikeSel.value;
        }

        const hiddenMultEl = tr.querySelector(".leg-multiple-strike");
        if (hiddenMultEl && multSel) {
            hiddenMultEl.value = multSel.value;
        }

        const badgeEl = tr.querySelector(".leg-mult-badge");
        if (badgeEl) {
            const isMult = (multSel && multSel.value === "500/100 multiple");
            badgeEl.style.display = isMult ? "inline-block" : "none";
        }
    }

    closeLegSettingsModal();
}

function toggleStraddleSettingsPanel(forceState = null) {
    const panel = document.getElementById("straddleSettingsPanel");
    if (!panel) return;
    const isCurrentlyVisible = panel.style.display === "block";
    const nextState = (forceState !== null) ? !!forceState : !isCurrentlyVisible;
    panel.style.display = nextState ? "block" : "none";

    if (nextState && window._straddleState.activeStrategy) {
        populateStraddleSettingsForm(window._straddleState.activeStrategy);
    }
}
window.toggleStraddleSettingsPanel = toggleStraddleSettingsPanel;

function populateStraddleSettingsForm(s) {
    if (!s) return;
    if (document.getElementById("cfgStrategyId")) {
        document.getElementById("cfgStrategyId").value = s.strategy_id || "";
    }
    if (document.getElementById("cfgStrategyName")) {
        document.getElementById("cfgStrategyName").value = s.name || "";
    }
    const titleEl = document.getElementById("straddleTerminalStrategyTitle");
    if (titleEl) {
        titleEl.textContent = s.name || s.strategy_id || "Custom Strategy";
    }

    document.getElementById("cfgSegment").value = s.segment || "Options";
    document.getElementById("cfgMultiplier").value = s.multiplier || 1;
    setStraddleTimeInputValue("cfgStartTime", s.start_time || "09:15:56");
    setStraddleTimeInputValue("cfgEndTime", s.end_time || "15:29:50");

    document.getElementById("cfgToggleMoveSlToCost").checked = !!s.move_sl_to_cost;
    document.getElementById("cfgToggleWaitAndTrade").checked = !!s.wait_and_trade;
    document.getElementById("cfgToggleReEntry").checked = s.re_entry_enabled !== false;
    document.getElementById("cfgToggleRangeBreakout").checked = !!s.range_breakout;

    setUnderlyingPriceSource(s.underlying_price_source || "Spot");
    setStraddleTradeType(s.trade_type || "Intraday");

    // MTM Target & SL
    const mtm = s.mtm_target || {};
    document.getElementById("cfgMtmTargetType").value = mtm.type || "MTM in total amount";
    document.getElementById("cfgMtmTargetValue").value = mtm.value !== undefined ? mtm.value : 10000;
    document.getElementById("cfgMtmTargetReEntry").value = mtm.re_entry || "None";
    document.getElementById("cfgMtmTargetAction").value = mtm.action || "Square off all positions";

    const sl = s.mtm_sl || {};
    document.getElementById("cfgMtmSlType").value = sl.type || "None";
    document.getElementById("cfgMtmSlValue").value = sl.value !== undefined ? sl.value : -5000;
    document.getElementById("cfgMtmProfitLock").value = sl.profit_lock || "None";
    document.getElementById("cfgMtmSlAction").value = sl.action || "Square off all positions";

    // Advanced sequencing
    const adv = s.advanced_settings || s.advanced || {};
    document.getElementById("cfgEntryOrderType").value = adv.entry_order_type || "Limit (L) / Stoploss Limit (SL-L)";
    document.getElementById("cfgEntryBufferType").value = adv.entry_buffer_type || "Percentage";
    document.getElementById("cfgEntryBufferVal").value = adv.entry_buffer_val !== undefined ? adv.entry_buffer_val : (adv.entry_buffer_value !== undefined ? adv.entry_buffer_value : 5.0);
    document.getElementById("cfgEntryMarketTimeoutSec").value = adv.entry_market_timeout_sec !== undefined ? adv.entry_market_timeout_sec : 0;
    document.getElementById("cfgEntryDelaySide").value = adv.entry_delay_side || adv.entry_order_delay_side || "Delay entry of Sell positions";
    document.getElementById("cfgEntryDelaySec").value = adv.entry_delay_sec !== undefined ? adv.entry_delay_sec : (adv.entry_order_delay_sec !== undefined ? adv.entry_order_delay_sec : 0);

    document.getElementById("cfgExitOrderType").value = adv.exit_order_type || "Limit (L) / Stoploss Limit (SL-L)";
    document.getElementById("cfgExitBufferType").value = adv.exit_buffer_type || "Percentage";
    document.getElementById("cfgExitBufferVal").value = adv.exit_buffer_val !== undefined ? adv.exit_buffer_val : (adv.exit_buffer_value !== undefined ? adv.exit_buffer_value : 0.1);
    document.getElementById("cfgExitMarketTimeoutSec").value = adv.exit_market_timeout_sec !== undefined ? adv.exit_market_timeout_sec : 0;
    document.getElementById("cfgExitDelaySide").value = adv.exit_delay_side || adv.exit_order_delay_side || "Delay exit of Buy positions";
    document.getElementById("cfgExitDelaySec").value = adv.exit_delay_sec !== undefined ? adv.exit_delay_sec : (adv.exit_order_delay_sec !== undefined ? adv.exit_order_delay_sec : 3);

    document.getElementById("cfgSlOrderPlacement").value = adv.sl_order_placement || "Keep with self";
    const tgtOrderEl = document.getElementById("cfgTargetOrderPlacement");
    if (tgtOrderEl) tgtOrderEl.value = adv.target_order_placement || "Keep with self";
    document.getElementById("cfgSlPlacementDelaySec").value = adv.sl_placement_delay_sec !== undefined ? adv.sl_placement_delay_sec : 0;
    document.getElementById("cfgMtmSlDelaySec").value = adv.mtm_sl_delay_sec !== undefined ? adv.mtm_sl_delay_sec : 0;

    document.getElementById("cfgAutoHandlingSl").checked = adv.auto_handling_sl_cancellation !== false;
    document.getElementById("cfgAutoExitMargin").checked = !!(adv.auto_exit_margin_error || adv.auto_exit_on_margin_error);

    // Render Leg rows
    const tbody = document.getElementById("straddleLegsBuilderBody");
    if (tbody) {
        tbody.innerHTML = "";
        const legs = s.legs || [];
        legs.forEach(leg => addStraddleLegRow(leg));
    }
}

function addStraddleLegRow(leg = null) {
    const tbody = document.getElementById("straddleLegsBuilderBody");
    if (!tbody) return;

    const rowId = "leg_" + Date.now() + "_" + Math.floor(Math.random() * 1000);
    const l = leg || {
        id: rowId,
        side: "Sell",
        product: "NRML",
        strike_selection: "ATM",
        multiple_strike_selection: "None",
        option_type: "CE",
        lots: 1,
        target_type: "TGT: %",
        target_value: 45.0,
        target_re_execute_type: "RE EXECUTE",
        target_re_execute_count: 6,
        sl_type: "SL: %",
        sl_value: 66.0,
        tsl_type: "None",
        sl_re_execute_type: "RE EXECUTE",
        sl_re_execute_count: 1,
        action: "Square off leg",
        underlying: "NIFTY",
        expiry: "Weekly"
    };

    // Normalize strike selection
    const curStrike = String(l.strike_selection || "ATM").toUpperCase().replace("_", " ").trim();
    let strikeOptionsHtml = `<option value="ATM" ${curStrike === "ATM" || curStrike === "ATM STRIKE" ? "selected" : ""}>ATM STRIKE</option>`;
    for (let i = 1; i <= 40; i++) {
        const isSel = (curStrike === `OTM ${i}` || curStrike === `OTM${i}`);
        strikeOptionsHtml += `<option value="OTM ${i}" ${isSel ? "selected" : ""}>OTM ${i}</option>`;
    }
    for (let i = 1; i <= 40; i++) {
        const isSel = (curStrike === `ITM ${i}` || curStrike === `ITM${i}`);
        strikeOptionsHtml += `<option value="ITM ${i}" ${isSel ? "selected" : ""}>ITM ${i}</option>`;
    }
    if (!isNaN(parseFloat(curStrike)) && !strikeOptionsHtml.includes(`"${curStrike}"`)) {
        strikeOptionsHtml += `<option value="${curStrike}" selected>${curStrike}</option>`;
    }

    // Target Options
    const curTgtType = l.target_type || "TGT: %";
    const targetTypeOptions = [
        "TGT: %",
        "TGT: pts",
        "TGT: UL %",
        "TGT: UL pts",
        "None"
    ];
    let tgtOptionsHtml = "";
    targetTypeOptions.forEach(opt => {
        const sel = (curTgtType.toLowerCase() === opt.toLowerCase() || (opt === "TGT: pts" && curTgtType.toLowerCase() === "tgt: pts")) ? "selected" : "";
        tgtOptionsHtml += `<option value="${opt}" ${sel}>${opt}</option>`;
    });

    // Stoploss Options
    const curSlType = l.sl_type || "SL: %";
    const slTypeOptions = [
        "SL: %",
        "SL: pts",
        "SL: UL %",
        "SL: UL pts",
        "None"
    ];
    let slOptionsHtml = "";
    slTypeOptions.forEach(opt => {
        const sel = (curSlType.toLowerCase() === opt.toLowerCase() || (opt === "SL: pts" && curSlType.toLowerCase() === "sl: pts")) ? "selected" : "";
        slOptionsHtml += `<option value="${opt}" ${sel}>${opt}</option>`;
    });

    // Re-execute / Re-entry Options
    const reOptions = [
        "NONE",
        "RE ENTRY",
        "RE COST",
        "RE EXECUTE",
        "RE ENTRY REVERSE",
        "RE COST REVERSE",
        "RE EXECUTE REVERSE"
    ];

    const curTgtRe = String(l.target_re_execute_type || "RE EXECUTE").toUpperCase().trim();
    let tgtReOptionsHtml = "";
    reOptions.forEach(opt => {
        const sel = (curTgtRe === opt || (opt === "NONE" && curTgtRe === "NONE")) ? "selected" : "";
        tgtReOptionsHtml += `<option value="${opt}" ${sel}>${opt}</option>`;
    });

    const curSlRe = String(l.sl_re_execute_type || "RE EXECUTE").toUpperCase().trim();
    let slReOptionsHtml = "";
    reOptions.forEach(opt => {
        const sel = (curSlRe === opt || (opt === "NONE" && curSlRe === "NONE")) ? "selected" : "";
        slReOptionsHtml += `<option value="${opt}" ${sel}>${opt}</option>`;
    });

    // Underlying assets
    const underlyings = ["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX"];
    const curUnd = (l.underlying || "NIFTY").toUpperCase();
    let undOptionsHtml = "";
    underlyings.forEach(u => {
        undOptionsHtml += `<option value="${u}" ${curUnd === u ? "selected" : ""}>${u}</option>`;
    });

    const hasMult = (l.multiple_strike_selection === "500/100 multiple" || l.multiple_strike_selection === "500_100_multiple");

    const tr = document.createElement("tr");
    tr.id = rowId;
    tr.innerHTML = `
        <td style="text-align:center; cursor:grab; color:#64748b;">☰</td>
        <td>
            <select class="straddle-select leg-side">
                <option value="Buy" ${l.side === "Buy" ? "selected" : ""}>Buy</option>
                <option value="Sell" ${l.side === "Sell" ? "selected" : ""}>Sell</option>
            </select>
        </td>
        <td>
            <select class="straddle-select leg-product">
                <option value="NRML" ${l.product === "NRML" ? "selected" : ""}>NRML</option>
                <option value="MIS" ${l.product === "MIS" ? "selected" : ""}>MIS</option>
            </select>
        </td>
        <td>
            <div style="display:flex; align-items:center; gap:4px;">
                <select class="straddle-select leg-strike" style="min-width:90px;">
                    ${strikeOptionsHtml}
                </select>
                <input type="hidden" class="leg-multiple-strike" value="${l.multiple_strike_selection || 'None'}">
                <button type="button" class="straddle-btn btn-outline leg-strike-cfg-btn" style="padding:2px 6px; font-size:11px;" title="Strike & 500/100 Multiple Settings" onclick="openLegSettingsModal('${rowId}')">⚙️</button>
                <span class="leg-mult-badge" style="display:${hasMult ? 'inline-block' : 'none'}; font-size:9px; font-weight:700; color:#38bdf8; background:rgba(56,189,248,0.15); border:1px solid rgba(56,189,248,0.3); border-radius:3px; padding:1px 3px;" title="500/100 Multiple Strike Active">500/100</span>
            </div>
        </td>
        <td>
            <select class="straddle-select leg-type">
                <option value="CE" ${l.option_type === "CE" ? "selected" : ""}>CE</option>
                <option value="PE" ${l.option_type === "PE" ? "selected" : ""}>PE</option>
            </select>
        </td>
        <td>
            <input type="number" class="straddle-input leg-lots" style="width:50px;" value="${l.lots || 1}" min="1" max="500">
        </td>
        <td>
            <div style="display:flex; gap:2px;">
                <select class="straddle-select leg-tgt-type" style="width:85px;">
                    ${tgtOptionsHtml}
                </select>
                <input type="number" class="straddle-input leg-tgt-val" style="width:50px;" value="${l.target_value !== undefined ? l.target_value : 45}">
            </div>
        </td>
        <td>
            <div style="display:flex; gap:2px;">
                <select class="straddle-select leg-tgt-re-type" style="width:120px;">
                    ${tgtReOptionsHtml}
                </select>
                <input type="number" class="straddle-input leg-tgt-re-cnt" style="width:40px;" value="${l.target_re_execute_count || 6}">
            </div>
        </td>
        <td>
            <div style="display:flex; gap:2px;">
                <select class="straddle-select leg-sl-type" style="width:80px;">
                    ${slOptionsHtml}
                </select>
                <input type="number" class="straddle-input leg-sl-val" style="width:50px;" value="${l.sl_value !== undefined ? l.sl_value : 66}">
            </div>
        </td>
        <td>
            <select class="straddle-select leg-tsl-type" style="width:80px;">
                <option value="None" ${l.tsl_type === "None" ? "selected" : ""}>None</option>
                <option value="TSL: %" ${l.tsl_type === "TSL: %" ? "selected" : ""}>TSL: %</option>
                <option value="TSL: pts" ${l.tsl_type === "TSL: pts" ? "selected" : ""}>TSL: pts</option>
            </select>
        </td>
        <td>
            <div style="display:flex; gap:2px;">
                <select class="straddle-select leg-sl-re-type" style="width:120px;">
                    ${slReOptionsHtml}
                </select>
                <input type="number" class="straddle-input leg-sl-re-cnt" style="width:40px;" value="${l.sl_re_execute_count || 1}">
            </div>
        </td>
        <td>
            <select class="straddle-select leg-action" style="width:110px;">
                <option value="Square off leg" ${l.action === "Square off leg" ? "selected" : ""}>Square off leg</option>
                <option value="Square off all" ${l.action === "Square off all" ? "selected" : ""}>Square off all</option>
            </select>
        </td>
        <td>
            <select class="straddle-select leg-underlying">
                ${undOptionsHtml}
            </select>
        </td>
        <td>
            <select class="straddle-select leg-expiry">
                <option value="Weekly" ${l.expiry === "Weekly" ? "selected" : ""}>Weekly</option>
                <option value="Next Weekly" ${l.expiry === "Next Weekly" ? "selected" : ""}>Next Weekly</option>
                <option value="Monthly" ${l.expiry === "Monthly" ? "selected" : ""}>Monthly</option>
            </select>
        </td>
        <td style="text-align:center;">
            <button type="button" class="straddle-btn btn-danger" style="padding:2px 6px; font-size:10px;" onclick="removeStraddleLegRow('${rowId}')">✕</button>
        </td>
    `;
    tbody.appendChild(tr);
}

function removeStraddleLegRow(rowId) {
    const tr = document.getElementById(rowId);
    if (tr) tr.remove();
}

function setUnderlyingPriceSource(source) {
    ["Spot", "Futures", "Synthetic Futures"].forEach(s => {
        const id = s === "Spot" ? "btnUndSpot" : (s === "Futures" ? "btnUndFutures" : "btnUndSynthetic");
        const btn = document.getElementById(id);
        if (btn) btn.classList.toggle("active-toggle", s === source);
    });
}

function setStraddleTradeType(type) {
    document.getElementById("btnTradeIntraday")?.classList.toggle("active-toggle", type === "Intraday");
    document.getElementById("btnTradePositional")?.classList.toggle("active-toggle", type === "Positional");
}

function applyStraddleBulkEdit(action) {
    if (!action) return;
    if (action === "TGT_45") {
        document.querySelectorAll(".leg-tgt-type").forEach(el => el.value = "TGT: %");
        document.querySelectorAll(".leg-tgt-val").forEach(el => el.value = 45);
    } else if (action === "SL_66") {
        document.querySelectorAll(".leg-sl-type").forEach(el => el.value = "SL: %");
        document.querySelectorAll(".leg-sl-val").forEach(el => el.value = 66);
    } else if (action === "EXP_WEEKLY") {
        document.querySelectorAll(".leg-expiry").forEach(el => el.value = "Weekly");
    } else if (action === "EXP_NEXT_WEEKLY") {
        document.querySelectorAll(".leg-expiry").forEach(el => el.value = "Next Weekly");
    } else if (action === "PROD_NRML") {
        document.querySelectorAll(".leg-product").forEach(el => el.value = "NRML");
    } else if (action === "PROD_MIS") {
        document.querySelectorAll(".leg-product").forEach(el => el.value = "MIS");
    }
    document.getElementById("cfgBulkEdit").value = "";
}

function toggleAlgoBuilderSection(secId) {
    const sec = document.getElementById(secId);
    const chevron = document.getElementById("chevron_" + secId);
    if (!sec) return;
    const isHidden = (sec.style.display === "none");
    sec.style.display = isHidden ? "block" : "none";
    if (chevron) {
        chevron.textContent = isHidden ? "▼" : "▶";
    }
}

function stepStraddleTimePart(baseId, unit, delta) {
    const hhInput = document.getElementById(baseId + "_hh");
    const mmInput = document.getElementById(baseId + "_mm");
    const ssInput = document.getElementById(baseId + "_ss");
    let h = parseInt(hhInput ? hhInput.value : 0, 10) || 0;
    let m = parseInt(mmInput ? mmInput.value : 0, 10) || 0;
    let s = parseInt(ssInput ? ssInput.value : 0, 10) || 0;

    if (unit === 'hh') {
        h = (h + delta + 24) % 24;
        if (hhInput) hhInput.value = String(h).padStart(2, '0');
    } else if (unit === 'mm') {
        m = (m + delta + 60) % 60;
        if (mmInput) mmInput.value = String(m).padStart(2, '0');
    } else if (unit === 'ss') {
        s = (s + delta + 60) % 60;
        if (ssInput) ssInput.value = String(s).padStart(2, '0');
    }
    const finalStr = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    const main = document.getElementById(baseId);
    if (main) main.value = finalStr;
}
window.stepStraddleTimePart = stepStraddleTimePart;

function syncTimeSegmentsToMain(baseId) {
    const hhInput = document.getElementById(baseId + "_hh");
    const mmInput = document.getElementById(baseId + "_mm");
    const ssInput = document.getElementById(baseId + "_ss");
    let h = Math.min(23, Math.max(0, parseInt(hhInput ? hhInput.value : 0, 10) || 0));
    let m = Math.min(59, Math.max(0, parseInt(mmInput ? mmInput.value : 0, 10) || 0));
    let s = Math.min(59, Math.max(0, parseInt(ssInput ? ssInput.value : 0, 10) || 0));
    if (hhInput) hhInput.value = String(h).padStart(2, '0');
    if (mmInput) mmInput.value = String(m).padStart(2, '0');
    if (ssInput) ssInput.value = String(s).padStart(2, '0');
    const main = document.getElementById(baseId);
    if (main) main.value = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}
window.syncTimeSegmentsToMain = syncTimeSegmentsToMain;

function setStraddleTimeInputValue(baseId, timeStr) {
    const main = document.getElementById(baseId);
    if (main) main.value = timeStr || "09:15:00";
    const parts = (timeStr || "09:15:00").split(":");
    const h = parts[0] ? parts[0].padStart(2, "0") : "09";
    const m = parts[1] ? parts[1].padStart(2, "0") : "15";
    const s = parts[2] ? parts[2].padStart(2, "0") : "00";
    const hhInput = document.getElementById(baseId + "_hh");
    const mmInput = document.getElementById(baseId + "_mm");
    const ssInput = document.getElementById(baseId + "_ss");
    if (hhInput) hhInput.value = h;
    if (mmInput) mmInput.value = m;
    if (ssInput) ssInput.value = s;
}
window.setStraddleTimeInputValue = setStraddleTimeInputValue;

function validateTimeDigits(input, maxVal) {
    let val = input.value.replace(/[^0-9]/g, "");
    if (val.length > 2) val = val.slice(0, 2);
    input.value = val;
}
window.validateTimeDigits = validateTimeDigits;

function stepStraddleTime(inputId, deltaSeconds) {
    const input = document.getElementById(inputId);
    if (!input) return;
    let timeStr = (input.value || "").trim();
    let parts = timeStr.split(":");
    let h = 9, m = 15, s = 0;
    if (parts.length >= 2) {
        h = parseInt(parts[0], 10) || 0;
        m = parseInt(parts[1], 10) || 0;
        s = parts.length >= 3 ? (parseInt(parts[2], 10) || 0) : 0;
    }
    let totalSec = h * 3600 + m * 60 + s + deltaSeconds;
    if (totalSec < 0) totalSec = 0;
    if (totalSec > 86399) totalSec = 86399;

    const newH = String(Math.floor(totalSec / 3600)).padStart(2, "0");
    const newM = String(Math.floor((totalSec % 3600) / 60)).padStart(2, "0");
    const newS = String(totalSec % 60).padStart(2, "0");

    setStraddleTimeInputValue(inputId, `${newH}:${newM}:${newS}`);
}

function serializeCurrentStrategySettings() {
    const active = window._straddleState.activeStrategy || {};
    const inputSid = document.getElementById("cfgStrategyId")?.value;
    const inputName = document.getElementById("cfgStrategyName")?.value;

    let undSource = "Spot";
    if (document.getElementById("btnUndFutures")?.classList.contains("active-toggle")) {
        undSource = "Futures";
    } else if (document.getElementById("btnUndSynthetic")?.classList.contains("active-toggle")) {
        undSource = "Synthetic Futures";
    }

    const strategy = {
        strategy_id: inputSid || active.strategy_id || ("strat_" + Date.now()),
        name: (inputName && inputName.trim()) ? inputName.trim() : (active.name || "Custom Straddle Strategy"),
        enabled: active.enabled !== false,
        multiplier: parseInt(document.getElementById("cfgMultiplier")?.value) || 1,
        segment: document.getElementById("cfgSegment")?.value || "Options",
        start_time: document.getElementById("cfgStartTime")?.value || "09:15:56",
        end_time: document.getElementById("cfgEndTime")?.value || "15:29:50",
        move_sl_to_cost: document.getElementById("cfgToggleMoveSlToCost")?.checked,
        wait_and_trade: document.getElementById("cfgToggleWaitAndTrade")?.checked,
        re_entry_enabled: document.getElementById("cfgToggleReEntry")?.checked,
        range_breakout: document.getElementById("cfgToggleRangeBreakout")?.checked,
        underlying_price_source: undSource,
        trade_type: document.getElementById("btnTradePositional")?.classList.contains("active-toggle") ? "Positional" : "Intraday",
        schedule_days: active.schedule_days || ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"],
        mtm_target: {
            type: document.getElementById("cfgMtmTargetType")?.value,
            value: parseFloat(document.getElementById("cfgMtmTargetValue")?.value) || 10000,
            re_entry: document.getElementById("cfgMtmTargetReEntry")?.value,
            action: document.getElementById("cfgMtmTargetAction")?.value
        },
        mtm_sl: {
            type: document.getElementById("cfgMtmSlType")?.value,
            value: parseFloat(document.getElementById("cfgMtmSlValue")?.value) || -5000,
            profit_lock: document.getElementById("cfgMtmProfitLock")?.value,
            action: document.getElementById("cfgMtmSlAction")?.value
        },
        advanced_settings: {
            entry_order_type: document.getElementById("cfgEntryOrderType")?.value || "Limit (L) / Stoploss Limit (SL-L)",
            entry_buffer_type: document.getElementById("cfgEntryBufferType")?.value || "Percentage",
            entry_buffer_val: isNaN(parseFloat(document.getElementById("cfgEntryBufferVal")?.value)) ? 5.0 : parseFloat(document.getElementById("cfgEntryBufferVal")?.value),
            entry_buffer_value: isNaN(parseFloat(document.getElementById("cfgEntryBufferVal")?.value)) ? 5.0 : parseFloat(document.getElementById("cfgEntryBufferVal")?.value),
            entry_market_timeout_sec: isNaN(parseInt(document.getElementById("cfgEntryMarketTimeoutSec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgEntryMarketTimeoutSec")?.value, 10),
            entry_delay_side: document.getElementById("cfgEntryDelaySide")?.value || "Delay entry of Sell positions",
            entry_order_delay_side: document.getElementById("cfgEntryDelaySide")?.value || "Delay entry of Sell positions",
            entry_delay_sec: isNaN(parseInt(document.getElementById("cfgEntryDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgEntryDelaySec")?.value, 10),
            entry_order_delay_sec: isNaN(parseInt(document.getElementById("cfgEntryDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgEntryDelaySec")?.value, 10),
            exit_order_type: document.getElementById("cfgExitOrderType")?.value || "Limit (L) / Stoploss Limit (SL-L)",
            exit_buffer_type: document.getElementById("cfgExitBufferType")?.value || "Percentage",
            exit_buffer_val: isNaN(parseFloat(document.getElementById("cfgExitBufferVal")?.value)) ? 0.1 : parseFloat(document.getElementById("cfgExitBufferVal")?.value),
            exit_buffer_value: isNaN(parseFloat(document.getElementById("cfgExitBufferVal")?.value)) ? 0.1 : parseFloat(document.getElementById("cfgExitBufferVal")?.value),
            exit_market_timeout_sec: isNaN(parseInt(document.getElementById("cfgExitMarketTimeoutSec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgExitMarketTimeoutSec")?.value, 10),
            exit_delay_side: document.getElementById("cfgExitDelaySide")?.value || "Delay exit of Buy positions",
            exit_order_delay_side: document.getElementById("cfgExitDelaySide")?.value || "Delay exit of Buy positions",
            exit_delay_sec: isNaN(parseInt(document.getElementById("cfgExitDelaySec")?.value, 10)) ? 3 : parseInt(document.getElementById("cfgExitDelaySec")?.value, 10),
            exit_order_delay_sec: isNaN(parseInt(document.getElementById("cfgExitDelaySec")?.value, 10)) ? 3 : parseInt(document.getElementById("cfgExitDelaySec")?.value, 10),
            sl_order_placement: document.getElementById("cfgSlOrderPlacement")?.value || "Keep with self",
            target_order_placement: document.getElementById("cfgTargetOrderPlacement")?.value || "Keep with self",
            sl_placement_delay_sec: isNaN(parseInt(document.getElementById("cfgSlPlacementDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgSlPlacementDelaySec")?.value, 10),
            mtm_sl_delay_sec: isNaN(parseInt(document.getElementById("cfgMtmSlDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgMtmSlDelaySec")?.value, 10),
            auto_handling_sl_cancellation: document.getElementById("cfgAutoHandlingSl")?.checked !== false,
            auto_exit_margin_error: !!document.getElementById("cfgAutoExitMargin")?.checked,
            auto_exit_on_margin_error: !!document.getElementById("cfgAutoExitMargin")?.checked
        },
        legs: []
    };

    const trs = document.querySelectorAll("#straddleLegsBuilderBody tr");
    trs.forEach(tr => {
        strategy.legs.push({
            id: tr.id,
            side: tr.querySelector(".leg-side")?.value,
            product: tr.querySelector(".leg-product")?.value,
            strike_selection: tr.querySelector(".leg-strike")?.value,
            multiple_strike_selection: tr.querySelector(".leg-multiple-strike")?.value || "None",
            option_type: tr.querySelector(".leg-type")?.value,
            lots: parseInt(tr.querySelector(".leg-lots")?.value) || 1,
            target_type: tr.querySelector(".leg-tgt-type")?.value,
            target_value: parseFloat(tr.querySelector(".leg-tgt-val")?.value) || 45.0,
            target_re_execute_type: tr.querySelector(".leg-tgt-re-type")?.value,
            target_re_execute_count: parseInt(tr.querySelector(".leg-tgt-re-cnt")?.value) || 6,
            sl_type: tr.querySelector(".leg-sl-type")?.value,
            sl_value: parseFloat(tr.querySelector(".leg-sl-val")?.value) || 66.0,
            tsl_type: tr.querySelector(".leg-tsl-type")?.value,
            sl_re_execute_type: tr.querySelector(".leg-sl-re-type")?.value,
            sl_re_execute_count: parseInt(tr.querySelector(".leg-sl-re-cnt")?.value) || 1,
            action: tr.querySelector(".leg-action")?.value,
            underlying: tr.querySelector(".leg-underlying")?.value,
            expiry: tr.querySelector(".leg-expiry")?.value
        });
    });

    return strategy;
}

async function saveStraddleStrategySettings() {
    const strat = serializeCurrentStrategySettings();
    showStraddleToast(`💾 Saving strategy '${strat.name}'...`, "info", 1500);

    // Optimistically update local state for instant responsiveness
    if (window._straddleState && window._straddleState.allStrategies) {
        const idx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === strat.strategy_id);
        if (idx >= 0) window._straddleState.allStrategies[idx] = strat;
        window._straddleState.activeStrategy = strat;
        renderStraddleAlgosTable(true);
    }

    try {
        const resp = await fetch("/api/straddle/strategy/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy: strat })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`💾 Strategy '${strat.name}' saved successfully!`, "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error saving: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3500);
    }
}

async function saveStraddleStrategyAndGoBack() {
    saveStraddleStrategySettings();
    switchStraddleSubView("algos");
}

async function saveAsNewStraddlePreset() {
    const name = prompt("Enter name for this new Strategy Preset:", "Custom Preset " + Date.now().toString().slice(-4));
    if (!name) return;
    const strat = serializeCurrentStrategySettings();
    strat.strategy_id = "strat_" + Date.now();
    strat.name = name;
    showStraddleToast(`📄 Creating preset '${name}'...`, "info", 1500);

    try {
        const resp = await fetch("/api/straddle/strategy/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy: strat })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`✅ New preset '${name}' saved!`, "success", 2500);
            await fetchStraddleState();
            onStraddleStrategySelect(strat.strategy_id);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3000);
    }
}

async function saveAndStartStraddleTrade() {
    const strat = serializeCurrentStrategySettings();
    strat.status = "TRADE_ACTIVE";
    strat.enabled = true;
    showStraddleToast(`⚡ Launching '${strat.name}' & punching orders...`, "info", 2500);

    // Instant optimistic state update
    if (window._straddleState && window._straddleState.allStrategies) {
        const idx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === strat.strategy_id);
        if (idx >= 0) window._straddleState.allStrategies[idx] = strat;
        window._straddleState.activeStrategy = strat;
        window._straddleState.activeStrategyId = strat.strategy_id;
        window._straddleState.status = "TRADE_ACTIVE";
        renderStraddleAlgosTable(true);
    }

    try {
        const savePromise = fetch("/api/straddle/strategy/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy: strat })
        });
        const startPromise = fetch("/api/straddle/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strat.strategy_id, force_fresh: true })
        });

        const [saveResp, startResp] = await Promise.all([savePromise, startPromise]);
        const startRes = await startResp.json();
        if (startRes.success) {
            showStraddleToast(`🚀 Trade started! Orders punched to Kotak Neo.`, "success", 3000);
        } else {
            showStraddleToast(`Notice: ${startRes.error || "Orders dispatched"}`, "warning", 3500);
        }
        await fetchStraddleState();
        await loadStraddleOpenPositions();
        renderStraddleAlgosTable(true);
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
    toggleStraddleSettingsPanel();
}

async function onStraddleStrategySelect(stratId) {
    try {
        await fetch("/api/straddle/strategy/set_active", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: stratId })
        });
        await fetchStraddleState();
    } catch (e) {
        console.error("Error setting active strategy:", e);
    }
}

async function straddleStartAlgo() {
    const stratId = window._straddleState.activeStrategyId;
    showStraddleToast("🚀 Dispatching strategy orders...", "info", 2000);
    try {
        const resp = await fetch("/api/straddle/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: stratId, force_fresh: true })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("✅ Algorithm started & orders punched!", "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Notice: " + (res.error || "Execution triggered"), "warning", 3500);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3500);
    }
}

async function straddleResetActiveStrategy() {
    return resetCurrentStraddleAlgo();
}
window.straddleResetActiveStrategy = straddleResetActiveStrategy;

async function clearStraddleClosedAndExpiredPositions() {
    showStraddleToast("🧹 Clearing closed & expired positions...", "info", 1500);
    try {
        const resp = await fetch("/api/straddle/positions/clear_closed", {
            method: "POST",
            headers: { "Content-Type": "application/json" }
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("✅ Closed positions cleared.", "success", 2000);
            await loadStraddleOpenPositions();
            await fetchStraddleState();
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3000);
    }
}

async function straddleTerminateAlgo() {
    showStraddleToast("⏹ Terminating algorithm execution...", "info", 1500);

    // Instant optimistic state change
    if (window._straddleState) {
        window._straddleState.status = "TERMINATED";
        if (window._straddleState.allStrategies) {
            window._straddleState.allStrategies.forEach(s => {
                s.status = "TERMINATED";
                s.enabled = false;
            });
        }
        if (window._straddleState.activeStrategy) {
            window._straddleState.activeStrategy.status = "TERMINATED";
            window._straddleState.activeStrategy.enabled = false;
        }
        renderStraddleHeader(window._straddleState);
        renderStraddleAlgosTable(true);
    }

    try {
        const resp = await fetch("/api/straddle/terminate", { method: "POST" });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("⏹ Algorithm terminated successfully.", "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3000);
    }
}

async function straddleSquareOffAllAndTerminate() {
    if (!confirm("EMERGENCY ACTION: Square off ALL open strategy positions and terminate algorithm immediately?")) return;
    showStraddleToast("⚠️ Executing Emergency Square-Off...", "warning", 2000);

    // Instant optimistic state change
    if (window._straddleState && window._straddleState.allStrategies) {
        window._straddleState.allStrategies.forEach(s => {
            s.status = "TERMINATED";
            s.enabled = false;
        });
        window._straddleState.status = "TERMINATED";
        renderStraddleHeader(window._straddleState);
        renderStraddleAlgosTable(true);
    }

    try {
        const resp = await fetch("/api/straddle/square_off_all", { method: "POST" });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("✅ All positions squared off & algorithms terminated.", "success", 3000);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3500);
    }
}

async function straddleSquareOffSelectedLegs() {
    const selected = document.querySelectorAll(".straddle-leg-checkbox:checked");
    if (selected.length === 0) {
        showStraddleToast("Please select at least one leg checkbox to square off.", "warning", 2500);
        return;
    }
    if (!confirm(`Square off the ${selected.length} selected leg(s)?`)) return;

    showStraddleToast(`Exiting ${selected.length} selected leg(s)...`, "info", 1500);
    for (const cb of selected) {
        const idx = parseInt(cb.dataset.index);
        try {
            await fetch("/api/straddle/square_off_leg", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ leg_index: idx })
            });
        } catch (e) {
            console.error("Error exiting leg index " + idx, e);
        }
    }
    showStraddleToast("✅ Selected leg(s) squared off.", "success", 2000);
    await fetchStraddleState();
    await loadStraddleOpenPositions();
}

function straddleShowBrokerInfo() {
    showStraddleToast("⚡ Kotak Neo Live Broker (Active) | Account: 144766 - SHIVEK GUPTA | 100% Real API Execution", "success", 3000);
    loadStraddleMargin();
    loadStraddleOpenPositions();
}

async function straddleToggleExecutionMode() {
    straddleShowBrokerInfo();
}


// =========================================================================
// OPTION CHAIN INTERACTIVE MATRIX (EXPRESSIVE, COLOURFUL + GREEKS & PCR)
// =========================================================================

let _optChainTickerInterval = null;
let _isLoadingOptionChain = false;

function startOptionChainTicker() {
    stopOptionChainTicker();
    _optChainTickerInterval = setInterval(() => {
        const modal = document.getElementById("straddleOptionChainModal");
        if (modal && modal.style.display === "flex") {
            loadStraddleOptionChain(null, true);
        } else {
            stopOptionChainTicker();
        }
    }, 500); // Continuous 500ms ultra-fast tick feed for Option Chain LTP & Bid/Ask
}

function stopOptionChainTicker() {
    if (_optChainTickerInterval) {
        clearInterval(_optChainTickerInterval);
        _optChainTickerInterval = null;
    }
}

async function openStraddleOptionChainModal(undParam) {
    const modal = document.getElementById("straddleOptionChainModal");
    if (!modal) return;
    modal.style.display = "flex";

    const undSel = document.getElementById("optChainUnderlyingSelect");
    const targetUnd = undParam || undSel?.value || window._straddleState.activeStrategy?.underlying || "NIFTY";
    if (undSel) undSel.value = targetUnd;

    await onChangeOptChainUnderlying(targetUnd);
    startOptionChainTicker();
}

function closeStraddleOptionChainModal() {
    const modal = document.getElementById("straddleOptionChainModal");
    if (modal) modal.style.display = "none";
    stopOptionChainTicker();
}

async function onChangeOptChainUnderlying(und) {
    window._straddleState.optionChainUnderlying = und;
    try {
        const resp = await fetch(`/api/straddle/expiries?underlying=${und}`);
        const data = await resp.json();
        if (data.success && data.expiries) {
            const sel = document.getElementById("optChainExpirySelect");
            if (sel) {
                sel.innerHTML = "";
                data.expiries.forEach(e => {
                    const opt = document.createElement("option");
                    opt.value = e.expiry_ms;
                    opt.textContent = e.formatted_label + " (" + e.tag + ")";
                    sel.appendChild(opt);
                });
            }
            if (data.expiries.length > 0) {
                await loadStraddleOptionChain(data.expiries[0].expiry_ms);
            } else {
                await loadStraddleOptionChain();
            }
        }
    } catch (e) {
        console.error("Error loading expiries for underlying:", e);
        await loadStraddleOptionChain();
    }
}

// =========================================================================
// BASKET MODE CONTROLLER FOR OPTION CHAIN (KOTAK NEO SCREENSHOT 5 GRADE)
// =========================================================================

window._optionChainBasketMode = false;
window._chainSelectedLegs = {};

function toggleOptionChainBasketMode() {
    window._optionChainBasketMode = !window._optionChainBasketMode;
    const isMode = window._optionChainBasketMode;
    const btn = document.getElementById("btnToggleBasketMode");
    const lbl = document.getElementById("basketModeStateLabel");
    const thCe = document.getElementById("thCeActionHeader") || document.getElementById("thCeBasketTick");
    const thPe = document.getElementById("thPeActionHeader") || document.getElementById("thPeBasketTick");
    const thCeSuper = document.getElementById("thCeSuperHeader");
    const thPeSuper = document.getElementById("thPeSuperHeader");
    const botBar = document.getElementById("optionChainBasketBottomBar");

    if (lbl) {
        lbl.textContent = isMode ? "ON" : "OFF";
        lbl.style.color = isMode ? "#10b981" : "#f87171";
    }
    if (btn) {
        if (isMode) {
            btn.style.background = "#1d4ed8";
            btn.style.borderColor = "#60a5fa";
            btn.style.color = "#ffffff";
            btn.style.boxShadow = "0 0 12px rgba(37,99,235,0.6)";
        } else {
            btn.style.background = "#0f172a";
            btn.style.borderColor = "#38bdf8";
            btn.style.color = "#38bdf8";
            btn.style.boxShadow = "none";
        }
    }
    if (thCe) {
        thCe.textContent = isMode ? "SELECT" : "ACTION";
        thCe.style.display = "";
    }
    if (thPe) {
        thPe.textContent = isMode ? "SELECT" : "ACTION";
        thPe.style.display = "";
    }
    if (thCeSuper) thCeSuper.setAttribute("colspan", "3");
    if (thPeSuper) thPeSuper.setAttribute("colspan", "3");
    if (botBar) botBar.style.display = isMode ? "flex" : "none";

    updateOptionChainBasketSummaryBar();
    loadStraddleOptionChain();
}
window.toggleOptionChainBasketMode = toggleOptionChainBasketMode;

function onToggleChainBasketLeg(strike, optType, side, symbol, token, ltp, lotSize, expDate, isChecked) {
    const key = `${strike}_${optType}_${side}`;
    const und = window._straddleState?.optionChainUnderlying || "NIFTY";
    const resolvedLot = Number(lotSize) > 0 ? Number(lotSize) : (und === "BANKNIFTY" ? 15 : (und === "FINNIFTY" ? 25 : 65));
    const resolvedStrike = Number(strike) || 0;
    const resolvedPrice = Number(ltp) || 0.05;

    if (isChecked) {
        window._chainSelectedLegs[key] = {
            strike: resolvedStrike,
            opt_type: optType,
            side: side,
            symbol: symbol,
            token: token || "",
            ltp: resolvedPrice,
            lot_size: resolvedLot,
            expiry_date: expDate || "Weekly"
        };

        // Directly add or sync with window._basketOrders
        if (!Array.isArray(window._basketOrders)) window._basketOrders = [];
        const exists = window._basketOrders.some(o => Number(o.strike) === resolvedStrike && o.opt_type === optType && o.side === side);
        if (!exists) {
            window._basketOrders.push({
                id: "b_" + Date.now() + "_" + Math.floor(Math.random() * 10000),
                selected: true,
                underlying: und,
                symbol: symbol || `${und} ${resolvedStrike} ${optType}`,
                trading_symbol: symbol || `${und} ${resolvedStrike} ${optType}`,
                token: token || "",
                strike: resolvedStrike,
                opt_type: (optType || "CE").toUpperCase(),
                side: (side || "SELL").toUpperCase(),
                expiry_date: expDate || "Weekly",
                product: "CF",
                lots: 1,
                lot_size: resolvedLot,
                quantity: resolvedLot,
                price: resolvedPrice,
                order_type: "LIMIT",
                ltp: resolvedPrice,
                change: 0,
                change_pct: 0
            });
            try {
                localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
            } catch(e) {}
        }
    } else {
        delete window._chainSelectedLegs[key];
        // Directly remove from window._basketOrders
        if (Array.isArray(window._basketOrders)) {
            window._basketOrders = window._basketOrders.filter(o => !(Number(o.strike) === resolvedStrike && o.opt_type === optType && o.side === side));
            try {
                localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
            } catch(e) {}
        }
    }
    updateOptionChainBasketSummaryBar();
    updateBasketBadges();
}
window.onToggleChainBasketLeg = onToggleChainBasketLeg;

function updateOptionChainBasketSummaryBar() {
    const keys = Object.keys(window._chainSelectedLegs);
    const count = keys.length;
    const countEl = document.getElementById("optionChainBasketSelectedCount");
    const pillsWrap = document.getElementById("optionChainBasketSummaryPills");
    const addBtn = document.getElementById("btnAddSelectedToBasket");
    const chainBasketTotalBadge = document.getElementById("chainBasketTotalCountBadge");

    if (chainBasketTotalBadge) {
        chainBasketTotalBadge.textContent = window._basketOrders?.length || 0;
    }
    if (countEl) countEl.textContent = count;
    if (addBtn) {
        addBtn.disabled = (count === 0);
        addBtn.style.opacity = (count === 0) ? "0.5" : "1";
        addBtn.style.cursor = (count === 0) ? "not-allowed" : "pointer";
    }

    if (pillsWrap) {
        pillsWrap.innerHTML = keys.map(k => {
            const item = window._chainSelectedLegs[k];
            const isBuy = item.side === "BUY";
            const bg = isBuy ? "rgba(16,185,129,0.2)" : "rgba(244,63,94,0.2)";
            const border = isBuy ? "rgba(16,185,129,0.4)" : "rgba(244,63,94,0.4)";
            const text = isBuy ? "#34d399" : "#fb7185";
            return `<span class="basket-pill-chip ${isBuy ? 'chip-buy' : 'chip-sell'}">
                ${isBuy ? '+B' : '-S'} ${item.strike} ${item.opt_type}
                <span style="cursor:pointer; font-size:10px; margin-left:3px;" onclick="onToggleChainBasketLeg(${item.strike}, '${item.opt_type}', '${item.side}', '', '', 0, 0, '', false); loadStraddleOptionChain();">✕</span>
            </span>`;
        }).join("");
    }
}
window.updateOptionChainBasketSummaryBar = updateOptionChainBasketSummaryBar;

function clearOptionChainBasketSelection() {
    window._chainSelectedLegs = {};
    updateOptionChainBasketSummaryBar();
    loadStraddleOptionChain();
}
window.clearOptionChainBasketSelection = clearOptionChainBasketSelection;

function addSelectedLegsToBasketModal() {
    const keys = Object.keys(window._chainSelectedLegs);
    if (keys.length === 0) {
        showStraddleToast("Please select at least one Call or Put option leg first.", "warning", 2500);
        return;
    }

    const und = window._straddleState?.optionChainUnderlying || "NIFTY";
    let addedCount = 0;

    keys.forEach(k => {
        const leg = window._chainSelectedLegs[k];
        const resolvedLot = Number(leg.lot_size) > 0 ? Number(leg.lot_size) : (und === "BANKNIFTY" ? 15 : (und === "FINNIFTY" ? 25 : 65));
        const resolvedStrike = Number(leg.strike) || 0;
        const resolvedPrice = Number(leg.ltp) || 0.05;

        const newOrder = {
            id: "b_" + Date.now() + "_" + Math.floor(Math.random() * 10000),
            selected: true,
            underlying: und,
            symbol: leg.symbol || `${und} ${resolvedStrike} ${leg.opt_type}`,
            trading_symbol: leg.symbol || `${und} ${resolvedStrike} ${leg.opt_type}`,
            token: leg.token || "",
            strike: resolvedStrike,
            opt_type: (leg.opt_type || "CE").toUpperCase(),
            side: (leg.side || "SELL").toUpperCase(),
            expiry_date: leg.expiry_date || "Weekly",
            product: "CF",
            lots: 1,
            lot_size: resolvedLot,
            quantity: resolvedLot,
            price: resolvedPrice,
            order_type: "LIMIT",
            ltp: resolvedPrice,
            change: 0,
            change_pct: 0
        };
        window._basketOrders.push(newOrder);
        addedCount++;
    });

    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
    } catch(e) {}

    window._chainSelectedLegs = {};
    updateOptionChainBasketSummaryBar();
    updateBasketBadges();
    showStraddleToast(`🧺 Added ${addedCount} leg(s) directly to your Basket!`, "success", 2500);
    openBasketOrderModal();
}
window.addSelectedLegsToBasketModal = addSelectedLegsToBasketModal;

async function loadStraddleOptionChain(expiryMs, isBackgroundUpdate = false) {
    if (isBackgroundUpdate && _isLoadingOptionChain) return;
    _isLoadingOptionChain = true;

    const undSel = document.getElementById("optChainUnderlyingSelect");
    const und = undSel?.value || window._straddleState.optionChainUnderlying || "NIFTY";
    
    const expSel = document.getElementById("optChainExpirySelect");
    const exp = expiryMs || expSel?.value || "";

    const rangeSel = document.getElementById("optChainStrikeRangeSelect");
    const strikeRange = rangeSel ? rangeSel.value : "15";

    const tbody = document.getElementById("straddleOptionChainBody");
    const spotLabel = document.getElementById("optChainSpotTicker");
    if (!tbody) {
        _isLoadingOptionChain = false;
        return;
    }

    if (!isBackgroundUpdate && (!tbody.children.length || tbody.querySelector("td[colspan]"))) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align:center; padding:35px; color:#94a3b8;"><span style="font-size:16px;">⏳</span> Loading Real-Time Option Depth & Greeks...</td></tr>`;
    }

    try {
        let url = `/api/straddle/option_chain?underlying=${und}&strike_range=${strikeRange}`;
        if (exp) url += `&expiry_ms=${exp}`;
        const resp = await fetch(url);
        const data = await resp.json();
        if (data.success && data.data) {
            const chain = data.data;
            const currentExpDateStr = chain.expiry_date || (expSel?.selectedOptions && expSel.selectedOptions[0] ? expSel.selectedOptions[0].text : "Weekly");

            // Update Header Spot Ticker
            if (spotLabel) {
                spotLabel.textContent = `${chain.underlying}: ₹${chain.spot_price.toFixed(2)} • ATM: ${chain.atm_strike}`;
            }

            // Update Market Insights Ribbon
            const spotEl = document.getElementById("optInsightSpot");
            if (spotEl) spotEl.textContent = `₹${chain.spot_price.toFixed(2)} (ATM: ${chain.atm_strike})`;

            const pcrEl = document.getElementById("optInsightPcr");
            const pcrTag = document.getElementById("optInsightPcrTag");
            if (pcrEl) {
                pcrEl.textContent = chain.pcr.toFixed(2);
                pcrEl.style.color = chain.sentiment_color || "#10b981";
            }
            if (pcrTag) {
                pcrTag.textContent = `(${chain.sentiment})`;
                pcrTag.style.color = chain.sentiment_color || "#10b981";
            }

            const maxPainEl = document.getElementById("optInsightMaxPain");
            if (maxPainEl) maxPainEl.textContent = `Strike ${chain.max_pain_strike}`;

            const supEl = document.getElementById("optInsightSupport");
            if (supEl) {
                const pLakhs = (chain.rows.find(r => r.strike_price === chain.max_pe_oi_strike)?.pe?.oi || 0) / 100000;
                supEl.textContent = `${chain.max_pe_oi_strike} (${pLakhs.toFixed(1)}L)`;
            }

            const resEl = document.getElementById("optInsightResistance");
            if (resEl) {
                const cLakhs = (chain.rows.find(r => r.strike_price === chain.max_ce_oi_strike)?.ce?.oi || 0) / 100000;
                resEl.textContent = `${chain.max_ce_oi_strike} (${cLakhs.toFixed(1)}L)`;
            }

            const totOiEl = document.getElementById("optInsightTotalOi");
            if (totOiEl) {
                const ceL = (chain.total_ce_oi / 100000).toFixed(1);
                const peL = (chain.total_pe_oi / 100000).toFixed(1);
                totOiEl.innerHTML = `<span style="color:#34d399; font-weight:800;">${ceL}L CE</span> vs <span style="color:#fb7185; font-weight:800;">${peL}L PE</span>`;
            }

            const isBasketMode = !!window._optionChainBasketMode;

            // Sync Header Colspan & Visibility
            const thCe = document.getElementById("thCeActionHeader") || document.getElementById("thCeBasketTick");
            const thPe = document.getElementById("thPeActionHeader") || document.getElementById("thPeBasketTick");
            const thCeSuper = document.getElementById("thCeSuperHeader");
            const thPeSuper = document.getElementById("thPeSuperHeader");
            if (thCe) {
                thCe.textContent = isBasketMode ? "SELECT" : "ACTION";
                thCe.style.display = "";
            }
            if (thPe) {
                thPe.textContent = isBasketMode ? "SELECT" : "ACTION";
                thPe.style.display = "";
            }
            if (thCeSuper) thCeSuper.setAttribute("colspan", "3");
            if (thPeSuper) thPeSuper.setAttribute("colspan", "3");

            let rowsHtml = "";
            chain.rows.forEach(r => {
                const isAtm = r.is_atm;
                const strike = r.strike_price;
                const isCeItm = strike < chain.spot_price;
                const isPeItm = strike > chain.spot_price;

                const rowClass = isAtm ? "opt-chain-atm-row" : "";
                const ceCellClass = isCeItm ? "opt-chain-itm-ce" : "";
                const peCellClass = isPeItm ? "opt-chain-itm-pe" : "";

                // CE Data
                const ceLtp = r.ce ? r.ce.ltp.toFixed(2) : "-";
                const ceLtpNum = r.ce ? (r.ce.ltp || 0) : 0;
                const ceBidAsk = r.ce ? `${r.ce.bid.toFixed(1)} / ${r.ce.ask.toFixed(1)}` : "-";
                const ceSym = r.ce ? (r.ce.trading_symbol || `${und} ${strike} CE`) : `${und} ${strike} CE`;
                const ceKey = r.ce ? (r.ce.instrument_key || "") : "";
                const ceLot = r.ce ? (r.ce.lot_size || 65) : 65;
                const ceChg = r.ce ? (r.ce.net_change || 0) : 0;
                const ceChgPct = r.ce ? (r.ce.net_change_pct || 0) : 0;

                // PE Data
                const peLtp = r.pe ? r.pe.ltp.toFixed(2) : "-";
                const peLtpNum = r.pe ? (r.pe.ltp || 0) : 0;
                const peBidAsk = r.pe ? `${r.pe.bid.toFixed(1)} / ${r.pe.ask.toFixed(1)}` : "-";
                const peSym = r.pe ? (r.pe.trading_symbol || `${und} ${strike} PE`) : `${und} ${strike} PE`;
                const peKey = r.pe ? (r.pe.instrument_key || "") : "";
                const peLot = r.pe ? (r.pe.lot_size || 65) : 65;
                const peChg = r.pe ? (r.pe.net_change || 0) : 0;
                const peChgPct = r.pe ? (r.pe.net_change_pct || 0) : 0;

                // Checkbox state in Basket Mode (inspect both chainSelection and active basketOrders)
                const ceBuyKey = `${strike}_CE_BUY`;
                const ceSellKey = `${strike}_CE_SELL`;
                const peBuyKey = `${strike}_PE_BUY`;
                const peSellKey = `${strike}_PE_SELL`;

                const isCeBuyChecked = !!(window._chainSelectedLegs[ceBuyKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "CE" && o.side === "BUY"));
                const isCeSellChecked = !!(window._chainSelectedLegs[ceSellKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "CE" && o.side === "SELL"));
                const isPeBuyChecked = !!(window._chainSelectedLegs[peBuyKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "PE" && o.side === "BUY"));
                const isPeSellChecked = !!(window._chainSelectedLegs[peSellKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "PE" && o.side === "SELL"));

                const ceActionCell = isBasketMode ? `
                    <td style="text-align:center; padding:4px 2px; white-space:nowrap; width:80px;">
                        <div class="chain-tick-wrap">
                            <label class="chain-tick-btn tick-buy ${isCeBuyChecked ? 'checked' : ''}" title="Tick to BUY CE in basket">
                                <input type="checkbox" ${isCeBuyChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'CE', 'BUY', '${ceSym}', '${ceKey}', ${ceLtpNum}, ${ceLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isCeBuyChecked ? '☑' : '☐'}</span><span class="tick-lbl">B</span>
                            </label>
                            <label class="chain-tick-btn tick-sell ${isCeSellChecked ? 'checked' : ''}" title="Tick to SELL CE in basket">
                                <input type="checkbox" ${isCeSellChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'CE', 'SELL', '${ceSym}', '${ceKey}', ${ceLtpNum}, ${ceLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isCeSellChecked ? '☑' : '☐'}</span><span class="tick-lbl">S</span>
                            </label>
                        </div>
                    </td>
                ` : `
                    <td style="text-align:center; padding:4px 2px; white-space:nowrap; width:80px;">
                        <div class="chain-quick-trade-wrap" style="display:inline-flex; align-items:center; gap:4px;">
                            <button type="button" class="chain-quick-trade-btn chain-btn-buy" style="background:linear-gradient(135deg, #059669, #10b981) !important; color:#ffffff !important; border:1px solid #34d399 !important; font-weight:900 !important; font-size:12px; width:26px; height:24px; border-radius:4px; box-shadow:0 0 8px rgba(16,185,129,0.5); cursor:pointer; display:inline-flex; align-items:center; justify-content:center;" title="Direct Trade: BUY ${ceSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${ceSym}', '${ceKey}', ${strike}, 'CE', 'BUY', ${ceLtpNum}, ${ceLot}, ${ceChg}, ${ceChgPct})">B</button>
                            <button type="button" class="chain-quick-trade-btn chain-btn-sell" style="background:linear-gradient(135deg, #dc2626, #ef4444) !important; color:#ffffff !important; border:1px solid #f87171 !important; font-weight:900 !important; font-size:12px; width:26px; height:24px; border-radius:4px; box-shadow:0 0 8px rgba(239,68,68,0.5); cursor:pointer; display:inline-flex; align-items:center; justify-content:center;" title="Direct Trade: SELL ${ceSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${ceSym}', '${ceKey}', ${strike}, 'CE', 'SELL', ${ceLtpNum}, ${ceLot}, ${ceChg}, ${ceChgPct})">S</button>
                        </div>
                    </td>
                `;

                const peActionCell = isBasketMode ? `
                    <td style="text-align:center; padding:4px 2px; white-space:nowrap; width:80px;">
                        <div class="chain-tick-wrap">
                            <label class="chain-tick-btn tick-buy ${isPeBuyChecked ? 'checked' : ''}" title="Tick to BUY PE in basket">
                                <input type="checkbox" ${isPeBuyChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'PE', 'BUY', '${peSym}', '${peKey}', ${peLtpNum}, ${peLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isPeBuyChecked ? '☑' : '☐'}</span><span class="tick-lbl">B</span>
                            </label>
                            <label class="chain-tick-btn tick-sell ${isPeSellChecked ? 'checked' : ''}" title="Tick to SELL PE in basket">
                                <input type="checkbox" ${isPeSellChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'PE', 'SELL', '${peSym}', '${peKey}', ${peLtpNum}, ${peLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isPeSellChecked ? '☑' : '☐'}</span><span class="tick-lbl">S</span>
                            </label>
                        </div>
                    </td>
                ` : `
                    <td style="text-align:center; padding:4px 2px; white-space:nowrap; width:80px;">
                        <div class="chain-quick-trade-wrap" style="display:inline-flex; align-items:center; gap:4px;">
                            <button type="button" class="chain-quick-trade-btn chain-btn-buy" style="background:linear-gradient(135deg, #059669, #10b981) !important; color:#ffffff !important; border:1px solid #34d399 !important; font-weight:900 !important; font-size:12px; width:26px; height:24px; border-radius:4px; box-shadow:0 0 8px rgba(16,185,129,0.5); cursor:pointer; display:inline-flex; align-items:center; justify-content:center;" title="Direct Trade: BUY ${peSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${peSym}', '${peKey}', ${strike}, 'PE', 'BUY', ${peLtpNum}, ${peLot}, ${peChg}, ${peChgPct})">B</button>
                            <button type="button" class="chain-quick-trade-btn chain-btn-sell" style="background:linear-gradient(135deg, #dc2626, #ef4444) !important; color:#ffffff !important; border:1px solid #f87171 !important; font-weight:900 !important; font-size:12px; width:26px; height:24px; border-radius:4px; box-shadow:0 0 8px rgba(239,68,68,0.5); cursor:pointer; display:inline-flex; align-items:center; justify-content:center;" title="Direct Trade: SELL ${peSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${peSym}', '${peKey}', ${strike}, 'PE', 'SELL', ${peLtpNum}, ${peLot}, ${peChg}, ${peChgPct})">S</button>
                        </div>
                    </td>
                `;

                rowsHtml += `
                    <tr class="${rowClass}">
                        ${ceActionCell}
                        <!-- CE LTP -->
                        <td class="${ceCellClass}" style="text-align:right; font-family:var(--font-mono); color:#10b981; font-weight:800; font-size:12.5px; padding:6px 10px; cursor:pointer;" onclick="openStraddleDirectOrderModal('${ceSym}', '${ceKey}', ${strike}, 'CE', 'BUY', ${ceLtpNum}, ${ceLot}, ${ceChg}, ${ceChgPct})" title="Click to trade ${ceSym}">
                            ₹${ceLtp}
                        </td>
                        <!-- CE Bid/Ask -->
                        <td class="${ceCellClass}" style="text-align:right; font-family:var(--font-mono); color:#94a3b8; font-size:11px; padding:6px 8px;">
                            ${ceBidAsk}
                        </td>

                        <!-- STRIKE (Center) -->
                        <td style="text-align:center; padding:6px 4px; width:100px; background:#0f172a; border-left:1px solid rgba(255,255,255,0.08); border-right:1px solid rgba(255,255,255,0.08);">
                            ${isAtm ? 
                                `<span class="opt-chain-atm-strike-badge">${strike} ★ ATM</span>` : 
                                `<span style="font-family:var(--font-mono); font-weight:800; color:#f8fafc; font-size:13px;">${strike}</span>`
                            }
                        </td>

                        <!-- PE Bid/Ask -->
                        <td class="${peCellClass}" style="text-align:left; font-family:var(--font-mono); color:#94a3b8; font-size:11px; padding:6px 8px;">
                            ${peBidAsk}
                        </td>
                        <!-- PE LTP -->
                        <td class="${peCellClass}" style="text-align:left; font-family:var(--font-mono); color:#f43f5e; font-weight:800; font-size:12.5px; padding:6px 10px; cursor:pointer;" onclick="openStraddleDirectOrderModal('${peSym}', '${peKey}', ${strike}, 'PE', 'BUY', ${peLtpNum}, ${peLot}, ${peChg}, ${peChgPct})" title="Click to trade ${peSym}">
                            ₹${peLtp}
                        </td>
                        ${peActionCell}
                    </tr>
                `;
            });

            tbody.innerHTML = rowsHtml;
            updateOptionChainBasketSummaryBar();
        }
    } catch (e) {
        if (!isBackgroundUpdate) {
            tbody.innerHTML = `<tr><td colspan="7" style="text-align:center; color:#f43f5e; padding:30px;">Error loading Option Matrix: ${e.message}</td></tr>`;
        }
    } finally {
        _isLoadingOptionChain = false;
    }
}

// =========================================================================
// DIRECT BUY / SELL ORDER MODAL (UPSTOX / ZERODHA GRADE)
// =========================================================================

window._directOrderState = {
    symbol: "NIFTY",
    instrument_key: "",
    strike: 0,
    opt_type: "CE",
    side: "BUY",
    product: "D",
    order_type: "LIMIT",
    price: 0,
    trigger_price: 0,
    lot_size: 65,
    quantity: 65,
    ltp: 0,
    net_change: 0,
    net_change_pct: 0
};

function openStraddleDirectOrderModal(symbol, instrumentKey, strike, optType, side = "BUY", ltp = 0, lotSize = 65, netChange = 0, netChangePct = 0) {
    const modal = document.getElementById("straddleDirectOrderModal");
    if (!modal) return;

    const und = window._straddleState.optionChainUnderlying || "NIFTY";
    const resolvedLot = lotSize > 0 ? lotSize : (und === "BANKNIFTY" ? 15 : (und === "FINNIFTY" ? 25 : 65));
    const resolvedLtp = Number(ltp) || 0;
    const resolvedSymbol = symbol && symbol.length > 3 ? symbol : `${und} ${strike} ${optType}`;

    window._directOrderState = {
        symbol: resolvedSymbol,
        instrument_key: instrumentKey || "",
        strike: Number(strike) || 0,
        opt_type: optType || "CE",
        side: (side || "BUY").toUpperCase(),
        product: "D", // Default Overnight NRML
        order_type: "LIMIT",
        price: resolvedLtp > 0 ? resolvedLtp : 0.05,
        trigger_price: 0,
        lot_size: resolvedLot,
        quantity: resolvedLot,
        ltp: resolvedLtp,
        net_change: Number(netChange) || 0,
        net_change_pct: Number(netChangePct) || 0
    };

    // Update Symbol & Subtitle Header
    const titleEl = document.getElementById("directOrderSymbolTitle");
    if (titleEl) titleEl.textContent = resolvedSymbol;

    const subEl = document.getElementById("directOrderSubtitle");
    if (subEl) subEl.textContent = `NFO • ${und} Strike ${strike} ${optType}`;

    // Update LTP & Change
    const ltpEl = document.getElementById("directOrderLtp");
    if (ltpEl) ltpEl.textContent = `₹${resolvedLtp.toFixed(2)}`;

    const chgEl = document.getElementById("directOrderChange");
    if (chgEl) {
        const sign = window._directOrderState.net_change >= 0 ? "+" : "";
        chgEl.textContent = `${sign}${window._directOrderState.net_change.toFixed(2)} (${sign}${window._directOrderState.net_change_pct.toFixed(2)}%)`;
        chgEl.style.color = window._directOrderState.net_change >= 0 ? "#10b981" : "#f43f5e";
    }

    // Set Quantity Input
    const qtyInput = document.getElementById("directOrderQuantityInput");
    if (qtyInput) {
        qtyInput.value = resolvedLot;
        qtyInput.step = resolvedLot;
        qtyInput.min = resolvedLot;
    }

    // Set Price Input
    const priceInput = document.getElementById("directOrderPriceInput");
    if (priceInput) {
        priceInput.value = resolvedLtp > 0 ? resolvedLtp.toFixed(2) : "0.05";
        priceInput.disabled = false;
    }

    // Reset Trigger Price
    const trigRow = document.getElementById("directOrderTriggerRow");
    if (trigRow) trigRow.style.display = "none";
    const trigInput = document.getElementById("directOrderTriggerPriceInput");
    if (trigInput) trigInput.value = "";

    // Set Side & Product & Type styles
    setOrderModalSide(window._directOrderState.side);
    setOrderModalProduct("D");
    setOrderModalType("LIMIT");
    updateOrderLotBadge();
    updateOrderMarginSummary();
    syncDirectOrderFunds();

    modal.style.display = "flex";
}

function closeStraddleDirectOrderModal() {
    const modal = document.getElementById("straddleDirectOrderModal");
    if (modal) modal.style.display = "none";
}

function setOrderModalSide(side) {
    const isBuy = side.toUpperCase() === "BUY";
    window._directOrderState.side = isBuy ? "BUY" : "SELL";

    const btnBuy = document.getElementById("btnOrderSideBuy");
    const btnSell = document.getElementById("btnOrderSideSell");
    const sideBadge = document.getElementById("directOrderSideBadge");
    const submitBtn = document.getElementById("directOrderSubmitBtn");

    if (btnBuy && btnSell) {
        if (isBuy) {
            btnBuy.style.background = "#10b981";
            btnBuy.style.color = "#ffffff";
            btnSell.style.background = "transparent";
            btnSell.style.color = "#94a3b8";
        } else {
            btnSell.style.background = "#ea580c";
            btnSell.style.color = "#ffffff";
            btnBuy.style.background = "transparent";
            btnBuy.style.color = "#94a3b8";
        }
    }

    if (sideBadge) {
        sideBadge.textContent = isBuy ? "BUY" : "SELL";
        sideBadge.style.background = isBuy ? "#10b981" : "#ea580c";
    }

    if (submitBtn) {
        const actionText = isBuy ? "BUY" : "SELL";
        submitBtn.textContent = `${actionText} ${window._directOrderState.symbol}`;
        submitBtn.style.background = isBuy 
            ? "linear-gradient(135deg, #10b981, #059669)" 
            : "linear-gradient(135deg, #ea580c, #dc2626)";
        submitBtn.style.boxShadow = isBuy 
            ? "0 4px 15px rgba(16,185,129,0.35)" 
            : "0 4px 15px rgba(234,88,12,0.35)";
    }

    updateOrderMarginSummary();
}

function setOrderModalProduct(product) {
    window._directOrderState.product = product;
    const btnDel = document.getElementById("btnProductDelivery");
    const btnIntra = document.getElementById("btnProductIntraday");

    if (btnDel && btnIntra) {
        if (product === "D") {
            btnDel.style.background = "#1e293b";
            btnDel.style.color = "#60a5fa";
            btnDel.style.border = "1px solid #3b82f6";
            btnIntra.style.background = "#0f172a";
            btnIntra.style.color = "#94a3b8";
            btnIntra.style.border = "1px solid rgba(255,255,255,0.08)";
        } else {
            btnIntra.style.background = "#1e293b";
            btnIntra.style.color = "#60a5fa";
            btnIntra.style.border = "1px solid #3b82f6";
            btnDel.style.background = "#0f172a";
            btnDel.style.color = "#94a3b8";
            btnDel.style.border = "1px solid rgba(255,255,255,0.08)";
        }
    }
    updateOrderMarginSummary();
}

function setOrderModalType(type) {
    window._directOrderState.order_type = type;
    const types = ["MARKET", "LIMIT", "SL", "SL-M"];
    const idMap = {
        "MARKET": "btnTypeMarket",
        "LIMIT": "btnTypeLimit",
        "SL": "btnTypeSL",
        "SL-M": "btnTypeSLM"
    };

    types.forEach(t => {
        const btn = document.getElementById(idMap[t]);
        if (btn) {
            if (t === type) {
                btn.style.background = "#1e293b";
                btn.style.color = "#38bdf8";
                btn.style.border = "1px solid #0284c7";
            } else {
                btn.style.background = "#0f172a";
                btn.style.color = "#94a3b8";
                btn.style.border = "1px solid rgba(255,255,255,0.08)";
            }
        }
    });

    const priceInput = document.getElementById("directOrderPriceInput");
    const trigRow = document.getElementById("directOrderTriggerRow");
    const trigInput = document.getElementById("directOrderTriggerPriceInput");

    if (type === "MARKET" || type === "SL-M") {
        if (priceInput) {
            priceInput.disabled = true;
            priceInput.value = "";
            priceInput.placeholder = "Market (LTP)";
        }
    } else {
        if (priceInput) {
            priceInput.disabled = false;
            priceInput.value = window._directOrderState.price > 0 ? window._directOrderState.price.toFixed(2) : window._directOrderState.ltp.toFixed(2);
            priceInput.placeholder = "Price";
        }
    }

    if (type === "SL" || type === "SL-M") {
        if (trigRow) trigRow.style.display = "block";
        if (trigInput && !trigInput.value) {
            trigInput.value = (window._directOrderState.ltp * 0.98).toFixed(2);
        }
    } else {
        if (trigRow) trigRow.style.display = "none";
    }

    updateOrderMarginSummary();
}

function adjustOrderQuantity(stepLots) {
    const lot = window._directOrderState.lot_size || 65;
    let currentQty = Number(document.getElementById("directOrderQuantityInput")?.value) || lot;
    let newQty = currentQty + (stepLots * lot);
    if (newQty < lot) newQty = lot;

    const input = document.getElementById("directOrderQuantityInput");
    if (input) input.value = newQty;
    window._directOrderState.quantity = newQty;

    updateOrderLotBadge();
    updateOrderMarginSummary();
}

function onDirectOrderQuantityChange() {
    const lot = window._directOrderState.lot_size || 65;
    const input = document.getElementById("directOrderQuantityInput");
    let val = Number(input?.value) || lot;
    if (val < 1) val = lot;
    window._directOrderState.quantity = val;

    updateOrderLotBadge();
    updateOrderMarginSummary();
}

function updateOrderLotBadge() {
    const badge = document.getElementById("directOrderLotBadge");
    if (!badge) return;
    const lot = window._directOrderState.lot_size || 65;
    const qty = window._directOrderState.quantity || lot;
    const lotsCount = (qty / lot).toFixed(1).replace(/\.0$/, "");
    badge.textContent = `${lotsCount} Lot${lotsCount === "1" ? "" : "s"} (${qty} Qty)`;
}

function syncDirectOrderFunds() {
    fetch("/api/straddle/margin")
        .then(r => r.json())
        .then(data => {
            const avail = (data && data.success && data.margin && data.margin.available_margin) ? data.margin.available_margin : 0.0;
            const el = document.getElementById("directOrderAvailMargin");
            if (el) el.textContent = `₹${avail.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        })
        .catch(() => {});
}

function updateOrderMarginSummary() {
    const reqEl = document.getElementById("directOrderReqMargin");
    if (!reqEl) return;

    const qty = Number(window._directOrderState.quantity) || window._directOrderState.lot_size || 65;
    const isBuy = window._directOrderState.side === "BUY";
    const priceVal = Number(document.getElementById("directOrderPriceInput")?.value) || window._directOrderState.ltp || 100.0;
    const und = window._straddleState.optionChainUnderlying || "NIFTY";
    const prod = window._directOrderState.product || "D";

    // Instant realistic Kotak Neo SPAN + Exposure calculation
    if (isBuy) {
        const req = qty * Math.max(0.05, priceVal);
        reqEl.textContent = req >= 100000 
            ? `₹ ${(req/100000).toFixed(2)} L (₹${req.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})})`
            : `₹ ${req.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
    } else {
        const spot = 22603.05;
        const basePct = und === "NIFTY" ? 0.111 : (und === "BANKNIFTY" ? 0.128 : 0.118);
        const prodMult = prod === "I" ? 0.75 : 1.0;
        const req = (spot * qty * basePct * prodMult);
        reqEl.textContent = `₹ ${(req/100000).toFixed(2)} L (₹${req.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})})`;
    }

    // Call server endpoint for exact live calculated broker margin
    fetch("/api/straddle/order_margin", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
            symbol: window._directOrderState.symbol,
            instrument_key: window._directOrderState.instrument_key,
            transaction_type: window._directOrderState.side,
            quantity: qty,
            product: prod,
            price: priceVal,
            underlying: und
        })
    })
    .then(r => r.json())
    .then(data => {
        if (data && data.success && data.required_margin) {
            const req = data.required_margin;
            reqEl.textContent = req >= 100000 
                ? `₹ ${(req/100000).toFixed(2)} L (₹${req.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})})`
                : `₹ ${req.toLocaleString('en-IN', {minimumFractionDigits: 2, maximumFractionDigits: 2})}`;
        }
    })
    .catch(() => {});
}

async function submitStraddleDirectOrder() {
    const submitBtn = document.getElementById("directOrderSubmitBtn");
    const qty = Number(document.getElementById("directOrderQuantityInput")?.value) || window._directOrderState.lot_size || 65;
    let price = Number(document.getElementById("directOrderPriceInput")?.value) || 0.0;
    let trigPrice = Number(document.getElementById("directOrderTriggerPriceInput")?.value) || 0.0;
    const ordType = window._directOrderState.order_type;

    if (ordType === "LIMIT" || ordType === "SL") {
        if (price <= 0) {
            alert("Please specify a valid limit price > 0");
            return;
        }
    }
    if (ordType === "SL" || ordType === "SL-M") {
        if (trigPrice <= 0) {
            alert("Please specify a valid trigger price > 0");
            return;
        }
    }

    const payload = {
        symbol: window._directOrderState.symbol,
        instrument_key: window._directOrderState.instrument_key,
        transaction_type: window._directOrderState.side,
        quantity: qty,
        product: window._directOrderState.product,
        order_type: ordType,
        price: price,
        trigger_price: trigPrice
    };

    if (submitBtn) {
        submitBtn.disabled = true;
        submitBtn.innerHTML = `⏳ Executing Exchange Order...`;
    }

    try {
        const resp = await fetch("/api/straddle/place_direct_order", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();

        if (res.success) {
            const orderId = res.order_id || "DIR_" + Date.now();
            if (typeof showToast === "function") {
                showToast(`🚀 Order Placed Successfully! Broker Order ID: ${orderId}`, "success");
            } else {
                alert(`Order Placed Successfully!\nBroker Order ID: ${orderId}`);
            }
            closeStraddleDirectOrderModal();

            // Real-time synchronization
            if (typeof loadStraddleOpenPositions === "function") loadStraddleOpenPositions();
            if (typeof loadStraddleOrderbook === "function") loadStraddleOrderbook();
            if (typeof loadStraddlePortfolioMargin === "function") loadStraddlePortfolioMargin();
        } else {
            const err = res.message || "Failed to place exchange order";
            alert(`Order Execution Error:\n${err}`);
        }
    } catch (e) {
        alert(`Network / API Error: ${e.message}`);
    } finally {
        if (submitBtn) {
            submitBtn.disabled = false;
            setOrderModalSide(window._directOrderState.side);
        }
    }
}


// =========================================================================
// AUDIT LOGS MODAL
// =========================================================================

function openStraddleLogsModal() {
    const modal = document.getElementById("straddleLogsModal");
    if (!modal) return;
    modal.style.display = "flex";
    renderStraddleAuditLogs();
}

function closeStraddleLogsModal() {
    const modal = document.getElementById("straddleLogsModal");
    if (modal) modal.style.display = "none";
}

function renderStraddleAuditLogs() {
    const container = document.getElementById("straddleAuditLogsContainer");
    if (!container) return;
    container.innerHTML = `<div style="color:#64748b; text-align:center; padding:20px;">Fetching live audit logs...</div>`;
    
    fetch("/api/straddle/logs")
        .then(r => r.json())
        .then(data => {
            let logs = (data && data.success && data.logs) ? data.logs : [];
            if (!logs || logs.length === 0) {
                // Fallback to /api/straddle/state if needed
                return fetch("/api/straddle/state").then(r => r.json()).then(sdata => {
                    return (sdata && sdata.success && sdata.state && sdata.state.logs) ? sdata.state.logs : [];
                });
            }
            return logs;
        })
        .then(logs => {
            if (logs && logs.length > 0) {
                let html = "";
                logs.forEach(l => {
                    const level = (l.level || "INFO").toUpperCase();
                    const color = level === "ERROR" ? "#f43f5e" : (level === "WARN" ? "#f59e0b" : "#38bdf8");
                    const bg = level === "ERROR" ? "rgba(244,63,94,0.08)" : (level === "WARN" ? "rgba(245,158,11,0.08)" : "rgba(56,189,248,0.04)");
                    const border = level === "ERROR" ? "rgba(244,63,94,0.3)" : (level === "WARN" ? "rgba(245,158,11,0.3)" : "rgba(56,189,248,0.2)");
                    const dateStr = l.date ? `${l.date} ` : "";
                    html += `
                        <div style="padding:8px 12px; background:${bg}; border:1px solid ${border}; border-radius:6px; display:flex; gap:12px; align-items:flex-start;">
                            <span style="color:#94a3b8; font-size:11px; white-space:nowrap; font-family:var(--font-mono); font-weight:600;">[${dateStr}${l.timestamp || ''}]</span>
                            <span style="color:${color}; font-weight:800; font-size:11px; text-transform:uppercase; min-width:55px;">${level}</span>
                            <span style="color:#e2e8f0; font-size:12px; line-height:1.4; word-break:break-word;">${l.message || ''}</span>
                        </div>
                    `;
                });
                container.innerHTML = html;
            } else {
                container.innerHTML = `<div style="color:#64748b; text-align:center; padding:25px;">No audit events recorded yet.</div>`;
            }
        })
        .catch(e => {
            container.innerHTML = `<div style="color:#f43f5e; text-align:center; padding:20px;">Error loading logs: ${e.message}</div>`;
        });
}

// Window global bindings for Straddle error retry and modal controls
window.renderStraddlePositions = renderStraddlePositions;
window.onStraddlePositionCheckboxChange = onStraddlePositionCheckboxChange;
window.toggleSelectAllStraddleLegs = toggleSelectAllStraddleLegs;
window.retrySelectedStraddleLegs = retrySelectedStraddleLegs;
window.resetCurrentStraddleAlgo = resetCurrentStraddleAlgo;
window.openLegSettingsModal = openLegSettingsModal;
window.closeLegSettingsModal = closeLegSettingsModal;
window.saveLegSettingsModal = saveLegSettingsModal;
window.showStraddleRejectedToast = showStraddleRejectedToast;
window.showStraddleSuccessToast = showStraddleSuccessToast;
window.restartOrRetryStraddleAlgo = restartOrRetryStraddleAlgo;
window.sortOrderbookBy = sortOrderbookBy;
window.onOrderbookClientIdChange = onOrderbookClientIdChange;
window.openStrategyFromOrderbook = openStrategyFromOrderbook;
window.switchOrderbookSubTab = switchOrderbookSubTab;
window.loadStraddleOrderbook = loadStraddleOrderbook;
window.renderStraddleOrderbookTable = renderStraddleOrderbookTable;
window.exportStraddleOrderbookCsv = exportStraddleOrderbookCsv;
window.parseMarginError = parseMarginError;
window.openRectifyLegModal = openRectifyLegModal;
window.closeRectifyLegModal = closeRectifyLegModal;
window.adjustRectifyLots = adjustRectifyLots;
window.onRectifyParamChange = onRectifyParamChange;
window.applyRectifyAutoFitLots = applyRectifyAutoFitLots;
window.submitRectifyLeg = submitRectifyLeg;
window.refreshLiveWalletMarginInModal = refreshLiveWalletMarginInModal;

// Core platform exports
window.initStraddlePlatform = initStraddlePlatform;
window.fetchStraddleState = fetchStraddleState;
window.switchStraddleSubView = switchStraddleSubView;
window.renderStraddleAlgosTable = renderStraddleAlgosTable;
window.openAddStraddleAlgoModal = openAddStraddleAlgoModal;
window.closeAddStraddleAlgoModal = closeAddStraddleAlgoModal;
window.submitCreateNewStraddleAlgo = submitCreateNewStraddleAlgo;
window.syncAndResetKotakNeoData = syncAndResetKotakNeoData;
window.showStraddleToast = showStraddleToast;

// Option chain & Logs exports
window.openStraddleOptionChainModal = openStraddleOptionChainModal;
window.closeStraddleOptionChainModal = closeStraddleOptionChainModal;
window.onChangeOptChainUnderlying = onChangeOptChainUnderlying;
window.loadStraddleOptionChain = loadStraddleOptionChain;
window.openStraddleLogsModal = openStraddleLogsModal;
window.closeStraddleLogsModal = closeStraddleLogsModal;
window.renderStraddleAuditLogs = renderStraddleAuditLogs;

// P&L Chart exports
window.loadStraddlePnlChart = loadStraddlePnlChart;
window.drawPnlCanvas = drawPnlCanvas;
window.selectPnlEntity = selectPnlEntity;
window.setPnlChartType = setPnlChartType;

// Strategy Builder & Terminal exports
window.toggleStraddleSettingsPanel = toggleStraddleSettingsPanel;
window.saveStraddleStrategySettings = saveStraddleStrategySettings;
window.saveAsNewStraddlePreset = saveAsNewStraddlePreset;
window.saveAndStartStraddleTrade = saveAndStartStraddleTrade;
window.toggleAlgoBuilderSection = toggleAlgoBuilderSection;
window.addStraddleLegRow = addStraddleLegRow;
window.removeStraddleLegRow = removeStraddleLegRow;
window.onStraddleStrategySelect = onStraddleStrategySelect;
window.straddleShowBrokerInfo = straddleShowBrokerInfo;
window.straddleStartAlgo = straddleStartAlgo;
window.straddleTerminateAlgo = straddleTerminateAlgo;
window.straddleSquareOffAllAndTerminate = straddleSquareOffAllAndTerminate;
window.straddleSquareOffSelectedLegs = straddleSquareOffSelectedLegs;

// Bulk Algos actions
window.toggleSelectAllStraddleAlgos = toggleSelectAllStraddleAlgos;
window.bulkToggleSelectedAlgos = bulkToggleSelectedAlgos;
window.bulkResetSelectedAlgos = bulkResetSelectedAlgos;
window.bulkDuplicateSelectedAlgos = bulkDuplicateSelectedAlgos;
window.bulkSetDaysSelectedAlgos = bulkSetDaysSelectedAlgos;
window.bulkDeleteSelectedAlgos = bulkDeleteSelectedAlgos;
window.clearAllSelectedAlgos = clearAllSelectedAlgos;

// Direct Order modal exports
window.openStraddleDirectOrderModal = openStraddleDirectOrderModal;
window.closeStraddleDirectOrderModal = closeStraddleDirectOrderModal;
window.submitStraddleDirectOrder = submitStraddleDirectOrder;
window.adjustOrderQuantity = adjustOrderQuantity;
window.setOrderModalSide = setOrderModalSide;
window.setOrderModalType = setOrderModalType;
window.setOrderModalProduct = setOrderModalProduct;
window.onDirectOrderQuantityChange = onDirectOrderQuantityChange;
window.updateOrderMarginSummary = updateOrderMarginSummary;
window.syncDirectOrderFunds = syncDirectOrderFunds;

// Positions exports
window.loadStraddleOpenPositions = loadStraddleOpenPositions;
window.renderStraddleOpenPositionsTable = renderStraddleOpenPositionsTable;
window.setStraddlePositionsViewFilter = setStraddlePositionsViewFilter;
window.confirmSquareOffAllOpenPositions = confirmSquareOffAllOpenPositions;
window.clearStraddleClosedAndExpiredPositions = clearStraddleClosedAndExpiredPositions;
window.loadStraddleMargin = loadStraddleMargin;

// Live trade editing and error customization exports
window.retrySingleStraddleLegInline = retrySingleStraddleLegInline;
window.saveRunningPositionChanges = saveRunningPositionChanges;
window.markRunningLegChanged = markRunningLegChanged;
window.squareOffSingleLegIndex = squareOffSingleLegIndex;
window.stepLiveStrikePrice = stepLiveStrikePrice;
window.stepLiveLots = stepLiveLots;
window.detachRunningPosition = detachRunningPosition;

// Broker position adoption functions and exports
let _adoptablePositionsList = [];
let _selectedAdoptablePositionIndex = -1;

async function openAdoptBrokerPositionModal() {
    const allStrats = window._straddleState?.allStrategies || [];
    let stratId = document.getElementById("adopt_target_strat_select")?.value ||
                  window._straddleState?.activeStrategy?.strategy_id || 
                  window._straddleState?.activeStrategyId || 
                  document.getElementById("straddleTerminalStrategySelect")?.value;
    
    if (!stratId && allStrats.length > 0) {
        stratId = allStrats[0].strategy_id;
    }

    const modal = document.getElementById("modalAdoptBrokerPosition");
    if (!modal) return;
    modal.style.display = "flex";

    // Populate target strategy selector dropdown
    const stratSel = document.getElementById("adopt_target_strat_select");
    if (stratSel && allStrats.length > 0) {
        stratSel.innerHTML = allStrats.map(s => {
            const isSel = (s.strategy_id === stratId) ? "selected" : "";
            const statusIcon = s.enabled ? "🟢" : "⚪";
            return `<option value="${s.strategy_id}" ${isSel}>${statusIcon} ${s.name} (${s.strategy_id})</option>`;
        }).join("");
    }

    await fetchAdoptablePositionsForStrategy(stratId || "");
}

async function fetchAdoptablePositionsForStrategy(stratId) {
    const loadingDiv = document.getElementById("adoptModalLoading");
    const emptyDiv = document.getElementById("adoptModalEmpty");
    const contentDiv = document.getElementById("adoptModalContent");
    const tbody = document.getElementById("adoptBrokerPositionsTableBody");
    const confirmBtn = document.getElementById("btnConfirmAdoptPosition");

    if (loadingDiv) loadingDiv.style.display = "block";
    if (emptyDiv) emptyDiv.style.display = "none";
    if (contentDiv) contentDiv.style.display = "none";
    if (confirmBtn) {
        confirmBtn.disabled = true;
        confirmBtn.textContent = "➕ Add Selected Position to Algo";
    }
    _selectedAdoptablePositionIndex = -1;
    _adoptablePositionsList = [];

    try {
        const url = stratId ? `/api/straddle/adoptable_positions?strategy_id=${encodeURIComponent(stratId)}` : `/api/straddle/adoptable_positions`;
        const resp = await fetch(url);
        const res = await resp.json();

        if (loadingDiv) loadingDiv.style.display = "none";

        if (!res.success) {
            showStraddleToast(res.error || "Failed to scan broker positions", "error", 3500);
            if (emptyDiv) {
                emptyDiv.style.display = "block";
                emptyDiv.innerHTML = `<div style="font-size:14px; font-weight:700; color:#f87171; margin-bottom:4px;">Error Accessing Kotak Neo</div><div style="font-size:12px;">${res.error || "Broker communication error"}</div>`;
            }
            return;
        }

        _adoptablePositionsList = res.positions || [];
        if (_adoptablePositionsList.length === 0) {
            if (emptyDiv) emptyDiv.style.display = "block";
            return;
        }

        if (contentDiv) contentDiv.style.display = "block";
        if (tbody) {
            tbody.innerHTML = _adoptablePositionsList.map((p, idx) => {
                const sideClass = (p.side === 'BUY') ? 'badge-buy' : 'badge-sell';
                const pnl = Number(p.pnl || 0);
                const pnlClass = pnl >= 0 ? 'color:#10b981;' : 'color:#f87171;';
                const statusBadge = (p.currently_in && !p.currently_in.includes("Unassigned")) 
                    ? `<span style="background:rgba(168,85,247,0.15); color:#c084fc; border:1px solid rgba(168,85,247,0.3); padding:2px 8px; border-radius:12px; font-size:10.5px; font-weight:700;">Running in ${p.currently_in}</span>`
                    : `<span style="background:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); padding:2px 8px; border-radius:12px; font-size:10.5px; font-weight:700;">🟢 Live on Kotak Neo</span>`;

                return `
                    <tr id="adopt_row_${idx}" style="cursor:pointer; transition:background 0.15s ease;" onclick="selectAdoptablePositionIndex(${idx})">
                        <td style="text-align:center;">
                            <input type="radio" name="adoptPosRadio" id="adopt_radio_${idx}" value="${idx}" onchange="selectAdoptablePositionIndex(${idx})">
                        </td>
                        <td>
                            <div style="font-weight:700; color:#f8fafc; font-family:var(--font-mono); font-size:12px;">${p.symbol}</div>
                            <div style="font-size:10px; color:#64748b;">${p.trading_symbol || p.symbol} (${p.exchange_segment || 'nfo'})</div>
                        </td>
                        <td>
                            <span class="straddle-badge ${sideClass}" style="font-size:10px; padding:2px 6px;">${p.side}</span>
                        </td>
                        <td style="font-family:var(--font-mono); font-weight:700; color:#cbd5e1; font-size:12px;">
                            ${p.quantity} <span style="font-size:10px; color:#94a3b8;">(${p.lots || 1} lots)</span>
                        </td>
                        <td style="font-family:var(--font-mono); color:#cbd5e1; font-size:12px;">
                            ₹${Number(p.avg_price || 0).toFixed(2)}
                        </td>
                        <td style="font-family:var(--font-mono); font-weight:700; color:#38bdf8; font-size:12px;">
                            ₹${Number(p.ltp || p.avg_price || 0).toFixed(2)}
                        </td>
                        <td style="text-align:right; font-family:var(--font-mono); font-weight:800; font-size:12px; ${pnlClass}">
                            ₹${pnl.toFixed(2)}
                        </td>
                        <td style="text-align:center;">
                            ${statusBadge}
                        </td>
                    </tr>
                `;
            }).join("");
        }

        // Auto-select first position
        selectAdoptablePositionIndex(0);

    } catch (err) {
        if (loadingDiv) loadingDiv.style.display = "none";
        if (emptyDiv) {
            emptyDiv.style.display = "block";
            emptyDiv.innerHTML = `<div style="font-size:14px; font-weight:700; color:#f87171; margin-bottom:4px;">Error Connecting to Server</div><div style="font-size:12px;">${err.message}</div>`;
        }
    }
}

function onAdoptTargetStratChange(targetId) {
    if (targetId) {
        fetchAdoptablePositionsForStrategy(targetId);
    }
}
window.onAdoptTargetStratChange = onAdoptTargetStratChange;

function selectAdoptablePositionIndex(idx) {
    _selectedAdoptablePositionIndex = idx;
    const confirmBtn = document.getElementById("btnConfirmAdoptPosition");
    if (confirmBtn) confirmBtn.disabled = false;

    // Check radio
    const radio = document.getElementById(`adopt_radio_${idx}`);
    if (radio) radio.checked = true;

    // Highlight row
    const tbody = document.getElementById("adoptBrokerPositionsTableBody");
    if (tbody) {
        const rows = tbody.querySelectorAll("tr");
        rows.forEach((r, i) => {
            if (i === idx) {
                r.style.background = "rgba(56, 189, 248, 0.12)";
                r.style.borderLeft = "3px solid #38bdf8";
            } else {
                r.style.background = "transparent";
                r.style.borderLeft = "none";
            }
        });
    }
}

function closeAdoptBrokerPositionModal() {
    const modal = document.getElementById("modalAdoptBrokerPosition");
    if (modal) modal.style.display = "none";
    _selectedAdoptablePositionIndex = -1;
    _adoptablePositionsList = [];
}

async function submitAdoptBrokerPosition() {
    if (_selectedAdoptablePositionIndex < 0 || !_adoptablePositionsList[_selectedAdoptablePositionIndex]) {
        showStraddleToast("Please select a position to adopt.", "error", 2500);
        return;
    }

    const stratId = document.getElementById("adopt_target_strat_select")?.value ||
                    window._straddleState?.activeStrategy?.strategy_id || 
                    window._straddleState?.activeStrategyId || 
                    document.getElementById("straddleTerminalStrategySelect")?.value;
    
    if (!stratId) {
        showStraddleToast("Please select a target algorithm first.", "error", 2500);
        return;
    }

    const pos = _adoptablePositionsList[_selectedAdoptablePositionIndex];

    const tgtMode = document.getElementById("adopt_tgt_mode")?.value || "%";
    const tgtVal = parseFloat(document.getElementById("adopt_tgt_val")?.value) || 25;
    const slMode = document.getElementById("adopt_sl_mode")?.value || "%";
    const slVal = parseFloat(document.getElementById("adopt_sl_val")?.value) || 15;

    const confirmBtn = document.getElementById("btnConfirmAdoptPosition");
    if (confirmBtn) {
        confirmBtn.disabled = true;
        confirmBtn.textContent = "Adopting Position...";
    }

    try {
        const resp = await fetch("/api/straddle/adopt_position", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                strategy_id: stratId,
                position_data: {
                    symbol: pos.symbol,
                    trading_symbol: pos.trading_symbol || pos.symbol,
                    token: pos.token,
                    exchange_segment: pos.exchange_segment || "nfo",
                    side: pos.side,
                    product: pos.product || "MIS",
                    quantity: pos.quantity,
                    lots: pos.lots || 1,
                    lot_size: pos.lot_size || 65,
                    strike_price: pos.strike_price,
                    option_type: pos.option_type,
                    expiry_date: pos.expiry_date,
                    avg_price: pos.avg_price,
                    ltp: pos.ltp || pos.avg_price,
                    target_mode: tgtMode,
                    target_val: tgtVal,
                    sl_mode: slMode,
                    sl_val: slVal
                }
            })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || `Successfully adopted ${pos.symbol} into ${stratId}!`, "success", 3000);
            closeAdoptBrokerPositionModal();
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to adopt position", "error", 4000);
            if (confirmBtn) {
                confirmBtn.disabled = false;
                confirmBtn.textContent = "➕ Add Selected Position to Algo";
            }
        }
    } catch (e) {
        showStraddleToast("Error adopting position: " + e.message, "error", 3500);
        if (confirmBtn) {
            confirmBtn.disabled = false;
            confirmBtn.textContent = "➕ Add Selected Position to Algo";
        }
    }
}

window.openAdoptBrokerPositionModal = openAdoptBrokerPositionModal;
window.selectAdoptablePositionIndex = selectAdoptablePositionIndex;
window.closeAdoptBrokerPositionModal = closeAdoptBrokerPositionModal;
window.submitAdoptBrokerPosition = submitAdoptBrokerPosition;

window.addEventListener("resize", () => {
    if (window._straddleState && window._straddleState.activeTab === "pnl" && window._straddleState.pnlDataCache) {
        drawPnlCanvas(window._straddleState.pnlDataCache, window._straddleState.pnlChartType || "line", null);
    }
});


// =========================================================================
// ANGELONE-GRADE BASKET ORDERS SYSTEM
// =========================================================================

window._basketOrders = [];
try {
    const saved = localStorage.getItem("straddle_basket_orders");
    if (saved) {
        window._basketOrders = JSON.parse(saved) || [];
    }
} catch (e) {}

try {
    const savedMult = localStorage.getItem("straddle_basket_multiplier");
    window._basketMultiplier = savedMult ? Math.max(1, parseInt(savedMult) || 1) : 1;
} catch (e) {
    window._basketMultiplier = 1;
}

function updateBasketBadges() {
    const count = window._basketOrders?.length || 0;
    const badge = document.getElementById("basketHeaderBadge");
    if (badge) badge.textContent = count;
    const navBadge = document.getElementById("navBasketCountBadge");
    if (navBadge) navBadge.textContent = count;
    const chainBadge = document.getElementById("chainBasketTotalCountBadge");
    if (chainBadge) chainBadge.textContent = count;
    const countBadge = document.getElementById("basketOrderCountBadge");
    if (countBadge) {
        const selCount = (window._basketOrders || []).filter(o => o.selected).length;
        countBadge.textContent = `(${selCount}/50 orders)`;
    }
}

let _basketQuoteTickerInterval = null;
let _isRefreshingBasketQuotes = false;

async function refreshAllBasketQuotes() {
    if (_isRefreshingBasketQuotes) return;
    if (!window._basketOrders || window._basketOrders.length === 0) return;
    _isRefreshingBasketQuotes = true;
    try {
        const resp = await fetch("/api/straddle/basket/quotes", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ orders: window._basketOrders })
        });
        const data = await resp.json();
        if (data.success && data.quotes) {
            let pricesChanged = false;
            data.quotes.forEach(q => {
                const idx = q.idx;
                if (window._basketOrders[idx]) {
                    const o = window._basketOrders[idx];
                    const freshLtp = Number(q.ltp) || 0;
                    if (freshLtp > 0 && Math.abs((o.ltp || 0) - freshLtp) > 0.01) {
                        o.ltp = freshLtp;
                        pricesChanged = true;

                        const row = document.querySelector(`tr[data-idx="${idx}"]`);
                        if (row) {
                            const ltpEl = row.querySelector(".basket-row-ltp");
                            if (ltpEl) {
                                ltpEl.textContent = `₹${freshLtp.toFixed(2)}`;
                                ltpEl.style.color = (freshLtp >= (o.prev_ltp || freshLtp)) ? "#10b981" : "#f43f5e";
                            }
                            o.prev_ltp = freshLtp;

                            const priceInput = row.querySelector(".basket-price-input");
                            if (priceInput && o.order_type !== "MARKET") {
                                const isUserEdited = priceInput.getAttribute("data-user-edited") === "true";
                                if (!isUserEdited || priceInput.value === "0.00" || priceInput.value === "") {
                                    priceInput.value = freshLtp.toFixed(2);
                                    priceInput.setAttribute("data-saved-price", freshLtp.toString());
                                    o.price = freshLtp;
                                }
                            }
                        }
                    }
                }
            });
            if (pricesChanged) {
                try {
                    localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
                } catch(e) {}
                updateBasketMarginCalculation();
            }
        }
    } catch (e) {
        console.warn("[Basket] Live quotes ticker fetch error:", e);
    } finally {
        _isRefreshingBasketQuotes = false;
    }
}

function startBasketQuoteTicker() {
    stopBasketQuoteTicker();
    refreshAllBasketQuotes();
    _basketQuoteTickerInterval = setInterval(() => {
        const modal = document.getElementById("modalBasketOrders");
        if (modal && modal.style.display === "flex") {
            refreshAllBasketQuotes();
        } else {
            stopBasketQuoteTicker();
        }
    }, 250); // Continuous 250ms ultra-fast tick feed (4 updates/sec)
}

function stopBasketQuoteTicker() {
    if (_basketQuoteTickerInterval) {
        clearInterval(_basketQuoteTickerInterval);
        _basketQuoteTickerInterval = null;
    }
}

function openBasketOrderModal() {
    const modal = document.getElementById("modalBasketOrders");
    if (!modal) return;
    modal.style.display = "flex";
    const multInput = document.getElementById("basketMultiplierInput");
    if (multInput) multInput.value = window._basketMultiplier || 1;
    updateBasketBadges();
    renderBasketOrdersTable();
    updateBasketMarginCalculation();
    startBasketQuoteTicker();
}

function closeBasketOrderModal() {
    const modal = document.getElementById("modalBasketOrders");
    if (modal) modal.style.display = "none";
    stopBasketQuoteTicker();
    clearBasketSearch();
}

function addLegToBasket(sym, tok, strike, optType, side = "SELL", ltp = 0, lotSize = 65, exp = "", product = "CF") {
    const und = window._straddleState?.optionChainUnderlying || "NIFTY";
    const resolvedLot = Number(lotSize) > 0 ? Number(lotSize) : (und === "BANKNIFTY" ? 15 : (und === "FINNIFTY" ? 25 : 65));
    const resolvedStrike = Number(strike) || 0;
    let resolvedPrice = Number(ltp) || 0.05;

    // Check if real-time updated quote exists in active search results
    if (window._basketSearchResults && Array.isArray(window._basketSearchResults)) {
        const found = window._basketSearchResults.find(r => 
            (tok && (r.token === tok || r.instrument_key === tok)) ||
            (Number(r.strike) === resolvedStrike && String(r.opt_type).toUpperCase() === String(optType).toUpperCase() && (!exp || r.expiry_date === exp))
        );
        if (found && Number(found.ltp) > 0) {
            resolvedPrice = Number(found.ltp);
        }
    }

    const newOrder = {
        id: "b_" + Date.now() + "_" + Math.floor(Math.random() * 1000),
        selected: true,
        underlying: und,
        symbol: sym || `${und} ${resolvedStrike} ${optType}`,
        trading_symbol: sym || `${und} ${resolvedStrike} ${optType}`,
        token: tok || "",
        strike: resolvedStrike,
        opt_type: (optType || "CE").toUpperCase(),
        side: (side || "SELL").toUpperCase(),
        expiry_date: exp || "Weekly",
        product: product || "CF",
        lots: 1,
        lot_size: resolvedLot,
        quantity: resolvedLot,
        price: resolvedPrice,
        order_type: "LIMIT",
        ltp: resolvedPrice,
        change: 0,
        change_pct: 0
    };

    window._basketOrders.push(newOrder);
    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
    } catch(e) {}

    updateBasketBadges();
    showStraddleToast(`Added ${newOrder.side} ${newOrder.symbol} to Basket 🧺`, "success", 2000);

    const modal = document.getElementById("modalBasketOrders");
    if (modal && modal.style.display === "flex") {
        renderBasketOrdersTable();
        updateBasketMarginCalculation();
        refreshAllBasketQuotes();
    } else {
        openBasketOrderModal();
    }
}

function renderBasketOrdersTable() {
    const tbody = document.getElementById("basketOrdersTableBody");
    const emptyState = document.getElementById("basketOrdersEmptyState");
    const noticeBanner = document.getElementById("basketNoticeBanner");
    if (!tbody) return;

    if (!window._basketOrders || window._basketOrders.length === 0) {
        tbody.innerHTML = "";
        if (emptyState) emptyState.style.display = "block";
        if (noticeBanner) noticeBanner.style.display = "none";
        updateBasketBadges();
        return;
    }

    if (emptyState) emptyState.style.display = "none";
    if (noticeBanner) noticeBanner.style.display = "flex";

    const multiplier = window._basketMultiplier || 1;

    let rowsHtml = "";
    window._basketOrders.forEach((o, idx) => {
        const isBuy = (o.side === "BUY");
        const actionClass = isBuy ? "basket-action-buy" : "basket-action-sell";
        const actionLabel = isBuy ? "B" : "S";
        const isCe = (o.opt_type === "CE");
        const typeClass = isCe ? "active-ce" : "active-pe";
        const effectiveQty = o.lots * o.lot_size * multiplier;
        const isMarket = (o.order_type === "MARKET");
        const rowBorderLeft = isBuy ? "border-left:3px solid #10b981;" : "border-left:3px solid #ef4444;";
        const rowBg = isBuy ? "background:rgba(16,185,129,0.035);" : "background:rgba(239,68,68,0.035);";

        const actionStyle = isBuy 
            ? "background:linear-gradient(135deg, #059669, #10b981) !important; color:#ffffff !important; border:1px solid #34d399 !important; box-shadow:0 0 10px rgba(16,185,129,0.5) !important;" 
            : "background:linear-gradient(135deg, #dc2626, #ef4444) !important; color:#ffffff !important; border:1px solid #f87171 !important; box-shadow:0 0 10px rgba(239,68,68,0.5) !important;";

        const typeStyle = isCe
            ? "background:rgba(16,185,129,0.2) !important; color:#34d399 !important; border:1px solid #10b981 !important; box-shadow:0 0 8px rgba(16,185,129,0.3) !important;"
            : "background:rgba(244,63,94,0.2) !important; color:#fb7185 !important; border:1px solid #f43f5e !important; box-shadow:0 0 8px rgba(244,63,94,0.3) !important;";

        rowsHtml += `
            <tr data-order-id="${o.id}" data-idx="${idx}" style="border-bottom:1px solid rgba(255,255,255,0.06); ${rowBorderLeft} ${rowBg}">
                <!-- Checkbox -->
                <td style="text-align:center; padding:6px 4px; width:32px;">
                    <input type="checkbox" ${o.selected ? "checked" : ""} onchange="toggleBasketOrderSelected(${idx}, this.checked)">
                </td>

                <!-- Scrip & Expiry (Merged for compact layout, no horizontal scrollbar) -->
                <td style="padding:6px 8px;">
                    <div style="display:flex; align-items:center; gap:6px;">
                        <span class="basket-row-sym" style="font-weight:800; color:#f8fafc; font-size:12.5px; font-family:var(--font-mono);">${o.symbol}</span>
                        <span title="Duplicate order" style="cursor:pointer; color:#64748b; font-size:11px;" onclick="duplicateBasketOrder(${idx})">📋</span>
                        <span title="Remove from basket" style="cursor:pointer; color:#ef4444; font-size:11px;" onclick="removeBasketOrder(${idx})">🗑️</span>
                    </div>
                    <div style="font-size:10.5px; margin-top:2px; display:flex; align-items:center; gap:5px; font-family:var(--font-mono); color:#94a3b8;">
                        <span class="basket-row-ltp" style="color:#cbd5e1; font-weight:700;">₹${Number(o.ltp || o.price || 0).toFixed(2)}</span>
                        <span>•</span>
                        <span style="color:#38bdf8; font-weight:700; background:rgba(56,189,248,0.12); border:1px solid rgba(56,189,248,0.25); border-radius:3px; padding:0 4px;">${o.expiry_date || 'Current Weekly'}</span>
                    </div>
                </td>

                <!-- Action (B/S Toggle Badge) -->
                <td style="text-align:center; padding:6px 4px; width:44px;">
                    <button type="button" class="basket-action-btn ${actionClass}" title="Click to toggle BUY / SELL" onclick="toggleBasketOrderSide(${idx})" style="${actionStyle} width:32px; height:26px; border-radius:6px; font-weight:900; font-size:12px; cursor:pointer; display:inline-flex; align-items:center; justify-content:center;">
                        ${actionLabel}
                    </button>
                </td>

                <!-- Strike Price (Stepper) -->
                <td style="text-align:center; padding:6px 4px; width:90px;">
                    <div class="basket-stepper-box" style="display:inline-flex; align-items:center; background:#0c101a; border:1px solid #334155; border-radius:6px; overflow:hidden;">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderStrike(${idx}, -1)">▼</button>
                        <input type="number" class="basket-input-num" style="width:48px; background:transparent; border:none; text-align:center; color:#f8fafc; font-weight:800; font-family:var(--font-mono); font-size:12px; outline:none;" value="${o.strike}" onchange="onBasketOrderStrikeChange(${idx}, this.value)">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderStrike(${idx}, 1)">▲</button>
                    </div>
                </td>

                <!-- CE / PE Toggle -->
                <td style="text-align:center; padding:6px 4px; width:46px;">
                    <button type="button" class="basket-badge-type ${typeClass}" onclick="toggleBasketOrderOptType(${idx})" style="${typeStyle} padding:3px 8px; border-radius:6px; font-weight:800; font-size:11px; cursor:pointer; display:inline-flex; align-items:center; justify-content:center;">
                        ${o.opt_type}
                    </button>
                </td>

                <!-- Product (CF / MIS / NRML) -->
                <td style="text-align:center; padding:6px 4px; width:62px;">
                    <select onchange="onBasketOrderProductChange(${idx}, this.value)" style="background:#0b1329; border:1px solid #2563eb; color:#60a5fa; font-size:11px; font-weight:800; border-radius:5px; padding:3px 6px; outline:none; cursor:pointer;">
                        <option value="CF" ${o.product === 'CF' ? 'selected' : ''}>CF</option>
                        <option value="MIS" ${o.product === 'MIS' ? 'selected' : ''}>MIS</option>
                        <option value="NRML" ${o.product === 'NRML' ? 'selected' : ''}>NRML</option>
                    </select>
                </td>

                <!-- Quantity / Lots (Stepper) -->
                <td style="text-align:center; padding:6px 4px; width:90px;">
                    <div class="basket-stepper-box" style="display:inline-flex; align-items:center; background:#0c101a; border:1px solid #334155; border-radius:6px; overflow:hidden;">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderLots(${idx}, -1)">▼</button>
                        <input type="number" class="basket-input-num" min="1" style="width:36px; background:transparent; border:none; text-align:center; color:#f8fafc; font-weight:800; font-family:var(--font-mono); font-size:12px; outline:none;" value="${o.lots}" onchange="onBasketOrderLotsChange(${idx}, this.value)">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderLots(${idx}, 1)">▲</button>
                    </div>
                    <div class="basket-effective-qty-label" style="font-size:10.5px; color:#60a5fa; margin-top:2px; font-family:var(--font-mono); font-weight:700;">
                        ${effectiveQty} qty
                    </div>
                </td>

                <!-- Price / Price Type (LIMIT / MARKET) -->
                <td style="padding:6px 8px; width:150px;">
                    <div style="display:flex; align-items:center; gap:6px;">
                        <input type="number" step="0.05" min="0.05" class="basket-input-num basket-price-input" data-saved-price="${o.price || o.ltp || 0.05}" style="width:68px; text-align:right; padding-right:5px; background:#0c101a; border:1px solid #1e293b; border-radius:4px; font-weight:700; font-size:11px; ${isMarket ? 'opacity:0.6;' : ''}" value="${isMarket ? '0.00' : Number(o.price || 0.05).toFixed(2)}" ${isMarket ? 'disabled placeholder="MKT"' : ''} onchange="onBasketOrderPriceChange(${idx}, this.value)">
                        
                        <!-- Limit / Market Pill Toggle Switch -->
                        <div style="display:inline-flex; align-items:center; background:#070a13; border:1px solid #1e293b; border-radius:12px; padding:2px; cursor:pointer;" onclick="toggleBasketOrderType(${idx})" title="Click to toggle LIMIT / MARKET">
                            <span class="basket-pill-limit" style="font-size:9px; font-weight:800; padding:2px 5px; border-radius:10px; transition:all 0.15s ease; ${!isMarket ? 'background:#2563eb; color:#fff;' : 'background:transparent; color:#64748b;'}">LIMIT</span>
                            <span class="basket-pill-market" style="font-size:9px; font-weight:800; padding:2px 5px; border-radius:10px; transition:all 0.15s ease; ${isMarket ? 'background:#ea580c; color:#fff;' : 'background:transparent; color:#64748b;'}">MARKET</span>
                        </div>
                    </div>
                </td>

                <!-- Delete -->
                <td style="text-align:center; padding:6px 4px; width:30px;">
                    <button type="button" onclick="removeBasketOrder(${idx})" style="background:none; border:none; color:#ef4444; font-size:13px; cursor:pointer; padding:2px;" title="Delete order">✕</button>
                </td>
            </tr>
        `;
    });

    tbody.innerHTML = rowsHtml;
    updateBasketBadges();
}

function toggleSelectAllBasketOrders(checked) {
    window._basketOrders.forEach(o => o.selected = checked);
    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
    } catch(e) {}
    renderBasketOrdersTable();
    updateBasketMarginCalculation();
}

function toggleBasketOrderSelected(idx, checked) {
    if (window._basketOrders[idx]) {
        window._basketOrders[idx].selected = checked;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        updateBasketBadges();
        updateBasketMarginCalculation();
    }
}

function toggleBasketOrderSide(idx) {
    if (window._basketOrders[idx]) {
        window._basketOrders[idx].side = (window._basketOrders[idx].side === "BUY") ? "SELL" : "BUY";
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        renderBasketOrdersTable();
        updateBasketMarginCalculation();
    }
}

let _basketQuoteDebounceTimers = {};

async function fetchQuoteForBasketOrder(idx) {
    if (!window._basketOrders || !window._basketOrders[idx]) return;
    const item = window._basketOrders[idx];

    if (_basketQuoteDebounceTimers[idx]) {
        clearTimeout(_basketQuoteDebounceTimers[idx]);
    }

    _basketQuoteDebounceTimers[idx] = setTimeout(async () => {
        try {
            const und = encodeURIComponent(item.underlying || "NIFTY");
            const stk = encodeURIComponent(item.strike || 0);
            const opt = encodeURIComponent(item.opt_type || "CE");
            const exp = encodeURIComponent(item.expiry_date || "");
            const expMs = encodeURIComponent(item.expiry_ms || "");

            const url = `/api/straddle/contract_quote?underlying=${und}&strike=${stk}&opt_type=${opt}&expiry_date=${exp}&expiry_ms=${expMs}`;
            const res = await fetch(url);
            const json = await res.json();
            if (json && json.success && json.data) {
                const q = json.data;
                const freshLtp = Number(q.ltp || q.price || 0.05);
                item.ltp = freshLtp;
                item.price = freshLtp;
                item.token = q.token || item.token;
                item.instrument_key = q.instrument_key || item.instrument_key;
                item.trading_symbol = q.trading_symbol || item.trading_symbol;
                item.symbol = q.symbol || `${item.underlying} ${item.strike} ${item.opt_type}`;
                if (q.lot_size) item.lot_size = q.lot_size;

                try {
                    localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
                } catch(e) {}

                // Update row in DOM immediately
                const row = document.querySelector(`tr[data-idx="${idx}"]`);
                if (row) {
                    const ltpEl = row.querySelector(".basket-row-ltp");
                    if (ltpEl) ltpEl.textContent = `₹${freshLtp.toFixed(2)}`;

                    const priceInput = row.querySelector(".basket-price-input");
                    if (priceInput && !item.is_market) {
                        priceInput.value = freshLtp.toFixed(2);
                        priceInput.setAttribute("data-saved-price", freshLtp.toString());
                    }

                    const symEl = row.querySelector(".basket-row-sym");
                    if (symEl) symEl.textContent = item.symbol;
                } else {
                    renderBasketOrdersTable();
                }

                updateBasketMarginCalculation();
            }
        } catch (e) {
            console.warn(`[Basket] Could not fetch quote for row ${idx}:`, e);
        }
    }, 120);
}

function toggleBasketOrderOptType(idx) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        item.opt_type = (item.opt_type === "CE") ? "PE" : "CE";
        item.symbol = `${item.underlying} ${item.strike} ${item.opt_type}`;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        renderBasketOrdersTable();
        fetchQuoteForBasketOrder(idx);
    }
}

function stepBasketOrderStrike(idx, step) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        const stepSize = (item.underlying === "BANKNIFTY" || item.underlying === "SENSEX") ? 100 : 50;
        item.strike = Math.max(0, item.strike + (step * stepSize));
        item.symbol = `${item.underlying} ${item.strike} ${item.opt_type}`;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        renderBasketOrdersTable();
        fetchQuoteForBasketOrder(idx);
    }
}

function onBasketOrderStrikeChange(idx, val) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        item.strike = Number(val) || item.strike;
        item.symbol = `${item.underlying} ${item.strike} ${item.opt_type}`;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        renderBasketOrdersTable();
        fetchQuoteForBasketOrder(idx);
    }
}

function stepBasketOrderLots(idx, step) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        item.lots = Math.max(1, item.lots + step);
        item.quantity = item.lots * item.lot_size;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}

        const row = document.querySelector(`tr[data-idx="${idx}"]`);
        if (row) {
            const numInput = row.querySelectorAll(".basket-input-num")[1];
            if (numInput) numInput.value = item.lots;
            const mult = window._basketMultiplier || 1;
            const qtyLbl = row.querySelector(".basket-effective-qty-label");
            if (qtyLbl) qtyLbl.textContent = `${item.lots * item.lot_size * mult} qty`;
        } else {
            renderBasketOrdersTable();
        }
        updateBasketMarginCalculation();
    }
}

function onBasketOrderLotsChange(idx, val) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        item.lots = Math.max(1, parseInt(val) || 1);
        item.quantity = item.lots * item.lot_size;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}

        const row = document.querySelector(`tr[data-idx="${idx}"]`);
        if (row) {
            const mult = window._basketMultiplier || 1;
            const qtyLbl = row.querySelector(".basket-effective-qty-label");
            if (qtyLbl) qtyLbl.textContent = `${item.lots * item.lot_size * mult} qty`;
        } else {
            renderBasketOrdersTable();
        }
        updateBasketMarginCalculation();
    }
}

function onBasketOrderProductChange(idx, val) {
    if (window._basketOrders[idx]) {
        window._basketOrders[idx].product = val;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
    }
}

function toggleBasketOrderType(idx) {
    if (!window._basketOrders[idx]) return;
    const item = window._basketOrders[idx];
    const newType = (item.order_type === "LIMIT") ? "MARKET" : "LIMIT";
    item.order_type = newType;
    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
    } catch(e) {}

    // In-place DOM update without destroying inputs!
    const row = document.querySelector(`tr[data-idx="${idx}"]`);
    if (row) {
        const priceInput = row.querySelector(".basket-price-input");
        const limitPill = row.querySelector(".basket-pill-limit");
        const mktPill = row.querySelector(".basket-pill-market");
        const isMarket = (newType === "MARKET");
        if (priceInput) {
            if (isMarket) {
                priceInput.dataset.savedPrice = priceInput.value || item.price || item.ltp || "0.05";
                priceInput.value = "0.00";
                priceInput.disabled = true;
                priceInput.placeholder = "MKT";
                priceInput.style.opacity = "0.6";
            } else {
                priceInput.disabled = false;
                priceInput.placeholder = "0.00";
                priceInput.style.opacity = "1";
                const restored = parseFloat(priceInput.dataset.savedPrice || item.price || item.ltp || 0.05);
                priceInput.value = (restored > 0 ? restored : 0.05).toFixed(2);
                item.price = parseFloat(priceInput.value);
            }
        }
        if (limitPill && mktPill) {
            if (isMarket) {
                limitPill.style.background = "transparent";
                limitPill.style.color = "#64748b";
                mktPill.style.background = "#ea580c";
                mktPill.style.color = "#fff";
            } else {
                limitPill.style.background = "#2563eb";
                limitPill.style.color = "#fff";
                mktPill.style.background = "transparent";
                mktPill.style.color = "#64748b";
            }
        }
    } else {
        renderBasketOrdersTable();
    }
    updateBasketMarginCalculation();
}

function onBasketOrderPriceChange(idx, val) {
    if (window._basketOrders[idx]) {
        const price = Math.max(0.05, parseFloat(val) || 0.05);
        window._basketOrders[idx].price = price;
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        const row = document.querySelector(`tr[data-idx="${idx}"]`);
        if (row) {
            const priceInput = row.querySelector(".basket-price-input");
            if (priceInput) {
                priceInput.dataset.savedPrice = price.toString();
                priceInput.setAttribute("data-user-edited", "true");
            }
        }
        updateBasketMarginCalculation();
    }
}

function duplicateBasketOrder(idx) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        const clone = JSON.parse(JSON.stringify(item));
        clone.id = "b_" + Date.now() + "_" + Math.floor(Math.random() * 1000);
        window._basketOrders.splice(idx + 1, 0, clone);
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        renderBasketOrdersTable();
        updateBasketMarginCalculation();
    }
}

function removeBasketOrder(idx) {
    if (window._basketOrders[idx]) {
        window._basketOrders.splice(idx, 1);
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        } catch(e) {}
        renderBasketOrdersTable();
        updateBasketMarginCalculation();
    }
}

function stepBasketMultiplier(step) {
    const cur = window._basketMultiplier || 1;
    const mult = Math.max(1, Math.min(100, cur + step));
    window._basketMultiplier = mult;
    try {
        localStorage.setItem("straddle_basket_multiplier", mult.toString());
    } catch(e) {}
    const input = document.getElementById("basketMultiplierInput");
    if (input) input.value = mult;

    // Update effective quantity labels in-place without rebuilding DOM
    document.querySelectorAll(".basket-effective-qty-label").forEach(el => {
        const row = el.closest("tr");
        const idx = parseInt(row?.getAttribute("data-idx"));
        if (!isNaN(idx) && window._basketOrders[idx]) {
            const o = window._basketOrders[idx];
            el.textContent = `${o.lots * o.lot_size * mult} qty`;
        }
    });
    updateBasketMarginCalculation();
}

function onBasketMultiplierChange(val) {
    const mult = Math.max(1, Math.min(100, parseInt(val) || 1));
    window._basketMultiplier = mult;
    try {
        localStorage.setItem("straddle_basket_multiplier", mult.toString());
    } catch(e) {}
    const input = document.getElementById("basketMultiplierInput");
    if (input) input.value = mult;

    document.querySelectorAll(".basket-effective-qty-label").forEach(el => {
        const row = el.closest("tr");
        const idx = parseInt(row?.getAttribute("data-idx"));
        if (!isNaN(idx) && window._basketOrders[idx]) {
            const o = window._basketOrders[idx];
            el.textContent = `${o.lots * o.lot_size * mult} qty`;
        }
    });
    updateBasketMarginCalculation();
}

let _basketMarginDebounceTimer = null;
async function updateBasketMarginCalculation() {
    clearTimeout(_basketMarginDebounceTimer);
    _basketMarginDebounceTimer = setTimeout(async () => {
        const reqEl = document.getElementById("basketRequiredMarginText");
        const availEl = document.getElementById("basketAvailableMarginText");
        const chargesEl = document.getElementById("basketTotalChargesText");
        const hedgeBox = document.getElementById("basketMarginHedgeSavings");
        const hedgeText = document.getElementById("basketHedgeSavingsText");

        const selected = window._basketOrders.filter(o => o.selected);
        if (selected.length === 0) {
            if (reqEl) reqEl.textContent = "0.00";
            if (chargesEl) chargesEl.textContent = "0.00";
            if (hedgeBox) hedgeBox.style.display = "none";
            return;
        }

        try {
            const resp = await fetch("/api/straddle/basket/margin", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    orders: selected,
                    multiplier: window._basketMultiplier || 1
                })
            });
            const data = await resp.json();
            if (data.success) {
                if (reqEl) reqEl.textContent = Number(data.required_margin || 0).toLocaleString('en-IN', {minimumFractionDigits: 2});
                if (availEl) availEl.textContent = Number(data.available_margin || 0).toLocaleString('en-IN', {minimumFractionDigits: 2});
                if (chargesEl) chargesEl.textContent = Number(data.total_charges || 0).toLocaleString('en-IN', {minimumFractionDigits: 2});

                if (data.hedge_benefit > 0 && hedgeBox && hedgeText) {
                    hedgeBox.style.display = "inline-flex";
                    hedgeText.textContent = Number(data.hedge_benefit).toLocaleString('en-IN', {minimumFractionDigits: 2});
                } else if (hedgeBox) {
                    hedgeBox.style.display = "none";
                }
            }
        } catch (e) {
            console.warn("Margin calculation update error:", e);
        }
    }, 200);
}

async function executeCurrentBasket() {
    const selected = window._basketOrders.filter(o => o.selected);
    if (selected.length === 0) {
        showStraddleToast("Please select at least one order in the basket to execute.", "warning", 3000);
        return;
    }

    const mult = window._basketMultiplier || 1;
    const confirmMsg = `Are you sure you want to execute ${selected.length} orders with ${mult}x Multiplier?\n\n` +
                       selected.map(o => `• ${o.side} ${o.lots * mult} lots of ${o.symbol} @ ${o.order_type === 'MARKET' ? 'MARKET' : '₹' + o.price}`).join("\n");

    if (!confirm(confirmMsg)) return;

    const btn = document.getElementById("btnExecuteBasket");
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = "⚡ Executing Basket...";
    }

    try {
        const resp = await fetch("/api/straddle/basket/execute", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                orders: selected,
                multiplier: mult
            })
        });
        const res = await resp.json();

        if (res.success) {
            const count = res.results ? res.results.length : selected.length;
            showStraddleToast(`🎉 Successfully executed ${count} basket orders!`, "success", 4000);
            
            // Remove executed orders from basket
            const executedIds = new Set(selected.map(o => o.id));
            window._basketOrders = window._basketOrders.filter(o => !executedIds.has(o.id));
            try {
                localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
            } catch(e) {}

            renderBasketOrdersTable();
            updateBasketMarginCalculation();
            loadStraddleMargin();
            if (typeof fetchStraddleState === "function") fetchStraddleState();
            if (typeof loadStraddleOpenPositions === "function") loadStraddleOpenPositions();
        } else {
            showStraddleToast(`Basket execution issue: ${res.error || "Failed to place orders"}`, "error", 4500);
        }
    } catch (e) {
        showStraddleToast("Network error executing basket: " + e.message, "error", 4000);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = "⚡ EXECUTE BASKET";
        }
    }
}

function saveCurrentBasket() {
    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
        const name = document.getElementById("basketModalName")?.innerText || "MY BASKET";
        localStorage.setItem("straddle_basket_name", name);
        showStraddleToast(`Basket "${name}" saved successfully!`, "success", 2500);
    } catch (e) {
        showStraddleToast("Error saving basket: " + e.message, "error", 2500);
    }
}

function analyseBasketPayoff() {
    const selected = window._basketOrders.filter(o => o.selected);
    if (selected.length === 0) {
        showStraddleToast("Add and select legs in the basket to analyse strategy payoff.", "warning", 2500);
        return;
    }

    const mult = window._basketMultiplier || 1;
    let netPremium = 0;
    selected.forEach(o => {
        const qty = o.lots * o.lot_size * mult;
        const prc = Number(o.price || o.ltp || 0);
        if (o.side === "SELL") netPremium += (prc * qty);
        else netPremium -= (prc * qty);
    });

    const isCredit = netPremium >= 0;
    const msg = `📊 Strategy Basket Summary:\n` +
                `• Total Legs: ${selected.length}\n` +
                `• Multiplier: ${mult}x\n` +
                `• Net Premium Flow: ₹${Math.abs(netPremium).toLocaleString('en-IN', {minimumFractionDigits: 2})} (${isCredit ? 'Net Credit Received' : 'Net Debit Paid'})\n` +
                `• Hedge Benefit Applied: Yes (BUY-first margin protection)`;
    alert(msg);
}

let _basketSearchDebounceTimer = null;
let _basketSearchQuoteTickerInterval = null;
let _isRefreshingBasketSearchQuotes = false;
window._basketSearchResults = [];

function onBasketSearchAddLeg(searchIdx, side) {
    if (!window._basketSearchResults || !window._basketSearchResults[searchIdx]) return;
    const r = window._basketSearchResults[searchIdx];
    const safeSym = r.symbol || r.trading_symbol || `${r.underlying} ${r.strike} ${r.opt_type}`;
    const safeToken = r.token || r.instrument_key || "";
    const currentLtp = Number(r.ltp) || Number(r.price) || 0.05;
    addLegToBasket(safeSym, safeToken, r.strike, r.opt_type, side, currentLtp, r.lot_size, r.expiry_date);
    clearBasketSearch();
}

async function refreshBasketSearchResultsQuotes() {
    const drop = document.getElementById("basketSearchResultsDropdown");
    if (!drop || drop.style.display === "none") {
        stopBasketSearchQuoteTicker();
        return;
    }
    if (_isRefreshingBasketSearchQuotes) return;
    if (!window._basketSearchResults || window._basketSearchResults.length === 0) return;

    _isRefreshingBasketSearchQuotes = true;
    try {
        const contractsToQuote = window._basketSearchResults.map((r, idx) => ({
            idx: idx,
            symbol: r.symbol,
            trading_symbol: r.trading_symbol,
            underlying: r.underlying,
            strike: r.strike,
            opt_type: r.opt_type,
            expiry_ms: r.expiry_ms,
            expiry_date: r.expiry_date,
            lot_size: r.lot_size,
            token: r.token,
            instrument_key: r.instrument_key
        }));

        const resp = await fetch("/api/straddle/basket/quotes", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ contracts: contractsToQuote })
        });
        const data = await resp.json();
        if (data && data.success && data.quotes) {
            data.quotes.forEach(q => {
                const idx = q.idx;
                if (window._basketSearchResults && window._basketSearchResults[idx]) {
                    const item = window._basketSearchResults[idx];
                    const freshLtp = Number(q.ltp) || 0;
                    if (freshLtp > 0 && Math.abs((item.ltp || 0) - freshLtp) > 0.01) {
                        const prevLtp = item.ltp || freshLtp;
                        item.ltp = freshLtp;
                        item.price = freshLtp;

                        const itemEl = document.querySelector(`.basket-search-result-item[data-search-idx="${idx}"]`);
                        if (itemEl) {
                            const ltpEl = itemEl.querySelector(".basket-search-item-ltp");
                            if (ltpEl) {
                                ltpEl.textContent = `₹${freshLtp.toFixed(2)}`;
                                const isUp = (freshLtp >= prevLtp);
                                ltpEl.style.color = isUp ? "#10b981" : "#f43f5e";
                                ltpEl.style.textShadow = isUp ? "0 0 10px rgba(16,185,129,0.8)" : "0 0 10px rgba(244,63,94,0.8)";
                                setTimeout(() => {
                                    if (ltpEl) {
                                        ltpEl.style.color = "#10b981";
                                        ltpEl.style.textShadow = "none";
                                    }
                                }, 600);
                            }
                        }
                    }
                }
            });
        }
    } catch (e) {
        console.warn("[Basket Search] Live ticker error:", e);
    } finally {
        _isRefreshingBasketSearchQuotes = false;
    }
}

function startBasketSearchQuoteTicker() {
    stopBasketSearchQuoteTicker();
    _basketSearchQuoteTickerInterval = setInterval(() => {
        const drop = document.getElementById("basketSearchResultsDropdown");
        if (drop && drop.style.display !== "none" && window._basketSearchResults && window._basketSearchResults.length > 0) {
            refreshBasketSearchResultsQuotes();
        } else {
            stopBasketSearchQuoteTicker();
        }
    }, 500); // 500ms real-time continuous ticker in live market
}

function stopBasketSearchQuoteTicker() {
    if (_basketSearchQuoteTickerInterval) {
        clearInterval(_basketSearchQuoteTickerInterval);
        _basketSearchQuoteTickerInterval = null;
    }
}

async function handleBasketSearchInput(query) {
    const drop = document.getElementById("basketSearchResultsDropdown");
    const clearBtn = document.getElementById("basketSearchClear");
    if (!drop) return;

    if (!query || query.trim().length < 2) {
        drop.style.display = "none";
        drop.innerHTML = "";
        if (clearBtn) clearBtn.style.display = "none";
        stopBasketSearchQuoteTicker();
        window._basketSearchResults = [];
        return;
    }

    if (clearBtn) clearBtn.style.display = "inline-block";

    const q = query.trim();
    const und = window._straddleState?.optionChainUnderlying || "NIFTY";

    if (_basketSearchDebounceTimer) {
        clearTimeout(_basketSearchDebounceTimer);
    }

    // Show searching spinner
    drop.innerHTML = `<div style="padding:14px; color:#38bdf8; font-size:12px; text-align:center;"><span style="display:inline-block; animation:spin 1s linear infinite;">⏳</span> Searching strikes & expiries for <strong>${q}</strong>...</div>`;
    drop.style.display = "block";

    _basketSearchDebounceTimer = setTimeout(async () => {
        try {
            const resp = await fetch(`/api/straddle/search_contracts?query=${encodeURIComponent(q)}&underlying=${encodeURIComponent(und)}&limit=24`);
            const data = await resp.json();

            if (!data.success || !data.results || data.results.length === 0) {
                drop.innerHTML = `<div style="padding:14px; color:#94a3b8; font-size:12px; text-align:center;">No matching option strikes found for "<span style="color:#f8fafc;">${q}</span>"</div>`;
                drop.style.display = "block";
                stopBasketSearchQuoteTicker();
                window._basketSearchResults = [];
                return;
            }

            window._basketSearchResults = data.results || [];

            const html = data.results.map((r, sIdx) => {
                const isCe = (r.opt_type === "CE");
                const typeColor = isCe ? "#34d399" : "#fb7185";
                const typeBg = isCe ? "rgba(16, 185, 129, 0.15)" : "rgba(244, 63, 94, 0.15)";
                const ltpFormatted = Number(r.ltp || 0).toFixed(2);
                const expTag = r.expiry_tag ? `<span style="font-size:10px; color:#60a5fa; background:rgba(37,99,235,0.15); border:1px solid rgba(59,130,246,0.3); border-radius:3px; padding:1px 5px; margin-left:4px;">${r.expiry_tag}</span>` : "";

                return `
                    <div class="basket-search-result-item" data-search-idx="${sIdx}" style="display:flex; justify-content:space-between; align-items:center; padding:9px 12px; border-bottom:1px solid rgba(255,255,255,0.06); gap:10px;">
                        <div style="flex:1; min-width:0;">
                            <div style="display:flex; align-items:center; gap:6px; flex-wrap:wrap;">
                                <span style="font-weight:800; color:#f8fafc; font-size:12.5px; font-family:var(--font-mono);">${r.strike}</span>
                                <span style="font-size:11px; font-weight:900; color:${typeColor} !important; background:${typeBg} !important; border:1px solid ${typeColor} !important; border-radius:4px; padding:2px 7px;">${r.opt_type}</span>
                                <span style="font-size:11px; color:#cbd5e1; font-family:var(--font-mono);">${r.expiry_date || ''}</span>
                                ${expTag}
                            </div>
                            <div style="font-size:10.5px; color:#94a3b8; margin-top:3px; font-family:var(--font-mono);">
                                ${r.underlying} • Lot: ${r.lot_size} • LTP: <strong class="basket-search-item-ltp" style="color:#10b981; font-size:11.5px; transition:all 0.25s ease;">₹${ltpFormatted}</strong>
                            </div>
                        </div>
                        <div style="display:flex; align-items:center; gap:6px;">
                            <button type="button" class="search-btn-buy" style="background:linear-gradient(135deg, #059669, #10b981) !important; color:#ffffff !important; border:1px solid #34d399 !important; border-radius:5px; padding:5px 12px; font-size:11.5px; font-weight:900 !important; cursor:pointer; box-shadow:0 0 10px rgba(16,185,129,0.5) !important;" title="Add BUY leg to basket" onclick="onBasketSearchAddLeg(${sIdx}, 'BUY')">
                                + B Buy
                            </button>
                            <button type="button" class="search-btn-sell" style="background:linear-gradient(135deg, #dc2626, #ef4444) !important; color:#ffffff !important; border:1px solid #f87171 !important; border-radius:5px; padding:5px 12px; font-size:11.5px; font-weight:900 !important; cursor:pointer; box-shadow:0 0 10px rgba(239,68,68,0.5) !important;" title="Add SELL leg to basket" onclick="onBasketSearchAddLeg(${sIdx}, 'SELL')">
                                - S Sell
                            </button>
                        </div>
                    </div>
                `;
            }).join("");

            drop.innerHTML = html;
            drop.style.display = "block";
            startBasketSearchQuoteTicker();
        } catch (e) {
            drop.innerHTML = `<div style="padding:12px; color:#ef4444; font-size:12px; text-align:center;">Search error: ${e.message}</div>`;
            drop.style.display = "block";
            stopBasketSearchQuoteTicker();
            window._basketSearchResults = [];
        }
    }, 180);
}

function clearBasketSearch() {
    const input = document.getElementById("basketSearchInput");
    const drop = document.getElementById("basketSearchResultsDropdown");
    const clearBtn = document.getElementById("basketSearchClear");
    if (input) input.value = "";
    if (drop) {
        drop.style.display = "none";
        drop.innerHTML = "";
    }
    if (clearBtn) clearBtn.style.display = "none";
    stopBasketSearchQuoteTicker();
    window._basketSearchResults = [];
}

// Close search dropdown on click outside
document.addEventListener("click", function(e) {
    const searchBox = document.getElementById("basketSearchInput")?.closest("div");
    const drop = document.getElementById("basketSearchResultsDropdown");
    if (drop && drop.style.display !== "none") {
        if (!searchBox?.contains(e.target) && !drop.contains(e.target)) {
            drop.style.display = "none";
            stopBasketSearchQuoteTicker();
        }
    }
});

// Window function exports
window.openBasketOrderModal = openBasketOrderModal;
window.openBasketModal = openBasketOrderModal;
window.refreshAllBasketQuotes = refreshAllBasketQuotes;
window.closeBasketOrderModal = closeBasketOrderModal;
window.addLegToBasket = addLegToBasket;
window.renderBasketOrdersTable = renderBasketOrdersTable;
window.toggleSelectAllBasketOrders = toggleSelectAllBasketOrders;
window.toggleBasketOrderSelected = toggleBasketOrderSelected;
window.toggleBasketOrderSide = toggleBasketOrderSide;
window.toggleBasketOrderOptType = toggleBasketOrderOptType;
window.stepBasketOrderStrike = stepBasketOrderStrike;
window.onBasketOrderStrikeChange = onBasketOrderStrikeChange;
window.stepBasketOrderLots = stepBasketOrderLots;
window.onBasketOrderLotsChange = onBasketOrderLotsChange;
window.onBasketOrderProductChange = onBasketOrderProductChange;
window.toggleBasketOrderType = toggleBasketOrderType;
window.onBasketOrderPriceChange = onBasketOrderPriceChange;
window.duplicateBasketOrder = duplicateBasketOrder;
window.removeBasketOrder = removeBasketOrder;
window.stepBasketMultiplier = stepBasketMultiplier;
window.onBasketMultiplierChange = onBasketMultiplierChange;
window.updateBasketMarginCalculation = updateBasketMarginCalculation;
window.executeCurrentBasket = executeCurrentBasket;
window.saveCurrentBasket = saveCurrentBasket;
window.analyseBasketPayoff = analyseBasketPayoff;
window.handleBasketSearchInput = handleBasketSearchInput;
window.clearBasketSearch = clearBasketSearch;
window.fetchQuoteForBasketOrder = fetchQuoteForBasketOrder;
window.onBasketSearchAddLeg = onBasketSearchAddLeg;
window.refreshBasketSearchResultsQuotes = refreshBasketSearchResultsQuotes;
window.startBasketSearchQuoteTicker = startBasketSearchQuoteTicker;
window.stopBasketSearchQuoteTicker = stopBasketSearchQuoteTicker;
window.straddleToggleExecutionMode = straddleToggleExecutionMode;
window.toggleStraddleExecutionMode = straddleToggleExecutionMode;
window.straddleSquareOffAllAndTerminate = straddleSquareOffAllAndTerminate;
window.emergencySquareOffAllStraddles = straddleSquareOffAllAndTerminate;
