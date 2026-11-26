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

function canonicalUnderlying(und) {
    if (!und) return "NIFTY";
    const clean = String(und).toUpperCase().replace(/[\s\-_]/g, '');
    if (clean === "NIFTY50" || clean === "NIFTY" || clean === "CNXNIFTY") return "NIFTY";
    if (clean === "BANKNIFTY" || clean === "NIFTYBANK") return "BANKNIFTY";
    if (clean === "FINNIFTY" || clean === "NIFTYFINSERVICE") return "FINNIFTY";
    if (clean === "MIDCPNIFTY" || clean === "MIDCAPNIFTY" || clean === "NIFTYMIDSELECT") return "MIDCPNIFTY";
    if (clean === "SENSEX" || clean === "BSESENSEX") return "SENSEX";
    if (clean === "BANKEX" || clean === "BSEBANKEX") return "BANKEX";
    if (clean === "CRUDEOIL" || clean === "CRUDE") return "CRUDEOIL";
    if (clean === "CRUDEOILM") return "CRUDEOILM";
    if (clean === "NATURALGAS" || clean === "NATGAS") return "NATURALGAS";
    if (clean === "NATGASMINI") return "NATGASMINI";
    if (clean === "GOLD") return "GOLD";
    if (clean === "GOLDM") return "GOLDM";
    if (clean === "SILVER") return "SILVER";
    if (clean === "SILVERM") return "SILVERM";
    if (clean === "COPPER") return "COPPER";
    return clean;
}
window.canonicalUnderlying = canonicalUnderlying;

function escapeHtml(str) {
    if (str === null || str === undefined) return "";
    return String(str)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}
window.escapeHtml = escapeHtml;


// =========================================================================
// LIGHTNING RESPONSIVE NON-BLOCKING TOAST NOTIFICATIONS
// =========================================================================

function showStraddleToast(message, type = 'info', duration = 2500) {
    if (window.StraddleSound) {
        if (type === 'error') {
            StraddleSound.play('error');
        } else if (type === 'warning') {
            StraddleSound.play('warning');
        } else if (type === 'success') {
            const mLower = String(message).toLowerCase();
            if (mLower.includes('fill') || mLower.includes('placed') || mLower.includes('started') || mLower.includes('retried') || mLower.includes('secured')) {
                StraddleSound.play('order');
            }
        }
    }
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

function parseTradingSymbol(raw) {
    if (!raw || typeof raw !== "string") return raw || "";
    const s = raw.trim().toUpperCase();
    if (s.includes(" ")) return s;
    
    // Monthly options: e.g. BANKNIFTY26NOV64300CE -> BANKNIFTY 64300 CE 26 NOV 2026
    const mMonth = s.match(/^([A-Z]+)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d+(?:\.\d+)?)(CE|PE)$/);
    if (mMonth) {
        const [_, und, yr, mon, strike, opt] = mMonth;
        return `${und} ${strike} ${opt} ${mon} 20${yr}`;
    }
    // Weekly options: e.g. BANKNIFTY24N2045000CE -> BANKNIFTY 45000 CE 20 NOV 2024
    const mWeek = s.match(/^([A-Z]+)(\d{2})([1-9OND])(\d{2})(\d+(?:\.\d+)?)(CE|PE)$/);
    if (mWeek) {
        const [_, und, yr, mcode, day, strike, opt] = mWeek;
        const monthMap = {'1':'JAN','2':'FEB','3':'MAR','4':'APR','5':'MAY','6':'JUN','7':'JUL','8':'AUG','9':'SEP','O':'OCT','N':'NOV','D':'DEC'};
        const mon = monthMap[mcode] || mcode;
        return `${und} ${strike} ${opt} ${day} ${mon} 20${yr}`;
    }
    // Generic options fallback
    const mGen = s.match(/^([A-Z]+?)(\d{2}[A-Z0-9]+?)(\d{4,6})(CE|PE)$/);
    if (mGen) {
        const [_, und, exp, strike, opt] = mGen;
        return `${und} ${strike} ${opt} ${exp}`;
    }
    // Futures: e.g. NIFTY26NOVFUT -> NIFTY FUT 26 NOV 2026
    const mFut = s.match(/^([A-Z]+)(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(FUT)$/);
    if (mFut) {
        const [_, und, yr, mon, fut] = mFut;
        return `${und} FUT ${mon} 20${yr}`;
    }
    return s;
}
window.parseTradingSymbol = parseTradingSymbol;

function formatSymbolColorCoded(symStr) {
    if (!symStr || typeof symStr !== "string") return symStr || "";
    const parsed = parseTradingSymbol(symStr);
    return parsed
        .replace(/\b(CE|CALL)\b/g, '<span style="color:#10b981; font-weight:900; background:rgba(16,185,129,0.18); border:1px solid rgba(16,185,129,0.45); border-radius:3px; padding:1px 5px; display:inline-block; font-size:0.92em; letter-spacing:0.5px;">$1</span>')
        .replace(/\b(PE|PUT)\b/g, '<span style="color:#f87171; font-weight:900; background:rgba(239,68,68,0.18); border:1px solid rgba(239,68,68,0.45); border-radius:3px; padding:1px 5px; display:inline-block; font-size:0.92em; letter-spacing:0.5px;">$1</span>');
}
window.formatSymbolColorCoded = formatSymbolColorCoded;

function showStraddleRejectedToast(opts = {}) {
    if (window.StraddleSound) {
        StraddleSound.play('error');
    }
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

    // Synchronize initial subview with active HTML tab
    const isAlgosActive = document.getElementById("straddleSubTab_algos")?.classList.contains("active");
    if (isAlgosActive) {
        window._straddleState.currentSubView = "algos";
    }

    // Restore saved active underlying from storage or default to NIFTY
    const savedActiveUnd = localStorage.getItem("straddle_active_underlying") || "NIFTY";
    window._activeGlobalUnderlying = savedActiveUnd;
    selectGlobalUnderlying(savedActiveUnd);

    // =========================================================================
    // 0ms INSTANT HYDRATION: Pre-hydrate strategies from Server Injection or LocalStorage Cache
    // =========================================================================
    let initialStrats = [];
    if (window._straddleServerInitialStrategies && Array.isArray(window._straddleServerInitialStrategies) && window._straddleServerInitialStrategies.length > 0) {
        initialStrats = window._straddleServerInitialStrategies;
    } else {
        try {
            const rawCached = localStorage.getItem("straddle_cached_strategies");
            if (rawCached) {
                const parsed = JSON.parse(rawCached);
                if (Array.isArray(parsed) && parsed.length > 0) {
                    initialStrats = parsed;
                }
            }
        } catch (_) { }
    }

    if (initialStrats.length > 0) {
        window._straddleState.allStrategies = initialStrats;
        let curTargetId = window._straddleState.activeStrategyId || localStorage.getItem("straddle_active_strategy_id");
        let active = null;
        if (curTargetId) {
            const targetLower = String(curTargetId).trim().toLowerCase();
            active = initialStrats.find(x => 
                String(x.strategy_id || "").trim().toLowerCase() === targetLower || 
                String(x.name || "").trim().toLowerCase() === targetLower
            );
        }
        if (!active) active = initialStrats[0];
        window._straddleState.activeStrategy = active;
        window._straddleState.activeStrategyId = active ? active.strategy_id : null;

        // Render algos table IMMEDIATELY (0ms instant display upon refresh!)
        renderStraddleAlgosTable(true);
        // Pre-hydrate settings form immediately with active strategy so it never shows blank/default HTML
        if (active && typeof populateStraddleSettingsForm === "function") {
            populateStraddleSettingsForm(active);
        }
    }

    // Initialize local storage & fast ticker pollers immediately without blocking
    loadActiveBasketFromStorage();
    fetchLiveSpotTick();
    window._straddleState.spotTimer = setInterval(fetchLiveSpotTick, 250);
    window._straddleState.pollTimer = setInterval(pollStraddleCycle, 250);

    // Fetch initial state asynchronously without blocking spot tickers
    try {
        await fetchStraddleState();
        renderStraddleAlgosTable(false);
        if (window._straddleState.activeStrategy && typeof populateStraddleSettingsForm === "function") {
            populateStraddleSettingsForm(window._straddleState.activeStrategy);
        }
        // If allStrategies is empty, retry once after 500ms in case server was still starting up or seeding
        if (!window._straddleState.allStrategies || window._straddleState.allStrategies.length === 0) {
            setTimeout(async () => {
                await fetchStraddleState();
                renderStraddleAlgosTable(true);
            }, 500);
        }
    } catch (errState) {
        console.warn("fetchStraddleState non-blocking note:", errState);
    }
    try {
        await loadStraddleOpenPositions();
    } catch (errPos) {
        console.warn("loadStraddleOpenPositions non-blocking note:", errPos);
    }

    // Expert Recommendation 1: Page Visibility API
    // Intelligently throttles background timers when tab is hidden/minimized to save Oracle Cloud bandwidth and CPU
    document.addEventListener("visibilitychange", function () {
        if (document.hidden) {
            // Tab is in background: slow pollers to gentle maintenance heartbeat
            if (window._straddleState.spotTimer) clearInterval(window._straddleState.spotTimer);
            if (window._straddleState.pollTimer) clearInterval(window._straddleState.pollTimer);

            window._straddleState.spotTimer = setInterval(fetchLiveSpotTick, 5000);
            window._straddleState.pollTimer = setInterval(pollStraddleCycle, 10000);

            if (typeof stopOptionChainTicker === "function") {
                stopOptionChainTicker();
            }
        } else {
            // Tab restored to focus: immediately restore lightning-fast polling and trigger fresh ticks
            if (window._straddleState.spotTimer) clearInterval(window._straddleState.spotTimer);
            if (window._straddleState.pollTimer) clearInterval(window._straddleState.pollTimer);

            window._straddleState.spotTimer = setInterval(fetchLiveSpotTick, 250);
            window._straddleState.pollTimer = setInterval(pollStraddleCycle, 250);

            fetchLiveSpotTick();
            pollStraddleCycle();

            const modal = document.getElementById("straddleOptionChainModal");
            const isOpen = modal && !modal.classList.contains("hidden") && (modal.style.display !== "none") && (getComputedStyle(modal).display !== "none");
            if (isOpen && typeof startOptionChainTicker === "function") {
                startOptionChainTicker();
                loadStraddleOptionChain(null, true);
            }
        }
    });

    // Attach global click listener to close dropdowns
    document.addEventListener("click", function (e) {
        if (!e.target.closest(".straddle-dropdown")) {
            document.querySelectorAll(".straddle-dropdown-menu").forEach(m => m.classList.remove("show"));
        }
    });

    // Global ESC key listener to close modals
    document.addEventListener("keydown", function (e) {
        if (e.key === "Escape" || e.keyCode === 27) {
            closeLiveChartModal();
            closeStraddleOptionChainModal();
            if (typeof closeBasketOrderModal === "function") closeBasketOrderModal();
            if (typeof closeMarketDepthModal === "function") closeMarketDepthModal();
        }
    });

    // Auto-save & cloud synchronization listener for strategy settings changes across devices
    const settingsPanel = document.getElementById("straddleSettingsPanel");
    if (settingsPanel && !settingsPanel._cloudSyncListenerAttached) {
        settingsPanel._cloudSyncListenerAttached = true;
        const onSettingsInputChanged = (e) => {
            if (e && e.target && (e.target.id === "straddleAlgosSearch" || e.target.classList.contains("search-input"))) return;
            window._straddleSettingsDirty = true;
            if (window._straddleAutoSaveTimer) clearTimeout(window._straddleAutoSaveTimer);
            window._straddleAutoSaveTimer = setTimeout(() => {
                if (window._straddleSettingsDirty && typeof saveStraddleStrategySettings === "function") {
                    saveStraddleStrategySettings({ silent: true });
                }
            }, 1200);
        };
        settingsPanel.addEventListener("input", onSettingsInputChanged);
        settingsPanel.addEventListener("change", onSettingsInputChanged);
    }

    // Auto-save on page exit / unload so unsaved changes are never lost across devices
    window.addEventListener("beforeunload", () => {
        if (window._straddleSettingsDirty && typeof serializeCurrentStrategySettings === "function") {
            try {
                const strat = serializeCurrentStrategySettings();
                navigator.sendBeacon("/api/straddle/strategy/save", new Blob([JSON.stringify({ strategy: strat })], { type: "application/json" }));
            } catch (_) {}
        }
    });

    // Attach mouse interaction for P&L Canvas crosshair
    setupPnlCanvasEvents();
}

window._lastKnownSpot = 0.0;

function updateSpotPriceDisplay(spot, atm, und) {
    const spotEl = document.getElementById("straddleSpotPrice");
    const undLabel = document.getElementById("straddleUnderlyingLabel");
    const clean = String(und || window._activeGlobalUnderlying || "NIFTY").toUpperCase().replace(/\s+/g, "");
    if (undLabel && und) {
        const disp = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[clean]) ? COMMODITY_DISPLAY_NAMES[clean] : und;
        undLabel.textContent = disp.toUpperCase() + ":";
    }
    if (spot && Number(spot) > 0) {
        const numSpot = Number(spot);
        const prev = window._lastKnownSpot;
        const fmtPrice = (numSpot >= 1000) ?
            numSpot.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) :
            numSpot.toFixed(2);

        if (spotEl) {
            spotEl.textContent = "₹" + fmtPrice;
            if (prev > 0 && Math.abs(numSpot - prev) > 0.01) {
                const isUp = numSpot > prev;
                spotEl.classList.remove("tick-flash-up", "tick-flash-down");
                spotEl.classList.add(isUp ? "tick-flash-up" : "tick-flash-down");
                setTimeout(() => {
                    if (spotEl) spotEl.classList.remove("tick-flash-up", "tick-flash-down");
                }, 300);
            }
        }

        // Live update topbar pill asset spot price
        const topbarSpot = document.getElementById("topbarAssetSpot");
        if (topbarSpot) {
            topbarSpot.textContent = "₹" + fmtPrice;
            if (prev > 0 && Math.abs(numSpot - prev) > 0.01) {
                const isUp = numSpot > prev;
                topbarSpot.classList.remove("tick-flash-up", "tick-flash-down");
                topbarSpot.classList.add(isUp ? "tick-flash-up" : "tick-flash-down");
                setTimeout(() => {
                    if (topbarSpot) topbarSpot.classList.remove("tick-flash-up", "tick-flash-down");
                }, 300);
            }
        }

        // Live update focused asset hero card LTP and ATM
        const heroLtpEl = document.getElementById("activeUndLtp");
        if (heroLtpEl) {
            heroLtpEl.textContent = "₹ " + fmtPrice;
            if (prev > 0 && Math.abs(numSpot - prev) > 0.01) {
                const isUp = numSpot > prev;
                heroLtpEl.classList.remove("tick-flash-up", "tick-flash-down");
                heroLtpEl.classList.add(isUp ? "tick-flash-up" : "tick-flash-down");
                setTimeout(() => {
                    if (heroLtpEl) heroLtpEl.classList.remove("tick-flash-up", "tick-flash-down");
                }, 300);
            }
        }

        const heroAtmEl = document.getElementById("activeUndAtm");
        if (heroAtmEl && atm && Number(atm) > 0) {
            const numAtm = Number(atm);
            heroAtmEl.textContent = (numAtm >= 1000) ?
                numAtm.toLocaleString("en-IN", { maximumFractionDigits: 0 }) :
                numAtm.toString();
        }

        // Live update active asset's legacy pill LTP
        const pillLtpEl = document.getElementById(`pillLtp_${clean}`);
        if (pillLtpEl) {
            pillLtpEl.textContent = "₹" + fmtPrice;
        }

        window._lastKnownSpot = numSpot;
    }
}

async function updateAllCommodityPillsLtp() {
    // Single active asset mode: Handled directly and continuously by fetchLiveSpotTick (<300ms)
}
window.updateAllCommodityPillsLtp = updateAllCommodityPillsLtp;

function onPillNameClick(und) {
    if (!und) return;
    selectGlobalUnderlying(und);
    openActiveAssetChartModal(und);
}
window.onPillNameClick = onPillNameClick;

function onPillPriceClick(und) {
    if (!und) return;
    selectGlobalUnderlying(und);
    openStraddleOptionChainModal(und);
}
window.onPillPriceClick = onPillPriceClick;

async function fetchLiveSpotTick() {
    try {
        const und = window._activeGlobalUnderlying || (window._straddleState.activeStrategy && window._straddleState.activeStrategy.underlying) || "NIFTY";
        const resp = await fetch(`/api/straddle/spot?underlying=${encodeURIComponent(und)}`);
        const data = await resp.json();
        if (data && data.success && data.spot > 0) {
            updateSpotPriceDisplay(data.spot, data.atm, data.underlying || und);
            const activeChart = String(window._activeChartUnderlying || "");
            const isOptionChart = activeChart.includes("CE") || activeChart.includes("PE");
            if (!isOptionChart && typeof updateLiveCandleTick === "function" && activeChart === und) {
                updateLiveCandleTick(data.spot);
            }
        }
    } catch (e) {
        // quiet for sub-second interval
    }
}

let _lastOrderbookAutoRefreshTs = 0;
let _lastPnlAutoRefreshTs = 0;

async function pollStraddleCycle() {
    try {
        await fetchStraddleState();
        const now = Date.now();
        if (window._straddleState.currentSubView === "open_positions") {
            await loadStraddleOpenPositions();
        } else if (window._straddleState.currentSubView === "option_chain") {
            if (typeof loadStraddleOptionChain === "function") {
                await loadStraddleOptionChain();
            }
        } else if (window._straddleState.currentSubView === "pnl") {
            if (now - _lastPnlAutoRefreshTs >= 5000) {
                _lastPnlAutoRefreshTs = now;
                if (typeof loadStraddlePnlChart === "function" && !window._straddleState._pnlHoverActive) {
                    await loadStraddlePnlChart();
                }
            }
        }

        // Automatic 5-second live refresh for Orderbook & Margins
        const orderbookSec = document.getElementById("orderbookOrdersSection");
        const isOrderbookVisible = (window._straddleState.currentSubView === "orderbook") ||
            (orderbookSec && orderbookSec.style.display !== "none");
        if (isOrderbookVisible && (now - _lastOrderbookAutoRefreshTs >= 5000)) {
            _lastOrderbookAutoRefreshTs = now;
            if (typeof loadStraddleOrderbook === "function") {
                await loadStraddleOrderbook(false);
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
            if (s.all_strategies && Array.isArray(s.all_strategies) && s.all_strategies.length > 0) {
                s.all_strategies.forEach(serverStrat => {
                    const sid = String(serverStrat.strategy_id || "").trim();
                    const pending = window._pendingMultiplierModes?.[sid];
                    if (pending && (Date.now() - pending.timestamp < 4000)) {
                        serverStrat.multiplier_type = pending.mode;
                    }
                });
                window._straddleState.allStrategies = s.all_strategies;
                window._straddleStateLoadDone = true;
                try {
                    localStorage.setItem("straddle_cached_strategies", JSON.stringify(s.all_strategies));
                } catch (_) { }
            } else if (Array.isArray(window._straddleState.allStrategies) && window._straddleState.allStrategies.length > 0) {
                // Infallible guard: Retain active strategies cache if server momentarily returns empty []
            } else {
                window._straddleState.allStrategies = s.all_strategies || [];
                window._straddleStateLoadDone = true;
            }

            // Determine active strategy strictly matching activeStrategyId or activeStrategy
            const prevActiveId = window._straddleState.activeStrategy ? window._straddleState.activeStrategy.strategy_id : null;
            let curTargetId = window._straddleState.activeStrategyId;
            if (!curTargetId) {
                try {
                    curTargetId = localStorage.getItem("straddle_active_strategy_id");
                } catch (_) { }
            }
            let active = null;
            if (curTargetId) {
                const targetLower = String(curTargetId).trim().toLowerCase();
                active = (s.all_strategies || []).find(x => 
                    String(x.strategy_id || "").trim().toLowerCase() === targetLower || 
                    String(x.name || "").trim().toLowerCase() === targetLower
                );
            }
            if (!active && s.active_strategy) {
                active = s.active_strategy;
            }
            if (!active) {
                active = (s.all_strategies && s.all_strategies[0]) || null;
            }
            window._straddleState.activeStrategy = active;
            window._straddleState.activeStrategyId = active ? active.strategy_id : null;
            if (active && active.strategy_id) {
                try {
                    localStorage.setItem("straddle_active_strategy_id", active.strategy_id);
                } catch (_) { }
            }
            if (s.closed_positions && Array.isArray(s.closed_positions)) {
                window._straddleState.closed_positions = s.closed_positions;
            } else {
                window._straddleState.closed_positions = [];
            }
            let activePositions = [];
            if (active) {
                const openList = (active.positions || []).filter(p => p && (String(p.status).toUpperCase() === "OPEN" || String(p.status).toUpperCase() === "LIVE OPEN" || String(p.status).toUpperCase() === "EXIT_PENDING"));
                const otherPos = (active.positions || []).filter(p => p && String(p.status).toUpperCase() !== "OPEN" && String(p.status).toUpperCase() !== "LIVE OPEN" && String(p.status).toUpperCase() !== "EXIT_PENDING");
                const closedList = [
                    ...(active.closed_positions || []),
                    ...(s.closed_positions || [])
                ];
                const combined = [...openList, ...otherPos, ...closedList];
                const seenKeys = new Set();
                activePositions = combined.filter(p => {
                    if (!p) return false;
                    const oid = String(p.order_id || p.broker_order_id || "").trim();
                    const isRealOid = oid && !oid.startsWith("ORD_") && !oid.startsWith("PAPER_");
                    const k = isRealOid
                        ? `${p.symbol || ""}_${oid}_${p.exit_order_id || ""}`
                        : `${p.id || p.symbol || "leg"}_${p.order_id || ""}_${p.entry_time || ""}_${p.exit_time || ""}_${p.avg_ent_price || ""}_${p.re_execute_count || 0}_${p.status || ""}`;
                    if (seenKeys.has(k)) return false;
                    seenKeys.add(k);
                    return true;
                });
            } else {
                const combined = [...(s.positions || []), ...(s.closed_positions || [])];
                const seenKeys = new Set();
                activePositions = combined.filter(p => {
                    if (!p) return false;
                    const oid = String(p.order_id || p.broker_order_id || "").trim();
                    const isRealOid = oid && !oid.startsWith("ORD_") && !oid.startsWith("PAPER_");
                    const k = isRealOid
                        ? `${p.symbol || ""}_${oid}_${p.exit_order_id || ""}`
                        : `${p.id || p.symbol || "leg"}_${p.order_id || ""}_${p.entry_time || ""}_${p.exit_time || ""}_${p.avg_ent_price || ""}_${p.re_execute_count || 0}_${p.status || ""}`;
                    if (seenKeys.has(k)) return false;
                    seenKeys.add(k);
                    return true;
                });
            }
            window._straddleState.positions = activePositions;

            if (s.logs && Array.isArray(s.logs)) {
                window._straddleState.logs = s.logs;

                // Real-time audio notification synthesizer for freshly arriving logs
                if (!window._soundSeenLogKeys) window._soundSeenLogKeys = new Set();
                s.logs.forEach(l => {
                    if (!l) return;
                    const logKey = `${l.date || ''}_${l.timestamp || ''}_${l.message || ''}`;
                    if (!window._soundSeenLogKeys.has(logKey)) {
                        window._soundSeenLogKeys.add(logKey);
                        if (window._soundInitialStateLoadDone && window.StraddleSound) {
                            const msgU = String(l.message || "").toUpperCase();
                            if (msgU.includes("TARGET HIT") || msgU.includes("PROFIT TARGET")) {
                                StraddleSound.play("target");
                            } else if (msgU.includes("STOPLOSS HIT") || msgU.includes("SL HIT") || msgU.includes("STOP LOSS") || msgU.includes("STOPLOSS ORDER")) {
                                StraddleSound.play("stoploss");
                            } else if (msgU.includes("ORDER REJECTED") || msgU.includes("FAILED") || l.level === "ERROR") {
                                StraddleSound.play("error");
                            } else if (msgU.includes("TRIGGERING RE EXECUTE") || msgU.includes("TRIGGERING RE-EXECUTE") || msgU.includes("ENTERED SELL") || msgU.includes("ENTERED HEDGE") || msgU.includes("RE-ENTRY")) {
                                StraddleSound.play("order");
                            } else if (msgU.includes("HEDGE SHIFT") || msgU.includes("HEDGE AUTO SQUARE-OFF")) {
                                StraddleSound.play("warning");
                            } else if (msgU.includes("SQUARED OFF") || msgU.includes("EXITTED") || msgU.includes("EXIT")) {
                                StraddleSound.play("exit");
                            }
                        }
                    }
                });
                window._soundInitialStateLoadDone = true;
            }

            // Sync live broker margin from state
            if (s.margin) {
                window._straddleState.margin = s.margin;
                window._straddleMargin = s.margin;
                if (typeof updateFormAutoMultiplierPreview === "function") {
                    updateFormAutoMultiplierPreview();
                    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
                        scheduleAutoMultiplierLiveRecalc();
                    }
                }
            }

            renderStraddleHeader(s);
            updateStraddleStrategyDropdown(s.all_strategies || [], active);

            // Update Terminal positions and synchronize settings form with latest cloud state
            if (active) {
                const settingsPanel = document.getElementById("straddleSettingsPanel");
                const isUserTyping = settingsPanel && settingsPanel.contains(document.activeElement);
                const needsPopulate = (!window._straddleFormPopulatedFor || window._straddleFormPopulatedFor !== active.strategy_id);
                if (!isUserTyping && (needsPopulate || (!window._straddleSettingsDirty && window._straddleState.currentSubView === "terminal"))) {
                    populateStraddleSettingsForm(active);
                }
            }
            if (window._straddleState.currentSubView === "terminal") {
                renderStraddlePositions(activePositions, active ? (active.group_mtm || 0.0) : 0.0, active ? (active.mtm || 0.0) : 0.0);
            }

            // Render Margin Error 15s Countdown Alert Banner (Quantiply Parity)
            renderStraddleMarginAutoExitAlert(active);

            // In-place smart update for Algos view (never wipes focus/open dropdowns)
            const algosPanel = document.getElementById("straddleSubView_algos");
            const isAlgosVisible = (window._straddleState.currentSubView === "algos") ||
                (algosPanel && algosPanel.style.display !== "none" && !algosPanel.classList.contains("hidden"));
            if (isAlgosVisible) {
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
            const hasOpen = Array.isArray(activeStrat.positions) && activeStrat.positions.some(p => p.status === "OPEN" || p.status === "LIVE OPEN");
            const hasCompleted = Array.isArray(activeStrat.positions) && activeStrat.positions.some(p => p.status === "COMPLETE" || p.status === "CLOSED" || p.status === "EXITED");
            const isEnabled = activeStrat.enabled !== false;

            if (rawStatus === "TERMINATED") {
                statusBadge.textContent = "Terminated";
                statusBadge.className = "straddle-pill-badge straddle-badge-terminated";
            } else if (rawStatus === "STOPPED" || (!hasOpen && (hasCompleted || activeStrat.has_traded_today) && !activeStrat._manual_reset)) {
                statusBadge.textContent = "Stopped";
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
    const undBadge = document.getElementById("straddleTerminalUnderlyingBadge");
    if (undBadge && activeStrat) {
        undBadge.textContent = canonicalUnderlying(activeStrat.underlying || "NIFTY");
    }

    if (typeof updateExecutionModeUI === "function") {
        updateExecutionModeUI(state.execution_mode || "LIVE_BROKER");
    }

    if (typeof updateBrokerConnectionUI === "function" && state.broker_connected !== undefined) {
        updateBrokerConnectionUI(state.broker_connected);
    }

    if (!window._activeGlobalUnderlying || window._activeGlobalUnderlying === state.underlying_symbol) {
        updateSpotPriceDisplay(state.underlying_spot, state.underlying_atm_strike, state.underlying_symbol);
    }

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
    if (bOrder) {
        const isOrderOn = !!state.order_api_running;
        bOrder.className = "straddle-running-badge " + (isOrderOn ? "badge-running" : "badge-off");
        bOrder.textContent = isOrderOn ? "Order API: Running" : "Order API: Stopped";
    }
    if (bFeed) {
        const isFeedOn = !!state.live_feed_running;
        bFeed.className = "straddle-running-badge " + (isFeedOn ? "badge-running" : "badge-off");
        bFeed.textContent = isFeedOn ? "Live Feed: Running" : "Live Feed: Stalled";
    }
    if (bAlgo) {
        const strat = activeStrat || state.activeStrategy;
        const stratStatus = strat ? String(strat.status || "").toUpperCase() : String(state.status || "READY").toUpperCase();
        const hasOpen = Array.isArray(strat?.positions) && strat.positions.some(p => p.status === "OPEN" || p.status === "LIVE OPEN");
        const isRunning = (state.algo_running && stratStatus === "TRADE_ACTIVE") || hasOpen;

        if (isRunning) {
            bAlgo.className = "straddle-running-badge badge-running";
            bAlgo.textContent = "● Algo: Running";
        } else if (stratStatus === "TERMINATED") {
            bAlgo.className = "straddle-running-badge badge-terminated";
            bAlgo.textContent = "■ Algo: Terminated";
        } else if (stratStatus === "ENTRY_FAILED") {
            bAlgo.className = "straddle-running-badge badge-failed";
            bAlgo.textContent = "⚠️ Algo: Entry Failed";
        } else if (stratStatus === "STOPPED") {
            bAlgo.className = "straddle-running-badge badge-stopped";
            bAlgo.textContent = "■ Algo: Stopped";
        } else if (stratStatus === "READY") {
            bAlgo.className = "straddle-running-badge badge-ready";
            bAlgo.textContent = "⏳ Algo: Armed (Ready)";
        } else {
            bAlgo.className = "straddle-running-badge badge-off";
            bAlgo.textContent = "○ Algo: Offline";
        }
    }

    // Update terminal action bar with clean, contextual states
    updateTerminalActionBar(activeStrat);
}


// =========================================================================
// SUB-VIEW NAVIGATION (OPEN POSITIONS, ALGOS, ALGO DETAIL / TERMINAL, P&L CHART)
// =========================================================================

function switchStraddleSubView(view) {
    if (view === "algo_detail") {
        view = "terminal";
    }

    // Auto-save pending settings changes if leaving terminal
    if (window._straddleState.currentSubView === "terminal" && view !== "terminal" && window._straddleSettingsDirty) {
        if (typeof saveStraddleStrategySettings === "function") {
            saveStraddleStrategySettings({ silent: true });
        }
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
        // Stop background chart, basket, and option chain tickers for maximum responsiveness & 0ms CPU waste
        if (typeof stopChartTickStream === "function") stopChartTickStream();
        if (typeof stopBasketQuoteTicker === "function") stopBasketQuoteTicker();
        if (typeof stopBasketSearchQuoteTicker === "function") stopBasketSearchQuoteTicker();
        if (typeof stopOptionChainTicker === "function") stopOptionChainTicker();
        if (typeof closeMarketDepthModal === "function") closeMarketDepthModal();

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
    const filterBtnMap = {
        open: "btnPosFilterOpen",
        closed: "btnPosFilterClosed",
        all: "btnPosFilterAll",
        orders: "btnPosFilterOrders",
        margin: "btnPosFilterMargin"
    };

    Object.entries(filterBtnMap).forEach(([fKey, btnId]) => {
        const btn = document.getElementById(btnId);
        if (btn) {
            const isMatch = (filter === fKey);
            btn.classList.toggle("active-toggle", isMatch);
            btn.classList.toggle("active", isMatch);
            if (isMatch && typeof btn.scrollIntoView === "function") {
                try {
                    btn.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
                } catch (_) { }
            }
        }
    });

    const posCard = document.getElementById("straddleOpenPositionsCard") || document.getElementById("straddleOpenPositionsTable")?.closest(".straddle-card");
    const summaryControls = document.getElementById("straddlePositionsSummaryControls");
    const ordersSection = document.getElementById("orderbookOrdersSection");
    const marginSection = document.getElementById("orderbookMarginSection");

    if (filter === "orders") {
        if (posCard) posCard.style.display = "none";
        if (summaryControls) summaryControls.style.display = "none";
        if (marginSection) marginSection.style.display = "none";
        if (ordersSection) ordersSection.style.display = "block";
        document.getElementById("btnSubTabOrderbook")?.classList.add("active");
        document.getElementById("btnSubTabMargin")?.classList.remove("active");
        loadStraddleOrderbook();
    } else if (filter === "margin") {
        if (posCard) posCard.style.display = "none";
        if (summaryControls) summaryControls.style.display = "none";
        if (ordersSection) ordersSection.style.display = "none";
        if (marginSection) marginSection.style.display = "block";
        document.getElementById("btnSubTabOrderbook")?.classList.remove("active");
        document.getElementById("btnSubTabMargin")?.classList.add("active");
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

// Interactive column sorting state for Open Positions Table
window._straddlePosSortCol = window._straddlePosSortCol || "default";
window._straddlePosSortDir = window._straddlePosSortDir || "asc";

function sortStraddlePositionsBy(col) {
    if (window._straddlePosSortCol === col) {
        window._straddlePosSortDir = (window._straddlePosSortDir === "asc") ? "desc" : "asc";
    } else {
        window._straddlePosSortCol = col;
        window._straddlePosSortDir = "asc";
    }
    renderStraddleOpenPositionsTable();
}
window.sortStraddlePositionsBy = sortStraddlePositionsBy;

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

    // Update sort header indicator icons on table columns
    const sortCols = ['status', 'side', 'symbol', 'product', 'quantity', 'buy_avg', 'sell_avg', 'avg_price', 'ltp', 'mtm', 'source'];
    sortCols.forEach(c => {
        const el = document.getElementById(`pos_sort_${c}`);
        if (el) {
            if (window._straddlePosSortCol === c) {
                el.textContent = window._straddlePosSortDir === 'asc' ? '▲' : '▼';
                el.style.color = '#38bdf8';
            } else {
                el.textContent = '↕';
                el.style.color = '#64748b';
            }
        }
    });

    const sortCol = window._straddlePosSortCol || "default";
    const sortDir = window._straddlePosSortDir || "asc";
    const sortMult = (sortDir === "desc") ? -1 : 1;

    const _posSafeNum = (val) => {
        const n = Number(val);
        return isNaN(n) ? 0 : n;
    };

    const _extractStrikeNum = (p) => {
        if (!p) return 0;
        const stk = _posSafeNum(p.strike_price);
        if (stk > 0) return stk;
        const sym = String(p.symbol || p.trading_symbol || "");
        const m = sym.match(/(\d{4,6})\s*(?:CE|PE)/i);
        if (m) {
            const numStr = m[1];
            if (numStr.length === 7) return Number(numStr.slice(2));
            return Number(numStr);
        }
        const m5 = sym.match(/\b\d{5}\b/);
        if (m5) return Number(m5[0]);
        return 0;
    };

    filtered.sort((a, b) => {
        // Mathematically stable tie-breaker: guarantees order NEVER flips randomly on polling
        const keyA = String(a.instrument_key || a.symbol || a.trading_symbol || '');
        const keyB = String(b.instrument_key || b.symbol || b.trading_symbol || '');
        const tieBreaker = keyA.localeCompare(keyB);

        if (sortCol === "default") {
            const isOpA = (a.is_open !== false && (a.net_qty || a.quantity || 0) !== 0) ? 0 : 1;
            const isOpB = (b.is_open !== false && (b.net_qty || b.quantity || 0) !== 0) ? 0 : 1;
            if (isOpA !== isOpB) return isOpA - isOpB;

            const stratA = String(a.strategy_name || a.source || "").toLowerCase();
            const stratB = String(b.strategy_name || b.source || "").toLowerCase();
            if (stratA !== stratB) return stratA.localeCompare(stratB);

            const strikeA = _extractStrikeNum(a);
            const strikeB = _extractStrikeNum(b);
            if (strikeA !== strikeB) return strikeA - strikeB;

            const symA = String(a.symbol || a.trading_symbol || "").toUpperCase();
            const symB = String(b.symbol || b.trading_symbol || "").toUpperCase();
            const isCeA = symA.includes("CE") ? 0 : (symA.includes("PE") ? 1 : 2);
            const isCeB = symB.includes("CE") ? 0 : (symB.includes("PE") ? 1 : 2);
            if (isCeA !== isCeB) return isCeA - isCeB;

            return tieBreaker;
        }

        let valA, valB;
        let isNumeric = false;
        if (sortCol === "symbol") {
            valA = String(a.symbol || a.trading_symbol || "").toUpperCase();
            valB = String(b.symbol || b.trading_symbol || "").toUpperCase();
            const comp = valA.localeCompare(valB);
            return comp !== 0 ? comp * sortMult : tieBreaker;
        } else if (sortCol === "status") {
            valA = (a.is_open !== false && (a.net_qty || a.quantity || 0) !== 0) ? "OPEN" : "CLOSED";
            valB = (b.is_open !== false && (b.net_qty || b.quantity || 0) !== 0) ? "OPEN" : "CLOSED";
            const comp = valA.localeCompare(valB);
            return comp !== 0 ? comp * sortMult : tieBreaker;
        } else if (sortCol === "side") {
            valA = String(a.side || "").toUpperCase();
            valB = String(b.side || "").toUpperCase();
            const comp = valA.localeCompare(valB);
            return comp !== 0 ? comp * sortMult : tieBreaker;
        } else if (sortCol === "product") {
            valA = String(a.product || "").toUpperCase();
            valB = String(b.product || "").toUpperCase();
            const comp = valA.localeCompare(valB);
            return comp !== 0 ? comp * sortMult : tieBreaker;
        } else if (sortCol === "source") {
            valA = String(a.strategy_name || a.source || "").toLowerCase();
            valB = String(b.strategy_name || b.source || "").toLowerCase();
            const comp = valA.localeCompare(valB);
            return comp !== 0 ? comp * sortMult : tieBreaker;
        } else if (sortCol === "quantity") {
            isNumeric = true;
            valA = _posSafeNum(a.net_qty !== undefined ? a.net_qty : (a.quantity || 0));
            valB = _posSafeNum(b.net_qty !== undefined ? b.net_qty : (b.quantity || 0));
        } else if (sortCol === "buy_avg") {
            isNumeric = true;
            valA = _posSafeNum(a.buy_avg);
            valB = _posSafeNum(b.buy_avg);
        } else if (sortCol === "sell_avg") {
            isNumeric = true;
            valA = _posSafeNum(a.sell_avg);
            valB = _posSafeNum(b.sell_avg);
        } else if (sortCol === "avg_price") {
            isNumeric = true;
            valA = _posSafeNum(a.avg_price || a.net_avg);
            valB = _posSafeNum(b.avg_price || b.net_avg);
        } else if (sortCol === "ltp") {
            isNumeric = true;
            valA = _posSafeNum(a.ltp || a.last_price);
            valB = _posSafeNum(b.ltp || b.last_price);
        } else if (sortCol === "mtm") {
            isNumeric = true;
            const isOpA = (a.is_open !== false && (a.net_qty || a.quantity || 0) !== 0);
            const isOpB = (b.is_open !== false && (b.net_qty || b.quantity || 0) !== 0);
            valA = _posSafeNum(isOpA ? (a.mtm !== undefined ? a.mtm : a.pnl) : (a.realised_pnl !== undefined ? a.realised_pnl : a.pnl));
            valB = _posSafeNum(isOpB ? (b.mtm !== undefined ? b.mtm : b.pnl) : (b.realised_pnl !== undefined ? b.realised_pnl : b.pnl));
        } else {
            return tieBreaker;
        }

        if (isNumeric) {
            const diff = valA - valB;
            if (diff !== 0) {
                return (diff > 0 ? 1 : -1) * sortMult;
            }
        }
        return tieBreaker;
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

    const prevMap = window._straddleState.prevLtpMap || {};

    // Sound alert transition detection across strategy positions (Target, Stoploss, Errors, Order Fills, Exits)
    if (!window._soundPositionStates) window._soundPositionStates = new Map();
    positions.forEach((pos, idx) => {
        if (!pos) return;
        const key = `${pos.id || idx}_${pos.symbol || ''}`;
        const curStatus = String(pos.status || "").toUpperCase();
        const prev = window._soundPositionStates.get(key);

        if (window.StraddleSound) {
            if (prev) {
                // 1. Transition to ERROR
                if (curStatus === "ERROR" && prev.status !== "ERROR") {
                    StraddleSound.play("error");
                }
                // 2. Transition to COMPLETE / CLOSED / EXITED (Check whether Target or Stoploss)
                else if ((curStatus === "COMPLETE" || curStatus === "CLOSED" || curStatus === "EXITED") && (prev.status === "OPEN" || prev.status === "LIVE OPEN")) {
                    const r = String(pos.exit_reason || "").toUpperCase();
                    if (r.includes("TARGET") || r.includes("TGT") || r.includes("PROFIT")) {
                        StraddleSound.play("target");
                    } else if (r.includes("STOPLOSS") || r.includes("SL") || r.includes("STOP LOSS") || r.includes("LOSS")) {
                        StraddleSound.play("stoploss");
                    } else if (r.includes("HEDGE")) {
                        StraddleSound.play("warning");
                    } else {
                        StraddleSound.play("exit");
                    }
                }
                // 3. Transition to OPEN (Fill confirmation on retry)
                else if ((curStatus === "OPEN" || curStatus === "LIVE OPEN") && prev.status !== "OPEN" && prev.status !== "LIVE OPEN") {
                    StraddleSound.play("order");
                }
            } else if (window._soundPositionsInitialLoadDone) {
                // Freshly filled position (e.g. re-execution entry or initial entry)
                if (curStatus === "OPEN" || curStatus === "LIVE OPEN") {
                    StraddleSound.play("order");
                }
            }
        }
        window._soundPositionStates.set(key, { status: curStatus, exit_reason: pos.exit_reason || "" });
    });
    window._soundPositionsInitialLoadDone = true;

    // In-place DOM update for Open Positions: prevents flickering & high CPU
    const existingPosRows = tbody.querySelectorAll("tr[data-pos-key]");
    if (existingPosRows.length === filtered.length && filtered.length > 0) {
        let keysMatch = true;
        for (let i = 0; i < filtered.length; i++) {
            const p = filtered[i];
            const pKey = String(p.instrument_key || p.symbol || "");
            const row = existingPosRows[i];
            const isOp = (p.is_open !== false && (p.net_qty || p.quantity || 0) !== 0);
            if (!row || row.getAttribute("data-pos-key") !== pKey || row.getAttribute("data-pos-status") !== (isOp ? 'OPEN' : 'CLOSED')) {
                keysMatch = false;
                break;
            }
        }
        if (keysMatch) {
            filtered.forEach((p, i) => {
                const row = existingPosRows[i];
                if (!row) return;
                const sym = p.symbol || p.trading_symbol || "";
                const isOpen = (p.is_open !== false && (p.net_qty || p.quantity || 0) !== 0);
                const ltp = Number(p.ltp || p.last_price || (window._straddleState?.prevLtpMap && window._straddleState.prevLtpMap[sym]) || 0.0);
                const prevLtp = prevMap[sym] || ltp;
                let ltpClass = "";
                if (isOpen && ltp > prevLtp) ltpClass = "straddle-ltp-up";
                else if (isOpen && ltp < prevLtp) ltpClass = "straddle-ltp-down";
                prevMap[sym] = ltp;

                const ltpEl = row.querySelector(".straddle-ltp");
                if (ltpEl) {
                    ltpEl.textContent = ltp > 0 ? '₹' + ltp.toFixed(2) : '-';
                    ltpEl.className = `straddle-ltp ${ltpClass}`;
                }

                const mtm = isOpen ? (p.mtm !== undefined ? p.mtm : (p.pnl || 0.0)) : (p.realised_pnl !== undefined ? p.realised_pnl : (p.pnl || 0.0));
                const mtmClass = mtm >= 0 ? "straddle-mtm-pos" : "straddle-mtm-neg";
                const mtmFormatted = (mtm >= 0 ? "+₹ " : "-₹ ") + Math.abs(mtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                const mtmEl = row.querySelector(".straddle-mtm");
                if (mtmEl) {
                    mtmEl.textContent = mtmFormatted;
                    mtmEl.className = `straddle-mtm ${mtmClass}`;
                }
            });
            return;
        }
    }

    let rowsHtml = "";

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
            <tr data-pos-key="${p.instrument_key || p.symbol || ''}" data-pos-status="${isOpen ? 'OPEN' : 'CLOSED'}">
                <td>${statusPill}</td>
                <td><span class="straddle-side-badge ${sideBadgeClass}">${side}</span></td>
                <td>
                    <div class="straddle-sym-card ${symAccentClass}">
                        <div class="straddle-sym-title">${formatSymbolColorCoded(p.symbol || p.trading_symbol)}</div>
                        <div class="straddle-sym-sub">${p.instrument_key || ''}</div>
                    </div>
                </td>
                <td>
                    <span style="background:#1e293b; color:#94a3b8; font-size:10.5px; padding:2px 6px; border-radius:4px; font-weight:700;">${p.product || 'NRML'}</span>
                </td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:800; color:#f8fafc;">
                    ${isOpen ? `${absQty} (${netQty > 0 ? '+' + netQty : netQty})` : '0 (Squared)'}
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

function getCurrentlyDisplayedStrategies() {
    const strategies = window._straddleState?.allStrategies || [];
    if (!strategies.length) return [];
    const searchVal = (document.getElementById("straddleAlgosSearch")?.value || "").toLowerCase().trim();
    const activeUnd = canonicalUnderlying(window._activeGlobalUnderlying || "NIFTY");
    let showAll = window._showAllUnderlyingsAlgos !== false;

    let filtered = strategies.filter(s => {
        if (searchVal) {
            const name = (s.name || s.strategy_id || "").toLowerCase();
            const status = (s.status || "Ready").toLowerCase();
            const und = canonicalUnderlying(s.underlying || "NIFTY").toLowerCase();
            return name.includes(searchVal) || und.includes(searchVal) || status.includes(searchVal);
        }
        if (showAll) return true;

        const hasActiveTrading = Array.isArray(s.positions) && s.positions.some(p => p.status === "OPEN" || p.status === "LIVE OPEN" || p.status === "EXIT_PENDING" || p.status === "ERROR");
        if (hasActiveTrading) return true;

        const und = canonicalUnderlying(s.underlying || "NIFTY");
        if (!showAll && und !== activeUnd) return false;
        return true;
    });

    if (filtered.length === 0 && strategies.length > 0 && !searchVal) {
        filtered = strategies;
    }
    return filtered;
}

function onToggleAlgoSelect(strategyId, isChecked) {
    if (isChecked) {
        window._selectedAlgoIds.add(strategyId);
    } else {
        window._selectedAlgoIds.delete(strategyId);
    }
    const tr = document.querySelector(`tr[data-strategy-id="${strategyId}"]`);
    if (tr) {
        tr.style.background = isChecked ? "rgba(59,130,246,0.08)" : "";
        const cb = tr.querySelector(".straddle-algo-checkbox");
        if (cb && cb.checked !== isChecked) cb.checked = isChecked;
    }
    updateBulkActionsBar();
    updateMasterCheckboxState();
}

function toggleSelectAllStraddleAlgos(masterCb) {
    const isChecked = masterCb ? !!masterCb.checked : false;
    const displayed = getCurrentlyDisplayedStrategies();
    if (isChecked) {
        displayed.forEach(s => {
            if (s && s.strategy_id) window._selectedAlgoIds.add(s.strategy_id);
        });
    } else {
        displayed.forEach(s => {
            if (s && s.strategy_id) window._selectedAlgoIds.delete(s.strategy_id);
        });
    }
    const tbody = document.getElementById("straddleAlgosTableBody");
    if (tbody) {
        const rows = tbody.querySelectorAll("tr[data-strategy-id]");
        rows.forEach(tr => {
            const sid = tr.getAttribute("data-strategy-id");
            const sel = window._selectedAlgoIds.has(sid);
            tr.style.background = sel ? "rgba(59,130,246,0.08)" : "";
            const cb = tr.querySelector(".straddle-algo-checkbox");
            if (cb) cb.checked = sel;
        });
    }
    updateBulkActionsBar();
    updateMasterCheckboxState();
}

function clearAllSelectedAlgos() {
    window._selectedAlgoIds.clear();
    const tbody = document.getElementById("straddleAlgosTableBody");
    if (tbody) {
        const rows = tbody.querySelectorAll("tr[data-strategy-id]");
        rows.forEach(tr => {
            tr.style.background = "";
            const cb = tr.querySelector(".straddle-algo-checkbox");
            if (cb) cb.checked = false;
        });
    }
    updateBulkActionsBar();
    updateMasterCheckboxState();
}

function updateMasterCheckboxState() {
    const masterCb = document.getElementById("straddleSelectAllAlgos");
    if (!masterCb) return;
    const displayed = getCurrentlyDisplayedStrategies();
    if (displayed.length === 0) {
        masterCb.checked = false;
        masterCb.indeterminate = false;
        return;
    }
    const allChecked = displayed.every(s => window._selectedAlgoIds.has(s.strategy_id));
    const someChecked = displayed.some(s => window._selectedAlgoIds.has(s.strategy_id));
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
    showStraddleToast(`${enabled ? 'Enabling' : 'Disabling'} ${ids.length} selected algo(s)...`, "info", 1500);
    try {
        const resp = await fetch("/api/straddle/strategy/batch_toggle_enable", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids, enabled: enabled })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`${ids.length} algo(s) ${enabled ? 'Enabled' : 'Disabled'}`, enabled ? "success" : "info", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}

async function bulkDuplicateSelectedAlgos() {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    showStraddleToast(`📑 Duplicating ${ids.length} algo(s)...`, "info", 1500);
    try {
        const resp = await fetch("/api/straddle/strategy/batch_duplicate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids })
        });
        const res = await resp.json();
        if (res.success) {
            window._selectedAlgoIds.clear();
            showStraddleToast(`✅ ${ids.length} algo(s) duplicated!`, "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}

async function bulkDeleteSelectedAlgos() {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    if (!confirm(`Are you sure you want to delete ${ids.length} selected algorithm(s) permanently?`)) return;
    try {
        const idSet = new Set(ids);
        if (window._straddleState && Array.isArray(window._straddleState.allStrategies)) {
            window._straddleState.allStrategies = window._straddleState.allStrategies.filter(s => !idSet.has(s.strategy_id));
        }
        window._selectedAlgoIds.clear();
        renderStraddleAlgosTable(true);

        const resp = await fetch("/api/straddle/strategy/batch_delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`🗑️ ${ids.length} algo(s) deleted successfully`, "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        } else {
            showStraddleToast("Error deleting: " + (res.error || "Failed"), "error", 3500);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}

async function bulkSetDaysSelectedAlgos(days) {
    const ids = Array.from(window._selectedAlgoIds);
    if (ids.length === 0) return;
    const targetDays = days || ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];

    // Optimistic local update
    if (window._straddleState && Array.isArray(window._straddleState.allStrategies)) {
        window._straddleState.allStrategies.forEach(s => {
            if (window._selectedAlgoIds.has(s.strategy_id)) {
                s.schedule_days = [...targetDays];
            }
        });
    }
    const activeStrat = window._straddleState?.activeStrategy;
    if (activeStrat && window._selectedAlgoIds.has(activeStrat.strategy_id)) {
        activeStrat.schedule_days = [...targetDays];
        document.querySelectorAll(".day-pill-btn").forEach(btn => {
            const btnDay = btn.getAttribute("data-day");
            if (btnDay) {
                btn.classList.toggle("active", targetDays.includes(btnDay));
            }
        });
    }
    renderStraddleAlgosTable(true);
    showStraddleToast(`📅 Updating working days for ${ids.length} algos...`, "info", 1500);

    try {
        const resp = await fetch("/api/straddle/strategy/batch_set_days", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_ids: ids, days: targetDays })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`📅 Working days set to Mon-Fri for ${ids.length} algos`, "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
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
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
    }
}
window.bulkResetSelectedAlgos = bulkResetSelectedAlgos;

function renderStraddleAlgosTable(forceRebuild = false) {
    try {
        const tbody = document.getElementById("straddleAlgosTableBody");
        if (!tbody) return;

        const strategies = window._straddleState.allStrategies || [];
        const searchVal = (document.getElementById("straddleAlgosSearch")?.value || "").toLowerCase().trim();

        const activeUnd = canonicalUnderlying(window._activeGlobalUnderlying || "NIFTY");
        // Quantiply Parity: By default, ALL user strategies across all underlyings are always visible!
        let showAll = window._showAllUnderlyingsAlgos !== false;

        // Filter strategies focused on active underlying
        let filtered = strategies.filter(s => {
            // IF searching, search across all algos globally regardless of active asset filter
            if (searchVal) {
                const name = (s.name || s.strategy_id || "").toLowerCase();
                const status = (s.status || "Ready").toLowerCase();
                const und = canonicalUnderlying(s.underlying || "NIFTY").toLowerCase();
                const dispName = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[und.toUpperCase()]) ? COMMODITY_DISPLAY_NAMES[und.toUpperCase()].toLowerCase() : und;
                const clientId = String(s.client_id || "").toLowerCase();
                const execMode = (s.execution_mode || "Paper").toLowerCase();
                const tradeType = (s.trade_type || "Intraday").toLowerCase();
                const startTime = String(s.start_time || "").toLowerCase();
                const days = Array.isArray(s.schedule_days) ? s.schedule_days.join(" ").toLowerCase() : "";

                return name.includes(searchVal) || 
                       und.includes(searchVal) || 
                       dispName.includes(searchVal) || 
                       status.includes(searchVal) || 
                       clientId.includes(searchVal) || 
                       execMode.includes(searchVal) || 
                       tradeType.includes(searchVal) || 
                       startTime.includes(searchVal) || 
                       days.includes(searchVal);
            }
            if (showAll) return true;

            // CRITICAL SAFETY RULE: Never hide active, running, selected, or trading strategies!
            const isCurrentActive = (s.strategy_id && s.strategy_id === window._straddleState?.activeStrategyId) ||
                                   (s.name && window._straddleState?.activeStrategy?.name === s.name);
            const isRunning = (s.status === "TRADE_ACTIVE" || s.status === "RUNNING");
            const hasActiveTrading = (Array.isArray(s.positions) && s.positions.some(p => p.status === "OPEN" || p.status === "LIVE OPEN" || p.status === "EXIT_PENDING" || p.status === "ERROR")) ||
                                     (Array.isArray(s.closed_positions) && s.closed_positions.length > 0);
            if (isCurrentActive || isRunning || hasActiveTrading) return true;

            const und = canonicalUnderlying(s.underlying || "NIFTY");
            return und === activeUnd;
        });

        // Guaranteed fallback: If filtered has 0 strategies but strategies exist in platform, automatically show all
        if (filtered.length === 0 && strategies.length > 0 && !searchVal) {
            showAll = true;
            window._showAllUnderlyingsAlgos = true;
            filtered = strategies;
        }

        const badgeEl = document.getElementById("algosFocusedUnderlyingBadge");
        const toggleBtn = document.getElementById("btnToggleShowAllAlgos");
        const dispName = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[activeUnd]) ? COMMODITY_DISPLAY_NAMES[activeUnd] : activeUnd;
        const undColor = (typeof COMMODITY_COLOR_MAP !== "undefined" && COMMODITY_COLOR_MAP[activeUnd]) ? COMMODITY_COLOR_MAP[activeUnd] : "#38bdf8";

        if (badgeEl) {
            if (showAll) {
                badgeEl.textContent = `Showing All Assets (${strategies.length})`;
                badgeEl.style.color = "#94a3b8";
                badgeEl.style.background = "rgba(148,163,184,0.12)";
                badgeEl.style.borderColor = "rgba(148,163,184,0.3)";
            } else {
                badgeEl.textContent = `Focused: ${dispName}`;
                badgeEl.style.color = undColor;
                badgeEl.style.background = undColor.startsWith("#") ? `${undColor}1a` : "rgba(56,189,248,0.15)";
                badgeEl.style.borderColor = undColor.startsWith("#") ? `${undColor}55` : "rgba(56,189,248,0.4)";
            }
        }
        if (toggleBtn) {
            toggleBtn.textContent = showAll ? `Filter by ${dispName}` : "Show All Assets";
            toggleBtn.classList.toggle("btn-primary", showAll);
            toggleBtn.classList.toggle("btn-outline", !showAll);
        }

        // Calculate total MTM of filtered
        let totalMtm = 0;
        filtered.forEach(s => { totalMtm += (s.mtm || 0.0); });
        const totMtmEl = document.getElementById("straddleAlgosTotalMtm");
        const totMtmLbl = document.getElementById("lblStraddleAlgosTotalMtm");
        if (totMtmLbl) {
            totMtmLbl.textContent = showAll ? "Total Algos MTM:" : `${dispName} Algos MTM:`;
        }
        if (totMtmEl) {
            if (Math.abs(totalMtm) < 0.005) {
                totMtmEl.textContent = "₹ 0.00";
                totMtmEl.style.color = "#94a3b8";
            } else if (totalMtm > 0) {
                totMtmEl.textContent = "+₹ " + totalMtm.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                totMtmEl.style.color = "#10b981";
            } else {
                totMtmEl.textContent = "-₹ " + Math.abs(totalMtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                totMtmEl.style.color = "#f43f5e";
            }
        }

        updateMasterCheckboxState();
        updateBulkActionsBar();

        if (filtered.length === 0) {
            if (!window._straddleStateLoadDone && (!window._straddleState.allStrategies || window._straddleState.allStrategies.length === 0)) {
                tbody.innerHTML = `
                    <tr>
                        <td colspan="11" style="text-align:center; padding:36px 14px; color:#94a3b8; font-size:13px;">
                            <span class="straddle-spinner" style="display:inline-block; width:18px; height:18px; border:2px solid #38bdf8; border-top-color:transparent; border-radius:50%; animation:spin 0.8s linear infinite; vertical-align:middle; margin-right:8px;"></span>
                            Loading algorithmic strategies...
                        </td>
                    </tr>
                `;
                return;
            }
            const dispName = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[activeUnd]) ? COMMODITY_DISPLAY_NAMES[activeUnd] : activeUnd;
            tbody.innerHTML = `
                <tr>
                    <td colspan="11" style="text-align:center; padding:36px 14px; color:#64748b;">
                        <div style="font-size:14px; font-weight:700; color:#cbd5e1; margin-bottom:4px;">No Algorithmic Strategies Found for ${dispName}</div>
                        <div style="font-size:12px; margin-bottom:12px;">Click <b>+ Add Algo</b> above to instantiate a strategy for ${dispName}, or click <b>Show All Assets</b> to view all algos.</div>
                        <div style="display:inline-flex; gap:8px;">
                            <button type="button" class="straddle-btn btn-primary" onclick="openAddStraddleAlgoModal()" style="font-size:11px; padding:4px 12px;">+ Add Algo</button>
                            <button type="button" class="straddle-btn btn-outline" onclick="toggleShowAllUnderlyingsAlgos()" style="font-size:11px; padding:4px 12px;">Show All Assets</button>
                        </div>
                    </td>
                </tr>
            `;
            return;
        }

        // Check if we can do an in-place update without rebuilding the DOM
        const _availForSig = window._straddleState?.margin?.available_margin ?? window._straddleMargin?.available_margin ?? 0;
        const curTableConfigSig = filtered.map(s => `${s.strategy_id}:${s.name}:${s.underlying}:${s.enabled}:${s.multiplier}:${s.multiplier_type}:${s.cushion_margin}:${s.auto_lots_raw ?? ''}:${Math.floor(Number(_availForSig)/1000)}:${(s.schedule_days||[]).join(',')}:${s.start_time}:${s.end_time}:${(s.legs||[]).length}`).join("||");
        const hasTableConfigChanged = (curTableConfigSig !== window._lastAlgosTableConfigSig);
        const existingRows = tbody.querySelectorAll("tr[data-strategy-id]");
        const activeEl = document.activeElement;
        const isUserInteracting = activeEl && (tbody.contains(activeEl) || activeEl.id === "straddleAlgosSearch");
        const hasOpenDropdown = !!document.querySelector(".straddle-dropdown-menu.show");

        if (!forceRebuild && !hasTableConfigChanged && existingRows.length === filtered.length) {
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
                    const hasOpen = Array.isArray(s.positions) && s.positions.some(p => p.status === "OPEN" || p.status === "LIVE OPEN");
                    const hasCompleted = Array.isArray(s.positions) && s.positions.length > 0 && s.positions.some(p => p.status === "COMPLETE" || p.status === "CLOSED" || p.status === "EXITED");
                    const hasError = (s.positions && Array.isArray(s.positions)) ? s.positions.some(p => p.status === "ERROR") : false;

                    const isAlgoEnabled = s.enabled !== false;
                    let displayStatus = "Ready";
                    let statusClass = "straddle-badge-ready";
                    let dotColor = "#38bdf8";

                    if (!isAlgoEnabled || rawStatus === "STOPPED" || (!hasOpen && (hasCompleted || s.has_traded_today) && !s._manual_reset)) {
                        displayStatus = "Stopped";
                        statusClass = "straddle-badge-terminated";
                        dotColor = "#64748b";
                    } else if (rawStatus === "TERMINATED") {
                        displayStatus = "Terminated";
                        statusClass = "straddle-badge-terminated";
                        dotColor = "#ef4444";
                    } else if (rawStatus === "ENTRY_FAILED" || (!hasOpen && hasError)) {
                        displayStatus = "Entry Failed";
                        statusClass = "straddle-badge-terminated";
                        dotColor = "#f43f5e";
                    } else if (hasOpen) {
                        displayStatus = "Trade Active";
                        statusClass = "straddle-badge-active";
                        dotColor = "#10b981";
                    } else if (rawStatus === "TRADE_ACTIVE" || rawStatus === "WAITING_ENTRY") {
                        displayStatus = "Waiting Entry";
                        statusClass = "straddle-badge-ready";
                        dotColor = "#38bdf8";
                    } else {
                        displayStatus = "Ready";
                        statusClass = "straddle-badge-ready";
                        dotColor = "#38bdf8";
                    }

                    const switchInput = document.querySelector(`input[data-enable-sid="${sid}"]`);
                    if (switchInput && switchInput !== document.activeElement) {
                        switchInput.checked = isAlgoEnabled;
                    }

                    const advSwitchInput = document.querySelector(`input[data-adv-sl-sid="${sid}"]`);
                    if (advSwitchInput && advSwitchInput !== document.activeElement) {
                        const adv = s.advanced || s.advanced_settings || {};
                        const isAdv = (adv.sl_order_placement === "Place in advance");
                        advSwitchInput.checked = isAdv;
                        const slider = advSwitchInput.nextElementSibling;
                        if (slider) slider.style.background = isAdv ? "#10b981" : "";
                    }

                    const multInput = document.getElementById(`algo_mult_input_${sid}`);
                    if (multInput && multInput !== document.activeElement) {
                        multInput.value = s.multiplier || 1;
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

                    const undPill = document.getElementById(`algo_und_pill_${sid}`);
                    if (undPill) {
                        undPill.textContent = canonicalUnderlying(s.underlying || "NIFTY");
                    }

                    const errCell = document.getElementById(`algo_entry_err_${sid}`);
                    const err = s.entry_error ? String(s.entry_error) : "-";
                    if (errCell) {
                        errCell.textContent = err;
                        errCell.style.color = (err !== "-" && !err.toLowerCase().includes("waiting")) ? "#f43f5e" : "#64748b";
                    }

                    const errBadgeEl = document.getElementById(`algo_err_badge_${sid}`);
                    const errCount = (s.positions && Array.isArray(s.positions)) ? s.positions.filter(p => p.status === "ERROR").length : ((err !== "-" && !err.toLowerCase().includes("waiting")) ? 1 : 0);
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
            const hasOpen = Array.isArray(s.positions) && s.positions.some(p => p.status === "OPEN" || p.status === "LIVE OPEN");
            const hasCompleted = Array.isArray(s.positions) && s.positions.length > 0 && s.positions.some(p => p.status === "COMPLETE" || p.status === "CLOSED" || p.status === "EXITED");
            const hasError = (s.positions && Array.isArray(s.positions)) ? s.positions.filter(p => p.status === "ERROR") : false;
            const errCount = (s.positions && Array.isArray(s.positions)) ? s.positions.filter(p => p.status === "ERROR").length : ((s.entry_error && s.entry_error !== "-" && !s.entry_error.toLowerCase().includes("waiting")) ? 1 : 0);

            const isAlgoEnabled = s.enabled !== false;
            let displayStatus = "Ready";
            let statusClass = "straddle-badge-ready";
            let dotColor = "#38bdf8";

            if (!isAlgoEnabled || rawStatus === "STOPPED" || (!hasOpen && (hasCompleted || s.has_traded_today) && !s._manual_reset)) {
                displayStatus = "Stopped";
                statusClass = "straddle-badge-terminated";
                dotColor = "#64748b";
            } else if (rawStatus === "TERMINATED") {
                displayStatus = "Terminated";
                statusClass = "straddle-badge-terminated";
                dotColor = "#ef4444";
            } else if (rawStatus === "ENTRY_FAILED" || (!hasOpen && hasError)) {
                displayStatus = "Entry Failed";
                statusClass = "straddle-badge-terminated";
                dotColor = "#f43f5e";
            } else if (hasOpen) {
                displayStatus = "Trade Active";
                statusClass = "straddle-badge-active";
                dotColor = "#10b981";
            } else if (rawStatus === "TRADE_ACTIVE" || rawStatus === "WAITING_ENTRY") {
                displayStatus = "Waiting Entry";
                statusClass = "straddle-badge-ready";
                dotColor = "#38bdf8";
            } else {
                displayStatus = "Ready";
                statusClass = "straddle-badge-ready";
                dotColor = "#38bdf8";
            }

            const multType = (s.multiplier_type || "MANUAL").toUpperCase();
            const isManual = (multType !== "AUTO");
            const multiplier = Math.max(1, parseInt(s.multiplier, 10) || 1);
            const cushionMargin = (s.cushion_margin !== undefined && s.cushion_margin !== null) ? Number(s.cushion_margin) : 15000;
            const brokerMargin = Number(s.broker_margin_per_lot) || 130750.0;
            const totalMarginPerLot = brokerMargin + cushionMargin;
            let liveAvailCap = 0.0;
            if (window._straddleState?.margin?.available_margin !== undefined && window._straddleState?.margin?.available_margin !== null) {
                liveAvailCap = parseFloat(window._straddleState.margin.available_margin);
            } else if (window._straddleMargin?.available_margin !== undefined && window._straddleMargin?.available_margin !== null) {
                liveAvailCap = parseFloat(window._straddleMargin.available_margin);
            } else if (window._straddleState?.margin?.available_cash !== undefined && window._straddleState?.margin?.available_cash !== null) {
                liveAvailCap = parseFloat(window._straddleState.margin.available_cash);
            }
            const effMult = isManual
                ? multiplier
                : (liveAvailCap > 0
                    ? Math.floor(liveAvailCap / Math.max(1, totalMarginPerLot))
                    : (s.auto_lots_raw !== undefined ? Number(s.auto_lots_raw) : 0));

            const mtm = s.mtm || 0.0;
            const mtmClass = mtm >= 0 ? "color:#10b981;" : "color:#f43f5e;";
            const mtmFormatted = (mtm >= 0 ? "+₹ " : "-₹ ") + Math.abs(mtm).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            const name = s.name || s.strategy_id || "";
            const startTime = s.start_time || "09:15:56";
            const endTime = s.end_time || "15:29:50";
            const schedDays = s.schedule_days || ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
            const entryErr = s.entry_error ? String(s.entry_error) : "-";
            const adv = s.advanced || s.advanced_settings || {};
            const isAdvSl = (adv.sl_order_placement === "Place in advance");
            const isErrActive = (entryErr !== "-" && !entryErr.toLowerCase().includes("waiting"));

            const isWaitingPrem = entryErr.toLowerCase().includes("waiting for premium");
            const entryErrColor = isErrActive ? '#f43f5e' : (isWaitingPrem ? '#38bdf8' : '#64748b');
            const entryErrWeight = (isErrActive || isWaitingPrem) ? '700' : '400';

            const safeJsSid = (s.strategy_id || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
            const safeJsName = (name || '').replace(/\\/g, '\\\\').replace(/'/g, "\\'");
            const safeDispName = escapeHtml(name);

            // Build interactive day selector badges
            let daysHtml = `<div style="display:inline-flex; gap:3px; flex-wrap:wrap;">`;
            dayLabels.forEach(d => {
                const isDayActive = schedDays.includes(d.key);
                const badgeBg = isDayActive ? "#2563eb" : "#1e293b";
                const badgeColor = isDayActive ? "#ffffff" : "#64748b";
                const border = isDayActive ? "1px solid #3b82f6" : "1px solid rgba(255,255,255,0.06)";
                daysHtml += `
                <button type="button" onclick="toggleAlgoDay('${safeJsSid}', '${d.key}')" style="background:${badgeBg}; color:${badgeColor}; border:${border}; border-radius:4px; padding:1px 5px; font-size:10px; font-weight:700; cursor:pointer;" title="Click to Toggle ${d.key}">
                    ${d.short}
                </button>
            `;
            });
            daysHtml += `</div>`;

            rowsHtml += `
            <tr data-strategy-id="${s.strategy_id}" style="${isSelected ? 'background:rgba(59,130,246,0.08);' : ''}">
                <td class="algo-col-select" style="text-align:center;">
                    <input type="checkbox" class="straddle-algo-checkbox" data-id="${s.strategy_id}" ${isSelected ? "checked" : ""} onchange="onToggleAlgoSelect('${safeJsSid}', this.checked)">
                </td>
                <td class="algo-col-enable">
                    <label class="quantiply-switch" title="Toggle Auto Execution">
                        <input type="checkbox" data-enable-sid="${s.strategy_id}" ${isAlgoEnabled ? "checked" : ""} onchange="onToggleAlgoEnable('${safeJsSid}', this.checked)">
                        <span class="quantiply-slider"></span>
                    </label>
                </td>
                <td class="algo-col-adv-sl" style="text-align:center;">
                    <label class="quantiply-switch" title="Place Stoploss order directly on Exchange matching engine (<1ms fill)">
                        <input type="checkbox" data-adv-sl-sid="${s.strategy_id}" ${isAdvSl ? "checked" : ""} onchange="toggleAlgoAdvanceSl('${safeJsSid}', this.checked)">
                        <span class="quantiply-slider" style="${isAdvSl ? 'background:#10b981;' : ''}"></span>
                    </label>
                </td>
                <td class="algo-col-status">
                    <span id="algo_status_pill_${s.strategy_id}" class="straddle-pill-badge ${statusClass}" style="font-size:10.5px; padding:2px 8px;">${displayStatus}</span>
                </td>
                <td class="algo-col-days">
                    ${daysHtml}
                </td>
                <td class="algo-col-mtm" id="algo_mtm_${s.strategy_id}" style="text-align:right; font-family:var(--font-mono); font-weight:800; ${mtmClass}">
                    ${mtmFormatted}
                </td>
                <td class="algo-col-name">
                    <div style="display:flex; align-items:center; gap:6px;">
                        <span id="algo_err_badge_${s.strategy_id}" style="display:${errCount > 0 ? 'inline-flex' : 'none'}; align-items:center; gap:4px; color:#f87171; cursor:pointer;" title="Click to inspect and retry error" onclick="openStrategyInTerminal('${safeJsSid}')">
                            <span style="font-size:12px;">⚠️</span><span class="algo-error-counter">${errCount}</span>
                        </span>
                        <span id="algo_dot_${s.strategy_id}" style="color:${dotColor}; font-size:8px;">●</span>
                        <span id="algo_und_pill_${s.strategy_id}" class="algo-underlying-pill" style="font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px; text-transform:uppercase; letter-spacing:0.3px; background:rgba(56,189,248,0.15); color:#38bdf8; border:1px solid rgba(56,189,248,0.35);">${canonicalUnderlying(s.underlying || 'NIFTY')}</span>
                        <a href="javascript:void(0)" onclick="openStrategyInTerminal('${safeJsSid}')" style="color:#60a5fa; font-weight:700; font-size:13px; text-decoration:none; cursor:pointer;" title="Click to open Builder & Live Legs" onmouseover="this.style.textDecoration='underline'" onmouseout="this.style.textDecoration='none'">${safeDispName}</a>
                        ${s.mtm_calc_mode === 'Per Lot' ? `<span style="font-size:9px; background:rgba(56,189,248,0.12); color:#38bdf8; border:1px solid rgba(56,189,248,0.3); border-radius:3px; padding:1px 4px; font-weight:800;" title="MTM Per Lot Mode Active">Per Lot</span>` : ''}
                        <div class="straddle-dropdown" style="display:inline-block; margin-left:4px;">
                            <button type="button" class="algo-btn-menu" title="Strategy Options" onclick="toggleDropdownMenu('dd_${s.strategy_id}', event)" style="padding:1px 6px; font-size:13px; line-height:1; height:20px; border-radius:4px; background:rgba(255,255,255,0.06); border:1px solid rgba(255,255,255,0.12); color:#cbd5e1; cursor:pointer;">⋮</button>
                            <div id="dd_${s.strategy_id}" class="straddle-dropdown-menu">
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="openRenameStraddleAlgoModal('${safeJsSid}', '${safeJsName}')">✏️ Rename Strategy</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="duplicateStraddleAlgo('${safeJsSid}')">📑 Duplicate Preset</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="openStrategyInTerminal('${safeJsSid}')">⚡ Builder & Live Legs</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="straddleResetStrategyDirect('${safeJsSid}')">🔄 Reset Strategy (Clean Start)</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item" onclick="restartOrRetryStraddleAlgo('${safeJsSid}')">🔄 Retry Failed Orders</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item danger" onclick="straddleSquareOffStrategyDirect('${safeJsSid}')">🚨 Square Off & Terminate</a>
                                <a href="javascript:void(0)" class="straddle-dropdown-item danger" onclick="deleteStraddleAlgo('${safeJsSid}')">🗑️ Delete Strategy</a>
                            </div>
                        </div>
                    </div>
                </td>
                <td class="algo-col-entry-err" id="algo_entry_err_${s.strategy_id}" style="color:${entryErrColor}; font-size:11px; font-weight:${entryErrWeight}; max-width:220px; min-width:120px; white-space:normal; word-break:break-word; line-height:1.3;" title="${escapeHtml(entryErr)}">${escapeHtml(entryErr)}</td>
                <td class="algo-col-exit-err" style="color:#64748b; font-size:11px; max-width:220px; min-width:120px; white-space:normal; word-break:break-word; line-height:1.3;">${escapeHtml(String(s.exit_error || '-'))}</td>
                <td class="algo-col-start" style="font-family:var(--font-mono); font-size:11px; color:#cbd5e1; white-space:nowrap;">${startTime}</td>
                <td class="algo-col-end" style="font-family:var(--font-mono); font-size:11px; color:#cbd5e1; white-space:nowrap;">
                    <div>${endTime}</div>
                    ${(s.no_reentry_after && s.no_reentry_after.enabled && s.no_reentry_after.time) ? `<div style="font-size:9.5px; color:#f59e0b; font-weight:700;" title="No new entry or re-execution after this time">Cutoff: ${s.no_reentry_after.time}</div>` : ''}
                </td>
            </tr>
        `;
        });

        tbody.innerHTML = rowsHtml;
        window._lastAlgosTableConfigSig = curTableConfigSig;
    } catch (err) {
        console.error("renderStraddleAlgosTable error:", err);
    }
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
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        } catch (e) {
            showStraddleToast("Error: " + e.message, "error", 3000);
        }
    } else {
        await startStraddleAlgoDirect(strategyId);
    }
}

async function toggleAlgoDay(strategyId, day) {
    const sIdStr = String(strategyId).trim();
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    if (!strat) return;

    let days = strat.schedule_days ? [...strat.schedule_days] : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    if (days.includes(day)) {
        days = days.filter(d => d !== day);
    } else {
        days.push(day);
    }
    strat.schedule_days = days;
    renderStraddleAlgosTable(true);

    // CRITICAL TWO-WAY SYNC: If active strategy in builder is this strategy, sync builder pills immediately!
    const activeStrat = window._straddleState?.activeStrategy;
    if (activeStrat && (String(activeStrat.strategy_id || "").trim() === sIdStr || String(activeStrat.name || "").trim() === sIdStr)) {
        activeStrat.schedule_days = [...days];
        document.querySelectorAll(".day-pill-btn").forEach(btn => {
            const btnDay = btn.getAttribute("data-day");
            if (btnDay) {
                btn.classList.toggle("active", days.includes(btnDay));
            }
        });
    }

    try {
        await fetch("/api/straddle/strategy/set_days", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strat.strategy_id || strategyId, days: days })
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

async function toggleAlgoAdvanceSl(strategyId, isChecked) {
    const sIdStr = String(strategyId).trim();
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    const val = isChecked ? "Place in advance" : "Keep with self";
    if (strat) {
        if (!strat.advanced) strat.advanced = {};
        strat.advanced.sl_order_placement = val;
        if (strat.advanced_settings) strat.advanced_settings.sl_order_placement = val;
        if (Array.isArray(strat.legs)) {
            strat.legs.forEach(leg => { leg.sl_order_placement = val; });
        }
        if (Array.isArray(strat.positions)) {
            strat.positions.forEach(pos => { pos.sl_order_placement = val; });
        }
        renderStraddleAlgosTable(false);
    }

    // Two-way sync: If this strategy is open in the builder, update all builder fields immediately!
    const activeStrat = window._straddleState?.activeStrategy;
    if (activeStrat && (String(activeStrat.strategy_id || "").trim() === sIdStr || String(activeStrat.name || "").trim() === sIdStr)) {
        if (!activeStrat.advanced) activeStrat.advanced = {};
        activeStrat.advanced.sl_order_placement = val;
        if (activeStrat.advanced_settings) activeStrat.advanced_settings.sl_order_placement = val;
        const slOrderEl = document.getElementById("cfgSlOrderPlacement");
        if (slOrderEl) slOrderEl.value = val;
        document.querySelectorAll(".leg-sl-placement").forEach(el => {
            el.value = val;
            el.style.color = (val === "Place in advance" ? "#10b981" : "#94a3b8");
        });
    }

    const name = strat ? strat.name : strategyId;
    showStraddleToast(
        isChecked ? `Strategy '${name}': Advance Exchange SL Activated (0ms Fill)` : `Strategy '${name}': Software SL Activated`,
        isChecked ? "success" : "info",
        2000
    );

    try {
        const resp = await fetch("/api/straddle/strategy/toggle_advance_sl", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strat ? strat.strategy_id : strategyId, enabled: isChecked })
        });
        const res = await resp.json();
        if (res.success) {
            await fetchStraddleState();
            renderStraddleAlgosTable(false);
        } else {
            showStraddleToast("Advance SL update failed: " + (res.error || "Unknown error"), "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Advance SL error: " + e.message, "error", 3000);
    }
}
window.toggleAlgoAdvanceSl = toggleAlgoAdvanceSl;

function toggleShowAllUnderlyingsAlgos() {
    if (window._showAllUnderlyingsAlgos === undefined) window._showAllUnderlyingsAlgos = true;
    window._showAllUnderlyingsAlgos = !window._showAllUnderlyingsAlgos;
    const btn = document.getElementById("btnToggleShowAllAlgos");
    if (btn) {
        const activeUnd = canonicalUnderlying(window._activeGlobalUnderlying || "NIFTY");
        const dispName = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[activeUnd]) ? COMMODITY_DISPLAY_NAMES[activeUnd] : activeUnd;
        btn.textContent = window._showAllUnderlyingsAlgos ? `Filter by ${dispName}` : "Show All Assets";
        btn.classList.toggle("btn-primary", window._showAllUnderlyingsAlgos);
        btn.classList.toggle("btn-outline", !window._showAllUnderlyingsAlgos);
    }
    renderStraddleAlgosTable(true);
}
window.toggleShowAllUnderlyingsAlgos = toggleShowAllUnderlyingsAlgos;

async function onToggleAlgoEnable(strategyId, isChecked) {
    const sIdStr = String(strategyId).trim();
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    if (strat) {
        strat.enabled = isChecked;
        if (isChecked) {
            strat.status = "READY";
            strat.entry_error = "-";
        } else {
            strat.status = "STOPPED";
        }
        renderStraddleAlgosTable(true);
        if (String(window._straddleState.activeStrategyId || "").trim() === sIdStr ||
            String(window._straddleState.activeStrategy?.strategy_id || "").trim() === sIdStr) {
            if (window._straddleState.activeStrategy) {
                window._straddleState.activeStrategy.enabled = isChecked;
                window._straddleState.activeStrategy.status = strat.status;
            }
            renderStraddleHeader(window._straddleState);
        }
    }
    showStraddleToast(`Strategy '${strat ? strat.name : strategyId}' ${isChecked ? 'Enabled' : 'Disabled'}`, isChecked ? "success" : "info", 1800);

    try {
        const resp = await fetch("/api/straddle/strategy/toggle_enable", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strat ? strat.strategy_id : strategyId, enabled: isChecked })
        });
        const res = await resp.json();
        if (res.success) {
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
            renderStraddleHeader(window._straddleState);
        }
    } catch (e) {
        console.error("Error toggling algo enable:", e);
    }
}

function stepAlgoMultiplier(strategyId, delta) {
    const sIdStr = String(strategyId).trim();
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    const curr = strat ? (strat.multiplier || 1) : 1;
    const next = Math.max(1, Math.min(100, curr + delta));
    onChangeAlgoMultiplier(strategyId, next);
}
window.stepAlgoMultiplier = stepAlgoMultiplier;

async function setAlgoMultiplierMode(strategyId, mode) {
    const sIdStr = String(strategyId).trim();
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    if (!strat) return;

    window._pendingMultiplierModes = window._pendingMultiplierModes || {};
    window._pendingMultiplierModes[sIdStr] = { mode: mode, timestamp: Date.now() };

    strat.multiplier_type = mode;
    showStraddleToast(`'${strat.name}' switched to ${mode} Multiplier`, "info", 1500);

    // Sync to strategy builder form if this strategy is active
    if (String(window._straddleState?.activeStrategyId || "").trim() === sIdStr || String(window._straddleState?.activeStrategy?.strategy_id || "").trim() === sIdStr) {
        setFormMultiplierMode(mode, false);
        if (window._straddleState?.activeStrategy) {
            window._straddleState.activeStrategy.multiplier_type = mode;
        }
    }

    renderStraddleAlgosTable(true);

    try {
        const resp = await fetch("/api/straddle/strategy/update_multiplier", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                strategy_id: strat.strategy_id || strategyId,
                multiplier_type: mode
            })
        });
        const res = await resp.json();
        if (res.success) {
            if (window._pendingMultiplierModes) delete window._pendingMultiplierModes[sIdStr];
            const currentStrat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
            if (currentStrat && res.strategy) {
                Object.assign(currentStrat, res.strategy);
                currentStrat.multiplier_type = mode;
            }
            renderStraddleAlgosTable(true);
        }
    } catch (e) {
        console.error("Error setting multiplier mode:", e);
    }
}
window.setAlgoMultiplierMode = setAlgoMultiplierMode;

const _cushionSyncTimers = {};
function onChangeAlgoCushionMargin(strategyId, val, immediate = false) {
    const sIdStr = String(strategyId).trim();
    const parsed = parseFloat(val);
    const cushion = (!isNaN(parsed) && parsed >= 0) ? parsed : 0;
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    if (!strat) return;
    strat.cushion_margin = cushion;

    // Sync to strategy builder form if active
    if (String(window._straddleState?.activeStrategyId || "").trim() === sIdStr || String(window._straddleState?.activeStrategy?.strategy_id || "").trim() === sIdStr) {
        const formInput = document.getElementById("cfgCushionMargin");
        if (formInput && parseFloat(formInput.value) !== cushion) formInput.value = cushion;
        if (window._straddleState?.activeStrategy) {
            window._straddleState.activeStrategy.cushion_margin = cushion;
        }
        updateFormAutoMultiplierPreview();
        if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
            scheduleAutoMultiplierLiveRecalc();
        }
    }

    renderStraddleAlgosTable(true);

    clearTimeout(_cushionSyncTimers[sIdStr]);
    _cushionSyncTimers[sIdStr] = setTimeout(async () => {
        try {
            const resp = await fetch("/api/straddle/strategy/update_multiplier", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    strategy_id: strat.strategy_id || strategyId,
                    cushion_margin: cushion
                })
            });
            const res = await resp.json();
            if (res.success) {
                const currentStrat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
                if (currentStrat && res.strategy) {
                    Object.assign(currentStrat, res.strategy);
                }
                renderStraddleAlgosTable(true);
            }
        } catch (e) {
            console.error("Error updating cushion margin:", e);
        }
    }, immediate ? 0 : 350);
}
window.onChangeAlgoCushionMargin = onChangeAlgoCushionMargin;

async function onChangeAlgoMultiplier(strategyId, val) {
    const sIdStr = String(strategyId).trim();
    const num = Math.max(1, parseInt(val) || 1);
    const strat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
    if (strat) {
        window._pendingMultiplierModes = window._pendingMultiplierModes || {};
        window._pendingMultiplierModes[sIdStr] = { mode: "MANUAL", timestamp: Date.now() };

        strat.multiplier = num;
        strat.multiplier_type = "MANUAL";

        if (String(window._straddleState?.activeStrategyId || "").trim() === sIdStr || String(window._straddleState?.activeStrategy?.strategy_id || "").trim() === sIdStr) {
            const formInput = document.getElementById("cfgMultiplier");
            if (formInput && parseInt(formInput.value) !== num) formInput.value = num;
            setFormMultiplierMode("MANUAL", false);
            if (window._straddleState?.activeStrategy) {
                window._straddleState.activeStrategy.multiplier = num;
                window._straddleState.activeStrategy.multiplier_type = "MANUAL";
            }
        }

        showStraddleToast(`Multiplier set to ${num}x for '${strat.name}'`, "info", 1500);
        renderStraddleAlgosTable(true);

        try {
            const resp = await fetch("/api/straddle/strategy/update_multiplier", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    strategy_id: strat.strategy_id || strategyId,
                    multiplier: num,
                    multiplier_type: "MANUAL"
                })
            });
            const res = await resp.json();
            if (res.success) {
                if (window._pendingMultiplierModes) delete window._pendingMultiplierModes[sIdStr];
                const currentStrat = (window._straddleState?.allStrategies || []).find(s => String(s.strategy_id || "").trim() === sIdStr || String(s.name || "").trim() === sIdStr);
                if (currentStrat && res.strategy) {
                    Object.assign(currentStrat, res.strategy);
                    currentStrat.multiplier = num;
                    currentStrat.multiplier_type = "MANUAL";
                }
                renderStraddleAlgosTable(true);
            }
        } catch (e) {
            console.error("Error updating multiplier:", e);
        }
    }
}
window.onChangeAlgoMultiplier = onChangeAlgoMultiplier;

function openStrategyInTerminal(strategyId, fallbackStrat = null) {
    let strat = (window._straddleState.allStrategies || []).find(s => 
        (s.strategy_id && String(s.strategy_id).trim().toLowerCase() === String(strategyId).trim().toLowerCase()) ||
        (s.name && String(s.name).trim().toLowerCase() === String(strategyId).trim().toLowerCase())
    );
    if (!strat && fallbackStrat) {
        strat = fallbackStrat;
        if (!window._straddleState.allStrategies) window._straddleState.allStrategies = [];
        const exIdx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === strat.strategy_id || s.name === strat.name);
        if (exIdx >= 0) {
            window._straddleState.allStrategies[exIdx] = strat;
        } else {
            window._straddleState.allStrategies.push(strat);
        }
    }
    if (strat) {
        window._straddleState.activeStrategy = strat;
        window._straddleState.activeStrategyId = strat.strategy_id;
        const undBadge = document.getElementById("straddleTerminalUnderlyingBadge");
        if (undBadge) {
            undBadge.textContent = canonicalUnderlying(strat.underlying || "NIFTY");
        }
        populateStraddleSettingsForm(strat);
        renderStraddlePositions(strat.positions || [], strat.group_mtm || 0.0, strat.mtm || 0.0);
    }

    // Switch view to terminal / algo detail
    switchStraddleSubView("terminal");

    // Asynchronously notify backend of active strategy
    onStraddleStrategySelect(strategyId);

    // Smooth scroll to top of page
    window.scrollTo({ top: 0, behavior: "smooth" });
}

async function saveStraddleStrategyAndGoBack() {
    await saveStraddleStrategySettings();
    switchStraddleSubView("algos");
    renderStraddleAlgosTable(true);
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
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
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
            s.closed_positions = [];
            s.shadow_benchmark = [];
            s.mtm = 0.0;
            s.max_mtm = 0.0;
            s.min_mtm = 0.0;
            s.status = "READY";
            s.enabled = false;
            s.entry_error = "-";
            s.exit_error = "-";
            s.last_error = null;
            s.repair_needed = false;
            s.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
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
                    l.re_execute_count = 0;
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
            window._straddleState.activeStrategy.closed_positions = [];
            window._straddleState.activeStrategy.shadow_benchmark = [];
            window._straddleState.activeStrategy.status = "READY";
            window._straddleState.activeStrategy.entry_error = "-";
            window._straddleState.activeStrategy.exit_error = "-";
            window._straddleState.activeStrategy.last_error = null;
            window._straddleState.activeStrategy.repair_needed = false;
            window._straddleState.activeStrategy.mtm = 0.0;
            window._straddleState.activeStrategy.enabled = false;
            window._straddleState.activeStrategy.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
        }
        window._straddleState.positions = [];
        window._straddleState.closed_positions = [];
        window._straddleState.closedPositionsCache = [];
        window._straddleState.status = "READY";
        window._straddleState.entry_error = "-";
        window._straddleState.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
        const grpMtmEl = document.getElementById("straddleGroupMtmDisplay");
        const totMtmEl = document.getElementById("straddleTotalMtmDisplay");
        if (grpMtmEl) { grpMtmEl.textContent = "0.00"; grpMtmEl.style.color = "#38bdf8"; }
        if (totMtmEl) { totMtmEl.textContent = "0.00"; totMtmEl.style.color = "#10b981"; }
        const elCeSl = document.getElementById("reexecCountCeSl");
        const elCeTgt = document.getElementById("reexecCountCeTgt");
        const elPeSl = document.getElementById("reexecCountPeSl");
        const elPeTgt = document.getElementById("reexecCountPeTgt");
        if (elCeSl) elCeSl.textContent = "0";
        if (elCeTgt) elCeTgt.textContent = "0";
        if (elPeSl) elPeSl.textContent = "0";
        if (elPeTgt) elPeTgt.textContent = "0";
        updateStraddleReexecBadges([]);
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
                    window._straddleState.closed_positions = updated.closed_positions || [];
                }
                populateStraddleSettingsForm(updated);
            }
            window._straddleState.closed_positions = [];
            window._straddleState.closedPositionsCache = [];
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
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        } else {
            showStraddleToast("Error duplicating: " + (res.error || "Failed"), "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3000);
    }
}

async function deleteStraddleAlgo(strategyId) {
    if (!confirm("Are you sure you want to delete this strategy algorithm permanently?")) return;
    try {
        if (window._straddleState && Array.isArray(window._straddleState.allStrategies)) {
            window._straddleState.allStrategies = window._straddleState.allStrategies.filter(s => s.strategy_id !== strategyId);
        }
        window._selectedAlgoIds.delete(strategyId);
        renderStraddleAlgosTable(true);

        const resp = await fetch("/api/straddle/strategy/delete", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strategyId })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("Strategy deleted successfully", "success", 2000);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        } else {
            showStraddleToast("Error deleting: " + (res.error || "Failed"), "error", 3500);
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
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
    const undSelect = document.getElementById("newAlgoUnderlying");
    if (undSelect) {
        const activeUnd = canonicalUnderlying(window._activeGlobalUnderlying || "NIFTY");
        if (Array.from(undSelect.options).some(o => o.value === activeUnd)) {
            undSelect.value = activeUnd;
        }
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

    const abortCtrl = new AbortController();
    const timeoutId = setTimeout(() => abortCtrl.abort(), 8000);

    try {
        const resp = await fetch("/api/straddle/strategy/create_preset", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            signal: abortCtrl.signal,
            body: JSON.stringify({
                name: name,
                preset_type: preset,
                underlying: underlying,
                days: days.length > 0 ? days : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]
            })
        });
        clearTimeout(timeoutId);

        if (!resp.ok) {
            const errTxt = await resp.text();
            throw new Error(`Server returned status ${resp.status}`);
        }
        const res = await resp.json();
        if (res.success && res.strategy) {
            closeAddStraddleAlgoModal();
            if (!window._straddleState) window._straddleState = {};
            if (!window._straddleState.allStrategies) window._straddleState.allStrategies = [];
            const exIdx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === res.strategy.strategy_id || s.name === res.strategy.name);
            if (exIdx >= 0) {
                window._straddleState.allStrategies[exIdx] = res.strategy;
            } else {
                window._straddleState.allStrategies.push(res.strategy);
            }
            window._straddleState.activeStrategy = res.strategy;
            window._straddleState.activeStrategyId = res.strategy.strategy_id;

            if (typeof openStrategyInTerminal === "function") {
                openStrategyInTerminal(res.strategy.strategy_id, res.strategy);
            }
            if (typeof toggleStraddleSettingsPanel === "function") {
                toggleStraddleSettingsPanel(true);
            }
            renderStraddleAlgosTable(true);
            showStraddleToast(`Strategy "${res.strategy.name}" created successfully!`, "success", 3000);
            try {
                await fetchStraddleState();
            } catch (e) {
                console.warn("fetchStraddleState post-create warning:", e);
            }
        } else {
            showStraddleToast("Error creating algo: " + (res.error || "Failed"), "error", 4000);
        }
    } catch (e) {
        clearTimeout(timeoutId);
        console.error("Error creating algo:", e);
        if (e.name === "AbortError") {
            showStraddleToast("Strategy creation took too long. Please refresh to see newly added algo.", "warning", 5000);
            closeAddStraddleAlgoModal();
            fetchStraddleState();
        } else {
            showStraddleToast("Error creating algo: " + e.message, "error", 4000);
        }
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
            showStraddleToast(`Strategy renamed to '${newName}'`, "success", 2000);

            // Sync with active strategy if currently open
            if (window._straddleState?.activeStrategy && window._straddleState.activeStrategy.strategy_id === strategyId) {
                window._straddleState.activeStrategy.name = newName;
                const cfgName = document.getElementById("cfgStrategyName");
                if (cfgName) cfgName.value = newName;
                const titleEl = document.getElementById("straddleTerminalStrategyTitle");
                if (titleEl) titleEl.textContent = newName;
                const opt = document.querySelector(`#straddleStrategySelect option[value="${strategyId}"]`);
                if (opt) opt.textContent = newName;
            }

            await fetchStraddleState();
            renderStraddleAlgosTable(true);
        } else {
            showStraddleToast("Error renaming: " + (res.error || "Failed"), "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3500);
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
        drawPnlCanvas(window._straddleState.pnlDataCache, type, null);
    }
}

function renderPnlEntitiesList() {
    const list = document.getElementById("pnlEntitiesList");
    if (!list) return;

    const searchVal = (document.getElementById("pnlSearchEntities")?.value || "").toLowerCase().trim();

    // Compute live portfolio total MTM
    let portfolioMtm = 0.0;
    const strats = window._straddleState.allStrategies || [];
    if (window._straddleState.pnlDataCache && typeof window._straddleState.pnlDataCache.total_pnl === "number") {
        portfolioMtm = window._straddleState.pnlDataCache.total_pnl;
    } else {
        portfolioMtm = strats.reduce((acc, s) => acc + (Number(s.mtm) || 0.0), 0.0);
    }

    let items = [];
    // Always pin Overall Portfolio Total P&L
    items.push({
        id: "OVERALL",
        name: "Overall Total P&L (All Positions)",
        is_overall: true,
        status: "LIVE_STREAM",
        mtm: portfolioMtm
    });

    strats.forEach(s => {
        const isEnabled = s.enabled !== false;
        const status = s.status || (isEnabled ? "Trade Active" : "Stopped");
        items.push({
            id: s.strategy_id,
            name: s.name || s.strategy_id,
            is_overall: false,
            status: status,
            enabled: isEnabled,
            mtm: Number(s.mtm) || 0.0
        });
    });

    if (searchVal) {
        items = items.filter(it => it.name.toLowerCase().includes(searchVal) || (it.id && it.id.toLowerCase().includes(searchVal)));
    }

    let html = "";
    items.forEach((it) => {
        const isSelected = (it.id === window._straddleState.pnlSelectedEntityId) || (!window._straddleState.pnlSelectedEntityId && it.id === "OVERALL");
        if (isSelected) window._straddleState.pnlSelectedEntityId = it.id;

        const isOverall = it.is_overall;
        let badgeHtml = "";
        if (isOverall) {
            badgeHtml = `<span style="background:rgba(59,130,246,0.25); color:#60a5fa; font-size:9.5px; padding:1px 6px; border-radius:4px; font-weight:800;">PORTFOLIO</span>`;
        } else {
            const isAct = (it.status === "TRADE_ACTIVE" || it.status === "Trade Active" || it.enabled);
            const bg = isAct ? "rgba(16,185,129,0.2)" : "rgba(244,63,94,0.15)";
            const col = isAct ? "#10b981" : "#94a3b8";
            badgeHtml = `<span style="background:${bg}; color:${col}; font-size:9px; padding:1px 5px; border-radius:4px; font-weight:700;">${isAct ? '● ACTIVE' : 'STOPPED'}</span>`;
        }

        const mtmVal = it.mtm || 0.0;
        const mtmColor = mtmVal >= 0 ? "#10b981" : "#f43f5e";
        const mtmFormatted = (mtmVal >= 0 ? "+₹" : "-₹") + Math.abs(mtmVal).toLocaleString("en-IN", { minimumFractionDigits: 1, maximumFractionDigits: 1 });

        html += `
            <div style="display:flex; align-items:center; justify-content:space-between; padding:8px 10px; border-radius:6px; cursor:pointer; background:${isSelected ? 'rgba(37,99,235,0.22)' : 'rgba(255,255,255,0.02)'}; border:${isSelected ? '1px solid #3b82f6' : '1px solid rgba(255,255,255,0.04)'}; color:${isSelected ? '#60a5fa' : '#cbd5e1'}; font-size:12px; font-weight:700; margin-bottom:4px; transition:all 0.15s ease;" onclick="selectPnlEntity('${it.id}', '${it.name.replace(/'/g, "\\'")}')" title="${it.name}">
                <div style="display:flex; align-items:center; gap:8px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; max-width:160px;">
                    <input type="radio" name="pnl_entity" ${isSelected ? "checked" : ""} style="cursor:pointer; accent-color:#3b82f6;">
                    <span style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap;" title="${it.name}">${it.name}</span>
                </div>
                <div style="display:flex; align-items:center; gap:6px;">
                    <span style="font-family:var(--font-mono); font-size:11px; font-weight:800; color:${mtmColor};">${mtmFormatted}</span>
                    ${badgeHtml}
                </div>
            </div>
        `;
    });

    list.innerHTML = html || `<div style="color:#64748b; font-size:11px; padding:12px; text-align:center;">No strategies match query.</div>`;
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
    
    const dateInput = document.getElementById("pnlDateSelect");
    if (dateInput && !dateInput.value) {
        const now = new Date();
        const yyyy = now.getFullYear();
        const mm = String(now.getMonth() + 1).padStart(2, "0");
        const dd = String(now.getDate()).padStart(2, "0");
        dateInput.value = `${yyyy}-${mm}-${dd}`;
    }

    const start = dateInput?.value || document.getElementById("pnlStartDate")?.value || "";
    const end = document.getElementById("pnlEndDate")?.value || start;
    const und = document.getElementById("pnlUnderlyingSelect")?.value || "None";
    const tf = document.getElementById("pnlTimeframeSelect")?.value || "5m";

    try {
        const resp = await fetch(`/api/straddle/pnl_chart?selector_type=${selector}&entity_id=${encodeURIComponent(entityId)}&start_date=${start}&end_date=${end}&underlying=${encodeURIComponent(und)}&timeframe=${encodeURIComponent(tf)}`);
        const data = await resp.json();
        if (data && data.success && data.data) {
            window._straddleState.pnlDataCache = data.data;
            drawPnlCanvas(data.data, window._straddleState.pnlChartType || "line", null);

            const titleEl = document.getElementById("pnlChartTitle");
            if (titleEl) {
                titleEl.textContent = data.data.strategy_name || data.data.strategy_id || "Overall Portfolio P&L";
            }

            // Update stats cards
            const maxEl = document.getElementById("pnlStatMax");
            const minEl = document.getElementById("pnlStatMin");
            const totEl = document.getElementById("pnlStatTotal");
            const maxVal = data.data.max_pnl || 0.0;
            const minVal = data.data.min_pnl || 0.0;
            const totVal = data.data.total_pnl || 0.0;
            const maxTime = data.data.max_pnl_time || "--:--";
            const minTime = data.data.min_pnl_time || "--:--";
            const drawdown = data.data.max_drawdown || 0.0;
            const drawdownPeriod = data.data.max_drawdown_period || "--";
            const profitKept = data.data.profit_kept || (totVal < 0 ? "0.0%" : "100%");

            if (maxEl) maxEl.textContent = (maxVal > 0 ? "+₹ " : "₹ ") + Math.abs(maxVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (document.getElementById("pnlStatMaxTime")) {
                document.getElementById("pnlStatMaxTime").textContent = maxVal > 0 ? `at ${maxTime}` : `at ${maxTime !== '--:--' ? maxTime : '09:15'}`;
            }

            if (minEl) minEl.textContent = (minVal < 0 ? "-₹ " : "₹ ") + Math.abs(minVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (document.getElementById("pnlStatMinTime")) {
                document.getElementById("pnlStatMinTime").textContent = minVal < 0 ? `at ${minTime}` : `at ${minTime !== '--:--' ? minTime : '--:--'}`;
            }

            if (document.getElementById("pnlStatDrawdown")) {
                document.getElementById("pnlStatDrawdown").textContent = "₹ " + Math.abs(drawdown).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            }
            if (document.getElementById("pnlStatDrawdownPeriod")) {
                document.getElementById("pnlStatDrawdownPeriod").textContent = drawdownPeriod;
            }

            if (document.getElementById("pnlStatProfitKept")) {
                document.getElementById("pnlStatProfitKept").textContent = profitKept;
            }

            if (totEl) {
                totEl.textContent = (totVal >= 0 ? "+₹ " : "-₹ ") + Math.abs(totVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                totEl.style.color = totVal >= 0 ? "#10b981" : "#f43f5e";
            }

            // Dynamically adjust Day's Net P&L card background & border
            const netCard = document.getElementById("pnlCardNetPnl");
            if (netCard) {
                if (totVal >= 0) {
                    netCard.style.background = "linear-gradient(135deg, rgba(16,185,129,0.18), rgba(6,182,212,0.1))";
                    netCard.style.borderColor = "rgba(16,185,129,0.45)";
                    netCard.style.boxShadow = "0 0 12px rgba(16,185,129,0.12)";
                } else {
                    netCard.style.background = "linear-gradient(135deg, rgba(244,63,94,0.18), rgba(244,63,94,0.06))";
                    netCard.style.borderColor = "rgba(244,63,94,0.45)";
                    netCard.style.boxShadow = "0 0 12px rgba(244,63,94,0.12)";
                }
            }

            // Realised vs Open breakdown
            const breakdownEl = document.getElementById("pnlStatRealisedBreakdown");
            if (breakdownEl) {
                const realVal = data.data.total_realised_pnl || 0.0;
                const openVal = data.data.total_open_mtm || 0.0;
                const realTxt = (realVal >= 0 ? "+₹" : "-₹") + Math.abs(realVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                const openTxt = (openVal >= 0 ? "+₹" : "-₹") + Math.abs(openVal).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                breakdownEl.innerHTML = `Realised: <span style="color:${realVal >= 0 ? '#10b981' : '#f43f5e'}; font-weight:700;">${realTxt}</span> | Open: <span style="color:${openVal >= 0 ? '#10b981' : '#f43f5e'}; font-weight:700;">${openTxt}</span>`;
            }

            // Re-render sidebar to update live MTM
            renderPnlEntitiesList();
        }
    } catch (e) {
        console.error("Error loading PnL chart:", e);
    }
}

// Helper: Convert "HH:MM" to minutes from midnight
function _pnlTimeToMin(t) {
    if (!t || typeof t !== "string") return 0;
    const parts = t.split(":").map(Number);
    return (parts[0] || 0) * 60 + (parts[1] || 0);
}

// Helper: Calculate nice tick intervals for axes
function _getNiceTickSteps(minVal, maxVal, maxTicks = 6) {
    let rawRange = maxVal - minVal;
    if (rawRange <= 0) rawRange = 100;
    const rawStep = rawRange / Math.max(1, maxTicks);
    const mag = Math.pow(10, Math.floor(Math.log10(rawStep || 1)));
    let step;
    const residual = rawStep / mag;
    if (residual <= 1.5) step = 1 * mag;
    else if (residual <= 3.5) step = 2 * mag;
    else if (residual <= 7.5) step = 5 * mag;
    else step = 10 * mag;

    const start = Math.floor(minVal / step) * step;
    const end = Math.ceil(maxVal / step) * step;
    const ticks = [];
    for (let v = start; v <= end + step * 0.01; v += step) {
        ticks.push(Math.round(v * 100) / 100);
    }
    return { ticks, step, min: start, max: end };
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

    // Resize canvas buffer only when physical dimensions change
    if (canvas.width !== Math.round(W * dpr) || canvas.height !== Math.round(H * dpr)) {
        canvas.style.width = W + "px";
        canvas.style.height = H + "px";
        canvas.width = Math.round(W * dpr);
        canvas.height = Math.round(H * dpr);
    }
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    const hasUnderlying = Boolean(pnlData && pnlData.underlying_points && pnlData.underlying_points.length > 0 && pnlData.underlying && pnlData.underlying !== "None");
    const padLeft = 78;
    const padRight = hasUnderlying ? 78 : 38;
    const padTop = 42;
    const padBottom = 46;
    const chartW = W - padLeft - padRight;
    const chartH = H - padTop - padBottom;

    // Clear background with rich dark institutional gradient
    const bgGrad = ctx.createLinearGradient(0, 0, 0, H);
    bgGrad.addColorStop(0, "#080c14");
    bgGrad.addColorStop(1, "#0a101d");
    ctx.fillStyle = bgGrad;
    ctx.fillRect(0, 0, W, H);

    let rawPoints = pnlData.points || [];
    let points = rawPoints;

    if (points.length === 0) {
        const curTot = pnlData.total_pnl || 0.0;
        points = [
            { timestamp: "09:15", pnl: 0.0, open: 0.0, high: 0.0, low: 0.0, close: 0.0 },
            { timestamp: "15:30", pnl: curTot, open: curTot, high: curTot, low: 0.0, close: curTot }
        ];
    } else if (points.length === 1) {
        const p0 = points[0];
        points = [
            { ...p0 },
            { ...p0, timestamp: "15:30" }
        ];
    }

    // Master Timeline bounds
    const sessionStartMin = _pnlTimeToMin(points[0].timestamp);
    const sessionEndMin = _pnlTimeToMin(points[points.length - 1].timestamp);
    const sessionSpan = Math.max(1, sessionEndMin - sessionStartMin);

    const getXByTime = (tStr) => {
        const tMin = _pnlTimeToMin(tStr);
        const ratio = Math.max(0, Math.min(1, (tMin - sessionStartMin) / sessionSpan));
        return padLeft + ratio * chartW;
    };

    const getX = (idx) => padLeft + (idx / Math.max(1, points.length - 1)) * chartW;

    // Determine Y-axis bounds with nice integer intervals for P&L
    let rawMax = Math.max(100, (pnlData.max_pnl || 0) * 1.15);
    let rawMin = Math.min(-100, (pnlData.min_pnl || 0) * 1.15);
    if (rawMax - rawMin < 100) {
        rawMax = Math.max(rawMax, 100);
        rawMin = Math.min(rawMin, -100);
    }
    const yTickObj = _getNiceTickSteps(rawMin, rawMax, 6);
    const yMin = yTickObj.min;
    const yMax = yTickObj.max;
    const yRange = Math.max(1, yMax - yMin);

    const getY = (val) => padTop + chartH - ((val - yMin) / yRange) * chartH;

    // Determine Y-axis bounds for Underlying Spot if active
    let undMin = 0, undMax = 0, undRange = 1, undTickObj = null;
    let getUndY = (val) => padTop + chartH / 2;
    if (hasUnderlying) {
        const undPrices = pnlData.underlying_points.map(u => Number(u.price || u.close || 0)).filter(p => p > 0);
        if (undPrices.length > 0) {
            let uMinRaw = Math.min(...undPrices);
            let uMaxRaw = Math.max(...undPrices);
            if (uMinRaw === uMaxRaw) {
                uMinRaw *= 0.995;
                uMaxRaw *= 1.005;
            }
            const buf = (uMaxRaw - uMinRaw) * 0.12;
            undTickObj = _getNiceTickSteps(uMinRaw - buf, uMaxRaw + buf, 6);
            undMin = undTickObj.min;
            undMax = undTickObj.max;
            undRange = Math.max(1, undMax - undMin);
            getUndY = (price) => padTop + chartH - ((price - undMin) / undRange) * chartH;
        }
    }

    // 1. Draw horizontal grid lines & Y labels (P&L on Left)
    ctx.lineWidth = 1;
    ctx.strokeStyle = "rgba(255, 255, 255, 0.05)";
    ctx.fillStyle = "#64748b";
    ctx.font = "10.5px monospace";
    ctx.textAlign = "right";

    yTickObj.ticks.forEach(val => {
        const y = getY(val);
        ctx.beginPath();
        ctx.moveTo(padLeft, y);
        ctx.lineTo(W - padRight, y);
        ctx.stroke();

        let formattedVal = "";
        if (val === 0) formattedVal = "₹ 0";
        else formattedVal = (val > 0 ? "+₹ " : "-₹ ") + Math.abs(Math.round(val)).toLocaleString("en-IN");
        ctx.fillText(formattedVal, padLeft - 8, y + 3.5);
    });

    // 1b. Draw Right Y-axis labels for Underlying Spot if active
    if (hasUnderlying && undTickObj && undTickObj.ticks) {
        ctx.fillStyle = "#38bdf8";
        ctx.font = "bold 10px monospace";
        ctx.textAlign = "left";
        undTickObj.ticks.forEach(uVal => {
            const uy = getUndY(uVal);
            if (uy >= padTop - 5 && uy <= padTop + chartH + 5) {
                ctx.fillText(Math.round(uVal).toLocaleString("en-IN"), W - padRight + 8, uy + 3.5);
            }
        });
    }

    // 2. Draw Zero Baseline Line
    const zeroY = getY(0);
    ctx.setLineDash([4, 4]);
    ctx.strokeStyle = "rgba(255, 255, 255, 0.4)";
    ctx.lineWidth = 1.3;
    ctx.beginPath();
    ctx.moveTo(padLeft, zeroY);
    ctx.lineTo(W - padRight, zeroY);
    ctx.stroke();
    ctx.setLineDash([]);

    // 3. Draw Max P&L Reference (Green Dashed)
    const maxPnl = pnlData.max_pnl !== undefined ? pnlData.max_pnl : 0;
    if (maxPnl > 0) {
        const maxY = getY(maxPnl);
        ctx.setLineDash([3, 3]);
        ctx.strokeStyle = "rgba(16, 185, 129, 0.55)";
        ctx.beginPath();
        ctx.moveTo(padLeft, maxY);
        ctx.lineTo(W - padRight, maxY);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = "#10b981";
        const badgeY = Math.max(padTop - 14, maxY - 17);
        if (ctx.roundRect) ctx.roundRect(padLeft, badgeY, 88, 16, 4);
        else ctx.rect(padLeft, badgeY, 88, 16);
        ctx.fill();
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 9px monospace";
        ctx.textAlign = "center";
        ctx.fillText(`Max +₹${Math.abs(Math.round(maxPnl))}`, padLeft + 44, badgeY + 11.5);
    }

    // 4. Draw Min P&L Reference (Red Dashed)
    const minPnl = pnlData.min_pnl !== undefined ? pnlData.min_pnl : 0;
    if (minPnl < 0) {
        const minY = getY(minPnl);
        ctx.setLineDash([3, 3]);
        ctx.strokeStyle = "rgba(244, 63, 94, 0.55)";
        ctx.beginPath();
        ctx.moveTo(padLeft, minY);
        ctx.lineTo(W - padRight, minY);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.fillStyle = "#f43f5e";
        const badgeMinY = Math.min(H - padBottom - 2, minY + 2);
        if (ctx.roundRect) ctx.roundRect(padLeft, badgeMinY, 88, 16, 4);
        else ctx.rect(padLeft, badgeMinY, 88, 16);
        ctx.fill();
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 9px monospace";
        ctx.textAlign = "center";
        ctx.fillText(`Min -₹${Math.abs(Math.round(minPnl))}`, padLeft + 44, badgeMinY + 11.5);
    }

    // 5. Draw Curve / Candlesticks
    if (chartType === "candle") {
        // Aggregate 1-minute points into 5-minute OHLC candlesticks
        const candleBuckets = [];
        const bucketSize = 5;
        for (let i = 0; i < points.length; i += bucketSize) {
            const slice = points.slice(i, i + bucketSize);
            if (slice.length > 0) {
                const cOpen = slice[0].open || slice[0].pnl;
                const cClose = slice[slice.length - 1].close || slice[slice.length - 1].pnl;
                const cHigh = Math.max(...slice.map(s => s.high !== undefined ? s.high : s.pnl));
                const cLow = Math.min(...slice.map(s => s.low !== undefined ? s.low : s.pnl));
                candleBuckets.push({
                    timestamp: slice[0].timestamp,
                    open: cOpen,
                    high: cHigh,
                    low: cLow,
                    close: cClose,
                    pnl: cClose
                });
            }
        }

        const barW = Math.max(3.5, Math.min(18, (chartW / candleBuckets.length) * 0.72));
        candleBuckets.forEach((cd) => {
            const x = getXByTime(cd.timestamp);
            const openY = getY(cd.open);
            const closeY = getY(cd.close);
            const highY = getY(cd.high);
            const lowY = getY(cd.low);
            const isUp = cd.close >= cd.open;

            ctx.strokeStyle = isUp ? "#10b981" : "#f43f5e";
            ctx.fillStyle = isUp ? "#10b981" : "#f43f5e";
            ctx.lineWidth = 1.2;

            // Wick
            ctx.beginPath();
            ctx.moveTo(x, highY);
            ctx.lineTo(x, lowY);
            ctx.stroke();

            // Candle Body
            const top = Math.min(openY, closeY);
            const bodyH = Math.max(2, Math.abs(closeY - openY));
            ctx.fillRect(x - barW / 2, top, barW, bodyH);
        });
    } else {
        // Line Mode with Spline & Zero-Split Area Glow
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
        topGrad.addColorStop(0, "rgba(16, 185, 129, 0.32)");
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
        btmGrad.addColorStop(1, "rgba(244, 63, 94, 0.32)");
        ctx.fillStyle = btmGrad;
        ctx.fill();
        ctx.restore();

        // Stroke line with dynamic zero-crossing color transition
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

        // Highlight endpoint dot
        if (points.length > 0) {
            const lastIdx = points.length - 1;
            const lx = getX(lastIdx);
            const ly = getY(points[lastIdx].pnl);
            ctx.beginPath();
            ctx.arc(lx, ly, 4.5, 0, Math.PI * 2);
            ctx.fillStyle = points[lastIdx].pnl >= 0 ? "#10b981" : "#f43f5e";
            ctx.fill();
            ctx.lineWidth = 1.5;
            ctx.strokeStyle = "#ffffff";
            ctx.stroke();
        }
    }

    // 5b. Draw Underlying Spot Intraday Curve (Electric Cyan Line Overlay with exact timestamp alignment)
    if (hasUnderlying && pnlData.underlying_points && pnlData.underlying_points.length > 0) {
        const uPts = pnlData.underlying_points;
        ctx.save();
        ctx.beginPath();
        uPts.forEach((uPt, uIdx) => {
            const ux = getXByTime(uPt.time);
            const uy = getUndY(Number(uPt.price || uPt.close || 0));
            if (uIdx === 0) ctx.moveTo(ux, uy);
            else ctx.lineTo(ux, uy);
        });
        ctx.lineWidth = 2;
        ctx.strokeStyle = "#38bdf8";
        ctx.shadowColor = "rgba(56, 189, 248, 0.45)";
        ctx.shadowBlur = 6;
        ctx.stroke();
        ctx.restore();
    }

    // 5c. Top Legend Chips
    ctx.textAlign = "left";
    ctx.font = "bold 11px Inter, sans-serif";
    // Strategy P&L Chip
    ctx.fillStyle = (pnlData.total_pnl || 0) >= 0 ? "#10b981" : "#f43f5e";
    ctx.fillRect(padLeft + 6, padTop - 22, 10, 10);
    ctx.fillStyle = "#cbd5e1";
    ctx.fillText("Strategy P&L (₹)", padLeft + 22, padTop - 13);

    // Underlying Spot Chip
    if (hasUnderlying) {
        const uSym = pnlData.underlying_meta?.display_symbol || pnlData.underlying || "Underlying";
        const curSpot = pnlData.underlying_meta?.spot_price || (pnlData.underlying_points[pnlData.underlying_points.length - 1]?.price) || 0;
        ctx.strokeStyle = "#38bdf8";
        ctx.lineWidth = 2.5;
        ctx.beginPath();
        ctx.moveTo(padLeft + 145, padTop - 17);
        ctx.lineTo(padLeft + 165, padTop - 17);
        ctx.stroke();
        ctx.fillStyle = "#38bdf8";
        ctx.fillText(`⚡ ${uSym} Intraday Spot (₹ ${Number(curSpot).toLocaleString('en-IN', { minimumFractionDigits: 1 })})`, padLeft + 172, padTop - 13);
    }

    // 6. X-axis Clean Timestamps (Round half-hour and hour ticks)
    ctx.fillStyle = "#64748b";
    ctx.font = "10px monospace";
    ctx.textAlign = "center";

    const drawnXTimes = new Set();
    points.forEach((pt, idx) => {
        const tm = pt.timestamp || "";
        const isStandardMark = tm.endsWith(":00") || tm.endsWith(":30") || tm === "09:15" || tm === "15:30" || idx === 0 || idx === points.length - 1;
        if (isStandardMark) {
            const x = getX(idx);
            // Avoid overlapping text within 45px
            const isTooClose = Array.from(drawnXTimes).some(dx => Math.abs(dx - x) < 45);
            if (!isTooClose) {
                drawnXTimes.add(x);
                // Draw tiny tick marker
                ctx.strokeStyle = "rgba(255, 255, 255, 0.1)";
                ctx.beginPath();
                ctx.moveTo(x, padTop + chartH);
                ctx.lineTo(x, padTop + chartH + 4);
                ctx.stroke();
                ctx.fillText(tm, x, H - 16);
            }
        }
    });

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

        // Snapping Circle
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
        if (ctx.roundRect) ctx.roundRect(ptX - 26, H - 32, 52, 18, 4);
        else ctx.rect(ptX - 26, H - 32, 52, 18);
        ctx.fill();
        ctx.stroke();
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 10px monospace";
        ctx.textAlign = "center";
        ctx.fillText(pt.timestamp, ptX, H - 19);

        // Side Left Y-Axis P&L Badge Pill
        const yBadgeW = 82;
        const yBadgeH = 18;
        ctx.fillStyle = (pt.pnl >= 0) ? "#10b981" : "#f43f5e";
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(padLeft - yBadgeW - 6, ptY - yBadgeH / 2, yBadgeW, yBadgeH, 4);
        else ctx.rect(padLeft - yBadgeW - 6, ptY - yBadgeH / 2, yBadgeW, yBadgeH);
        ctx.fill();
        ctx.fillStyle = "#ffffff";
        ctx.font = "bold 10px monospace";
        ctx.textAlign = "center";
        const badgeTxt = (pt.pnl >= 0 ? "+₹ " : "-₹ ") + Math.abs(pt.pnl).toLocaleString("en-IN", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
        ctx.fillText(badgeTxt, padLeft - yBadgeW / 2 - 6, ptY + 3.5);

        // Side Right Y-Axis Underlying Badge Pill (if active)
        let currUndPrice = null;
        if (hasUnderlying && pnlData.underlying_points && pnlData.underlying_points.length > 0) {
            const uPts = pnlData.underlying_points;
            const targetMin = _pnlTimeToMin(pt.timestamp);
            let closestU = uPts[0];
            let minDiff = 99999;
            for (let u of uPts) {
                const diff = Math.abs(_pnlTimeToMin(u.time) - targetMin);
                if (diff < minDiff) {
                    minDiff = diff;
                    closestU = u;
                }
            }
            if (closestU) {
                currUndPrice = Number(closestU.price || closestU.close || 0);
                const uY = getUndY(currUndPrice);
                ctx.fillStyle = "#0284c7";
                ctx.beginPath();
                if (ctx.roundRect) ctx.roundRect(W - padRight + 6, uY - yBadgeH / 2, yBadgeW - 8, yBadgeH, 4);
                else ctx.rect(W - padRight + 6, uY - yBadgeH / 2, yBadgeW - 8, yBadgeH);
                ctx.fill();
                ctx.fillStyle = "#ffffff";
                ctx.font = "bold 10px monospace";
                ctx.textAlign = "center";
                ctx.fillText(`₹ ${Math.round(currUndPrice).toLocaleString('en-IN')}`, W - padRight + 6 + (yBadgeW - 8) / 2, uY + 3.5);
            }
        }

        // Floating Glassmorphism Tooltip Card
        const tipW = hasUnderlying ? 200 : 170;
        const tipH = hasUnderlying ? 90 : 70;
        let tipX = (ptX > W - padRight - tipW - 20) ? (ptX - tipW - 14) : (ptX + 14);
        let tipY = Math.max(padTop + 8, Math.min(padTop + chartH - tipH - 8, ptY - tipH / 2));

        ctx.fillStyle = "rgba(13, 21, 39, 0.95)";
        ctx.strokeStyle = "rgba(255, 255, 255, 0.18)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        if (ctx.roundRect) ctx.roundRect(tipX, tipY, tipW, tipH, 8);
        else ctx.rect(tipX, tipY, tipW, tipH);
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

        const drawdownVal = Math.max(0, ((pnlData.max_pnl || 0) - pt.pnl));
        if (hasUnderlying && currUndPrice !== null) {
            ctx.font = "bold 11px monospace";
            ctx.fillStyle = "#38bdf8";
            ctx.fillText(`Spot: ₹ ${currUndPrice.toLocaleString('en-IN', { minimumFractionDigits: 2 })}`, tipX + 10, tipY + 56);

            ctx.font = "10px Inter, sans-serif";
            ctx.fillStyle = "#94a3b8";
            ctx.fillText(`Drawdown: -₹ ${drawdownVal.toLocaleString("en-IN", { minimumFractionDigits: 2 })}`, tipX + 10, tipY + 74);
        } else {
            ctx.font = "10.5px Inter, sans-serif";
            ctx.fillStyle = "#cbd5e1";
            ctx.fillText(`Drawdown: -₹ ${drawdownVal.toLocaleString("en-IN", { minimumFractionDigits: 2 })}`, tipX + 10, tipY + 56);
        }
    }
}

function setupPnlCanvasEvents() {
    const canvas = document.getElementById("straddlePnlCanvas");
    if (!canvas) return;

    const handlePointerMove = (clientX, clientY) => {
        const pnlData = window._straddleState.pnlDataCache;
        if (!pnlData || !pnlData.points || pnlData.points.length === 0) return;

        window._straddleState._pnlHoverActive = true;

        const rect = canvas.getBoundingClientRect();
        const mouseX = clientX - rect.left;
        const mouseY = clientY - rect.top;
        const hasUnderlying = Boolean(pnlData && pnlData.underlying_points && pnlData.underlying_points.length > 0 && pnlData.underlying && pnlData.underlying !== "None");
        const padLeft = 78;
        const padRight = hasUnderlying ? 78 : 38;
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
            let spotText = "";
            if (hasUnderlying && pnlData.underlying_points && pnlData.underlying_points.length > 0) {
                const targetMin = _pnlTimeToMin(pt.timestamp);
                let closestU = pnlData.underlying_points[0];
                let minDiff = 99999;
                for (let u of pnlData.underlying_points) {
                    const diff = Math.abs(_pnlTimeToMin(u.time) - targetMin);
                    if (diff < minDiff) {
                        minDiff = diff;
                        closestU = u;
                    }
                }
                if (closestU) {
                    spotText = ` | Spot: <b style="color:#38bdf8;">₹ ${Number(closestU.price || closestU.close || 0).toLocaleString('en-IN', { minimumFractionDigits: 1 })}</b>`;
                }
            }
            cursorLabel.innerHTML = `Time: <b style="color:#ffffff;">${pt.timestamp}</b> | P&L: <b style="color:${color};">${pnlFormatted}</b>${spotText}`;
        }
    };

    canvas.addEventListener("mouseenter", () => {
        window._straddleState._pnlHoverActive = true;
    });

    canvas.addEventListener("mousemove", (e) => {
        handlePointerMove(e.clientX, e.clientY);
    });

    canvas.addEventListener("mouseleave", () => {
        window._straddleState._pnlHoverActive = false;
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
        window._straddleState._pnlHoverActive = false;
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

    const stratName = data.strategy_name || "portfolio";
    const dateStr = data.date || new Date().toISOString().split("T")[0];

    let csv = `Date,Time,Strategy,P&L (INR),Open,High,Low,Close,Underlying Spot\n`;
    data.points.forEach(p => {
        let spotVal = "";
        if (data.underlying_points && data.underlying_points.length > 0) {
            const pMin = _pnlTimeToMin(p.timestamp);
            let closestU = data.underlying_points[0];
            let minDiff = 99999;
            for (let u of data.underlying_points) {
                const diff = Math.abs(_pnlTimeToMin(u.time) - pMin);
                if (diff < minDiff) { minDiff = diff; closestU = u; }
            }
            if (closestU) spotVal = closestU.price || closestU.close || "";
        }
        csv += `${dateStr},${p.timestamp},"${stratName}",${p.pnl},${p.open || p.pnl},${p.high || p.pnl},${p.low || p.pnl},${p.close || p.pnl},${spotVal}\n`;
    });

    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    const sanitized = stratName.replace(/[^a-zA-Z0-9_-]/g, "_").toLowerCase();
    link.download = `pnl_${sanitized}_${dateStr}.csv`;
    link.click();
}

function captureStraddlePnlScreenshot() {
    const canvas = document.getElementById("straddlePnlCanvas");
    if (!canvas) return;
    const pnlData = window._straddleState.pnlDataCache || {};

    const dpr = window.devicePixelRatio || 1;
    const cw = canvas.width / dpr;
    const ch = canvas.height / dpr;

    // Create high-res offscreen canvas with branded header & summary strip
    const outCanvas = document.createElement("canvas");
    const headerH = 68;
    const footerH = 64;
    outCanvas.width = cw * dpr;
    outCanvas.height = (headerH + ch + footerH) * dpr;
    const outCtx = outCanvas.getContext("2d");
    outCtx.scale(dpr, dpr);

    // Background
    outCtx.fillStyle = "#070a12";
    outCtx.fillRect(0, 0, cw, headerH + ch + footerH);

    // Header Card
    outCtx.fillStyle = "#0c101d";
    outCtx.fillRect(0, 0, cw, headerH);
    outCtx.strokeStyle = "rgba(255, 255, 255, 0.08)";
    outCtx.beginPath();
    outCtx.moveTo(0, headerH);
    outCtx.lineTo(cw, headerH);
    outCtx.stroke();

    outCtx.font = "bold 15px Inter, sans-serif";
    outCtx.fillStyle = "#f8fafc";
    outCtx.textAlign = "left";
    outCtx.fillText("⚡ STRADDLE PRO | P&L Analysis", 16, 28);

    outCtx.font = "12px Inter, sans-serif";
    outCtx.fillStyle = "#94a3b8";
    outCtx.fillText(`${pnlData.strategy_name || 'Portfolio'} • ${pnlData.date || new Date().toISOString().split('T')[0]}`, 16, 48);

    const netTot = pnlData.total_pnl || 0.0;
    const netFormatted = (netTot >= 0 ? "+₹ " : "-₹ ") + Math.abs(netTot).toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    outCtx.textAlign = "right";
    outCtx.font = "bold 16px monospace";
    outCtx.fillStyle = netTot >= 0 ? "#10b981" : "#f43f5e";
    outCtx.fillText(`Net P&L: ${netFormatted}`, cw - 16, 38);

    // Draw main chart canvas
    outCtx.drawImage(canvas, 0, 0, canvas.width, canvas.height, 0, headerH, cw, ch);

    // Footer summary card
    outCtx.fillStyle = "#0a0e1a";
    outCtx.fillRect(0, headerH + ch, cw, footerH);
    outCtx.beginPath();
    outCtx.moveTo(0, headerH + ch);
    outCtx.lineTo(cw, headerH + ch);
    outCtx.stroke();

    const maxP = pnlData.max_pnl || 0.0;
    const minP = pnlData.min_pnl || 0.0;
    const dd = pnlData.max_drawdown || 0.0;
    const kept = pnlData.profit_kept || "0.0%";

    outCtx.textAlign = "left";
    outCtx.font = "11px Inter, sans-serif";
    outCtx.fillStyle = "#94a3b8";
    outCtx.fillText("Max Profit:", 20, headerH + ch + 24);
    outCtx.font = "bold 12px monospace";
    outCtx.fillStyle = "#10b981";
    outCtx.fillText(`+₹${Math.abs(maxP).toLocaleString('en-IN')}`, 20, headerH + ch + 44);

    outCtx.font = "11px Inter, sans-serif";
    outCtx.fillStyle = "#94a3b8";
    outCtx.fillText("Max Loss:", 170, headerH + ch + 24);
    outCtx.font = "bold 12px monospace";
    outCtx.fillStyle = "#f43f5e";
    outCtx.fillText(`-₹${Math.abs(minP).toLocaleString('en-IN')}`, 170, headerH + ch + 44);

    outCtx.font = "11px Inter, sans-serif";
    outCtx.fillStyle = "#94a3b8";
    outCtx.fillText("Max Drawdown:", 320, headerH + ch + 24);
    outCtx.font = "bold 12px monospace";
    outCtx.fillStyle = "#f59e0b";
    outCtx.fillText(`₹${Math.abs(dd).toLocaleString('en-IN')}`, 320, headerH + ch + 44);

    outCtx.font = "11px Inter, sans-serif";
    outCtx.fillStyle = "#94a3b8";
    outCtx.fillText("Profit Kept:", 470, headerH + ch + 24);
    outCtx.font = "bold 12px monospace";
    outCtx.fillStyle = "#06b6d4";
    outCtx.fillText(`${kept}`, 470, headerH + ch + 44);

    const url = outCanvas.toDataURL("image/png");
    const link = document.createElement("a");
    link.href = url;
    const sanitized = (pnlData.strategy_name || "pnl").replace(/[^a-zA-Z0-9_-]/g, "_").toLowerCase();
    link.download = `pnl_chart_${sanitized}_${pnlData.date || 'today'}.png`;
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
    if (!strategyId) {
        switchStraddleSubView("algos");
        return;
    }
    const strategies = window._straddleState?.allStrategies || [];
    const strat = strategies.find(s => 
        (s.strategy_id && String(s.strategy_id).trim().toLowerCase() === String(strategyId).trim().toLowerCase()) ||
        (s.name && String(s.name).trim().toLowerCase() === String(strategyId).trim().toLowerCase())
    );
    if (strat && strat.strategy_id) {
        openStrategyInTerminal(strat.strategy_id);
    } else {
        switchStraddleSubView("algos");
        const searchInput = document.getElementById("straddleAlgosSearch");
        if (searchInput) {
            searchInput.value = strategyId;
            renderStraddleAlgosTable(true);
        }
    }
}

function switchOrderbookSubTab(tab) {
    window._straddleState.orderbookSubTab = tab;
    if (tab === "orders") {
        setStraddlePositionsViewFilter("orders");
    } else {
        setStraddlePositionsViewFilter("margin");
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
                    <td colspan="12" style="text-align:center; padding:36px; color:#94a3b8;">
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
    const optTypeVal = document.getElementById("orderbookOptionTypeSelect")?.value || "ALL";
    const sideVal = document.getElementById("orderbookSideSelect")?.value || "ALL";
    const statusVal = document.getElementById("orderbookStatusSelect")?.value || "ALL";
    const clientVal = window._straddleOrderbookClientId || "ALL";

    // Update Header Sort Icons
    const currentSort = window._straddleOrderbookSort || { col: 'time', dir: 'desc' };
    const sortCols = ['algo_name', 'order_id', 'category', 'symbol', 'side', 'status', 'message', 'time', 'quantity', 'order_type', 'price', 'trigger_price'];
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
        // Client ID Filter (if active)
        if (clientVal !== "ALL") {
            const oClient = (o.client_id || "Kotak Neo Live").trim();
            if (oClient !== clientVal && !oClient.includes(clientVal)) return false;
        }

        // Option Type Filter (Call / Put)
        if (optTypeVal !== "ALL") {
            const sym = String(o.symbol || o.trading_symbol || "").toUpperCase();
            if (optTypeVal === "CE" && !sym.includes("CE")) return false;
            if (optTypeVal === "PE" && !sym.includes("PE")) return false;
        }

        // Side Filter (Buy / Sell)
        if (sideVal !== "ALL") {
            const side = String(o.side || o.transaction_type || "").toUpperCase();
            if (sideVal === "BUY" && !side.includes("BUY")) return false;
            if (sideVal === "SELL" && !side.includes("SELL")) return false;
        }

        // Status Filter (Completed / Rejected / Cancelled / Open)
        if (statusVal !== "ALL") {
            const st = String(o.status || "").toUpperCase();
            if (statusVal === "OPEN" && !(st.includes("OPEN") || st.includes("PENDING") || st.includes("TRIG"))) return false;
            if (statusVal === "COMPLETE" && !(st.includes("COMP") || st.includes("TRAD") || st.includes("EXEC"))) return false;
            if (statusVal === "REJECTED" && !st.includes("REJ")) return false;
            if (statusVal === "CANCELLED" && !st.includes("CAN")) return false;
        }

        // Search Filter
        if (searchVal) {
            const sym = String(o.symbol || o.trading_symbol || "").toLowerCase();
            const id = String(o.order_id || "").toLowerCase();
            const st = String(o.status || "").toLowerCase();
            const algo = String(o.algo_name || "").toLowerCase();
            const cat = String(o.category || "").toLowerCase();
            const msg = String(o.message || "").toLowerCase();
            const side = String(o.side || o.transaction_type || "").toLowerCase();
            const otype = String(o.order_type || "").toLowerCase();
            if (!sym.includes(searchVal) && !id.includes(searchVal) && !st.includes(searchVal) && !algo.includes(searchVal) && !cat.includes(searchVal) && !msg.includes(searchVal) && !side.includes(searchVal) && !otype.includes(searchVal)) return false;
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
                <td colspan="12" style="text-align:center; padding:45px 14px; color:#64748b;">
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

        const orderIdRaw = String(o.order_id || "-");
        const orderIdFormatted = orderIdRaw.replace(/,\s*/g, ', ');

        rowsHtml += `
            <tr class="${rowRejectedClass}" style="transition:background 0.15s ease;">
                <td class="col-orderbook-algo">
                    <a href="javascript:void(0)" onclick="openStrategyFromOrderbook('${o.strategy_id || ''}')" class="orderbook-algo-link" title="Open Algo ${algoNameDisplay}">${algoNameDisplay}</a>
                </td>
                <td class="col-orderbook-orderid" style="font-weight:600; color:#94a3b8; letter-spacing:0.3px;">${orderIdFormatted}</td>
                <td style="font-size:12px; color:#f1f5f9; font-weight:500;">${categoryDisplay}</td>
                <td style="font-weight:700; color:#f8fafc; font-size:12px; letter-spacing:0.3px;">${symFormatted}</td>
                <td><span class="orderbook-side-pill ${sideClass}">${sideText}</span></td>
                <td><span class="orderbook-status-pill ${stClass}">${stDisplay}</span></td>
                <td style="font-size:11.5px; ${messageColor} max-width:260px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="${o.message || (isRejected ? 'Order Rejected' : 'Complete')}">${o.message || (isRejected ? 'Order Rejected' : 'Complete')}</td>
                <td style="font-family:var(--font-mono); font-size:11px; color:#cbd5e1;">${o.time || '-'}</td>
                <td style="text-align:right; font-family:var(--font-mono); font-weight:700; font-size:12.5px; color:#f8fafc;">${o.quantity || 65}</td>
                <td style="font-size:11.5px; color:#cbd5e1;">${orderTypeDisplay}</td>
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

    let csv = "Algo name,Order ID,Category,Symbol,Side,Status,Message,Time,Qty,Order type,Price,Trigger Price\n";
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

        const sym = (o.symbol || "").replace(/"/g, '""');
        const st = (o.status || "Complete").replace(/"/g, '""');
        const msg = (o.message || "").replace(/"/g, '""');
        const time = (o.time || "").replace(/"/g, '""');
        const qty = o.quantity || 65;
        const otype = (o.order_type || "Limit (L)").replace(/"/g, '""');

        const price = parseFloat(o.price || 0).toFixed(2);
        const trigPrice = parseFloat(o.trigger_price || 0).toFixed(2);

        csv += `"${algo}","${oid}","${cat}","${sym}","${side}","${st}","${msg}","${time}",${qty},"${otype}",${price},${trigPrice}\n`;
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
                <!-- Hero Available to Trade Banner (Pitch Black OLED Theme) -->
                <div style="background:#0c0d12; border:1px solid rgba(16,185,129,0.35); border-radius:12px; padding:18px 22px; display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:14px; margin-bottom:16px; box-shadow:0 8px 24px rgba(0,0,0,0.6);">
                    <div>
                        <div style="font-size:11.5px; font-weight:800; color:#94a3b8; text-transform:uppercase; letter-spacing:0.8px; margin-bottom:4px;">Available to trade (Cash + Collateral)</div>
                        <div style="font-size:28px; font-weight:900; color:#10b981; font-family:var(--font-mono); letter-spacing:-0.5px;">₹ ${availCash.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</div>
                        <div style="font-size:11.5px; color:#34d399; font-weight:700; margin-top:4px; display:flex; align-items:center; gap:6px;">
                            <span style="width:7px; height:7px; border-radius:50%; background:#10b981; box-shadow:0 0 8px #10b981; display:inline-block;"></span>
                            Instant intraday & F&O execution limit active
                        </div>
                    </div>
                    <div style="display:flex; flex-direction:column; align-items:flex-end; gap:6px;">
                        <div style="background:#111319; border:1px solid rgba(255,255,255,0.08); border-radius:8px; padding:6px 14px; text-align:right;">
                            <div style="font-size:10.5px; color:#94a3b8; font-weight:700;">Margin Utilization</div>
                            <div style="font-size:16px; font-weight:800; color:${usedMargin > 0 ? '#38bdf8' : '#64748b'}; font-family:var(--font-mono);">${utilPct}%</div>
                        </div>
                        <div style="width:140px; height:4px; background:rgba(255,255,255,0.08); border-radius:2px; overflow:hidden;">
                            <div style="width:${Math.max(2, Math.min(100, utilPct * 10))}%; height:100%; background:linear-gradient(90deg, #38bdf8, #10b981); border-radius:2px;"></div>
                        </div>
                    </div>
                </div>

                <!-- 4 Detailed Metric Cards Grid (OLED Dark Pitch Black) -->
                <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:10px; margin-bottom:14px;">
                    <!-- Card 1: Cash -->
                    <div style="background:#111319; border:1px solid rgba(255,255,255,0.08); border-radius:8px; padding:14px; transition:border-color 0.2s ease;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <span style="color:#94a3b8; font-size:11.5px; font-weight:700;">Cash Balance</span>
                            <span style="font-size:14px;">💵</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#f8fafc; font-size:18px; margin-bottom:2px;">₹ ${availCash.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</div>
                        <div style="font-size:10.5px; color:#64748b;">Direct ledger funds for trading</div>
                    </div>

                    <!-- Card 2: Margin from Pledge -->
                    <div style="background:#111319; border:1px solid rgba(255,255,255,0.08); border-radius:8px; padding:14px; transition:border-color 0.2s ease;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <span style="color:#94a3b8; font-size:11.5px; font-weight:700;">Margin from Pledge</span>
                            <span style="font-size:14px;">📈</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#38bdf8; font-size:18px; margin-bottom:2px;">₹ ${collateral.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</div>
                        <div style="font-size:10.5px; color:#64748b;">Collateral from stock / ETF pledges</div>
                    </div>

                    <!-- Card 3: Margin Used -->
                    <div style="background:#111319; border:1px solid rgba(255,255,255,0.08); border-radius:8px; padding:14px; transition:border-color 0.2s ease;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <span style="color:#94a3b8; font-size:11.5px; font-weight:700;">Margin Used</span>
                            <span style="font-size:14px;">⚡</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#f43f5e; font-size:18px; margin-bottom:2px;">₹ ${usedMargin.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</div>
                        <div style="font-size:10.5px; color:#64748b;">Margin committed for open positions</div>
                    </div>

                    <!-- Card 4: Unavailable to trade -->
                    <div style="background:#111319; border:1px solid rgba(255,255,255,0.08); border-radius:8px; padding:14px; transition:border-color 0.2s ease;">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:6px;">
                            <span style="color:#94a3b8; font-size:11.5px; font-weight:700;">Unavailable to Trade</span>
                            <span style="font-size:14px;">🛑</span>
                        </div>
                        <div style="font-family:var(--font-mono); font-weight:800; color:#fbbf24; font-size:18px; margin-bottom:2px;">₹ ${unavail.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</div>
                        <div style="font-size:10.5px; color:#64748b;">Funds in T+1 clearing / unsettled credits</div>
                    </div>
                </div>

                <!-- Margin Breakdown Grid (Pitch Black OLED Theme) -->
                <div style="background:#0c0d12; border:1px solid rgba(255,255,255,0.08); border-radius:10px; padding:14px 16px;">
                    <div style="font-size:12px; font-weight:800; color:#38bdf8; margin-bottom:10px; letter-spacing:0.5px; text-transform:uppercase;">F&O Margin Breakdown (Kotak Neo Live)</div>
                    <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(200px, 1fr)); gap:10px;">
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 12px; background:#111319; border:1px solid rgba(255,255,255,0.05); border-radius:6px;">
                            <span style="color:#94a3b8; font-size:11.5px;">SPAN Margin (Regulatory):</span>
                            <span style="font-family:var(--font-mono); font-weight:700; color:#cbd5e1; font-size:12.5px;">₹ ${span.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 12px; background:#111319; border:1px solid rgba(255,255,255,0.05); border-radius:6px;">
                            <span style="color:#94a3b8; font-size:11.5px;">Exposure Margin:</span>
                            <span style="font-family:var(--font-mono); font-weight:700; color:#cbd5e1; font-size:12.5px;">₹ ${exposure.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 12px; background:#111319; border:1px solid rgba(255,255,255,0.05); border-radius:6px;">
                            <span style="color:#94a3b8; font-size:11.5px;">Payin Received Today:</span>
                            <span style="font-family:var(--font-mono); font-weight:700; color:#10b981; font-size:12.5px;">₹ ${payin.toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
                        </div>
                        <div style="display:flex; justify-content:space-between; align-items:center; padding:8px 12px; background:linear-gradient(135deg, rgba(37,99,235,0.15), rgba(16,185,129,0.15)); border:1px solid rgba(59,130,246,0.4); border-radius:6px;">
                            <span style="color:#f8fafc; font-size:12px; font-weight:800;">Total Account Value:</span>
                            <span style="font-family:var(--font-mono); font-weight:900; color:#ffffff; font-size:13.5px;">₹ ${(availCash + usedMargin + unavail).toLocaleString('en-IN', { minimumFractionDigits: 2 })}</span>
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

    const activeId = activeStrat ? activeStrat.strategy_id : (window._straddleState?.activeStrategyId || null);
    const curOpts = Array.from(sel.options).map(o => o.value).join(",");
    const newOpts = (strategies || []).map(s => s.strategy_id).join(",");

    if (curOpts !== newOpts || sel.options.length !== (strategies || []).length) {
        sel.innerHTML = "";
        (strategies || []).forEach(s => {
            const opt = document.createElement("option");
            opt.value = s.strategy_id;
            opt.textContent = s.name || s.strategy_id;
            if (activeId && s.strategy_id === activeId) opt.selected = true;
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

    if (!window._straddleErrorLegEdits) window._straddleErrorLegEdits = {};
    window._straddleErrorLegEdits[idx] = {
        lots: lotsInput ? parseInt(lotsInput.value, 10) || 1 : 1,
        strike: strikeInput ? strikeInput.value.trim() : ""
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

window._straddleErrorLegEdits = window._straddleErrorLegEdits || {};

function preserveErrorLegChange(idx) {
    if (!window._straddleErrorLegEdits) window._straddleErrorLegEdits = {};
    const lotsInput = document.getElementById(`err_lots_${idx}`);
    const strikeInput = document.getElementById(`err_strike_${idx}`);
    const lotsVal = lotsInput ? (parseInt(lotsInput.value, 10) || 1) : 1;
    const strikeVal = strikeInput ? strikeInput.value.trim() : "";
    window._straddleErrorLegEdits[idx] = {
        lots: lotsVal,
        strike: strikeVal,
        ts: Date.now()
    };
}
window.preserveErrorLegChange = preserveErrorLegChange;

function stepErrorStrikePrice(idx, direction) {
    const input = document.getElementById(`err_strike_${idx}`);
    if (!input) return;
    let curVal = parseFloat(input.value.replace(/[^0-9.]/g, '')) || 0;
    let posUnd = "NIFTY";
    if (window._straddleState?.activeStrategy) {
        posUnd = window._straddleState.activeStrategy.underlying || "NIFTY";
        const positions = window._straddleState.activeStrategy.positions || window._straddleState.positions || [];
        if (positions[idx]) {
            posUnd = positions[idx].underlying || (positions[idx].contract && positions[idx].contract.underlying) || posUnd;
        }
    }
    const step = (typeof getStrikeStepForUnderlying === "function") ? getStrikeStepForUnderlying(posUnd) : 50;
    if (curVal === 0) {
        const spot = Number(window._straddleSpotPrices?.[posUnd] || 23300);
        curVal = Math.round(spot / step) * step;
    }
    curVal += direction * step;
    input.value = curVal;
    preserveErrorLegChange(idx);
}
window.stepErrorStrikePrice = stepErrorStrikePrice;

function stepErrorLots(idx, direction) {
    const input = document.getElementById(`err_lots_${idx}`);
    if (!input) return;
    let curVal = parseInt(input.value, 10) || 1;
    curVal = Math.max(1, Math.min(200, curVal + direction));
    input.value = curVal;
    preserveErrorLegChange(idx);
}
window.stepErrorLots = stepErrorLots;

function stepLegStrikeSelect(rowId, dir) {
    const row = document.getElementById(rowId);
    if (!row) return;
    const sel = row.querySelector(".leg-strike");
    if (!sel || !sel.options || sel.options.length === 0) return;
    const curIdx = sel.selectedIndex >= 0 ? sel.selectedIndex : 0;
    const nextIdx = Math.max(0, Math.min(sel.options.length - 1, curIdx + dir));
    sel.selectedIndex = nextIdx;
    syncStrategyLegChange(rowId);
}
window.stepLegStrikeSelect = stepLegStrikeSelect;

function stepLegLotsInput(rowId, dir) {
    const row = document.getElementById(rowId);
    if (!row) return;
    const input = row.querySelector(".leg-lots");
    if (!input) return;
    let curVal = parseInt(input.value, 10) || 1;
    curVal = Math.max(1, Math.min(500, curVal + dir));
    input.value = curVal;
    syncStrategyLegChange(rowId);
}
window.stepLegLotsInput = stepLegLotsInput;

function getStrategyConfigSignature(strat) {
    if (!strat) return "";
    try {
        return JSON.stringify({
            id: strat.strategy_id,
            name: strat.name,
            und: strat.underlying,
            st: strat.start_time,
            et: strat.end_time,
            mult: strat.multiplier,
            multType: strat.multiplier_type,
            cush: strat.cushion_margin,
            days: strat.schedule_days,
            tradeType: strat.trade_type,
            undSrc: strat.underlying_price_source,
            mtmMode: strat.mtm_calc_mode,
            mtmTgt: strat.mtm_target,
            mtmSl: strat.mtm_stoploss,
            toggles: strat.toggles,
            adv: strat.advanced_settings || strat.advanced,
            pm: strat.premium_matching,
            legs: (strat.legs || []).map(l => ({
                side: l.side,
                opt: l.option_type,
                strike: l.strike_selection,
                lots: l.lots,
                tgt: l.target_value,
                sl: l.sl_value,
                tgtType: l.target_type,
                slType: l.sl_type,
                slPlace: l.sl_order_placement,
                tgtPlace: l.target_order_placement,
                hedge: l.associated_hedge
            }))
        });
    } catch (_) {
        return "";
    }
}
window.getStrategyConfigSignature = getStrategyConfigSignature;

function markStraddleSettingsDirty() {
    window._straddleSettingsDirty = true;
    if (window._straddleAutoSaveTimer) {
        clearTimeout(window._straddleAutoSaveTimer);
    }
    window._straddleAutoSaveTimer = setTimeout(() => {
        if (window._straddleSettingsDirty && typeof saveStraddleStrategySettings === "function") {
            saveStraddleStrategySettings({ silent: true });
        }
    }, 1200);
}
window.markStraddleSettingsDirty = markStraddleSettingsDirty;

function syncStrategyLegChange(rowId) {
    if (!window._straddleState?.activeStrategy) return;
    if (!Array.isArray(window._straddleState.activeStrategy.legs)) {
        window._straddleState.activeStrategy.legs = [];
    }
    const legs = window._straddleState.activeStrategy.legs;
    const tr = document.getElementById(rowId);
    if (!tr) return;
    const legIdx = Array.from(tr.parentElement ? tr.parentElement.children : []).indexOf(tr);
    let legObj = (legIdx >= 0 && legs[legIdx]) ? legs[legIdx] : legs.find(l => l.id === rowId);
    if (!legObj) {
        legObj = { id: rowId };
        legs.push(legObj);
    }
    legObj.id = rowId;
    legObj.side = tr.querySelector(".leg-side")?.value || "Sell";
    legObj.product = tr.querySelector(".leg-product")?.value || "NRML";
    legObj.lots = parseInt(tr.querySelector(".leg-lots")?.value, 10) || 1;
    legObj.strike_selection = tr.querySelector(".leg-strike")?.value || "ATM";
    legObj.multiple_strike_selection = tr.querySelector(".leg-multiple-strike")?.value || "None";
    legObj.option_type = tr.querySelector(".leg-type")?.value || legObj.option_type || "CE";
    legObj.underlying = tr.querySelector(".leg-underlying")?.value || window._straddleState.activeStrategy.underlying || "NIFTY";
    legObj.expiry = tr.querySelector(".leg-expiry")?.value || "Weekly";

    // Target, SL & Re-execution fields
    legObj.target_type = tr.querySelector(".leg-tgt-type")?.value || "TGT: %";
    const parsedTgtVal = parseFloat(tr.querySelector(".leg-tgt-val")?.value);
    legObj.target_value = !Number.isNaN(parsedTgtVal) ? parsedTgtVal : 25.0;
    legObj.target_order_placement = tr.querySelector(".leg-tgt-placement")?.value || "Place in advance";
    legObj.target_re_execute_type = tr.querySelector(".leg-tgt-re-type")?.value || "RE EXECUTE";
    const parsedTgtReCnt = parseInt(tr.querySelector(".leg-tgt-re-cnt")?.value, 10);
    legObj.target_re_execute_count = !Number.isNaN(parsedTgtReCnt) ? parsedTgtReCnt : 6;

    legObj.sl_type = tr.querySelector(".leg-sl-type")?.value || "SL: %";
    const parsedSlVal = parseFloat(tr.querySelector(".leg-sl-val")?.value);
    legObj.sl_value = !Number.isNaN(parsedSlVal) ? parsedSlVal : 25.0;
    legObj.sl_order_placement = tr.querySelector(".leg-sl-placement")?.value || "Place in advance";
    legObj.tsl_type = tr.querySelector(".leg-tsl-type")?.value || "None";
    legObj.tsl_profit_pts = parseFloat(tr.querySelector(".leg-tsl-profit")?.value) || 0.0;
    legObj.tsl_trail_pts = parseFloat(tr.querySelector(".leg-tsl-trail")?.value) || 0.0;
    legObj.sl_re_execute_type = tr.querySelector(".leg-sl-re-type")?.value || "RE EXECUTE";
    const parsedSlReCnt = parseInt(tr.querySelector(".leg-sl-re-cnt")?.value, 10);
    legObj.sl_re_execute_count = !Number.isNaN(parsedSlReCnt) ? parsedSlReCnt : 1;
    legObj.action = tr.querySelector(".leg-action")?.value || "Square off leg";

    const hChk = tr.querySelector(".leg-hedge-enabled");
    const hInitVal = parseInt(tr.querySelector(".leg-hedge-initial-otm")?.value, 10) || 20;
    const hThreshVal = parseInt(tr.querySelector(".leg-hedge-max-thresh")?.value, 10) || 22;
    const hResetVal = parseInt(tr.querySelector(".leg-hedge-reset-otm")?.value, 10) || 20;
    if (!legObj.associated_hedge) legObj.associated_hedge = {};
    if (hChk) legObj.associated_hedge.enabled = hChk.checked;
    legObj.associated_hedge.initial_otm = hInitVal;
    legObj.associated_hedge.max_distance_threshold = hThreshVal;
    legObj.associated_hedge.reset_otm = hResetVal;

    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
        scheduleAutoMultiplierLiveRecalc();
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.syncStrategyLegChange = syncStrategyLegChange;

function stepLiveStrikePrice(idx, direction) {
    const input = document.getElementById(`live_strike_${idx}`);
    if (!input) return;
    let curVal = parseInt(input.value.replace(/[^0-9]/g, '')) || 0;

    // Detect strike interval dynamically for underlying
    const symEl = document.querySelector(`tr[data-leg-index="${idx}"] .straddle-sym-title`);
    const symText = (symEl ? symEl.textContent : "").toUpperCase();
    let posUnd = window._straddleState?.activeStrategy?.underlying || "NIFTY";
    if (symText.includes("BANKNIFTY")) posUnd = "BANKNIFTY";
    else if (symText.includes("SENSEX")) posUnd = "SENSEX";
    else if (symText.includes("BANKEX")) posUnd = "BANKEX";
    else if (symText.includes("MIDCPNIFTY")) posUnd = "MIDCPNIFTY";
    else if (symText.includes("FINNIFTY")) posUnd = "FINNIFTY";
    else if (symText.includes("CRUDEOIL")) posUnd = "CRUDEOIL";
    else if (symText.includes("NATURALGAS")) posUnd = "NATURALGAS";
    else if (symText.includes("GOLD")) posUnd = "GOLD";
    else if (symText.includes("SILVER")) posUnd = "SILVER";
    else if (symText.includes("COPPER")) posUnd = "COPPER";

    let step = (typeof getStrikeStepForUnderlying === "function") ? getStrikeStepForUnderlying(posUnd) : 50;

    if (curVal === 0) {
        const spot = Number(window._straddleSpotPrices?.[posUnd] || 23300);
        curVal = Math.round(spot / step) * step;
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
    const tgtPlacementSelect = document.getElementById(`live_tgt_placement_${legIndex}`);
    const slPlacementSelect = document.getElementById(`live_sl_placement_${legIndex}`);

    const newLots = lotsInput ? parseInt(lotsInput.value) || 1 : null;
    const newStrike = strikeInput ? strikeInput.value.trim() : null;
    const targetVal = tgtValInput ? parseFloat(tgtValInput.value) : null;
    const targetMode = tgtModeSelect ? tgtModeSelect.value : "%";
    const slVal = slValInput ? parseFloat(slValInput.value) : null;
    const slMode = slModeSelect ? slModeSelect.value : "%";
    const targetPlacement = tgtPlacementSelect ? tgtPlacementSelect.value : null;
    const slPlacement = slPlacementSelect ? slPlacementSelect.value : null;

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
            sl_mode: slMode,
            target_order_placement: targetPlacement,
            sl_order_placement: slPlacement
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
            // Discard pending edits on error so inputs and lot counters don't stay stuck with failed values
            if (window._straddlePendingLegEdits && window._straddlePendingLegEdits[legIndex]) {
                delete window._straddlePendingLegEdits[legIndex];
            }
            const btn = document.getElementById(`btn_save_running_${legIndex}`);
            if (btn) btn.style.display = "none";
            showStraddleToast(res.error || "Failed to update position", "error", 4500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        }
    } catch (e) {
        showStraddleToast("Error updating position: " + e.message, "error", 3000);
    }
}

async function changeLiveLegPlacement(stratId, legId, legIndex, type, placement) {
    const isBroker = (placement === "Place in advance");
    const name = type === "TARGET" ? "Target" : "Stoploss";
    showStraddleToast(`Updating ${name} placement to ${isBroker ? 'Broker 🏦' : 'Platform 💻'}...`, "info", 1500);
    try {
        const payload = {
            strategy_id: stratId || window._straddleState?.activeStrategy?.strategy_id,
            leg_id: legId || null,
            leg_index: legIndex
        };
        if (type === "TARGET") payload.target_order_placement = placement;
        else payload.sl_order_placement = placement;

        const resp = await fetch("/api/straddle/set_leg_placement", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || `${name} placement updated!`, "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || `Failed to update ${name} placement`, "error", 4000);
            await fetchStraddleState();
        }
    } catch (e) {
        showStraddleToast(`Error updating placement: ${e.message}`, "error", 3000);
    }
}
window.changeLiveLegPlacement = changeLiveLegPlacement;

async function changeLiveLegPlacementCombined(stratId, legId, legIndex, choice) {
    if (!choice) return;
    showStraddleToast(`Updating order placement for leg ${legIndex + 1}...`, "info", 1500);
    try {
        let tgtPlacement = "Keep with self";
        let slPlacement = "Keep with self";
        if (choice === "BOTH_BROKER") {
            tgtPlacement = "Place in advance";
            slPlacement = "Place in advance";
        } else if (choice === "SL_BROKER_TGT_SELF") {
            tgtPlacement = "Keep with self";
            slPlacement = "Place in advance";
        } else if (choice === "TGT_BROKER_SL_SELF") {
            tgtPlacement = "Place in advance";
            slPlacement = "Keep with self";
        }

        const payload = {
            strategy_id: stratId || window._straddleState?.activeStrategy?.strategy_id,
            leg_id: legId || null,
            leg_index: legIndex,
            target_order_placement: tgtPlacement,
            sl_order_placement: slPlacement
        };

        const resp = await fetch("/api/straddle/set_leg_placement", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || "Order placement updated successfully!", "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to update placement", "error", 4000);
            await fetchStraddleState();
        }
    } catch (e) {
        showStraddleToast(`Error updating placement: ${e.message}`, "error", 3000);
    }
}
window.changeLiveLegPlacementCombined = changeLiveLegPlacementCombined;

async function resendLegOrderToBroker(stratId, legId, legIndex, orderType, symbol) {
    const ot = (orderType || "BOTH").toUpperCase();
    showStraddleToast(`Placing ${ot} order on Broker...`, "info", 2000);
    try {
        const payload = {
            strategy_id: stratId || window._straddleState?.activeStrategy?.strategy_id,
            leg_id: legId || null,
            leg_index: legIndex,
            order_type: ot,
            symbol: symbol || null
        };
        const resp = await fetch("/api/straddle/position/resend_to_broker", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || `Order placed on broker successfully!`, "success", 3000);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || `Failed to place ${ot} order on broker`, "error", 4500);
            await fetchStraddleState();
        }
    } catch (e) {
        showStraddleToast(`Error placing order: ${e.message}`, "error", 3000);
    }
}
window.resendLegOrderToBroker = resendLegOrderToBroker;

async function changeLegReferenceBasis(stratId, legId, legIndex, choice) {
    if (!choice) return;
    const sId = stratId || window._straddleState?.activeStrategy?.strategy_id;
    let customPrice = null;
    let bmTime = null;

    if (choice === "CUSTOM") {
        const inputVal = prompt("Enter custom reference price to recalculate Target and Stoploss:");
        if (!inputVal || isNaN(parseFloat(inputVal)) || parseFloat(inputVal) <= 0) {
            showStraddleToast("Invalid reference price entered.", "warning", 3000);
            return;
        }
        customPrice = parseFloat(inputVal);
    } else if (choice === "SPECIFIC_TIME") {
        const defaultT = (window._straddleState?.last_tick_time || "10:20:00").substring(0, 5);
        const inputTime = prompt("Enter 1-minute candle time (HH:MM, e.g. 10:20 or 09:16) for Target/SL parity:", defaultT);
        if (!inputTime) return;
        bmTime = inputTime.trim();
    }

    showStraddleToast(`Updating reference price basis...`, "info", 1500);
    try {
        const resp = await fetch("/api/straddle/set_leg_reference_price", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                strategy_id: sId,
                leg_id: legId || null,
                leg_index: legIndex,
                reference_mode: choice,
                custom_price: customPrice,
                benchmark_time: bmTime
            })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || "Reference price updated successfully!", "success", 3000);
            const refBadge = document.getElementById(`live_ref_badge_${legIndex}`);
            if (refBadge && res.reference_price) {
                const rp = Number(res.reference_price);
                const modeT = res.reference_candle_time ? `${res.reference_candle_time} Close` : (res.reference_price_mode === "ENTRY_CANDLE_CLOSE" ? "Entry Close" : "Ref");
                refBadge.innerHTML = `<span style="font-size:9.5px; color:#38bdf8; font-weight:800; background:rgba(56,189,248,0.15); border:1px solid rgba(56,189,248,0.35); border-radius:3px; padding:1px 4px; display:inline-block;">🎯 ${modeT}: ₹${rp.toFixed(2)}</span>`;
            }
            const tgtSub = document.getElementById(`live_tgt_sub_${legIndex}`);
            if (tgtSub && res.target_price) {
                const sp = tgtSub.querySelector("span") || tgtSub;
                sp.textContent = `TGT: ₹${Number(res.target_price).toFixed(2)}`;
            }
            const slSub = document.getElementById(`live_sl_sub_${legIndex}`);
            if (slSub && res.sl_price) {
                const sp = slSub.querySelector("span") || slSub;
                sp.textContent = `SL: ₹${Number(res.sl_price).toFixed(2)}`;
            }
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to update reference price", "error", 4000);
        }
    } catch (e) {
        showStraddleToast(`Error updating reference price: ${e.message}`, "error", 3000);
    }
}
window.changeLegReferenceBasis = changeLegReferenceBasis;

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
    const lotsInput = document.getElementById(`live_lots_${legIndex}`) || document.getElementById(`err_lots_${legIndex}`);
    const strikeInput = document.getElementById(`live_strike_${legIndex}`) || document.getElementById(`err_strike_${legIndex}`);
    const savedEdit = window._straddleErrorLegEdits?.[legIndex];
    const newLots = lotsInput ? (parseInt(lotsInput.value, 10) || 1) : (savedEdit?.lots || 1);

    // Check if position is a hedge
    const activeStrat = window._straddleState?.activeStrategy || (window._straddleState?.allStrategies || []).find(s => s.strategy_id === stratId);
    const pos = (activeStrat?.positions && activeStrat.positions[legIndex]) || null;
    const isHedge = !!(pos?.is_associated_hedge || String(legId || "").startsWith("hedge_") || pos?.parent_leg_id);

    // If it's a hedge, do NOT send the current hedge strike as a strike_selection override for the parent leg
    const newStrike = (!isHedge && strikeInput) ? strikeInput.value.trim() : (savedEdit?.strike || "");

    showStraddleToast(`⚡ Retrying ${isHedge ? 'hedge ' : ''}order ${legIndex + 1}...`, "info", 2000);
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
            is_hedge: isHedge
        };
        if (!isHedge && newStrike) {
            payload.updated_legs[legKey].strike = newStrike;
            payload.updated_legs[legKey].strike_selection = newStrike;
        }
        payload.updated_legs[legIndex] = payload.updated_legs[legKey];

        const resp = await fetch("/api/straddle/retry_legs", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });
        const res = await resp.json();
        if (res.success) {
            if (window._straddleErrorLegEdits) {
                delete window._straddleErrorLegEdits[legIndex];
            }
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

function autoFitErrorLegLots(idx, marginInfo) {
    const lotInput = document.getElementById(`live_lots_${idx}`) || document.getElementById(`err_lots_${idx}`);
    if (!lotInput) return;
    if (marginInfo && marginInfo.available > 0 && marginInfo.required > 0) {
        const curLots = parseInt(lotInput.value, 10) || 1;
        const marginPerLot = marginInfo.required / curLots;
        const fittedLots = Math.max(1, Math.floor(marginInfo.available / marginPerLot));
        lotInput.value = fittedLots;
        markRunningLegChanged(idx);
        if (marginInfo.available < marginPerLot) {
            showStraddleToast(`⚠️ Available cash (₹${marginInfo.available.toLocaleString('en-IN', { maximumFractionDigits: 2 })}) is less than 1 lot required margin (₹${marginPerLot.toLocaleString('en-IN', { maximumFractionDigits: 2 })}). Set to 1 lot. Consider buying hedge first to reduce margin.`, "warning", 4500);
        } else {
            showStraddleToast(`⚙️ Auto-fitted lots to ${fittedLots} based on available cash ₹${marginInfo.available.toLocaleString('en-IN', { maximumFractionDigits: 2 })}.`, "success", 3000);
        }
    } else {
        lotInput.value = 1;
        markRunningLegChanged(idx);
        showStraddleToast("⚙️ Lots set to 1.", "info", 2000);
    }
}
window.autoFitErrorLegLots = autoFitErrorLegLots;

async function dismissErrorLeg(stratId, legId, idx) {
    try {
        const sId = stratId || window._straddleState?.activeStrategy?.strategy_id;
        showStraddleToast("✕ Dismissing error leg...", "info", 1500);
        const resp = await fetch("/api/straddle/dismiss_error_leg", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: sId, leg_id: legId, leg_index: idx })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("✅ Error dismissed.", "success", 2000);
            if (window._straddleErrorLegEdits) delete window._straddleErrorLegEdits[idx];
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast("Error: " + (res.error || "Failed to dismiss"), "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3000);
    }
}
window.dismissErrorLeg = dismissErrorLeg;

function updateTerminalActionBar(strat = null, positions = null) {
    const btnRetry = document.getElementById("btnStraddleRetryErrors");
    const btnReset = document.getElementById("btnStraddleResetAlgo");
    const btnClearClosed = document.getElementById("btnStraddleClearClosed");
    const btnStart = document.getElementById("btnStraddleStartAlgo");
    const btnStop = document.getElementById("btnStraddleStopAlgo");
    const btnSqAll = document.getElementById("btnStraddleSquareOffAll");
    const btnToggleSettings = document.getElementById("btnStraddleToggleSettings");

    const active = strat || window._straddleState?.activeStrategy;
    const posList = positions || (active ? active.positions : []) || window._straddleState?.positions || [];
    const hasOpen = posList.some(p => String(p.status || "").toUpperCase() === "OPEN");
    const hasErrors = posList.some(p => String(p.status || "").toUpperCase() === "ERROR");
    const hasClosed = posList.some(p => {
        const st = String(p.status || "").toUpperCase();
        return st === "COMPLETE" || st === "CLOSED" || (p.avg_exit_price && Number(p.avg_exit_price) > 0);
    });

    const rawStatus = active ? String(active.status || "").toUpperCase() : "READY";
    const isRunning = hasOpen || rawStatus === "TRADE_ACTIVE" || (active && active.enabled !== false && active.status !== "STOPPED" && active.status !== "TERMINATED" && active.status !== "ENTRY_FAILED");
    const isFailedOrTerminated = rawStatus === "ENTRY_FAILED" || rawStatus === "TERMINATED" || (active && active._manual_reset);

    // 1. Contextual Start / Stop Algo (Only show the valid one)
    if (btnStart && btnStop) {
        if (isRunning) {
            btnStart.style.display = "none";
            btnStop.style.display = "inline-flex";
        } else {
            btnStart.style.display = "inline-flex";
            btnStop.style.display = "none";
        }
    }

    // 2. Square Off All - prominent when open positions exist, dimmed when no open positions
    if (btnSqAll) {
        if (hasOpen) {
            btnSqAll.style.opacity = "1";
            btnSqAll.style.cursor = "pointer";
        } else {
            btnSqAll.style.opacity = "0.5";
        }
    }

    // 3. Reset Algo - show ONLY when algo is stopped, terminated, failed, or has errors
    if (btnReset) {
        if (isFailedOrTerminated || hasErrors) {
            btnReset.style.display = "inline-flex";
            btnReset.style.background = "#7c3aed";
            btnReset.style.borderColor = "#8b5cf6";
            btnReset.style.color = "#ffffff";
        } else {
            btnReset.style.display = "none";
        }
    }

    // 4. Clear Closed Trades - show ONLY when closed trades actually exist in table
    if (btnClearClosed) {
        if (hasClosed) {
            btnClearClosed.style.display = "inline-flex";
            const closedCount = posList.filter(p => {
                const st = String(p.status || "").toUpperCase();
                return st === "COMPLETE" || st === "CLOSED" || (p.avg_exit_price && Number(p.avg_exit_price) > 0);
            }).length;
            btnClearClosed.innerHTML = `<span>🧹 Clear Closed (${closedCount})</span>`;
        } else {
            btnClearClosed.style.display = "none";
        }
    }

    // 5. Settings Panel Button text & style sync
    if (btnToggleSettings) {
        const panel = document.getElementById("straddleSettingsPanel");
        const isOpen = panel && panel.style.display === "block";
        btnToggleSettings.innerHTML = isOpen ? "✕ Close Settings" : "⚙️ Settings";
        if (isOpen) {
            btnToggleSettings.style.background = "#1e293b";
            btnToggleSettings.style.borderColor = "#64748b";
        } else {
            btnToggleSettings.style.background = "#2563eb";
            btnToggleSettings.style.borderColor = "#3b82f6";
        }
    }
}
window.updateTerminalActionBar = updateTerminalActionBar;

function renderStraddleMarginAutoExitAlert(strat) {
    const container = document.getElementById("straddleMarginAutoExitAlertContainer");
    if (!container) return;

    if (!strat || !strat._margin_auto_exit_pending) {
        if (container.innerHTML !== "") {
            container.innerHTML = "";
        }
        return;
    }

    const stratId = strat.strategy_id || "";
    const countdown = strat.margin_exit_countdown !== undefined ? Math.max(0, parseInt(strat.margin_exit_countdown, 10)) : 15;
    const source = strat._margin_auto_exit_source || "Order Execution";

    // If banner already exists in DOM, update countdown numbers in place without touching buttons/DOM structure
    const existingCountdownBadge = document.getElementById("marginCountdownSec");
    const existingCountdownSub = document.getElementById("marginCountdownSecSub");
    if (existingCountdownBadge && existingCountdownSub) {
        existingCountdownBadge.textContent = countdown;
        existingCountdownSub.textContent = countdown;
        return;
    }

    container.innerHTML = `
        <div class="straddle-margin-countdown-banner" style="background:linear-gradient(135deg, rgba(239,68,68,0.22), rgba(185,28,28,0.35)); border:1.5px solid #ef4444; border-radius:8px; padding:12px 18px; margin-bottom:12px; display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px; box-shadow:0 4px 16px rgba(239,68,68,0.25);">
            <div style="display:flex; align-items:center; gap:14px; flex:1; min-width:280px;">
                <div style="font-size:26px; animation: pulse 1s infinite;">🚨</div>
                <div>
                    <div style="color:#ffffff; font-weight:800; font-size:13px; display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                        <span>MARGIN ERROR DETECTED (${source.toUpperCase()})</span>
                        <span class="countdown-badge" style="background:#ef4444; color:#ffffff; padding:2px 10px; border-radius:12px; font-size:11.5px; font-weight:900; font-family:var(--font-mono); box-shadow:0 0 10px rgba(239,68,68,0.8);">Auto-Exiting in <span id="marginCountdownSec">${countdown}</span>s</span>
                    </div>
                    <div style="color:#fca5a5; font-size:11px; margin-top:4px; line-height:1.4;">
                        Due to margin shortfall on one or more legs, all open positions will be automatically squared off in <b><span id="marginCountdownSecSub">${countdown}</span> seconds</b> to prevent naked exposure.
                    </div>
                </div>
            </div>
            <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                <button type="button" class="straddle-btn" style="background:#dc2626; color:#ffffff; font-weight:800; font-size:12px; padding:8px 16px; border-radius:6px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:6px; box-shadow:0 2px 8px rgba(220,38,38,0.5);" onclick="triggerImmediateMarginAutoExit('${stratId}')">
                    🚨 Square Off Now
                </button>
                <button type="button" class="straddle-btn btn-outline" style="border:1px solid rgba(255,255,255,0.3); color:#f1f5f9; font-weight:700; font-size:12px; padding:8px 14px; border-radius:6px;" onclick="cancelMarginAutoExit('${stratId}')">
                    ✕ Cancel Auto-Exit
                </button>
            </div>
        </div>
    `;
}
window.renderStraddleMarginAutoExitAlert = renderStraddleMarginAutoExitAlert;

async function triggerImmediateMarginAutoExit(strategyId) {
    const stratId = strategyId || window._straddleState?.activeStrategyId;
    try {
        const resp = await fetch("/api/straddle/trigger_margin_auto_exit", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: stratId })
        });
        const res = await resp.json();
        if (res.success) {
            if (typeof showStraddleToast === "function") showStraddleToast("Square off executed successfully", "success", 2000);
            await fetchStraddleState();
        } else {
            alert(res.error || "Failed to trigger immediate margin auto-exit");
        }
    } catch (e) {
        console.error("Error triggering margin auto exit:", e);
    }
}
window.triggerImmediateMarginAutoExit = triggerImmediateMarginAutoExit;

async function cancelMarginAutoExit(strategyId) {
    const stratId = strategyId || window._straddleState?.activeStrategyId;
    try {
        const resp = await fetch("/api/straddle/cancel_margin_auto_exit", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: stratId })
        });
        const res = await resp.json();
        if (res.success) {
            if (typeof showStraddleToast === "function") showStraddleToast("Margin auto-exit cancelled. Positions kept open.", "info", 2000);
            await fetchStraddleState();
        } else {
            alert(res.error || "Failed to cancel margin auto-exit");
        }
    } catch (e) {
        console.error("Error cancelling margin auto exit:", e);
    }
}
window.cancelMarginAutoExit = cancelMarginAutoExit;

function renderStraddlePositions(positions, groupMtm, totalMtm) {
    const tbody = document.getElementById("straddlePositionsTableBody");
    if (!tbody) return;

    const strat = window._straddleState?.activeStrategy;
    let effectivePositions = (positions && Array.isArray(positions) && positions.length > 0)
        ? positions
        : (window._straddleState?.positions || strat?.positions || []);

    const closedFromStrat = (strat && Array.isArray(strat.closed_positions)) ? strat.closed_positions : [];
    const closedFromState = (window._straddleState && Array.isArray(window._straddleState.closed_positions)) ? window._straddleState.closed_positions : [];
    const allClosed = [...closedFromStrat, ...closedFromState];

    if (allClosed.length > 0) {
        const seenKeys = new Set();
        const combined = [...effectivePositions, ...allClosed];
        effectivePositions = combined.filter(p => {
            if (!p) return false;
            const oid = String(p.order_id || p.broker_order_id || "").trim();
            const isRealOid = oid && !oid.startsWith("ORD_") && !oid.startsWith("PAPER_");
            const k = isRealOid
                ? `${p.symbol || ""}_${oid}_${p.exit_order_id || ""}`
                : `${p.id || p.symbol || "leg"}_${p.order_id || ""}_${p.entry_time || ""}_${p.exit_time || ""}_${p.avg_ent_price || ""}_${p.re_execute_count || 0}_${p.status || ""}`;
            if (seenKeys.has(k)) return false;
            seenKeys.add(k);
            return true;
        });
    }
    positions = effectivePositions;

    // Update Algo Trades Toolbar Counts & Toggle State
    const allCount = (positions || []).length;
    const openCount = (positions || []).filter(p => {
        const s = String(p.status || "OPEN").toUpperCase();
        return s === "OPEN" || s === "LIVE OPEN" || s === "EXIT_PENDING";
    }).length;
    const closedCount = (positions || []).filter(p => {
        const s = String(p.status || "OPEN").toUpperCase();
        return s === "COMPLETE" || s === "CLOSED" || s === "EXITED";
    }).length;

    if (openCount === 0 && closedCount > 0 && typeof window._straddleAlgoShowClosed === "undefined") {
        window._straddleAlgoShowClosed = true;
    }

    const elAll = document.getElementById("algoCountAll");
    if (elAll) elAll.textContent = allCount;
    const elOpen = document.getElementById("algoCountOpen");
    if (elOpen) elOpen.textContent = openCount;
    const elClosed = document.getElementById("algoCountClosed");
    if (elClosed) elClosed.textContent = closedCount;

    const masterHeaderCb = document.getElementById("straddleSelectAllLegs");
    if (masterHeaderCb) {
        if (openCount === 0) {
            masterHeaderCb.checked = false;
            masterHeaderCb.disabled = true;
            masterHeaderCb.title = "No open positions to select";
        } else {
            masterHeaderCb.disabled = false;
            masterHeaderCb.title = "Select / deselect all open positions";
        }
    }

    const btnToggle = document.getElementById("btnToggleAlgoClosedTrades");
    const textToggle = document.getElementById("textToggleAlgoClosed");
    const iconToggle = document.getElementById("iconToggleAlgoClosed");
    if (btnToggle && textToggle && iconToggle) {
        if (window._straddleAlgoShowClosed !== false) {
            textToggle.textContent = `Hide Closed (${closedCount})`;
            iconToggle.textContent = "🙈";
            btnToggle.style.borderColor = "#0284c7";
            btnToggle.style.color = "#38bdf8";
        } else {
            textToggle.textContent = `Show Closed (${closedCount})`;
            iconToggle.textContent = "👁️";
            btnToggle.style.borderColor = "#64748b";
            btnToggle.style.color = "#94a3b8";
        }
    }

    // Render MTM Banner Strip immediately (ensures Group MTM & Total MTM reset to 0.00 even on 0 positions)
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

    // Auto-clear stale pre-market error banners once strategy is active, running, or has trades
    const errBanner = document.getElementById("stratSettingsErrorBanner");
    const activeStrat = window._straddleState?.activeStrategy;
    if (errBanner && activeStrat) {
        const hasPositions = (positions && positions.length > 0) || (activeStrat.positions && activeStrat.positions.length > 0);
        if (activeStrat.status === "TRADE_ACTIVE" || hasPositions || !activeStrat.entry_error || activeStrat.entry_error === "-") {
            errBanner.style.display = "none";
            errBanner.textContent = "";
        }
    }

    // Sync MTM Target display if activeStrategy has mtm_target configured
    const activeStratForTarget = window._straddleState?.activeStrategy;
    if (activeStratForTarget && activeStratForTarget.mtm_target) {
        const mtmTgtEl = document.getElementById("straddleMtmTargetDisplay");
        if (mtmTgtEl) {
            const m = activeStratForTarget.mtm_target;
            const isPerLot = (activeStratForTarget.mtm_calc_mode === "Per Lot");
            const lots = (typeof getActiveStrategyLots === "function") ? getActiveStrategyLots() : 1;
            const mult = isPerLot ? lots : 1;

            if (activeStratForTarget.mtm_locked_floor !== undefined && activeStratForTarget.mtm_locked_floor !== null) {
                mtmTgtEl.innerHTML = `<span style="color:#10b981; font-weight:800;" title="Floor locked & trailing">🔒 Floor: ₹ ${Number(activeStratForTarget.mtm_locked_floor).toFixed(2)}</span>`;
            } else if (m.type === "Lock and Trail" || m.profit_reaches || m.profit_lock_trail) {
                const reach = Number((m.profit_reaches !== undefined ? m.profit_reaches : (m.profit_lock_trail?.threshold || 0)) * mult);
                const lock = Number((m.lock_profit !== undefined ? m.lock_profit : (m.profit_lock_trail?.lock_profit || 0)) * mult);
                if (reach > 0) {
                    mtmTgtEl.innerHTML = `<span title="Lock ₹${lock.toFixed(0)} when profit reaches ₹${reach.toFixed(0)}">Lock ₹${lock.toFixed(0)} @ ₹${reach.toFixed(0)}${isPerLot ? ' (per lot)' : ''}</span>`;
                } else {
                    mtmTgtEl.textContent = "Lock & Trail (0)";
                }
            } else if (m.type === "Lock Profit") {
                const reach = Number((m.profit_reaches || 0) * mult);
                const lock = Number((m.lock_profit || 0) * mult);
                mtmTgtEl.textContent = `Lock ₹${lock.toFixed(0)} @ ₹${reach.toFixed(0)}`;
            } else if (m.type === "Trail Profit") {
                const step = Number((m.trail_step || 0) * mult);
                const val = Number((m.trail_val || 0) * mult);
                mtmTgtEl.textContent = `Trail +₹${val.toFixed(0)} / +₹${step.toFixed(0)}`;
            } else {
                const tv = Number((m.value !== undefined ? m.value : (m.target_value || 0)) * mult);
                if (!isNaN(tv) && tv > 0) {
                    mtmTgtEl.textContent = `₹ ${tv.toFixed(2)}${isPerLot ? ' (per lot)' : ''}`;
                } else {
                    mtmTgtEl.textContent = "None";
                }
            }

            // Also display configured MTM Stoploss if active
            const slCfg = activeStratForTarget.mtm_stoploss || activeStratForTarget.stoploss;
            if (slCfg && slCfg.type && slCfg.type !== "None") {
                const slVal = Number(slCfg.value || 0) * mult;
                if (slVal !== 0) {
                    mtmTgtEl.innerHTML += ` <span style="color:#64748b; margin:0 4px;">|</span> <span style="color:#f43f5e; font-weight:800;" title="MTM Stoploss">SL: ₹ ${Math.abs(slVal).toFixed(2)}</span>`;
                }
            }
        }
    }

    // Always update re-execution count badges (CE SL, CE Target, PE SL, PE Target)
    updateStraddleReexecBadges(positions || []);

    if (!positions || positions.length === 0) {
        const masterCb = document.getElementById("straddleSelectAllLegs");
        if (masterCb) masterCb.checked = false;
        onStraddlePositionCheckboxChange();
        tbody.innerHTML = `
            <tr>
                <td colspan="14" style="text-align:center; padding:36px 14px; color:#64748b;">
                    <div style="font-size:14px; font-weight:700; color:#cbd5e1; margin-bottom:4px;">No Active Positions</div>
                    <div style="font-size:11.5px; margin-bottom:8px;">Click <b>Settings</b> to configure legs and start trading, or hit <b>Start Algo</b>.</div>
                    <div>
                        <button type="button" class="straddle-btn btn-outline" onclick="openAdoptBrokerPositionModal()" style="border-color:#38bdf8; color:#38bdf8; font-size:11px; font-weight:700; padding:3px 10px;">📥 Adopt Existing Broker Position</button>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    // Attach original strategy positions index to each leg
    const indexedPositions = positions.map((pos, originalIndex) => ({
        ...pos,
        _origIdx: originalIndex
    }));

    // Multi-Criteria Filtering (Show/Hide closed, CE, PE, Main, Hedge)
    let displayPositions = indexedPositions.filter(p => {
        const rawStatus = String(p.status || "OPEN").toUpperCase();
        const isOpen = (rawStatus === "OPEN" || rawStatus === "LIVE OPEN");
        const isClosed = (rawStatus === "COMPLETE" || rawStatus === "CLOSED" || rawStatus === "EXITED");

        if (window._straddleAlgoShowClosed === false && isClosed) {
            return false;
        }

        const filter = window._straddleAlgoTradeFilter || "all";
        if (filter === "open" && !isOpen) return false;
        if (filter === "closed" && !isClosed) return false;

        const sym = String(p.symbol || p.trading_symbol || "").toUpperCase();
        const optType = String(p.option_type || (sym.includes("CE") ? "CE" : (sym.includes("PE") ? "PE" : ""))).toUpperCase();
        if (filter === "ce" && optType !== "CE") return false;
        if (filter === "pe" && optType !== "PE") return false;

        const isHedge = !!(p.is_associated_hedge || p.parent_leg_id || String(p.id || "").toLowerCase().includes("hedge") || String(p.entry_reason || "").toUpperCase().startsWith("HEDGE"));
        if (filter === "main" && isHedge) return false;
        if (filter === "hedge" && !isHedge) return false;

        return true;
    });

    // Multi-Criteria Sorting (Execution Order, Open/Closed, CE/PE, Main/Hedges, MTM)
    const sortOrder = window._straddleAlgoSortOrder || "default";
    const sortSelect = document.getElementById("algoPositionsSortSelect");
    if (sortSelect && sortSelect.value !== sortOrder && document.activeElement !== sortSelect) {
        sortSelect.value = sortOrder;
    }
    const _getPosExecEpoch = (p) => {
        if (p._entry_epoch_ts && Number(p._entry_epoch_ts) > 0) return Number(p._entry_epoch_ts);
        const tStr = String(p.entry_time || p.exit_time || "").trim();
        const m = tStr.match(/(\d{1,2}):(\d{2}):(\d{2})/);
        if (m) {
            return parseInt(m[1], 10) * 3600 + parseInt(m[2], 10) * 60 + parseInt(m[3], 10);
        }
        const idStr = String(p.id || "");
        const idM = idStr.match(/(\d{10,13})/);
        if (idM) {
            const rawTs = parseInt(idM[1], 10);
            return rawTs > 1e12 ? rawTs / 1000 : rawTs;
        }
        return 0;
    };
    if (sortOrder === "default") {
        displayPositions.sort((a, b) => {
            const ta = _getPosExecEpoch(a);
            const tb = _getPosExecEpoch(b);
            if (ta !== tb) return ta - tb;
            return a._origIdx - b._origIdx;
        });
    } else {
        displayPositions.sort((a, b) => {
            const rawA = String(a.status || "OPEN").toUpperCase();
            const rawB = String(b.status || "OPEN").toUpperCase();
            const isOpenA = (rawA === "OPEN" || rawA === "LIVE OPEN") ? 1 : 0;
            const isOpenB = (rawB === "OPEN" || rawB === "LIVE OPEN") ? 1 : 0;
            const isClosedA = (rawA === "COMPLETE" || rawA === "CLOSED" || rawA === "EXITED") ? 1 : 0;
            const isClosedB = (rawB === "COMPLETE" || rawB === "CLOSED" || rawB === "EXITED") ? 1 : 0;

            const isHedgeA = (a.is_associated_hedge || a.parent_leg_id || String(a.id || "").toLowerCase().includes("hedge") || String(a.entry_reason || "").toUpperCase().startsWith("HEDGE")) ? 1 : 0;
            const isHedgeB = (b.is_associated_hedge || b.parent_leg_id || String(b.id || "").toLowerCase().includes("hedge") || String(b.entry_reason || "").toUpperCase().startsWith("HEDGE")) ? 1 : 0;

            const symA = String(a.symbol || a.trading_symbol || "").toUpperCase();
            const symB = String(b.symbol || b.trading_symbol || "").toUpperCase();
            const optA = String(a.option_type || (symA.includes("CE") ? "CE" : (symA.includes("PE") ? "PE" : ""))).toUpperCase();
            const optB = String(b.option_type || (symB.includes("CE") ? "CE" : (symB.includes("PE") ? "PE" : ""))).toUpperCase();

            const mtmA = Number(a.mtm || 0.0);
            const mtmB = Number(b.mtm || 0.0);

            if (sortOrder === "open_first") {
                if (isOpenA !== isOpenB) return isOpenB - isOpenA;
                return a._origIdx - b._origIdx;
            }
            if (sortOrder === "closed_first") {
                if (isClosedA !== isClosedB) return isClosedB - isClosedA;
                return a._origIdx - b._origIdx;
            }
            if (sortOrder === "main_first") {
                if (isHedgeA !== isHedgeB) return isHedgeA - isHedgeB;
                return a._origIdx - b._origIdx;
            }
            if (sortOrder === "hedge_first") {
                if (isHedgeA !== isHedgeB) return isHedgeB - isHedgeA;
                return a._origIdx - b._origIdx;
            }
            if (sortOrder === "ce_first") {
                if (optA !== optB) return optA === "CE" ? -1 : 1;
                return a._origIdx - b._origIdx;
            }
            if (sortOrder === "pe_first") {
                if (optA !== optB) return optA === "PE" ? -1 : 1;
                return a._origIdx - b._origIdx;
            }
            if (sortOrder === "mtm_desc") {
                return mtmB - mtmA;
            }
            if (sortOrder === "mtm_asc") {
                return mtmA - mtmB;
            }
            return a._origIdx - b._origIdx;
        });
    }

    if (displayPositions.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="14" style="text-align:center; padding:32px 14px; color:#64748b;">
                    <div style="font-size:14px; font-weight:700; color:#cbd5e1; margin-bottom:4px;">No Trades Match Current Filter</div>
                    <div style="font-size:12px; margin-bottom:8px;">${closedCount} closed trades hidden. Adjust filter or toggle below to view.</div>
                    <button type="button" class="straddle-btn btn-outline" onclick="window._straddleAlgoShowClosed=true; setAlgoTradeFilter('all');" style="border-color:#38bdf8; color:#38bdf8; font-size:11px; font-weight:700; padding:3px 10px;">👁️ Show All Trades</button>
                </td>
            </tr>
        `;
        return;
    }

    const prevMap = window._straddleState.prevLtpMap || {};

    // Sound alert transition detection across strategy positions (Target, Stoploss, Errors, Order Fills, Exits)
    if (!window._soundPositionStates) window._soundPositionStates = new Map();
    positions.forEach((pos, idx) => {
        if (!pos) return;
        const key = `${pos.id || idx}_${pos.symbol || ''}`;
        const curStatus = String(pos.status || "").toUpperCase();
        const prev = window._soundPositionStates.get(key);

        if (window.StraddleSound) {
            if (prev) {
                // 1. Transition to ERROR
                if (curStatus === "ERROR" && prev.status !== "ERROR") {
                    StraddleSound.play("error");
                }
                // 2. Transition to COMPLETE / CLOSED / EXITED (Check whether Target or Stoploss)
                else if ((curStatus === "COMPLETE" || curStatus === "CLOSED" || curStatus === "EXITED") && (prev.status === "OPEN" || prev.status === "LIVE OPEN")) {
                    const r = String(pos.exit_reason || "").toUpperCase();
                    if (r.includes("TARGET") || r.includes("TGT") || r.includes("PROFIT")) {
                        StraddleSound.play("target");
                    } else if (r.includes("STOPLOSS") || r.includes("SL") || r.includes("STOP LOSS") || r.includes("LOSS")) {
                        StraddleSound.play("stoploss");
                    } else if (r.includes("HEDGE")) {
                        StraddleSound.play("warning");
                    } else {
                        StraddleSound.play("exit");
                    }
                }
                // 3. Transition to OPEN (Fill confirmation on retry)
                else if ((curStatus === "OPEN" || curStatus === "LIVE OPEN") && prev.status !== "OPEN" && prev.status !== "LIVE OPEN") {
                    StraddleSound.play("order");
                }
            } else if (window._soundPositionsInitialLoadDone) {
                // Freshly filled position (e.g. re-execution entry or initial entry)
                if (curStatus === "OPEN" || curStatus === "LIVE OPEN") {
                    StraddleSound.play("order");
                }
            }
        }
        window._soundPositionStates.set(key, { status: curStatus, exit_reason: pos.exit_reason || "" });
    });
    window._soundPositionsInitialLoadDone = true;

    // Snapshot any active error banner inputs into persistent store so they never reset
    positions.forEach((pos, idx) => {
        const elLots = document.getElementById(`err_lots_${idx}`);
        const elStrike = document.getElementById(`err_strike_${idx}`);
        if (elLots && elLots.value) {
            if (!window._straddleErrorLegEdits) window._straddleErrorLegEdits = {};
            if (!window._straddleErrorLegEdits[idx]) window._straddleErrorLegEdits[idx] = {};
            window._straddleErrorLegEdits[idx].lots = parseInt(elLots.value, 10) || 1;
        }
        if (elStrike && elStrike.value) {
            if (!window._straddleErrorLegEdits) window._straddleErrorLegEdits = {};
            if (!window._straddleErrorLegEdits[idx]) window._straddleErrorLegEdits[idx] = {};
            window._straddleErrorLegEdits[idx].strike = elStrike.value.trim();
        }
    });

    // Check if we can perform smooth in-place DOM update instead of destroying innerHTML
    const existingRows = tbody.querySelectorAll("tr[data-leg-index]:not(.straddle-error-banner-row)");
    const existingErrorBanners = tbody.querySelectorAll("tr.straddle-error-banner-row");
    const numErrorsExpected = displayPositions.filter(p => (String(p.status || "").toUpperCase() === "ERROR" || !!p.error_message || !!p.sl_cancelled_unexpectedly || !!p.target_cancelled_unexpectedly)).length;

    // Active focus check - NEVER destroy DOM if user is actively interacting inside the table!
    const activeEl = document.activeElement;
    const isUserInteractingInTable = activeEl && tbody.contains(activeEl) && (activeEl.tagName === "INPUT" || activeEl.tagName === "SELECT");

    // Verify if any leg's status, exit reason, or symbol has changed
    let anyStateMismatch = false;
    if (existingRows.length !== displayPositions.length) {
        anyStateMismatch = true;
    } else {
        for (let i = 0; i < displayPositions.length; i++) {
            const pos = displayPositions[i];
            const realIdx = pos._origIdx;
            const row = existingRows[i];
            if (!row || row.getAttribute("data-leg-index") !== String(realIdx)) { anyStateMismatch = true; break; }
            const rowStatus = row.getAttribute("data-status");
            const rowReason = row.getAttribute("data-exit-reason");
            const rowSym = row.getAttribute("data-symbol");
            const rowOid = row.getAttribute("data-order-id") || "";
            const rowLegId = row.getAttribute("data-leg-id") || "";
            const rowSlOid = row.getAttribute("data-sl-broker-oid") || "";
            const rowTgtOid = row.getAttribute("data-tgt-broker-oid") || "";
            const rowSlCanc = row.getAttribute("data-sl-cancelled") === "true";
            const rowTgtCanc = row.getAttribute("data-tgt-cancelled") === "true";
            const rowRefP = row.getAttribute("data-ref-price") || "";
            const rowSlP = row.getAttribute("data-sl-price") || "";
            const rowTgtP = row.getAttribute("data-tgt-price") || "";
            const rowEntryR = row.getAttribute("data-entry-reason") || "";

            const posStatus = String(pos.status || "OPEN").toUpperCase();
            const posReason = String(pos.exit_reason || "");
            const sym = pos.symbol || pos.trading_symbol || '';
            const posOid = String(pos.order_id || pos.broker_order_id || "");
            const posLegId = String(pos.id || "");
            const posSlOid = String(pos.sl_broker_order_id || "");
            const posTgtOid = String(pos.target_broker_order_id || "");
            const posSlCanc = !!pos.sl_cancelled_unexpectedly;
            const posTgtCanc = !!pos.target_cancelled_unexpectedly;
            const posRefP = String(pos.reference_price || "");
            const posSlP = String(pos.sl_price || "");
            const posTgtP = String(pos.target_price || "");
            const posEntryR = String(pos.entry_reason || "");

            if (rowStatus !== posStatus || rowReason !== posReason || rowSym !== sym ||
                rowOid !== posOid || rowLegId !== posLegId ||
                rowSlOid !== posSlOid || rowTgtOid !== posTgtOid ||
                rowSlCanc !== posSlCanc || rowTgtCanc !== posTgtCanc ||
                rowRefP !== posRefP || rowSlP !== posSlP || rowTgtP !== posTgtP ||
                rowEntryR !== posEntryR) {
                anyStateMismatch = true;
                break;
            }
        }
    }

    const canUpdateInPlace = (
        !anyStateMismatch &&
        existingRows.length === displayPositions.length &&
        displayPositions.length > 0 &&
        (existingErrorBanners.length === numErrorsExpected || isUserInteractingInTable)
    );

    if (canUpdateInPlace) {
        displayPositions.forEach((pos) => {
            const idx = pos._origIdx;
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

            let ltpColor = '#cbd5e1';
            if (isOpen) {
                ltpColor = (side === 'SELL' ? (ltp <= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171') : (ltp >= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171'));
            } else {
                const exitP = Number(pos.avg_exit_price || 0);
                if (exitP > 0) {
                    ltpColor = (side === 'SELL') ? (ltp <= exitP ? '#10b981' : '#f87171') : (ltp >= exitP ? '#10b981' : '#f87171');
                } else {
                    ltpColor = '#cbd5e1';
                }
            }
            const mtm = Number(pos.mtm || 0.0);
            const mtmClass = (mtm >= 0) ? "straddle-mtm-pos" : "straddle-mtm-neg";

            // Update LTP cell
            const ltpEl = row.querySelector(".straddle-ltp");
            if (ltpEl) {
                ltpEl.textContent = ltp > 0 ? ltp.toFixed(2) : "-";
                ltpEl.style.color = ltpColor;
                ltpEl.className = `straddle-ltp ${ltpClass}`;
                if (!isOpen && pos.avg_exit_price) {
                    ltpEl.setAttribute("title", `Live Market LTP: ₹${ltp.toFixed(2)} (Exited @ ₹${Number(pos.avg_exit_price).toFixed(2)})`);
                }
            }

            // Update MTM & PnL% cell
            const mtmEl = row.querySelector(".straddle-mtm");
            if (mtmEl) {
                mtmEl.textContent = mtm.toFixed(2);
                mtmEl.className = `straddle-mtm ${mtmClass}`;
            }
            const pnlPctEl = row.querySelector(".straddle-pnl-pct");
            if (pnlPctEl) {
                let pnlPctVal = (pos.pnl_pct !== undefined && pos.pnl_pct !== null) ? Number(pos.pnl_pct) : null;
                if (pnlPctVal === null && pos.avg_ent_price && Number(pos.avg_ent_price) > 0) {
                    const entP = Number(pos.avg_ent_price);
                    pnlPctVal = (side === "SELL") ? ((entP - ltp) / entP) * 100 : ((ltp - entP) / entP) * 100;
                }
                if (pnlPctVal !== null && !isNaN(pnlPctVal)) {
                    const isPos = pnlPctVal >= 0;
                    const pnlSign = isPos ? "+" : "";
                    const pnlPctStr = `${pnlSign}${pnlPctVal.toFixed(2)}%`;
                    pnlPctEl.textContent = pnlPctStr;
                    pnlPctEl.style.background = isPos ? "rgba(16,185,129,0.18)" : "rgba(244,63,94,0.18)";
                    pnlPctEl.style.color = isPos ? "#10b981" : "#f43f5e";
                    pnlPctEl.style.borderColor = isPos ? "rgba(16,185,129,0.4)" : "rgba(244,63,94,0.4)";
                }
            }

            // Sync checkbox selection state
            const legCb = row.querySelector(".straddle-leg-checkbox, input[type='checkbox']");
            if (legCb) {
                const isSelected = (window._straddleSelectedLegIndices && window._straddleSelectedLegIndices.has(idx)) || (pos.id && window._straddleSelectedLegIds && window._straddleSelectedLegIds.has(pos.id));
                legCb.checked = !!isSelected;
            }

            // Check pending edits or active focus
            const hasPending = !!(window._straddlePendingLegEdits && window._straddlePendingLegEdits[idx]);
            const isFocusedInRow = activeEl && row.contains(activeEl);

            const btnSave = document.getElementById(`btn_save_running_${idx}`);
            if (hasPending) {
                if (btnSave) btnSave.style.display = "inline-flex";
            } else if (!isFocusedInRow) {
                if (btnSave) btnSave.style.display = "none";
                const tgtSub = document.getElementById(`live_tgt_sub_${idx}`);
                if (tgtSub && pos.target_price) {
                    const tv = pos.target_val || pos.target_value;
                    const sp = tgtSub.querySelector("span") || tgtSub;
                    sp.textContent = tv ? `TGT: ${tv}% ₹${Number(pos.target_price).toFixed(2)}` : `TGT: ₹${Number(pos.target_price).toFixed(2)}`;
                }
                const slSub = document.getElementById(`live_sl_sub_${idx}`);
                if (slSub && pos.sl_price) {
                    const sv = pos.sl_val || pos.sl_value;
                    const sp = slSub.querySelector("span") || slSub;
                    sp.textContent = sv ? `SL: -${sv}% ₹${Number(pos.sl_price).toFixed(2)}` : `SL: ₹${Number(pos.sl_price).toFixed(2)}`;
                }
                // Persistent Reference Price Badge update in-place so it never disappears on tick/refresh
                const refBadge = document.getElementById(`live_ref_badge_${idx}`);
                if (refBadge) {
                    const curRef = Number(pos.reference_price || pos.entry_candle_close || pos.avg_ent_price || 0);
                    const curAvg = Number(pos.avg_ent_price || 0);
                    if (pos.benchmark_pending) {
                        const pendT = (pos.benchmark_time || pos.reference_candle_time || '09:16').substring(0, 5);
                        refBadge.innerHTML = `<span style="font-size:9px; color:#f59e0b; background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.35); border-radius:3px; padding:1px 4px; font-weight:800; display:inline-block;" title="Interim protection active. Will lock to exact ${pendT} 1-min candle close price when complete.">⏳ Ref @ ${pendT} Close</span>`;
                    } else if (curRef > 0) {
                        let modeTag = pos.reference_candle_time ? `${pos.reference_candle_time} Close` : (pos.reference_price_mode === "ENTRY_CANDLE_CLOSE" ? "Entry Close" : (pos.reference_price_mode === "SPECIFIC_TIME" ? `${(pos.benchmark_time || '09:16').substring(0, 5)} Close` : (pos.reference_price_mode === "CURRENT_LTP" ? "LTP Ref" : (pos.reference_price_mode === "CUSTOM" ? "Custom Ref" : "Avg Fill"))));
                        refBadge.innerHTML = `<span style="font-size:9.5px; color:#38bdf8; font-weight:800; background:rgba(56,189,248,0.15); border:1px solid rgba(56,189,248,0.35); border-radius:3px; padding:1px 4px; display:inline-block;" title="Target and Stoploss calculated from ${modeTag} Price ₹${curRef.toFixed(2)} (Entry fill was ₹${curAvg.toFixed(2)})">🎯 ${modeTag}: ₹${curRef.toFixed(2)}</span>`;
                    } else {
                        refBadge.innerHTML = '';
                    }
                }
                const refSel = document.getElementById(`live_ref_select_${idx}`);
                if (refSel && !isFocusedInRow) {
                    const curRef = Number(pos.reference_price || 0);
                    const curRefLabel = curRef > 0 ? `₹${curRef.toFixed(1)}` : 'Avg';
                    const firstOpt = refSel.querySelector("option[disabled]");
                    if (firstOpt) firstOpt.textContent = `Ref: ${curRefLabel} ▾`;
                }
            }
        });
    } else {
        // Full render with preserved pending edits and error leg inputs
        const activeId = (activeEl && tbody.contains(activeEl) && activeEl.id) ? activeEl.id : null;
        const selStart = (activeEl && typeof activeEl.selectionStart === "number") ? activeEl.selectionStart : null;
        const selEnd = (activeEl && typeof activeEl.selectionEnd === "number") ? activeEl.selectionEnd : null;

        let html = "";
        displayPositions.forEach((pos) => {
            const idx = pos._origIdx;
            const rawStatus = String(pos.status || "OPEN").toUpperCase();
            const isError = (rawStatus === "ERROR" || !!pos.error_message);
            const isCompleteOrClosed = (rawStatus === "COMPLETE" || rawStatus === "CLOSED" || rawStatus === "EXITED" || (pos.avg_exit_price && Number(pos.avg_exit_price) > 0 && pos.exit_reason && pos.exit_reason !== "-"));
            const isOpen = !isError && !isCompleteOrClosed && (rawStatus === "OPEN" || rawStatus === "LIVE OPEN");
            const isClosed = !isError && (isCompleteOrClosed || (!isOpen && !isError));
            const isAdopted = !!(
                pos.is_adopted ||
                (pos.id && String(pos.id).startsWith("leg_adopted_")) ||
                (pos.leg_id && String(pos.leg_id).startsWith("leg_adopted_")) ||
                (pos.id && String(pos.id).startsWith("leg_reexec_adopted_")) ||
                (pos.entry_reason && String(pos.entry_reason).toLowerCase().includes("adopt"))
            );
            const isReEntry = !isAdopted && !!(pos.is_re_entry || pos.re_entry || (pos.re_execute_count && pos.re_execute_count > 0));
            const stratId = pos.strategy_id || window._straddleState?.activeStrategy?.strategy_id || '';
            const isFilledOrPartial = isOpen && (Number(pos.entry_qty || 0) > 0 || (pos.avg_ent_price && Number(pos.avg_ent_price) > 0));
            const isInteractiveRow = isFilledOrPartial;
            const canAdjustStrikeAndLots = (isOpen || isError) && !isCompleteOrClosed;

            // Retrieve any pending user edits for this leg so they are never lost
            const pending = (window._straddlePendingLegEdits && window._straddlePendingLegEdits[idx]) || null;
            const hasPending = !!pending;

            // Status Badge
            let statusHtml = "";
            const adoptedBadgeHtml = '<span class="status-badge-adopted" style="font-size:9.5px; font-weight:800; background:rgba(168,85,247,0.2); color:#c084fc; border:1px solid rgba(168,85,247,0.45); border-radius:3px; padding:1px 5px; letter-spacing:0.3px;">ADOPTED</span>';
            const reEntryBadgeHtml = '<span class="status-badge-reentry">RE-ENTRY</span>';
            const secondaryStatusBadge = isAdopted ? adoptedBadgeHtml : (isReEntry ? reEntryBadgeHtml : '');

            if (isError) {
                statusHtml = `
                        <div class="straddle-status-container">
                            <span class="straddle-status-pill status-badge-error">ERROR</span>
                            ${secondaryStatusBadge}
                        </div>
                    `;
            } else if (isOpen) {
                statusHtml = `
                        <div class="straddle-status-container">
                            <span class="straddle-status-pill status-badge-open">OPEN</span>
                            ${secondaryStatusBadge}
                        </div>
                    `;
            } else {
                statusHtml = `
                        <div class="straddle-status-container">
                            <span class="straddle-status-pill status-badge-closed">CLOSED</span>
                            ${secondaryStatusBadge}
                            ${pos.re_execution_pending ? `<span style="font-size:9.5px; font-weight:800; color:#f59e0b; background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.3); border-radius:3px; padding:1px 5px; margin-top:2px; display:inline-block;" title="Next Re-entry scheduled at ${pos.re_execution_target_time || ''}">⏳ Re-Entering @ ${pos.re_execution_target_time || 'Next Candle'}</span>` : ''}
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

            // Expiry subtext & exchange segment prefix
            const exSeg = (pos.exchange_segment || (pos.contract && (pos.contract.segment || pos.contract.exchange_segment)) || '').toLowerCase();
            const posUnd = (pos.underlying || (pos.contract && pos.contract.underlying) || sym).toUpperCase();
            const isCommPos = ["CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM", "SILVER", "SILVERM", "COPPER"].some(c => posUnd.includes(c)) || exSeg.includes("mcx");
            const isBsePos = ["SENSEX", "BANKEX"].some(b => posUnd.includes(b)) || exSeg.includes("bse");
            const segPrefix = isCommPos ? "MCX" : (isBsePos ? "BFO" : "NFO");

            let rawExp = pos.contract ? (pos.contract.expiry_date || pos.contract.expiry_str || '') : (pos.expiry_str || pos.expiry || '');
            rawExp = String(rawExp).replace(/^(NFO|MCX|BFO|BSE|NSE)\s*/i, '').trim();
            let expiryText = rawExp ? `${segPrefix} ${rawExp}` : segPrefix;

            // Quantity column - derive lots from actual filled quantity so failed orders never display ghost lots
            const lotSizeVal = Number(pos.lot_size || 65);
            const actualFilledLots = Math.max(1, Math.round((Number(pos.quantity || pos.entry_qty || 65)) / lotSizeVal));
            const savedErr = (window._straddleErrorLegEdits && window._straddleErrorLegEdits[idx]) || null;
            const lots = pending ? pending.lots : (savedErr && savedErr.lots !== undefined ? savedErr.lots : (pos.quantity ? actualFilledLots : (pos.lots || 1)));
            const totalQty = pos.quantity || (lots * lotSizeVal);
            const entryQty = pos.entry_qty || (isError ? "-" : totalQty);
            const exitQty = (isClosed || (pos.exit_qty && pos.exit_qty > 0)) ? (pos.exit_qty || totalQty) : "-";

            // Quantity HTML (Interactive with stepper arrows for OPEN and editable error legs)
            let qtyHtml = "";
            if (canAdjustStrikeAndLots) {
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

            // Symbol HTML (With shift strike option and step arrows for OPEN and editable legs)
            let symHtml = "";
            const curStrikeMatch = pending ? pending.strike : (savedErr && savedErr.strike !== undefined ? savedErr.strike : (pos.strike_price || (sym.match(/\d{5}/) || [''])[0]));
            let hedgeTagHtml = "";
            if (pos.is_associated_hedge) {
                const pType = pos.hedge_parent_type || "MAIN";
                hedgeTagHtml = `<div style="margin-top:3px;"><span style="display:inline-block; font-size:9.5px; font-weight:800; color:#10b981; background:rgba(16,185,129,0.15); border:1px solid #10b981; border-radius:4px; padding:1px 5px;">🛡️ HEDGE (${formatSymbolColorCoded(pType)})</span></div>`;
            } else if (pos.has_associated_hedge || pos.associated_hedge_id || side === "SELL" || pos.is_re_entry) {
                hedgeTagHtml = `<div style="margin-top:3px;"><span style="display:inline-block; font-size:9.5px; font-weight:900; color:#ef4444; background:rgba(239,68,68,0.15); border:1px solid rgba(239,68,68,0.45); border-radius:4px; padding:1px 5px;" title="Main Trading Leg">🎯 MAIN</span></div>`;
            } else if (isAdopted) {
                hedgeTagHtml = `<div style="margin-top:3px;"><span style="display:inline-block; font-size:9px; font-weight:800; color:#c084fc; background:rgba(168,85,247,0.15); border:1px solid rgba(168,85,247,0.4); border-radius:4px; padding:1px 5px;" title="Adopted broker position">💼 ADOPTED</span></div>`;
            }

            if (canAdjustStrikeAndLots) {
                symHtml = `
                        <div class="straddle-sym-card ${symAccentClass}">
                            <div class="straddle-sym-title">${formatSymbolColorCoded(sym)}</div>
                            <div class="straddle-sym-sub">${expiryText}</div>
                            ${hedgeTagHtml}
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
                            <div class="straddle-sym-title">${formatSymbolColorCoded(sym)}</div>
                            <div class="straddle-sym-sub">${expiryText}</div>
                            ${hedgeTagHtml}
                        </div>
                    `;
            }

            // Avg Entry Price & Entry Time
            const avgEntPrice = pos.avg_ent_price ? Number(pos.avg_ent_price).toFixed(2) : '-';
            const entryTime = pos.entry_time || '-';

            // Reference Price badges and basis selector (AlgoTest Parity)
            let refPriceBadgeHtml = "";
            const curRefPrice = Number(pos.reference_price || pos.entry_candle_close || pos.avg_ent_price || 0);
            const curAvgPrice = Number(pos.avg_ent_price || 0);

            if (pos.benchmark_pending) {
                const pendTime = (pos.benchmark_time || pos.reference_candle_time || '09:16').substring(0, 5);
                refPriceBadgeHtml = `
                        <div id="live_ref_badge_${idx}" style="margin-top:2px;">
                            <span style="font-size:9px; color:#f59e0b; background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.35); border-radius:3px; padding:1px 4px; font-weight:800; display:inline-block;" title="Interim protection active. Will lock to exact ${pendTime} 1-min candle close price when complete.">⏳ Ref @ ${pendTime} Close</span>
                        </div>
                    `;
            } else if (curRefPrice > 0) {
                let modeTag = pos.reference_candle_time ? `${pos.reference_candle_time} Close` : (pos.reference_price_mode === "ENTRY_CANDLE_CLOSE" ? "Entry Close" : (pos.reference_price_mode === "SPECIFIC_TIME" ? `${(pos.benchmark_time || '09:16').substring(0, 5)} Close` : (pos.reference_price_mode === "CURRENT_LTP" ? "LTP Ref" : (pos.reference_price_mode === "CUSTOM" ? "Custom Ref" : "Avg Fill"))));
                refPriceBadgeHtml = `
                        <div id="live_ref_badge_${idx}" style="margin-top:2px;">
                            <span style="font-size:9.5px; color:#38bdf8; font-weight:800; background:rgba(56,189,248,0.15); border:1px solid rgba(56,189,248,0.35); border-radius:3px; padding:1px 4px; display:inline-block;" title="Target and Stoploss calculated from ${modeTag} Price ₹${curRefPrice.toFixed(2)} (Entry fill was ₹${curAvgPrice.toFixed(2)})">🎯 ${modeTag}: ₹${curRefPrice.toFixed(2)}</span>
                        </div>
                    `;
            } else {
                refPriceBadgeHtml = `<div id="live_ref_badge_${idx}" style="margin-top:2px;"></div>`;
            }

            let refBasisSelectHtml = "";
            if (isOpen) {
                const curRefLabel = curRefPrice > 0 ? `₹${curRefPrice.toFixed(1)}` : 'Avg';
                const entryMinStr = (pos.entry_time || '09:37:00').substring(0, 5);
                const cCloseVal = pos.entry_candle_close || (pos.reference_price_mode === 'ENTRY_CANDLE_CLOSE' && pos.reference_price ? pos.reference_price : null) || pos.avg_ent_price || pos.reference_price;
                const cCloseStr = (cCloseVal && Number(cCloseVal) > 0) ? ` - ₹${Number(cCloseVal).toFixed(2)}` : '';
                refBasisSelectHtml = `
                        <div style="margin-top:3px;">
                            <select id="live_ref_select_${idx}" class="straddle-select" style="padding:1px 2px; font-size:8.5px; height:18px; font-weight:700; color:#38bdf8; background:#0b1120; border-color:#0284c7; border-radius:3px; cursor:pointer;" onchange="changeLegReferenceBasis('${stratId}', '${pos.id || ''}', ${idx}, this.value)" title="Choose basis for Target & Stoploss: Entry Fill Price, Entry Candle Close, Specific Minute Candle (e.g. 10:20), Current LTP, or Custom Price">
                                <option value="" disabled selected>Ref: ${curRefLabel} ▾</option>
                                <option value="AVERAGE_ENTRY">Avg Fill (₹${avgEntPrice})</option>
                                <option value="ENTRY_CANDLE_CLOSE">Entry Candle Close (${entryMinStr}${cCloseStr})</option>
                                <option value="PREV_CANDLE_CLOSE">Prior Minute (T-1) Close (0ms Parity)</option>
                                <option value="ENTRY_CANDLE_OPEN">Entry Candle Open</option>
                                <option value="SPECIFIC_TIME">Minute Candle Time (HH:MM)...</option>
                                <option value="UNDERLYING_CLOSE">Underlying Spot Close</option>
                                <option value="CURRENT_LTP">Current LTP (₹${Number(pos.ltp || 0).toFixed(1)})</option>
                                <option value="CUSTOM">Custom Price...</option>
                            </select>
                        </div>
                    `;
            }

            // Per Leg Target (Interactive ONLY for live filled/partially filled trades)
            const isTgtBroker = (pos.target_order_placement === "Place in advance" || pos.target_order_placement === "With Broker" || pos.target_order_placement === "BROKER" || !!pos.target_broker_order_id || !!pos.last_target_broker_order_id);
            let tgtStatusHtml = "";
            if (pos.target_cancelled_unexpectedly) {
                tgtStatusHtml = `
                        <div style="margin-top:2px; display:flex; flex-direction:column; gap:2px;">
                            <span style="font-size:9px; color:#f87171; font-weight:800; background:rgba(239,68,68,0.15); border:1px solid rgba(239,68,68,0.4); border-radius:3px; padding:1px 4px; display:inline-block;" title="Target order was cancelled on broker! Algo has taken over and is monitoring on platform.">⚠️ CANCELLED ON BROKER</span>
                            <button type="button" class="straddle-btn" style="padding:1px 6px; font-size:9px; font-weight:700; background:#0284c7; color:#fff; border:none; border-radius:3px; cursor:pointer;" onclick="resendLegOrderToBroker('${stratId}', '${pos.id || ''}', ${idx}, 'TARGET', '${sym}')">🏦 Re-send to Broker</button>
                        </div>
                    `;
            } else if (pos.target_broker_order_id) {
                const tOidStr = String(pos.target_broker_order_id);
                const isMultiT = tOidStr.includes(",");
                const dispT = isMultiT ? `${tOidStr.split(",")[0]} (+${tOidStr.split(",").length - 1} slices)` : tOidStr;
                tgtStatusHtml = `<span style="font-size:9px; color:#10b981; font-weight:700;" title="Live Target on Kotak Neo: Orders #${tOidStr}">🏦 #${dispT}</span>`;
            } else if (pos.last_target_broker_order_id) {
                const lastTOidStr = String(pos.last_target_broker_order_id);
                const isMultiLT = lastTOidStr.includes(",");
                const dispLT = isMultiLT ? `${lastTOidStr.split(",")[0]} (+${lastTOidStr.split(",").length - 1} slices)` : lastTOidStr;
                tgtStatusHtml = `<span style="font-size:9px; color:#10b981; font-weight:700;" title="Kotak Neo Target Orders #${lastTOidStr}">🏦 #${dispLT}</span>`;
            } else if (pos.target_placement_error && isTgtBroker) {
                tgtStatusHtml = `
                        <div style="margin-top:2px; display:flex; flex-direction:column; gap:2px;">
                            <span style="font-size:9px; color:#f59e0b; font-weight:800; background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.4); border-radius:3px; padding:1px 4px; display:inline-block;" title="${pos.target_placement_error}">⚠️ BROKER PLACEMENT FAILED</span>
                            <button type="button" class="straddle-btn" style="padding:1px 6px; font-size:9px; font-weight:700; background:#0284c7; color:#fff; border:none; border-radius:3px; cursor:pointer;" onclick="resendLegOrderToBroker('${stratId}', '${pos.id || ''}', ${idx}, 'TARGET', '${sym}')">🏦 Retry Broker Placement</button>
                        </div>
                    `;
            } else if (isTgtBroker) {
                tgtStatusHtml = `<span style="font-size:9px; color:#10b981; font-weight:700;" title="Placed with Broker (Limit)">🏦 Broker</span>`;
            } else {
                tgtStatusHtml = `<span style="font-size:9px; color:#94a3b8; font-weight:600;" title="Monitored by Straddle Engine platform">💻 Platform</span>`;
            }

            let targetHtml = "-";
            if (isInteractiveRow) {
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
                        <div id="live_tgt_sub_${idx}" style="font-size:10px; color:#38bdf8; font-weight:700; margin-top:2px; display:flex; align-items:center; justify-content:space-between; gap:4px;">
                            <span>TGT: ₹${pos.target_price ? Number(pos.target_price).toFixed(2) : '-'}</span>
                            <select id="live_tgt_placement_${idx}" class="straddle-select" style="padding:1px 2px; font-size:9px; font-weight:700; border-radius:3px; color:${isTgtBroker ? '#10b981' : '#94a3b8'}; background:#0b1120; border-color:${isTgtBroker ? '#059669' : '#334155'}; cursor:pointer;" onchange="changeLiveLegPlacement('${stratId}', '${pos.id || ''}', ${idx}, 'TARGET', this.value)" title="Choose Target placement: Broker (advance Limit order) or Platform (continuous monitoring)">
                                <option value="Place in advance" ${isTgtBroker ? 'selected' : ''}>🏦 Broker</option>
                                <option value="Keep with self" ${!isTgtBroker ? 'selected' : ''}>💻 Platform</option>
                            </select>
                        </div>
                        ${tgtStatusHtml}
                    `;
            } else if (isError) {
                const rawTgtV = pos.target_value ? Math.abs(Number(pos.target_value)) : null;
                const tgtValLabel = rawTgtV ? `${rawTgtV}${pos.target_type && pos.target_type.includes("Pts") ? " Pts" : "%"}` : "-";
                targetHtml = `<div style="color:#94a3b8; font-size:11px; font-weight:600;"><span class="tgt-label-badge" style="opacity:0.8;">TGT: ${tgtValLabel}</span></div>`;
            } else if (pos.target_price && pos.target_price > 0) {
                const rawTgtV = pos.target_value !== undefined ? Math.abs(Number(pos.target_value)) : 45;
                const tgtLabel = pos.target_type_label || `TGT: ${rawTgtV}%`;
                targetHtml = `
                        <div style="white-space:nowrap;"><span class="tgt-label-badge">${tgtLabel}</span> <span class="straddle-val-badge">${Number(pos.target_price).toFixed(2)}</span></div>
                        ${tgtStatusHtml ? `<div style="font-size:9.5px; margin-top:2px; font-family:var(--font-mono); white-space:nowrap;">${tgtStatusHtml}</div>` : ''}
                    `;
            } else if (pos.target_value && pos.target_value > 0) {
                const rawTgtV = Math.abs(Number(pos.target_value));
                const tgtLabel = `TGT: ${rawTgtV}%`;
                targetHtml = `
                        <div style="white-space:nowrap;"><span class="tgt-label-badge">${tgtLabel}</span> <span class="straddle-val-badge">-</span></div>
                        ${tgtStatusHtml ? `<div style="font-size:9.5px; margin-top:2px; font-family:var(--font-mono); white-space:nowrap;">${tgtStatusHtml}</div>` : ''}
                    `;
            }

            // Per Leg Stoploss (Interactive ONLY for live filled/partially filled trades)
            const isSlBroker = (pos.sl_order_placement === "Place in advance" || pos.sl_order_placement === "With Broker" || pos.sl_order_placement === "BROKER" || !!pos.sl_broker_order_id || !!pos.last_sl_broker_order_id);
            let slStatusHtml = "";
            if (pos.sl_cancelled_unexpectedly) {
                slStatusHtml = `
                        <div style="margin-top:2px; display:flex; flex-direction:column; gap:2px;">
                            <span style="font-size:9px; color:#f87171; font-weight:800; background:rgba(239,68,68,0.15); border:1px solid rgba(239,68,68,0.4); border-radius:3px; padding:1px 4px; display:inline-block;" title="Stoploss order was cancelled on broker! Algo has taken over and is monitoring on platform.">⚠️ CANCELLED ON BROKER</span>
                            <button type="button" class="straddle-btn" style="padding:1px 6px; font-size:9px; font-weight:700; background:#0284c7; color:#fff; border:none; border-radius:3px; cursor:pointer;" onclick="resendLegOrderToBroker('${stratId}', '${pos.id || ''}', ${idx}, 'SL', '${sym}')">🏦 Re-send to Broker</button>
                        </div>
                    `;
            } else if (pos.sl_broker_order_id) {
                const sOidStr = String(pos.sl_broker_order_id);
                const isMultiS = sOidStr.includes(",");
                const dispS = isMultiS ? `${sOidStr.split(",")[0]} (+${sOidStr.split(",").length - 1} slices)` : sOidStr;
                slStatusHtml = `<span style="font-size:9px; color:#10b981; font-weight:700;" title="Live Stoploss on Kotak Neo: Orders #${sOidStr}">🏦 #${dispS}</span>`;
            } else if (pos.last_sl_broker_order_id) {
                const lastSOidStr = String(pos.last_sl_broker_order_id);
                const isMultiLS = lastSOidStr.includes(",");
                const dispLS = isMultiLS ? `${lastSOidStr.split(",")[0]} (+${lastSOidStr.split(",").length - 1} slices)` : lastSOidStr;
                slStatusHtml = `<span style="font-size:9px; color:#10b981; font-weight:700;" title="Kotak Neo Stoploss Orders #${lastSOidStr}">🏦 #${dispLS}</span>`;
            } else if (pos.sl_placement_error && isSlBroker) {
                slStatusHtml = `
                        <div style="margin-top:2px; display:flex; flex-direction:column; gap:2px;">
                            <span style="font-size:9px; color:#f59e0b; font-weight:800; background:rgba(245,158,11,0.15); border:1px solid rgba(245,158,11,0.4); border-radius:3px; padding:1px 4px; display:inline-block;" title="${pos.sl_placement_error}">⚠️ BROKER PLACEMENT FAILED</span>
                            <button type="button" class="straddle-btn" style="padding:1px 6px; font-size:9px; font-weight:700; background:#0284c7; color:#fff; border:none; border-radius:3px; cursor:pointer;" onclick="resendLegOrderToBroker('${stratId}', '${pos.id || ''}', ${idx}, 'SL', '${sym}')">🏦 Retry Broker Placement</button>
                        </div>
                    `;
            } else if (isSlBroker) {
                slStatusHtml = `<span style="font-size:9px; color:#10b981; font-weight:700;" title="Placed with Broker (SL-L)">🏦 Broker</span>`;
            } else {
                slStatusHtml = `<span style="font-size:9px; color:#94a3b8; font-weight:600;" title="Monitored by Straddle Engine platform">💻 Platform</span>`;
            }

            let slHtml = "-";
            if (isInteractiveRow) {
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
                        <div id="live_sl_sub_${idx}" style="font-size:10px; color:#f87171; font-weight:700; margin-top:2px; display:flex; align-items:center; justify-content:space-between; gap:4px;">
                            <span>SL: ₹${pos.sl_price ? Number(pos.sl_price).toFixed(2) : '-'}</span>
                            <select id="live_sl_placement_${idx}" class="straddle-select" style="padding:1px 2px; font-size:9px; font-weight:700; border-radius:3px; color:${isSlBroker ? '#10b981' : '#94a3b8'}; background:#0b1120; border-color:${isSlBroker ? '#059669' : '#334155'}; cursor:pointer;" onchange="changeLiveLegPlacement('${stratId}', '${pos.id || ''}', ${idx}, 'SL', this.value)" title="Choose Stoploss placement: Broker (advance SL order) or Platform (continuous monitoring)">
                                <option value="Place in advance" ${isSlBroker ? 'selected' : ''}>🏦 Broker</option>
                                <option value="Keep with self" ${!isSlBroker ? 'selected' : ''}>💻 Platform</option>
                            </select>
                        </div>
                        ${slStatusHtml}
                    `;
            } else if (isError) {
                const rawSlV = pos.sl_value ? Math.abs(Number(pos.sl_value)) : null;
                const slValLabel = rawSlV ? `-${rawSlV}${pos.sl_type && pos.sl_type.includes("Pts") ? " Pts" : "%"}` : "-";
                slHtml = `<div style="color:#94a3b8; font-size:11px; font-weight:600;"><span class="sl-label-badge" style="opacity:0.8;">SL: ${slValLabel}</span></div>`;
            } else if (pos.sl_price && pos.sl_price > 0) {
                const rawSlV = pos.sl_value !== undefined ? Math.abs(Number(pos.sl_value)) : 65;
                const slLabel = (pos.sl_type_label || `SL: -${rawSlV}%`).replace(/--+/, '-');
                let origSlLabel = (pos.original_sl_label || (pos.sl_value ? `ORIGINAL SL: -${rawSlV}%` : 'ORIGINAL SL: -65%')).replace(/--+/, '-');
                if (origSlLabel && !origSlLabel.toUpperCase().startsWith("ORIGINAL")) {
                    origSlLabel = `ORIGINAL SL: ${origSlLabel.replace(/^SL:\s*/i, '')}`;
                }
                origSlLabel = origSlLabel.replace(/--+/, '-');
                const cleanSl = slLabel.replace(/^SL:\s*/i, '').trim();
                const cleanOrig = origSlLabel.replace(/^ORIGINAL SL:\s*/i, '').trim();
                const isTrailed = !!pos.sl_trailed || (pos.original_sl_price && Math.abs(Number(pos.sl_price) - Number(pos.original_sl_price)) > 0.05);
                const showOrig = isTrailed || (cleanSl && cleanOrig && cleanSl !== cleanOrig);

                slHtml = `
                        <div style="white-space:nowrap;"><span class="sl-label-badge">${slLabel}</span> <span class="straddle-val-badge">${Number(pos.sl_price).toFixed(2)}</span></div>
                        ${slStatusHtml ? `<div style="font-size:9.5px; margin-top:2px; font-family:var(--font-mono); white-space:nowrap;">${slStatusHtml}</div>` : ''}
                        ${showOrig ? `<div class="orig-sl-sub" style="font-size:9px; margin-top:1px;">${origSlLabel}</div>` : ''}
                    `;
            } else if (pos.sl_value && pos.sl_value > 0) {
                const rawSlV = Math.abs(Number(pos.sl_value));
                const slLabel = `SL: -${rawSlV}%`;
                const origSlLabel = `ORIGINAL SL: -${rawSlV}%`;
                slHtml = `
                        <div style="white-space:nowrap;"><span class="sl-label-badge">${slLabel}</span> <span class="straddle-val-badge">-</span></div>
                        ${slStatusHtml ? `<div style="font-size:9.5px; margin-top:2px; font-family:var(--font-mono); white-space:nowrap;">${slStatusHtml}</div>` : ''}
                    `;
            }

            // Avg Exit Price & Exit Time
            const avgExitPrice = (pos.avg_exit_price && pos.avg_exit_price > 0) ? Number(pos.avg_exit_price).toFixed(2) : '-';
            const exitTime = (isClosed && pos.exit_time) ? pos.exit_time : (pos.avg_exit_price && pos.avg_exit_price > 0 ? (pos.exit_time || '-') : '-');

            // Exit Reason
            let exitReasonHtml = `<span style="color:#64748b; font-size:11px;">-</span>`;
            if (pos.exit_reason && pos.exit_reason !== "-") {
                const rStr = String(pos.exit_reason).trim();
                const rLower = rStr.toLowerCase();
                if (rLower.includes("hedge shift") || rLower.includes("shift hedge") || rLower.includes("realigned to")) {
                    exitReasonHtml = `<span class="straddle-reason-pill entry-shifted" title="${rStr}">🔄 ${rStr}</span>`;
                } else if (rLower.includes("hedge")) {
                    exitReasonHtml = `<span class="straddle-reason-pill entry-hedge" title="${rStr}">🛡️ ${rStr}</span>`;
                } else if (rLower.includes("target")) {
                    exitReasonHtml = `<span class="exit-reason-tgt" title="${rStr}">🎯 ${rStr}</span>`;
                } else if (rLower.includes("stoploss") || rLower.includes("sl")) {
                    exitReasonHtml = `<span class="exit-reason-sl" title="${rStr}">🛑 ${rStr}</span>`;
                } else if (rLower.includes("square off") || rLower.includes("manual") || rLower.includes("exit all")) {
                    const exitLabel = (rLower === "square off" || rLower === "square off all") ? "Square Off (Broker Exit)" : rStr;
                    exitReasonHtml = `<span class="straddle-reason-pill exit-reason-squareoff" style="display:inline-block; font-size:9.5px; font-weight:800; color:#fbbf24; background:rgba(245,158,11,0.18); border:1px solid rgba(245,158,11,0.45); border-radius:3px; padding:2px 6px; letter-spacing:0.3px;" title="${rStr}">🚨 ${exitLabel}</span>`;
                } else if (rLower.includes("time") || rLower.includes("eod") || rLower.includes("market close")) {
                    exitReasonHtml = `<span class="straddle-reason-pill" style="display:inline-block; font-size:9.5px; font-weight:800; color:#c084fc; background:rgba(168,85,247,0.18); border:1px solid rgba(168,85,247,0.4); border-radius:3px; padding:2px 6px;" title="${rStr}">⏰ ${rStr}</span>`;
                } else if (rLower.includes("mtm")) {
                    exitReasonHtml = `<span class="straddle-reason-pill" style="display:inline-block; font-size:9.5px; font-weight:800; color:#f87171; background:rgba(239,68,68,0.18); border:1px solid rgba(239,68,68,0.4); border-radius:3px; padding:2px 6px;" title="${rStr}">📊 ${rStr}</span>`;
                } else {
                    exitReasonHtml = `<span class="straddle-reason-pill entry-initial" title="${rStr}">${rStr}</span>`;
                }
            }

            // Entry Reason (Tracking origin, parent leg, re-execute, hedge shift, adopted from Kotak Neo)
            let entryReasonHtml = `<span style="color:#64748b; font-size:11px;">-</span>`;

            const isReEntryReason = pos.is_re_entry || (pos.entry_reason && (String(pos.entry_reason).toLowerCase().includes("re-execute") || String(pos.entry_reason).toLowerCase().includes("re-entry")));
            if (isAdopted && !isReEntryReason) {
                const sSide = String(pos.side || "SELL").toUpperCase();
                const sType = String(pos.option_type || (String(pos.symbol || "").includes("CE") ? "CE" : "PE")).toUpperCase();
                let adoptLabel = pos.entry_reason && String(pos.entry_reason).trim() !== "-" && !String(pos.entry_reason).startsWith("Initial")
                    ? String(pos.entry_reason).trim()
                    : (pos.is_associated_hedge ? `Adopted Hedge from Kotak Neo (${sType})` : `Adopted from Kotak Neo [${sSide} ${sType}]`);
                if (!adoptLabel.startsWith("📥")) adoptLabel = `📥 ${adoptLabel}`;
                entryReasonHtml = `<span class="straddle-reason-pill entry-adopted" title="${adoptLabel}">${adoptLabel}</span>`;
            } else if (pos.entry_reason && pos.entry_reason !== "-") {
                const eStr = String(pos.entry_reason).trim();
                const eLower = eStr.toLowerCase();
                const eUpper = eStr.toUpperCase();
                if (eLower.includes("hedge") && (eLower.includes("shift") || eLower.includes("re-aligned") || eLower.includes("replaced"))) {
                    entryReasonHtml = `<span class="straddle-reason-pill entry-shifted" title="${eStr}">🔄 ${eStr}</span>`;
                } else if (eLower.includes("hedge")) {
                    entryReasonHtml = `<span class="straddle-reason-pill entry-hedge" title="${eStr}">🛡️ ${eStr}</span>`;
                } else if (eLower.includes("re-execute") || eLower.includes("re-entry") || eLower.includes("re-cost") || pos.is_re_entry || (pos.re_execute_count || 0) > 0) {
                    // Determine option type (CE or PE)
                    let optType = pos.re_execute_leg_type || "";
                    if (!optType) {
                        if (eUpper.includes(" CE ") || eUpper.includes(" CE-") || eUpper.includes(" CALL ") || (pos.symbol && pos.symbol.includes(" CE "))) optType = "CE";
                        else if (eUpper.includes(" PE ") || eUpper.includes(" PE-") || eUpper.includes(" PUT ") || (pos.symbol && pos.symbol.includes(" PE "))) optType = "PE";
                        else optType = pos.option_type || (String(pos.symbol || "").includes("CE") ? "CE" : "PE");
                    }
                    // Determine trigger type (STOPLOSS or TARGET)
                    let trigger = pos.re_execute_trigger || "";
                    if (!trigger) {
                        if (eUpper.includes("STOPLOSS") || eUpper.includes(" SL ") || eUpper.includes("SL HIT") || eUpper.includes("STOP LOSS")) trigger = "STOPLOSS";
                        else if (eUpper.includes("TARGET") || eUpper.includes(" TGT ") || eUpper.includes("TARGET HIT") || eUpper.includes("TGT HIT")) trigger = "TARGET";
                    }
                    // Determine count
                    let count = pos.re_execute_count || 0;
                    if (!count) {
                        const m = eStr.match(/#(\d+)/);
                        if (m) count = parseInt(m[1], 10);
                        else count = 1;
                    }
                    const isCe = (optType === "CE");
                    const optColor = isCe ? "#38bdf8" : "#e879f9";
                    const optBorder = isCe ? "rgba(56,189,248,0.4)" : "rgba(232,121,249,0.4)";
                    const optBg = isCe ? "rgba(56,189,248,0.12)" : "rgba(232,121,249,0.12)";
                    let triggerBadge = "";
                    if (trigger === "STOPLOSS") {
                        triggerBadge = `<span style="display:inline-block; padding:1px 5px; font-size:9.5px; font-weight:800; border-radius:3px; background:rgba(239,68,68,0.22); color:#f87171; border:1px solid rgba(239,68,68,0.45); letter-spacing:0.3px;">🔴 STOPLOSS</span>`;
                    } else if (trigger === "TARGET") {
                        triggerBadge = `<span style="display:inline-block; padding:1px 5px; font-size:9.5px; font-weight:800; border-radius:3px; background:rgba(16,185,129,0.22); color:#34d399; border:1px solid rgba(16,185,129,0.45); letter-spacing:0.3px;">🟢 TARGET</span>`;
                    }
                    let detailText = "";
                    const bracketMatch = eStr.match(/\[(.*?)\]/);
                    if (bracketMatch) {
                        detailText = bracketMatch[1].replace(/Triggered by\s*/i, "").trim();
                    } else if (!eStr.startsWith("⚡ Re-Execute") && !eStr.startsWith("⚡ RE-EXECUTE")) {
                        detailText = eStr;
                    }
                    entryReasonHtml = `
                            <div style="display:flex; flex-direction:column; gap:3px; align-items:flex-start;">
                                <div style="display:inline-flex; align-items:center; gap:5px; flex-wrap:wrap;">
                                    <span style="font-size:11px; font-weight:900; font-family:var(--font-mono); padding:2px 6px; border-radius:4px; background:${optBg}; color:${optColor}; border:1px solid ${optBorder}; display:inline-flex; align-items:center; gap:3px;">
                                        ⚡ ${optType} Re-Execute #${count}
                                    </span>
                                    ${triggerBadge}
                                </div>
                                ${detailText ? `<div style="font-size:9.5px; color:#94a3b8; line-height:1.2; word-break:break-word; font-family:var(--font-mono);" title="${eStr}">${detailText}</div>` : ''}
                            </div>
                        `;
                } else {
                    entryReasonHtml = `<span class="straddle-reason-pill entry-initial" title="${eStr}">📌 ${eStr}</span>`;
                }
            } else if (pos.is_associated_hedge) {
                const pType = pos.hedge_parent_type || "MAIN";
                entryReasonHtml = `<span class="straddle-reason-pill entry-hedge">🛡️ HEDGE (${pType}) [Protecting Main]</span>`;
            } else if (pos.is_re_entry || (pos.re_execute_count || 0) > 0) {
                const reCount = pos.re_execute_count || 1;
                const optType = pos.re_execute_leg_type || pos.option_type || (String(pos.symbol || "").includes("CE") ? "CE" : "PE");
                entryReasonHtml = `<span class="straddle-reason-pill entry-reentry">⚡ ${optType} RE-EXECUTE #${reCount}</span>`;
            } else {
                const sSide = String(pos.side || "SELL").toUpperCase();
                const sType = String(pos.option_type || (String(pos.symbol || "").includes("CE") ? "CE" : "PE")).toUpperCase();
                entryReasonHtml = `<span class="straddle-reason-pill entry-initial">📌 Initial Entry [${sSide} ${sType}]</span>`;
            }

            // Action buttons (Save on live change + Exit + Detach + Combined Placement)
            let actionHtml = `<span class="badge-closed-tag" style="font-size:9.5px; font-weight:800; color:#64748b; background:rgba(100,116,139,0.12); border:1px solid rgba(100,116,139,0.25); border-radius:3px; padding:1.5px 6px; letter-spacing:0.3px;">EXITED</span>`;
            if (isInteractiveRow) {
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
                            <div style="margin-top:2px; width:100%;">
                                <select class="straddle-select" style="padding:1px 2px; font-size:9px; font-weight:700; width:100%; border-radius:3px; color:#38bdf8; background:#0f172a; border-color:#0284c7; cursor:pointer;" onchange="changeLiveLegPlacementCombined('${stratId}', '${pos.id || ''}', ${idx}, this.value)" title="Fast combined placement: Broker vs Platform for Target and Stoploss">
                                    <option value="" disabled selected>⚡ Placement ▾</option>
                                    <option value="BOTH_BROKER">Both Broker 🏦</option>
                                    <option value="BOTH_SELF">Both Platform 💻</option>
                                    <option value="SL_BROKER_TGT_SELF">SL Broker + TG Plat</option>
                                    <option value="TGT_BROKER_SL_SELF">TG Broker + SL Plat</option>
                                </select>
                            </div>
                        </div>
                    `;
            } else if (isError) {
                actionHtml = `
                        <button type="button" class="straddle-btn btn-primary" style="padding:2px 7px; font-size:10px; font-weight:700;" onclick="retrySingleStraddleLeg('${stratId}', '${pos.id || ''}', ${idx})" title="Retry placing this leg">
                            🔄 Retry
                        </button>
                    `;
            } else if (isOpen) {
                actionHtml = `
                        <button type="button" class="straddle-btn btn-danger" style="padding:2px 7px; font-size:10.5px; font-weight:700;" onclick="squareOffSingleLegIndex(${idx})" title="Square off this leg on broker">
                            Exit
                        </button>
                    `;
            }

            // LTP & Dynamic Color Tick
            const ltp = Number(pos.ltp || pos.avg_ent_price || 0.0);
            const prevLtp = prevMap[sym] || ltp;
            let ltpClass = "";
            if (ltp > prevLtp) ltpClass = "straddle-ltp-up";
            else if (ltp < prevLtp) ltpClass = "straddle-ltp-down";
            prevMap[sym] = ltp;

            let ltpColor = '#cbd5e1';
            if (isOpen) {
                ltpColor = (side === 'SELL' ? (ltp <= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171') : (ltp >= (pos.avg_ent_price || 1) ? '#10b981' : '#f87171'));
            } else if (isClosed) {
                const exitP = Number(pos.avg_exit_price || 0);
                if (exitP > 0) {
                    ltpColor = (side === 'SELL') ? (ltp <= exitP ? '#10b981' : '#f87171') : (ltp >= exitP ? '#10b981' : '#f87171');
                } else {
                    ltpColor = '#cbd5e1';
                }
            }

            // MTM (₹) & PnL %
            const mtm = Number(pos.mtm || 0.0);
            const mtmClass = (mtm >= 0) ? "straddle-mtm-pos" : "straddle-mtm-neg";
            const mtmFormatted = mtm.toFixed(2);

            let pnlPctVal = (pos.pnl_pct !== undefined && pos.pnl_pct !== null) ? Number(pos.pnl_pct) : null;
            if (pnlPctVal === null && pos.avg_ent_price && Number(pos.avg_ent_price) > 0) {
                const entP = Number(pos.avg_ent_price);
                pnlPctVal = (side === "SELL") ? ((entP - ltp) / entP) * 100 : ((ltp - entP) / entP) * 100;
            }

            let pnlPctBadgeHtml = "";
            if (pnlPctVal !== null && !isNaN(pnlPctVal)) {
                const isPos = pnlPctVal >= 0;
                const pnlSign = isPos ? "+" : "";
                const pnlPctStr = `${pnlSign}${pnlPctVal.toFixed(2)}%`;
                const pnlBg = isPos ? "rgba(16,185,129,0.18)" : "rgba(244,63,94,0.18)";
                const pnlColor = isPos ? "#10b981" : "#f43f5e";
                const pnlBorder = isPos ? "rgba(16,185,129,0.4)" : "rgba(244,63,94,0.4)";
                const pnlTitle = isOpen ? `Live PnL: ${pnlPctStr} (Track against Target / Stoploss %)` : `Realized PnL: ${pnlPctStr}`;
                pnlPctBadgeHtml = `<span class="straddle-pnl-pct" style="display:inline-block; font-size:10.5px; font-weight:800; font-family:var(--font-mono); padding:1.5px 5px; border-radius:4px; background:${pnlBg}; color:${pnlColor}; border:1px solid ${pnlBorder};" title="${pnlTitle}">${pnlPctStr}</span>`;
            }

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

            // Check if cancellation toast needs to be shown
            if (pos.target_cancelled_unexpectedly || pos.sl_cancelled_unexpectedly) {
                if (!window._straddleToastedCancellations) window._straddleToastedCancellations = new Set();
                const cKey = `${sym}_${pos.target_cancelled_unexpectedly ? 'TGT' : ''}_${pos.sl_cancelled_unexpectedly ? 'SL' : ''}_${pos.entry_time || ''}`;
                if (!window._straddleToastedCancellations.has(cKey)) {
                    window._straddleToastedCancellations.add(cKey);
                    const orderName = (pos.target_cancelled_unexpectedly && pos.sl_cancelled_unexpectedly) ? "Target & Stoploss" : (pos.target_cancelled_unexpectedly ? "Target" : "Stoploss");
                    showStraddleToast(`⚠️ ${sym}: ${orderName} order cancelled on broker! Algo actively monitoring on platform.`, "warning", 6000);
                }
            }

            const isChecked = window._straddleSelectedLegIndices.has(idx) || (pos.id && window._straddleSelectedLegIds.has(pos.id));

            html += `
                    <tr class="${rowClass}" data-leg-index="${idx}" data-leg-id="${pos.id || ''}" data-order-id="${pos.order_id || pos.broker_order_id || ''}" data-status="${rawStatus}" data-entry-reason="${pos.entry_reason || ''}" data-exit-reason="${pos.exit_reason || ''}" data-symbol="${sym}" data-sl-broker-oid="${pos.sl_broker_order_id || ''}" data-tgt-broker-oid="${pos.target_broker_order_id || ''}" data-sl-cancelled="${!!pos.sl_cancelled_unexpectedly}" data-tgt-cancelled="${!!pos.target_cancelled_unexpectedly}" data-ref-price="${pos.reference_price || ''}" data-sl-price="${pos.sl_price || ''}" data-tgt-price="${pos.target_price || ''}">
                        <td class="pos-col-select" style="text-align:center;">
                            <input type="checkbox" class="straddle-checkbox straddle-leg-checkbox" data-index="${idx}" data-leg-id="${pos.id || ''}" data-status="${isError ? 'ERROR' : (isOpen ? 'OPEN' : 'CLOSED')}" ${isChecked ? 'checked' : ''} ${isClosed ? 'disabled style="opacity:0.25; cursor:not-allowed;" title="Position is already closed"' : ''} onchange="onStraddlePositionCheckboxChange(this, ${idx}, '${pos.id || ''}')">
                        </td>
                        <td class="pos-col-status">${statusHtml}</td>
                        <td class="pos-col-side"><span class="straddle-side-badge ${sideClass}">${side}</span></td>
                        <td class="pos-col-sym">${symHtml}</td>
                        <td class="pos-col-qty">${qtyHtml}</td>
                        <td class="pos-col-ent-price">
                            <div class="straddle-ent-price-val" style="font-weight:700; color:#f8fafc; font-family:var(--font-mono);">${avgEntPrice}</div>
                            <div class="straddle-subtext">${entryTime}</div>
                            ${refPriceBadgeHtml}
                            ${refBasisSelectHtml}
                        </td>
                        <td class="pos-col-ent-reason" style="white-space:normal; min-width:130px; max-width:170px; word-break:break-word; line-height:1.25;">${entryReasonHtml}</td>
                        <td class="pos-col-target">${targetHtml}</td>
                        <td class="pos-col-sl">${slHtml}</td>
                        <td class="pos-col-exit-price">
                            <div style="font-weight:700; color:#f8fafc; font-family:var(--font-mono);">${avgExitPrice}</div>
                            ${exitTime !== '-' ? `<div class="straddle-subtext">${exitTime}</div>` : ''}
                        </td>
                        <td class="pos-col-exit-reason" style="white-space:normal; max-width:125px; word-break:break-word; line-height:1.25;">${exitReasonHtml}</td>
                        <td class="pos-col-ltp" style="text-align:right;">
                            <span class="straddle-ltp ${ltpClass}" style="color:${ltpColor}; font-weight:700; font-family:var(--font-mono);" ${isClosed && pos.avg_exit_price ? `title="Live Market LTP: ₹${ltp.toFixed(2)} (Exited @ ₹${Number(pos.avg_exit_price).toFixed(2)})"` : ''}>${ltp > 0 ? ltp.toFixed(2) : '-'}</span>
                        </td>
                        <td class="pos-col-mtm" style="text-align:right; white-space:nowrap;">
                            <div style="display:inline-flex; align-items:center; justify-content:flex-end; gap:6px;">
                                <span class="straddle-mtm ${mtmClass}" style="font-weight:800; font-family:var(--font-mono);">${mtmFormatted}</span>
                                ${pnlPctBadgeHtml}
                            </div>
                        </td>
                        <td class="pos-col-action" style="text-align:center;">${actionHtml}</td>
                    </tr>
                `;

            // Quantiply Unexpected Order Cancellation Warning Banner (for OPEN legs)
            if ((pos.sl_cancelled_unexpectedly || pos.target_cancelled_unexpectedly) && !isError) {
                const legId = pos.id || '';
                const isBothCancelled = pos.sl_cancelled_unexpectedly && pos.target_cancelled_unexpectedly;
                const isSlCancelled = pos.sl_cancelled_unexpectedly;
                const isTgtCancelled = pos.target_cancelled_unexpectedly;
                const titleText = isBothCancelled ?
                    "Target & Stoploss orders cancelled on Broker" :
                    (isTgtCancelled ? "Target order cancelled on Broker" : "Stoploss order cancelled on Broker");
                const subText = isBothCancelled ?
                    `Both Target and Stoploss orders were cancelled on Broker. The algo is actively monitoring live prices on the platform (SL ₹${pos.sl_price || 0}, TG ₹${pos.target_price || 0}) and will auto-exit when breached. You can re-send orders to the broker matching engine anytime.` :
                    (isTgtCancelled ?
                        `Target order was cancelled on Broker. The algo is actively monitoring live price on the platform (TG ₹${pos.target_price || 0}) and will auto-exit on hit. You can re-send the Target order to the broker matching engine anytime.` :
                        `Stoploss order was cancelled on Broker. The algo is actively monitoring live price on the platform (SL ₹${pos.sl_price || 0}) and will auto-exit on hit. You can re-send the Stoploss order to the broker matching engine anytime.`);

                html += `
                        <tr class="straddle-error-banner-row">
                            <td colspan="14">
                                <div class="straddle-error-banner-cell" style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:12px; background:rgba(245, 158, 11, 0.12); border:1px solid rgba(245, 158, 11, 0.45); padding:10px 16px; border-radius:6px; margin: 4px 0;">
                                    <div style="flex:1; min-width:280px;">
                                        <div style="display:flex; align-items:center; gap:8px;">
                                            <span style="font-size:16px;">⚠️</span>
                                            <span style="color:#f87171; font-weight:800; font-size:12px;">${titleText} (Client or Broker Cancellation)</span>
                                        </div>
                                        <div style="color:#fbbf24; font-size:11px; margin-top:4px; font-style:italic; line-height:1.4;">
                                            ${subText}
                                        </div>
                                    </div>
                                    <div style="display:flex; align-items:center; gap:8px;">
                                        ${isBothCancelled || isSlCancelled ? `
                                            <button type="button" class="algo-btn-retry" style="background:linear-gradient(135deg, #0284c7, #0ea5e9); color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:5px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:5px; box-shadow:0 2px 6px rgba(14,165,233,0.4);" onclick="resendLegOrderToBroker('${stratId}', '${legId}', ${idx}, 'SL', '${sym}')" title="Place a fresh Stoploss order on broker for this leg">
                                                🔄 RE-SEND SL ORDER
                                            </button>
                                        ` : ''}
                                        ${isBothCancelled || isTgtCancelled ? `
                                            <button type="button" class="algo-btn-retry" style="background:linear-gradient(135deg, #059669, #10b981); color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:5px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:5px; box-shadow:0 2px 6px rgba(16,185,129,0.4);" onclick="resendLegOrderToBroker('${stratId}', '${legId}', ${idx}, 'TARGET', '${sym}')" title="Place a fresh Target order on broker for this leg">
                                                🔄 RE-SEND TARGET ORDER
                                            </button>
                                        ` : ''}
                                        ${isBothCancelled ? `
                                            <button type="button" class="algo-btn-retry" style="background:linear-gradient(135deg, #7c3aed, #8b5cf6); color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:5px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:5px; box-shadow:0 2px 6px rgba(139,92,246,0.4);" onclick="resendLegOrderToBroker('${stratId}', '${legId}', ${idx}, 'BOTH', '${sym}')" title="Place fresh Target and Stoploss orders on broker">
                                                🔄 RE-SEND BOTH
                                            </button>
                                        ` : ''}
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
            }

            // If error, append error banner directly underneath row with streamlined, unambiguous actions
            if (isError) {
                const legId = pos.id || '';
                const errDetail = pos.error_message || pos.error || "Order placement rejected by broker / exchange RMS.";
                const marginInfo = parseMarginError(errDetail);
                let marginBadgeHtml = "";
                let marketClosedBadgeHtml = "";
                if (/adapter.*logged\s*off/i.test(errDetail)) {
                    marketClosedBadgeHtml = `
                            <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-top:4px;">
                                <span style="background:rgba(245,158,11,0.22); color:#fcd34d; font-weight:700; font-size:11px; padding:3px 10px; border-radius:4px; font-family:var(--font-mono); border:1px solid rgba(245,158,11,0.35);">
                                    ⏸️ Exchange Closed: Market is closed (Exchange Gateway is offline on weekends/holidays). Order routing is 100% verified, orders will execute when market opens (Mon 9:00 AM MCX / 9:15 AM NSE).
                                </span>
                            </div>
                        `;
                }
                if (marginInfo && marginInfo.hasMarginDetails) {
                    marginBadgeHtml = `
                            <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap; margin-top:4px;">
                                <span style="background:rgba(56,189,248,0.15); color:#38bdf8; font-weight:700; font-size:11px; padding:2px 8px; border-radius:4px; font-family:var(--font-mono);">💳 Available: ₹${marginInfo.available.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                                <span style="background:rgba(245,158,11,0.15); color:#fbbf24; font-weight:700; font-size:11px; padding:2px 8px; border-radius:4px; font-family:var(--font-mono);">⚡ Required: ₹${marginInfo.required.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                                <span style="background:rgba(239,68,68,0.22); color:#f87171; font-weight:700; font-size:11px; padding:2px 8px; border-radius:4px; font-family:var(--font-mono);">🔴 Shortfall: ₹${marginInfo.shortfall.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                        `;
                }

                const safeMarginInfoJson = marginInfo ? JSON.stringify(marginInfo).replace(/'/g, "\\'") : "null";

                html += `
                        <tr class="straddle-error-banner-row" data-error-leg-index="${idx}">
                            <td colspan="14">
                                <div class="straddle-error-banner-cell" style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:10px; background:rgba(239, 68, 68, 0.12); border:1px solid rgba(239, 68, 68, 0.35); padding:10px 14px; border-radius:6px; margin: 4px 0;">
                                    <div style="display:flex; flex-direction:column; gap:3px; flex:1; min-width:260px;">
                                        <div style="display:flex; align-items:center; gap:8px;">
                                            <span class="straddle-error-banner-icon" style="font-size:16px;">⚠️</span>
                                            <span class="straddle-error-banner-text" style="color:#fca5a5; font-size:11.5px; font-weight:600; line-height:1.4;">
                                                ${errDetail}
                                            </span>
                                        </div>
                                        ${marginBadgeHtml}
                                        ${marketClosedBadgeHtml}
                                    </div>
                                    <div style="display:flex; align-items:center; gap:8px; flex-wrap:wrap;">
                                        ${marginInfo && marginInfo.hasMarginDetails ? `
                                            <button type="button" class="algo-btn-retry" style="background:#0284c7; color:#ffffff; font-weight:700; font-size:11px; padding:6px 12px; border-radius:4px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:4px; box-shadow:0 1px 3px rgba(0,0,0,0.3);" onclick='autoFitErrorLegLots(${idx}, ${safeMarginInfoJson})' title="Automatically adjust lots to fit within available cash balance">
                                                ⚙️ Auto-Fit Lots
                                            </button>
                                        ` : ''}
                                        <button type="button" class="algo-btn-retry" style="background:#10b981; color:#ffffff; font-weight:700; font-size:11px; padding:6px 14px; border-radius:4px; border:none; cursor:pointer; display:inline-flex; align-items:center; gap:4px; box-shadow:0 1px 3px rgba(0,0,0,0.3);" onclick="retrySingleStraddleLegInline('${stratId}', '${legId}', ${idx})" title="Immediately retry placing order with the Lots & Shift Strike set above">
                                            ⚡ Retry Order
                                        </button>
                                        <button type="button" class="algo-btn-rectify" style="background:#334155; border:1px solid #475569; color:#f1f5f9; font-weight:700; font-size:11px; padding:6px 10px; border-radius:4px; cursor:pointer; display:inline-flex; align-items:center; gap:4px;" onclick="openRectifyLegModal('${stratId}', '${legId}', ${idx})" title="Open leg builder modal to edit configuration">
                                            🛠️ Edit
                                        </button>
                                        <button type="button" class="straddle-btn" style="background:rgba(239,68,68,0.2); border:1px solid rgba(239,68,68,0.4); color:#fca5a5; font-weight:700; font-size:11px; padding:6px 10px; border-radius:4px; cursor:pointer;" onclick="dismissErrorLeg('${stratId}', '${legId}', ${idx})" title="Dismiss and remove this error leg">
                                            ✕ Dismiss
                                        </button>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `;
            }
        });
        tbody.innerHTML = html;
        if (activeId) {
            const el = document.getElementById(activeId);
            if (el) {
                el.focus();
                if (selStart !== null && selEnd !== null && typeof el.setSelectionRange === "function") {
                    try { el.setSelectionRange(selStart, selEnd); } catch (e) { }
                }
            }
        }
    }

    // Sync master select all checkbox state
    const masterCb = document.getElementById("straddleSelectAllLegs");
    if (masterCb) {
        masterCb.checked = (positions && positions.length > 0 && positions.every((p, i) => window._straddleSelectedLegIndices.has(i) || (p.id && window._straddleSelectedLegIds.has(p.id))));
    }

    // Update action buttons state
    onStraddlePositionCheckboxChange();
}

/**
 * Updates summary re-execution count badges (CE SL, CE Target, PE SL, PE Target)
 * adjacent to MTM Target in the lower summary strip.
 */
function updateStraddleReexecBadges(positions) {
    const elCeSl = document.getElementById("reexecCountCeSl") || document.getElementById("reExecCountCeSl");
    const elCeTgt = document.getElementById("reexecCountCeTgt") || document.getElementById("reExecCountCeTgt") || document.getElementById("reexecCountCeTarget") || document.getElementById("reExecCountCeTarget");
    const elPeSl = document.getElementById("reexecCountPeSl") || document.getElementById("reExecCountPeSl");
    const elPeTgt = document.getElementById("reexecCountPeTgt") || document.getElementById("reExecCountPeTgt") || document.getElementById("reexecCountPeTarget") || document.getElementById("reExecCountPeTarget");
    if (!elCeSl && !elCeTgt && !elPeSl && !elPeTgt) return;

    const strat = window._straddleState?.activeStrategy;
    const topState = window._straddleState || {};

    let ceSl = 0, ceTgt = 0, peSl = 0, peTgt = 0;

    // Check strategy-level counts
    if (strat && strat.re_execution_counts) {
        ceSl = Math.max(ceSl, Number(strat.re_execution_counts.ce_sl || 0));
        ceTgt = Math.max(ceTgt, Number(strat.re_execution_counts.ce_target || 0));
        peSl = Math.max(peSl, Number(strat.re_execution_counts.pe_sl || 0));
        peTgt = Math.max(peTgt, Number(strat.re_execution_counts.pe_target || 0));
    }
    // Check top-level engine counts
    if (topState.re_execution_counts) {
        ceSl = Math.max(ceSl, Number(topState.re_execution_counts.ce_sl || 0));
        ceTgt = Math.max(ceTgt, Number(topState.re_execution_counts.ce_target || 0));
        peSl = Math.max(peSl, Number(topState.re_execution_counts.pe_sl || 0));
        peTgt = Math.max(peTgt, Number(topState.re_execution_counts.pe_target || 0));
    }

    // Direct scan across all available position collections
    const combinedPositions = [
        ...(Array.isArray(positions) ? positions : []),
        ...(strat && Array.isArray(strat.positions) ? strat.positions : []),
        ...(strat && Array.isArray(strat.closed_positions) ? strat.closed_positions : []),
        ...(topState.positions || []),
        ...(topState.closed_positions || [])
    ];

    const seenReexecPos = new Set();
    let scanCeSl = 0, scanCeTgt = 0, scanPeSl = 0, scanPeTgt = 0;

    combinedPositions.forEach(pos => {
        if (!pos) return;
        const pUid = String(pos.order_id || pos.id || `${pos.symbol}_${pos.entry_time}`);
        if (seenReexecPos.has(pUid)) return;
        seenReexecPos.add(pUid);

        const isRe = !!(pos.is_re_entry || (pos.re_execute_count || 0) > 0 || 
                        (pos.entry_reason && String(pos.entry_reason).toLowerCase().includes("re-execute")));
        if (!isRe) return;

        let legType = String(pos.re_execute_leg_type || "").toUpperCase();
        if (!legType) {
            const sym = String(pos.symbol || pos.trading_symbol || "");
            const eStr = String(pos.entry_reason || "");
            if (sym.includes(" CE ") || sym.includes("CE") || eStr.includes(" CE ")) legType = "CE";
            else if (sym.includes(" PE ") || sym.includes("PE") || eStr.includes(" PE ")) legType = "PE";
            else legType = String(pos.option_type || "").toUpperCase();
        }

        let trigger = String(pos.re_execute_trigger || "").toUpperCase();
        if (!trigger) {
            const eStr = String(pos.entry_reason || "").toUpperCase();
            if (eStr.includes("STOPLOSS") || eStr.includes(" SL ") || eStr.includes("SL HIT")) trigger = "STOPLOSS";
            else if (eStr.includes("TARGET") || eStr.includes(" TGT ") || eStr.includes("TARGET HIT")) trigger = "TARGET";
        }

        if (legType === "CE") {
            if (trigger === "STOPLOSS") scanCeSl++;
            else if (trigger === "TARGET") scanCeTgt++;
        } else if (legType === "PE") {
            if (trigger === "STOPLOSS") scanPeSl++;
            else if (trigger === "TARGET") scanPeTgt++;
        }
    });

    ceSl = Math.max(ceSl, scanCeSl);
    ceTgt = Math.max(ceTgt, scanCeTgt);
    peSl = Math.max(peSl, scanPeSl);
    peTgt = Math.max(peTgt, scanPeTgt);

    if (elCeSl) elCeSl.textContent = ceSl;
    if (elCeTgt) elCeTgt.textContent = ceTgt;
    if (elPeSl) elPeSl.textContent = peSl;
    if (elPeTgt) elPeTgt.textContent = peTgt;
}

// =========================================================================
// ALGO POSITIONS FILTERING & SORTING CONTROLS
// =========================================================================

function toggleAlgoClosedTrades() {
    window._straddleAlgoShowClosed = !window._straddleAlgoShowClosed;
    const strat = window._straddleState?.activeStrategy;
    if (strat) {
        renderStraddlePositions(window._straddleState?.positions || strat.positions || [], strat.group_mtm || 0.0, strat.mtm || 0.0);
    }
}

function setAlgoTradeFilter(filter) {
    window._straddleAlgoTradeFilter = filter;
    document.querySelectorAll(".algo-filter-pill").forEach(btn => {
        const f = btn.getAttribute("data-filter");
        const isActive = (f === filter);
        btn.classList.toggle("active", isActive);

        if (isActive) {
            if (f === "ce") {
                btn.style.background = "#10b981";
                btn.style.color = "#ffffff";
                btn.style.border = "1px solid #10b981";
            } else if (f === "pe") {
                btn.style.background = "#ef4444";
                btn.style.color = "#ffffff";
                btn.style.border = "1px solid #ef4444";
            } else if (f === "main") {
                btn.style.background = "rgba(245,158,11,0.22)";
                btn.style.color = "#f59e0b";
                btn.style.border = "1px solid #f59e0b";
            } else if (f === "hedge") {
                btn.style.background = "rgba(56,189,248,0.22)";
                btn.style.color = "#38bdf8";
                btn.style.border = "1px solid #38bdf8";
            } else if (f === "open") {
                btn.style.background = "rgba(16,185,129,0.2)";
                btn.style.color = "#10b981";
                btn.style.border = "1px solid #10b981";
            } else if (f === "closed") {
                btn.style.background = "#334155";
                btn.style.color = "#f8fafc";
                btn.style.border = "1px solid #64748b";
            } else {
                btn.style.background = "#1e293b";
                btn.style.color = "#f8fafc";
                btn.style.border = "1px solid rgba(255,255,255,0.2)";
            }
        } else {
            if (f === "ce") {
                btn.style.background = "rgba(16,185,129,0.12)";
                btn.style.color = "#10b981";
                btn.style.border = "1px solid rgba(16,185,129,0.45)";
            } else if (f === "pe") {
                btn.style.background = "rgba(239,68,68,0.12)";
                btn.style.color = "#ef4444";
                btn.style.border = "1px solid rgba(239,68,68,0.45)";
            } else if (f === "main") {
                btn.style.background = "transparent";
                btn.style.color = "#f59e0b";
                btn.style.border = "1px solid transparent";
            } else if (f === "hedge") {
                btn.style.background = "transparent";
                btn.style.color = "#38bdf8";
                btn.style.border = "1px solid transparent";
            } else if (f === "open") {
                btn.style.background = "transparent";
                btn.style.color = "#10b981";
                btn.style.border = "1px solid transparent";
            } else if (f === "closed") {
                btn.style.background = "transparent";
                btn.style.color = "#94a3b8";
                btn.style.border = "1px solid transparent";
            } else {
                btn.style.background = "transparent";
                btn.style.color = "#cbd5e1";
                btn.style.border = "1px solid transparent";
            }
        }
    });
    const strat = window._straddleState?.activeStrategy;
    if (strat) {
        renderStraddlePositions(window._straddleState?.positions || strat.positions || [], strat.group_mtm || 0.0, strat.mtm || 0.0);
    }
}

function onAlgoPositionsSortChange(sortVal) {
    window._straddleAlgoSortOrder = sortVal;
    const strat = window._straddleState?.activeStrategy;
    if (strat) {
        renderStraddlePositions(window._straddleState?.positions || strat.positions || [], strat.group_mtm || 0.0, strat.mtm || 0.0);
    }
}

window.toggleAlgoClosedTrades = toggleAlgoClosedTrades;
window.setAlgoTradeFilter = setAlgoTradeFilter;
window.onAlgoPositionsSortChange = onAlgoPositionsSortChange;

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
    const actionableCbs = Array.from(document.querySelectorAll(".straddle-leg-checkbox:not(:disabled)"));
    if (masterCb) {
        if (actionableCbs.length === 0) {
            masterCb.checked = false;
            masterCb.disabled = true;
            masterCb.title = "No open positions to select";
        } else {
            masterCb.disabled = false;
            masterCb.checked = actionableCbs.every(cb => cb.checked);
            masterCb.title = "Select / deselect all open positions";
        }
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

    if (btnSquareOffSel) {
        if (openCount > 0) {
            btnSquareOffSel.style.display = "inline-flex";
            btnSquareOffSel.innerHTML = `<span>🚨 Square off selected (${openCount})</span>`;
            btnSquareOffSel.style.borderColor = "#f43f5e";
            btnSquareOffSel.style.color = "#f43f5e";
            btnSquareOffSel.style.fontWeight = "800";
        } else {
            btnSquareOffSel.style.display = "none";
        }
    }

    updateTerminalActionBar();
}

function toggleSelectAllStraddleLegs(masterCb) {
    try {
        const isChecked = masterCb ? !!masterCb.checked : false;
        if (!window._straddleSelectedLegIndices) window._straddleSelectedLegIndices = new Set();
        if (!window._straddleSelectedLegIds) window._straddleSelectedLegIds = new Set();

        const tbody = document.getElementById("straddlePositionsTableBody");
        const cbs = tbody ? tbody.querySelectorAll("input[type='checkbox'].straddle-leg-checkbox, input[type='checkbox']") : document.querySelectorAll(".straddle-leg-checkbox");

        cbs.forEach(cb => {
            if (cb.disabled) {
                cb.checked = false;
                return;
            }
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

        const activeStrat = window._straddleState?.activeStrategy;
        if (isChecked && activeStrat && activeStrat.positions) {
            activeStrat.positions.forEach((p, i) => {
                const s = String(p.status || "OPEN").toUpperCase();
                const isClosed = (s === "COMPLETE" || s === "CLOSED" || s === "EXITED");
                if (!isClosed) {
                    window._straddleSelectedLegIndices.add(i);
                    if (p.id) window._straddleSelectedLegIds.add(p.id);
                }
            });
        } else if (!isChecked) {
            window._straddleSelectedLegIndices.clear();
            window._straddleSelectedLegIds.clear();
        }

        onStraddlePositionCheckboxChange();
    } catch (e) {
        console.error("Error in toggleSelectAllStraddleLegs:", e);
    }
}
window.toggleSelectAllStraddleLegs = toggleSelectAllStraddleLegs;

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

    const savedEdit = (legIndex !== undefined && window._straddleErrorLegEdits) ? window._straddleErrorLegEdits[legIndex] : null;
    const und = (pos?.underlying || legCfg?.underlying || strat.underlying || "NIFTY").toUpperCase();
    const lotSize = Number(pos?.lot_size || legCfg?.lot_size || (typeof getLotSizeForUnderlying === "function" ? getLotSizeForUnderlying(und) : (und === "BANKNIFTY" ? 15 : (und === "FINNIFTY" ? 25 : (und === "MIDCPNIFTY" ? 50 : (und === "SENSEX" ? 10 : 65))))));
    const currentLots = Number(savedEdit?.lots || pos?.lots || legCfg?.lots || 1);
    const side = (pos?.side || legCfg?.side || "SELL").toUpperCase();
    const prod = (pos?.product || legCfg?.product || "NRML").toUpperCase();
    const optType = (pos?.option_type || legCfg?.option_type || "CE").toUpperCase();
    const strike = String(savedEdit?.strike || pos?.strike_selection || legCfg?.strike_selection || "ATM").toUpperCase().replace("_", " ").trim();
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
        if (prevReqEl) prevReqEl.textContent = `₹ ${parsedErr.required.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        if (prevShortEl) prevShortEl.textContent = `₹ ${parsedErr.shortfall.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
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
        availEl.textContent = `₹ ${avail.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
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
        reqEl.textContent = `₹ ${requiredMargin.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    }

    const statEl = document.getElementById("rectifyStatusSurplusDisplay");
    const calcBadge = document.getElementById("rectifyLiveCalcBadge");
    const btnRetry = document.getElementById("btnRectifyAndRetry");

    if (statEl) {
        if (isSufficient) {
            statEl.innerHTML = `<span style="color:#10b981;">🟢 SUFFICIENT (+₹${diff.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })})</span>`;
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
            statEl.innerHTML = `<span style="color:#ef4444;">🔴 SHORTFALL: ₹${Math.abs(diff).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>`;
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
    window._straddleErrorLegEdits = {};
    window._straddlePendingLegEdits = {};

    const btnRetry = document.getElementById("btnStraddleRetryErrors");
    if (btnRetry) btnRetry.style.display = "none";
    const btnReset = document.getElementById("btnStraddleResetAlgo");
    if (btnReset) btnReset.style.display = "none";
    const errBanner = document.getElementById("straddleErrorBannerRow");
    if (errBanner) errBanner.style.display = "none";

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
            s.closed_positions = [];
            s.shadow_benchmark = [];
            s.mtm = 0.0;
            s.max_mtm = 0.0;
            s.min_mtm = 0.0;
            s.status = "READY";
            s.enabled = false;
            s.entry_error = "-";
            s.exit_error = "-";
            s.last_error = null;
            s.repair_needed = false;
            s.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
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
                    l.re_execute_count = 0;
                    l.repair_needed = false;
                });
            }
        }
    });

    activeStrat.positions = [];
    activeStrat.closed_positions = [];
    activeStrat.shadow_benchmark = [];
    activeStrat.mtm = 0.0;
    activeStrat.max_mtm = 0.0;
    activeStrat.min_mtm = 0.0;
    activeStrat.status = "READY";
    activeStrat.enabled = false;
    activeStrat.entry_error = "-";
    activeStrat.exit_error = "-";
    activeStrat.last_error = null;
    activeStrat.repair_needed = false;
    activeStrat.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
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
            l.re_execute_count = 0;
            l.repair_needed = false;
        });
    }

    if (window._straddleState) {
        window._straddleState.positions = [];
        window._straddleState.status = "READY";
        window._straddleState.entry_error = "-";
        window._straddleState.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
        window._straddleState.activeStrategy = activeStrat;
        window._straddleState.activeStrategyId = activeStrat.strategy_id;
    }

    const grpMtmEl = document.getElementById("straddleGroupMtmDisplay");
    const totMtmEl = document.getElementById("straddleTotalMtmDisplay");
    if (grpMtmEl) { grpMtmEl.textContent = "0.00"; grpMtmEl.style.color = "#38bdf8"; }
    if (totMtmEl) { totMtmEl.textContent = "0.00"; totMtmEl.style.color = "#10b981"; }
    const elCeSl = document.getElementById("reexecCountCeSl");
    const elCeTgt = document.getElementById("reexecCountCeTgt");
    const elPeSl = document.getElementById("reexecCountPeSl");
    const elPeTgt = document.getElementById("reexecCountPeTgt");
    if (elCeSl) elCeSl.textContent = "0";
    if (elCeTgt) elCeTgt.textContent = "0";
    if (elPeSl) elPeSl.textContent = "0";
    if (elPeTgt) elPeTgt.textContent = "0";

    updateStraddleReexecBadges([]);
    renderStraddlePositions([], 0.0, 0.0);
    renderStraddleAlgosTable(true);
    renderStraddleHeader(window._straddleState);
    if (typeof updateTerminalActionBar === "function") {
        updateTerminalActionBar(activeStrat, []);
    }
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
                window._straddleState.closed_positions = updated.closed_positions || [];
                populateStraddleSettingsForm(updated);
            }
            window._straddleState.closed_positions = [];
            window._straddleState.closedPositionsCache = [];
            await fetchStraddleState();
            await loadStraddleOpenPositions();
            renderStraddlePositions(window._straddleState.activeStrategy?.positions || [], 0.0, 0.0);
            renderStraddleAlgosTable(true);
            renderStraddleHeader(window._straddleState);
            if (typeof updateTerminalActionBar === "function") {
                updateTerminalActionBar(window._straddleState.activeStrategy, window._straddleState.activeStrategy?.positions || []);
            }
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

    const subtitleEl = document.getElementById("modalLegSettingsSubtitle") || document.getElementById("legSettingsSubTitle");
    if (subtitleEl) {
        subtitleEl.textContent = `(${und} ${side.toUpperCase()} ${prod.toUpperCase()} ${type.toUpperCase()})`;
    }

    // Populate strike select in modal with ATM, OTM 1-40, ITM 1-40 matching the main table
    const strikeSel = document.getElementById("modalLegStrikeSelect") || document.getElementById("legSettingsStrikeSelect");
    if (strikeSel) {
        strikeSel.innerHTML = "";
        const normStrike = curStrike.toUpperCase().replace("_", " ").trim();

        // ATM
        const atmOpt = document.createElement("option");
        atmOpt.value = "ATM";
        atmOpt.textContent = "ATM STRIKE";
        if (normStrike === "ATM" || normStrike === "ATM STRIKE") atmOpt.selected = true;
        strikeSel.appendChild(atmOpt);

        // OTM 1 to 40
        for (let i = 1; i <= 40; i++) {
            const opt = document.createElement("option");
            opt.value = `OTM ${i}`;
            opt.textContent = `OTM ${i}`;
            if (normStrike === `OTM ${i}` || normStrike === `OTM${i}`) opt.selected = true;
            strikeSel.appendChild(opt);
        }

        // ITM 1 to 40
        for (let i = 1; i <= 40; i++) {
            const opt = document.createElement("option");
            opt.value = `ITM ${i}`;
            opt.textContent = `ITM ${i}`;
            if (normStrike === `ITM ${i}` || normStrike === `ITM${i}`) opt.selected = true;
            strikeSel.appendChild(opt);
        }

        // Custom numeric strike fallback
        if (!isNaN(parseFloat(normStrike)) && !Array.from(strikeSel.options).some(o => o.value === normStrike)) {
            const opt = document.createElement("option");
            opt.value = normStrike;
            opt.textContent = normStrike;
            opt.selected = true;
            strikeSel.appendChild(opt);
        }
    }

    const multSel = document.getElementById("modalLegMultipleStrikeSelect") || document.getElementById("legSettingsMultipleSelect");
    if (multSel) {
        multSel.value = (curMult === "500/100 multiple" || curMult === "500_100_multiple") ? "500/100 multiple" : "None";
    }

    // Associated Dynamic Hedge Settings in Modal
    const isSell = (side.toUpperCase() === "SELL");
    const hCard = document.getElementById("legSettingsHedgeCard");
    if (hCard) {
        hCard.style.display = isSell ? "flex" : "none";
        const hEnabled = tr.querySelector(".leg-hedge-enabled") ? tr.querySelector(".leg-hedge-enabled").checked : false;
        const hInit = tr.querySelector(".leg-hedge-initial-otm")?.value || "20";
        const hThresh = tr.querySelector(".leg-hedge-max-thresh")?.value || "22";
        const hReset = tr.querySelector(".leg-hedge-reset-otm")?.value || "20";

        const chkHedge = document.getElementById("legSettingsHedgeEnabled");
        if (chkHedge) {
            chkHedge.checked = hEnabled;
            toggleLegModalHedgeFields(hEnabled);
        }
        const selInit = document.getElementById("legSettingsHedgeInitialOtm");
        if (selInit) {
            if (!Array.from(selInit.options).some(o => o.value === String(hInit))) {
                const opt = document.createElement("option");
                opt.value = hInit;
                opt.textContent = `OTM ${hInit}`;
                selInit.appendChild(opt);
            }
            selInit.value = hInit;
        }
        const inpThresh = document.getElementById("legSettingsHedgeMaxThresh");
        if (inpThresh) inpThresh.value = hThresh;
        const selReset = document.getElementById("legSettingsHedgeResetOtm");
        if (selReset) {
            if (!Array.from(selReset.options).some(o => o.value === String(hReset))) {
                const opt = document.createElement("option");
                opt.value = hReset;
                opt.textContent = `OTM ${hReset}`;
                selReset.appendChild(opt);
            }
            selReset.value = hReset;
        }
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

function toggleLegModalHedgeFields(isChecked) {
    const params = document.getElementById("legModalHedgeParams");
    if (params) {
        params.style.opacity = isChecked ? "1" : "0.5";
        params.style.pointerEvents = isChecked ? "auto" : "none";
    }
}

function saveLegSettingsModal() {
    const rowId = window._activeLegSettingsRowId;
    if (!rowId) {
        closeLegSettingsModal();
        return;
    }

    const tr = document.getElementById(rowId);
    if (tr) {
        const strikeSel = document.getElementById("modalLegStrikeSelect") || document.getElementById("legSettingsStrikeSelect");
        const multSel = document.getElementById("modalLegMultipleStrikeSelect") || document.getElementById("legSettingsMultipleSelect");

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

        // Save Associated Hedge settings into leg row
        const chkHedge = document.getElementById("legSettingsHedgeEnabled");
        const selInit = document.getElementById("legSettingsHedgeInitialOtm");
        const inpThresh = document.getElementById("legSettingsHedgeMaxThresh");
        const selReset = document.getElementById("legSettingsHedgeResetOtm");

        const legHedgeChk = tr.querySelector(".leg-hedge-enabled");
        if (legHedgeChk && chkHedge) legHedgeChk.checked = chkHedge.checked;
        const legHedgeInit = tr.querySelector(".leg-hedge-initial-otm");
        if (legHedgeInit && selInit) legHedgeInit.value = selInit.value;
        const legHedgeThresh = tr.querySelector(".leg-hedge-max-thresh");
        if (legHedgeThresh && inpThresh) legHedgeThresh.value = inpThresh.value;
        const legHedgeReset = tr.querySelector(".leg-hedge-reset-otm");
        if (legHedgeReset && selReset) legHedgeReset.value = selReset.value;

        const btnHedge = tr.querySelector(".leg-hedge-btn");
        if (btnHedge && chkHedge) {
            const isH = chkHedge.checked;
            const initV = selInit ? selInit.value : "20";
            const thrV = inpThresh ? inpThresh.value : "22";
            btnHedge.style.color = isH ? "#38bdf8" : "#64748b";
            btnHedge.style.borderColor = isH ? "#38bdf8" : "#334155";
            btnHedge.style.background = isH ? "rgba(56,189,248,0.1)" : "transparent";
            btnHedge.textContent = isH ? `OTM ${initV} (≤${thrV})` : "Hedge Off";
        }

        // Sync changes into in-memory activeStrategy
        if (typeof syncStrategyLegChange === "function") {
            syncStrategyLegChange(rowId);
        }
    }

    closeLegSettingsModal();
}

function onLegSideChanged(rowId, sideVal) {
    const tr = document.getElementById(rowId);
    if (!tr) return;
    const isSell = (String(sideVal).toUpperCase() === "SELL");
    const hedgeBox = tr.querySelector(".leg-hedge-container");
    const hedgeNa = tr.querySelector(".leg-hedge-na");
    if (hedgeBox) hedgeBox.style.display = isSell ? "flex" : "none";
    if (hedgeNa) hedgeNa.style.display = isSell ? "none" : "block";

    if (!isSell) {
        const chk = tr.querySelector(".leg-hedge-enabled");
        if (chk) chk.checked = false;
        const btn = tr.querySelector(".leg-hedge-btn");
        if (btn) {
            btn.style.color = "#64748b";
            btn.style.borderColor = "#334155";
            btn.style.background = "transparent";
            btn.textContent = "Hedge Off";
        }
    } else {
        const dynHedgeChecked = document.getElementById("cfgToggleDynamicHedges") ? document.getElementById("cfgToggleDynamicHedges").checked : true;
        const chk = tr.querySelector(".leg-hedge-enabled");
        if (chk && dynHedgeChecked) {
            chk.checked = true;
            const btn = tr.querySelector(".leg-hedge-btn");
            const initV = tr.querySelector(".leg-hedge-initial-otm")?.value || "20";
            const thrV = tr.querySelector(".leg-hedge-max-thresh")?.value || "22";
            if (btn) {
                btn.style.color = "#38bdf8";
                btn.style.borderColor = "#38bdf8";
                btn.style.background = "rgba(56,189,248,0.1)";
                btn.textContent = `OTM ${initV} (≤${thrV})`;
            }
        }
    }
    if (typeof syncStrategyLegChange === "function") {
        syncStrategyLegChange(rowId);
    }
}

function toggleLegHedgeRow(rowId, isChecked) {
    const tr = document.getElementById(rowId);
    if (!tr) return;
    const btn = tr.querySelector(".leg-hedge-btn");
    const initV = tr.querySelector(".leg-hedge-initial-otm")?.value || "20";
    const thrV = tr.querySelector(".leg-hedge-max-thresh")?.value || "22";
    if (btn) {
        btn.style.color = isChecked ? "#38bdf8" : "#64748b";
        btn.style.borderColor = isChecked ? "#38bdf8" : "#334155";
        btn.style.background = isChecked ? "rgba(56,189,248,0.1)" : "transparent";
        btn.textContent = isChecked ? `OTM ${initV} (≤${thrV})` : "Hedge Off";
    }
    if (typeof syncStrategyLegChange === "function") {
        syncStrategyLegChange(rowId);
    }
}

function toggleStraddleSettingsPanel(forceState = null) {
    const panel = document.getElementById("straddleSettingsPanel");
    if (!panel) return;
    const isCurrentlyVisible = panel.style.display === "block";
    const nextState = (forceState !== null) ? !!forceState : !isCurrentlyVisible;
    panel.style.display = nextState ? "block" : "none";

    const btnToggle = document.getElementById("btnStraddleToggleSettings");
    if (btnToggle) {
        btnToggle.innerHTML = nextState ? "✕ Close Settings" : "⚙️ Settings";
        if (nextState) {
            btnToggle.style.background = "#1e293b";
            btnToggle.style.borderColor = "#64748b";
        } else {
            btnToggle.style.background = "#2563eb";
            btnToggle.style.borderColor = "#3b82f6";
        }
    }

    if (nextState) {
        const curStrat = window._straddleState?.activeStrategy || (window._straddleState?.allStrategies && window._straddleState.allStrategies[0]);
        if (curStrat) {
            window._straddleState.activeStrategy = curStrat;
            window._straddleState.activeStrategyId = curStrat.strategy_id;
            populateStraddleSettingsForm(curStrat);
        }
    } else {
        if (window._straddleSettingsDirty && typeof saveStraddleStrategySettings === "function") {
            saveStraddleStrategySettings({ silent: true });
        }
    }
}
window.toggleStraddleSettingsPanel = toggleStraddleSettingsPanel;

function populateStraddleSettingsForm(s) {
    if (!s) return;
    window._straddleFormPopulatedFor = s.strategy_id;
    window._straddleSettingsDirty = false;

    // Attach dirty-tracking and debounced auto-save listener once to settings panel
    const panel = document.getElementById("straddleSettingsPanel");
    if (panel && !panel._dirtyListenersAttached) {
        panel._dirtyListenersAttached = true;
        const onSettingChanged = (e) => {
            if (e && e.target && e.target.tagName === "BUTTON" && !e.target.classList.contains("day-pill-btn") && !e.target.classList.contains("time-step-btn")) {
                return;
            }
            window._straddleSettingsDirty = true;
            if (window._straddleAutoSaveTimer) {
                clearTimeout(window._straddleAutoSaveTimer);
            }
            window._straddleAutoSaveTimer = setTimeout(() => {
                if (window._straddleSettingsDirty && typeof saveStraddleStrategySettings === "function") {
                    saveStraddleStrategySettings({ silent: true });
                }
            }, 1200);
        };
        panel.addEventListener("input", onSettingChanged);
        panel.addEventListener("change", onSettingChanged);
    }
    const errBanner = document.getElementById("stratSettingsErrorBanner");
    if (errBanner) {
        const hasPositions = s.positions && s.positions.length > 0;
        if (s.status === "TRADE_ACTIVE" || hasPositions || !s.entry_error || s.entry_error === "-") {
            errBanner.style.display = "none";
            errBanner.textContent = "";
        }
    }
    if (document.getElementById("cfgStrategyId")) {
        document.getElementById("cfgStrategyId").value = s.strategy_id || "";
    }
    if (document.getElementById("cfgStrategyName")) {
        document.getElementById("cfgStrategyName").value = s.name || "";
    }
    if (document.getElementById("cfgStrategyUnderlying")) {
        document.getElementById("cfgStrategyUnderlying").value = (s.underlying || "NIFTY").toUpperCase();
    }
    if (document.getElementById("cfgStrategyExecutionMode")) {
        document.getElementById("cfgStrategyExecutionMode").value = s.execution_mode || "LIVE_BROKER";
    }
    if (document.getElementById("cfgStrategyShadowPaper")) {
        document.getElementById("cfgStrategyShadowPaper").checked = (s.shadow_paper_trading !== false);
    }
    const titleEl = document.getElementById("straddleTerminalStrategyTitle");
    if (titleEl) {
        titleEl.textContent = s.name || s.strategy_id || "Custom Strategy";
    }
    const undBadge = document.getElementById("straddleTerminalUnderlyingBadge");
    if (undBadge) {
        undBadge.textContent = canonicalUnderlying(s.underlying || "NIFTY");
    }

    document.getElementById("cfgSegment").value = s.segment || "Options";
    const multType = (s.multiplier_type || "MANUAL").toUpperCase();
    setFormMultiplierMode(multType, false);
    document.getElementById("cfgMultiplier").value = s.multiplier || 1;
    const cushionInput = document.getElementById("cfgCushionMargin");
    if (cushionInput) {
        cushionInput.value = (s.cushion_margin !== undefined && s.cushion_margin !== null) ? s.cushion_margin : 15000;
    }
    updateFormAutoMultiplierPreview();
    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
        scheduleAutoMultiplierLiveRecalc();
    }
    if (typeof populateAutoSizingForm === "function") {
        populateAutoSizingForm(s);
    }
    const curUnd = (s.underlying || "NIFTY").toUpperCase();
    const isComm = ["CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM", "SILVER", "SILVERM", "COPPER"].includes(curUnd);
    const defaultSt = isComm ? "09:00:00" : "09:15:56";
    const defaultEt = isComm ? "23:30:00" : "15:29:50";
    let initSt = s.start_time || defaultSt;
    let initEt = s.end_time || defaultEt;
    if (isComm && (initEt === "15:29:50" || initEt === "15:30:00")) initEt = "23:30:00";
    if (isComm && initSt === "09:15:56") initSt = "09:00:00";
    setStraddleTimeInputValue("cfgStartTime", initSt);
    setStraddleTimeInputValue("cfgEndTime", initEt);
    initStraddleTimeSteppers();

    // No Re-Entry After Cutoff
    const noReCfg = s.no_reentry_after || {};
    const advCfg = s.advanced_settings || s.advanced || {};
    const noReEnabled = (noReCfg.enabled !== undefined) ? !!noReCfg.enabled : !!advCfg.no_reentry_after_enabled;
    const defaultNoReTime = isComm ? "23:00:00" : "15:00:00";
    const noReTime = noReCfg.time || advCfg.no_reentry_after_time || advCfg.re_entry_before_time || defaultNoReTime;
    const noReToggle = document.getElementById("cfgToggleNoReEntryAfter");
    if (noReToggle) {
        noReToggle.checked = noReEnabled;
    }
    setStraddleTimeInputValue("cfgNoReEntryAfterTime", noReTime);
    if (typeof syncNoReEntryAfterState === "function") {
        syncNoReEntryAfterState();
    }

    const moveSlEl = document.getElementById("cfgToggleMoveSlToCost");
    if (moveSlEl) {
        moveSlEl.checked = !!(s.move_sl_to_cost || (s.toggles && s.toggles.move_sl_to_cost));
        moveSlEl.closest(".straddle-toggle-chip")?.classList.toggle("chip-checked", moveSlEl.checked);
    }
    const waitTradeEl = document.getElementById("cfgToggleWaitAndTrade");
    if (waitTradeEl) {
        waitTradeEl.checked = !!(s.wait_and_trade || (s.toggles && s.toggles.wait_and_trade));
        waitTradeEl.closest(".straddle-toggle-chip")?.classList.toggle("chip-checked", waitTradeEl.checked);
    }
    const reEntryEl = document.getElementById("cfgToggleReEntry");
    if (reEntryEl) {
        reEntryEl.checked = (s.toggles && s.toggles.re_entry_active !== undefined) ? s.toggles.re_entry_active : (s.re_entry_enabled !== false);
        reEntryEl.closest(".straddle-toggle-chip")?.classList.toggle("chip-checked", reEntryEl.checked);
    }
    const rangeBreakEl = document.getElementById("cfgToggleRangeBreakout");
    if (rangeBreakEl) {
        rangeBreakEl.checked = !!(s.range_breakout || (s.toggles && s.toggles.range_breakout));
        rangeBreakEl.closest(".straddle-toggle-chip")?.classList.toggle("chip-checked", rangeBreakEl.checked);
    }
    const dynHedgeEl = document.getElementById("cfgToggleDynamicHedges");
    if (dynHedgeEl) {
        dynHedgeEl.checked = s.toggles ? (s.toggles.dynamic_hedging !== false) : (s.dynamic_hedging !== false);
        dynHedgeEl.closest(".straddle-toggle-chip")?.classList.toggle("chip-checked", dynHedgeEl.checked);
    }

    // Schedule days
    const stratDays = Array.isArray(s.schedule_days) ? s.schedule_days : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    document.querySelectorAll(".day-pill-btn").forEach(btn => {
        const d = btn.getAttribute("data-day");
        btn.classList.toggle("active", stratDays.includes(d));
    });

    setUnderlyingPriceSource(s.underlying_price_source || "Spot");
    setStraddleTradeType(s.trade_type || "Intraday");
    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }

    // MTM Calculation Mode (Overall vs Per Lot)
    const mtmMode = s.mtm_calc_mode || "Overall";
    if (typeof setMtmCalcMode === "function") {
        setMtmCalcMode(mtmMode);
    }

    // MTM Target & SL
    const mtm = s.mtm_target || {};
    const sl = s.mtm_stoploss || s.mtm_sl || {};

    const mtmSecToggle = document.getElementById("cfgToggleMtmSection");
    const mtmTgtToggle = document.getElementById("cfgToggleMtmTarget");
    const mtmSlToggle = document.getElementById("cfgToggleMtmSl");

    const mtmEnabled = (s.mtm_enabled !== undefined) ? !!s.mtm_enabled : ((mtm.type && mtm.type !== "None") || (sl.type && sl.type !== "None"));
    if (mtmSecToggle) mtmSecToggle.checked = mtmEnabled;

    if (mtmTgtToggle) {
        mtmTgtToggle.checked = (mtm.enabled !== undefined) ? !!mtm.enabled : (mtm.type !== "None");
    }
    if (mtmSlToggle) {
        mtmSlToggle.checked = (sl.enabled !== undefined) ? !!sl.enabled : (sl.type !== "None");
    }

    const mtmType = mtm.type || (mtm.profit_reaches ? "Lock and Trail" : "Lock and Trail");
    const tgtTypeEl = document.getElementById("cfgMtmTargetType");
    if (tgtTypeEl) tgtTypeEl.value = mtmType;

    const reachVal = (mtm.profit_reaches !== undefined) ? mtm.profit_reaches : (mtm.profit_lock_trail?.threshold !== undefined ? mtm.profit_lock_trail.threshold : 100000);
    const lockVal = (mtm.lock_profit !== undefined) ? mtm.lock_profit : (mtm.profit_lock_trail?.lock_profit !== undefined ? mtm.profit_lock_trail.lock_profit : 80000);
    const stepVal = (mtm.trail_step !== undefined) ? mtm.trail_step : (mtm.profit_lock_trail?.trail_step !== undefined ? mtm.profit_lock_trail.trail_step : 8000);
    const trailVal = (mtm.trail_val !== undefined) ? mtm.trail_val : (mtm.profit_lock_trail?.trail_profit !== undefined ? mtm.profit_lock_trail.trail_profit : 5000);

    if (document.getElementById("cfgMtmTargetReach")) document.getElementById("cfgMtmTargetReach").value = reachVal;
    if (document.getElementById("cfgMtmTargetLock")) document.getElementById("cfgMtmTargetLock").value = lockVal;
    if (document.getElementById("cfgMtmTargetTrailStep")) document.getElementById("cfgMtmTargetTrailStep").value = stepVal;
    if (document.getElementById("cfgMtmTargetTrailVal")) document.getElementById("cfgMtmTargetTrailVal").value = trailVal;
    if (document.getElementById("cfgMtmTargetValue")) document.getElementById("cfgMtmTargetValue").value = (mtm.value !== undefined) ? mtm.value : 10000;
    if (document.getElementById("cfgMtmTargetReEntry")) document.getElementById("cfgMtmTargetReEntry").value = mtm.re_entry || "None";
    if (document.getElementById("cfgMtmTargetAction")) document.getElementById("cfgMtmTargetAction").value = mtm.action || "Square off all positions";

    if (document.getElementById("cfgMtmSlType")) document.getElementById("cfgMtmSlType").value = sl.type || "None";
    if (document.getElementById("cfgMtmSlValue")) document.getElementById("cfgMtmSlValue").value = (sl.value !== undefined) ? sl.value : -5000;
    if (document.getElementById("cfgMtmSlAction")) document.getElementById("cfgMtmSlAction").value = sl.action || "Square off all positions";

    syncMtmFieldStates();

    // Premium Matching & Range
    const pm = s.premium_matching || {};
    const premSecToggle = document.getElementById("cfgTogglePremSection");
    if (premSecToggle) {
        premSecToggle.checked = !!pm.enabled;
    }
    const pmMode = pm.mode || "No matching of premiums";
    if (pmMode === "Match premiums with maximum difference") {
        const r = document.getElementById("radPremMaxDiff");
        if (r) r.checked = true;
    } else if (pmMode === "Premium close to") {
        const r = document.getElementById("radPremCloseTo");
        if (r) r.checked = true;
    } else if (pmMode === "Premium range") {
        const r = document.getElementById("radPremRange");
        if (r) r.checked = true;
    } else {
        const r = document.getElementById("radPremNone");
        if (r) r.checked = true;
    }
    if (document.getElementById("cfgPremMaxDiffVal")) {
        document.getElementById("cfgPremMaxDiffVal").value = pm.max_difference || "";
    }
    if (document.getElementById("cfgPremMaxDiffType")) {
        document.getElementById("cfgPremMaxDiffType").value = pm.max_difference_type || "Points";
    }
    if (document.getElementById("cfgPremCloseToVal")) {
        document.getElementById("cfgPremCloseToVal").value = pm.close_to || "";
    }
    if (document.getElementById("cfgPremRangeMin")) {
        document.getElementById("cfgPremRangeMin").value = pm.range_min || "";
    }
    if (document.getElementById("cfgPremRangeMax")) {
        document.getElementById("cfgPremRangeMax").value = pm.range_max || "";
    }

    const combToggle = document.getElementById("cfgToggleCombinedPrem");
    if (combToggle) {
        combToggle.checked = !!(pm.combined_premium_type && pm.combined_premium_type !== "None");
    }
    if (document.getElementById("cfgCombinedPremType")) {
        document.getElementById("cfgCombinedPremType").value = pm.combined_premium_type || "Lower than";
    }
    if (document.getElementById("cfgCombinedPremVal")) {
        document.getElementById("cfgCombinedPremVal").value = pm.combined_premium_value !== undefined ? pm.combined_premium_value : "";
    }
    if (document.getElementById("cfgPremFrequency")) {
        document.getElementById("cfgPremFrequency").value = pm.condition_frequency || "LTP (Continuous Monitoring)";
    }

    syncPremiumMatchingFieldStates();

    // Advanced sequencing
    const adv = s.advanced_settings || s.advanced || {};
    document.getElementById("cfgEntryOrderType").value = adv.entry_order_type || "Limit (L) / Stoploss Limit (SL-L)";
    if (document.getElementById("cfgHedgeOrderType")) {
        document.getElementById("cfgHedgeOrderType").value = adv.hedge_order_type || "Marketable Limit";
    }
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

    document.getElementById("cfgSlOrderPlacement").value = adv.sl_order_placement || "Place in advance";
    const tgtOrderEl = document.getElementById("cfgTargetOrderPlacement");
    if (tgtOrderEl) tgtOrderEl.value = adv.target_order_placement || "Place in advance";

    const reMethodEl = document.getElementById("cfgReExecuteMethod");
    if (reMethodEl) reMethodEl.value = adv.re_execute_method || "Immediate";
    const reDelayEl = document.getElementById("cfgReExecuteOrderDelaySec");
    if (reDelayEl) reDelayEl.value = (adv.re_execute_order_delay_sec !== undefined && adv.re_execute_order_delay_sec !== null) ? adv.re_execute_order_delay_sec : 0;
    const reFilterEl = document.getElementById("cfgReExecuteTriggerFilter");
    if (reFilterEl) reFilterEl.value = adv.re_execute_trigger_filter || "BOTH";
    const marginGraceEl = document.getElementById("cfgMarginExitGraceSec");
    if (marginGraceEl) marginGraceEl.value = adv.margin_error_grace_sec !== undefined ? String(adv.margin_error_grace_sec) : "15";
    const rePlacementEl = document.getElementById("cfgReEntryPlacement");
    if (rePlacementEl) rePlacementEl.value = adv.re_entry_placement || "Place in advance";
    const autoRelMarginEl = document.getElementById("cfgAutoReleaseMarginReEntry");
    if (autoRelMarginEl) autoRelMarginEl.checked = (adv.auto_release_margin_reentry !== false);

    const refModeEl = document.getElementById("cfgRefPriceMode");
    if (refModeEl) refModeEl.value = adv.reference_price_mode || "ENTRY_PRICE";
    const bmTimeEl = document.getElementById("cfgBenchmarkTime");
    if (bmTimeEl) bmTimeEl.value = adv.benchmark_time || "09:16:00";
    const bmScopeEl = document.getElementById("cfgBenchmarkScope");
    if (bmScopeEl) {
        bmScopeEl.value = adv.benchmark_scope || (adv.benchmark_initial_only === false ? "ALL_ENTRIES" : "INITIAL_ONLY");
    }
    const bmInitEl = document.getElementById("cfgBenchmarkInitialOnly");
    if (bmInitEl) bmInitEl.checked = (adv.benchmark_scope !== "ALL_ENTRIES" && adv.benchmark_initial_only !== false);
    const bmInterimEl = document.getElementById("cfgBenchmarkInterimMode");
    if (bmInterimEl) bmInterimEl.value = adv.benchmark_interim_mode || "FILL_PRICE";
    const bmMaxDevEl = document.getElementById("cfgBenchmarkMaxDevPct");
    if (bmMaxDevEl) bmMaxDevEl.value = (adv.max_benchmark_deviation_pct !== undefined && adv.max_benchmark_deviation_pct !== null) ? adv.max_benchmark_deviation_pct : 0;
    const bmBrokerActionEl = document.getElementById("cfgBenchmarkBrokerAction");
    if (bmBrokerActionEl) bmBrokerActionEl.value = adv.benchmark_broker_action || "AUTO_MODIFY";

    if (typeof onRefPriceModeChange === "function") {
        onRefPriceModeChange();
    }

    const slEl = document.getElementById("cfgSlOrderPlacement");
    if (slEl && !slEl._syncAttached) {
        slEl._syncAttached = true;
        slEl.addEventListener("change", (e) => {
            const val = e.target.value;
            document.querySelectorAll(".leg-sl-placement").forEach(el => {
                el.value = val;
                el.style.color = (val === "Place in advance" ? "#10b981" : "#94a3b8");
            });
            if (typeof syncBuilderAdvanceSlToTable === "function") {
                syncBuilderAdvanceSlToTable(val);
            }
            if (typeof syncStrategyLegChange === "function") syncStrategyLegChange();
        });
    }
    const tgtEl = document.getElementById("cfgTargetOrderPlacement");
    if (tgtEl && !tgtEl._syncAttached) {
        tgtEl._syncAttached = true;
        tgtEl.addEventListener("change", (e) => {
            const val = e.target.value;
            document.querySelectorAll(".leg-tgt-placement").forEach(el => {
                el.value = val;
                el.style.color = (val === "Place in advance" ? "#10b981" : "#94a3b8");
            });
            if (typeof syncStrategyLegChange === "function") syncStrategyLegChange();
        });
    }
    document.getElementById("cfgSlPlacementDelaySec").value = adv.sl_placement_delay_sec !== undefined ? adv.sl_placement_delay_sec : 0;
    document.getElementById("cfgMtmSlDelaySec").value = adv.mtm_sl_delay_sec !== undefined ? adv.mtm_sl_delay_sec : 0;

    document.getElementById("cfgAutoHandlingSl").checked = adv.auto_handling_sl_cancellation !== false;
    document.getElementById("cfgAutoExitMargin").checked = !!(adv.auto_exit_margin_error || adv.auto_exit_on_margin_error);
    if (typeof onReExecuteSettingsChange === "function") {
        onReExecuteSettingsChange();
    }

    // Render Leg rows
    const tbody = document.getElementById("straddleLegsBuilderBody");
    if (tbody) {
        tbody.innerHTML = "";
        const legs = s.legs || [];
        legs.forEach(leg => addStraddleLegRow(leg));
        if (typeof updateLegsCountBadge === "function") {
            updateLegsCountBadge();
        }
    }
}

function addStraddleLegRow(leg = null) {
    const tbody = document.getElementById("straddleLegsBuilderBody");
    if (!tbody) return;

    const rowId = (leg && leg.id) ? leg.id : ("leg_" + Date.now() + "_" + Math.floor(Math.random() * 1000));
    const l = leg || {
        id: rowId,
        side: "Sell",
        product: "NRML",
        strike_selection: "ATM",
        multiple_strike_selection: "None",
        option_type: "CE",
        lots: 1,
        associated_hedge: {
            enabled: true,
            initial_otm: 20,
            max_distance_threshold: 22,
            reset_otm: 20,
            exit_with_parent: true
        },
        target_type: "TGT: %",
        target_value: 45.0,
        target_re_execute_type: "RE EXECUTE",
        target_re_execute_count: 6,
        sl_type: "SL: %",
        sl_value: 66.0,
        sl_order_placement: document.getElementById("cfgSlOrderPlacement")?.value || "Place in advance",
        target_order_placement: document.getElementById("cfgTargetOrderPlacement")?.value || "Place in advance",
        tsl_type: "None",
        tsl_profit_pts: 0.0,
        tsl_trail_pts: 0.0,
        sl_re_execute_type: "RE EXECUTE",
        sl_re_execute_count: 1,
        action: "Square off leg",
        underlying: window._straddleState?.activeStrategy?.underlying || "NIFTY",
        expiry: "Weekly"
    };

    if (!leg && window._straddleState?.activeStrategy) {
        if (!Array.isArray(window._straddleState.activeStrategy.legs)) {
            window._straddleState.activeStrategy.legs = [];
        }
        window._straddleState.activeStrategy.legs.push(l);
    }

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
    const stratUnd = document.getElementById("cfgStrategyUnderlying")?.value || window._straddleState?.activeStrategy?.underlying || "NIFTY";
    const curUnd = (l.underlying || stratUnd).toUpperCase();
    const isCommodity = ["CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM", "SILVER", "SILVERM", "COPPER"].includes(curUnd);

    const underlyings = [
        "NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX",
        "CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM", "SILVER", "SILVERM", "COPPER"
    ];
    let undOptionsHtml = "";
    underlyings.forEach(u => {
        undOptionsHtml += `<option value="${u}" ${curUnd === u ? "selected" : ""}>${u}</option>`;
    });

    let expOptionsHtml = "";
    if (isCommodity) {
        const curExp = l.expiry || "Current Month";
        expOptionsHtml = `
            <option value="Current Month" ${curExp === "Current Month" || curExp === "Monthly" ? "selected" : ""}>Current Month</option>
            <option value="Next Month" ${curExp === "Next Month" || curExp === "Next Monthly" ? "selected" : ""}>Next Month</option>
            <option value="Far Month" ${curExp === "Far Month" ? "selected" : ""}>Far Month</option>
        `;
    } else {
        const curExp = l.expiry || "Weekly";
        expOptionsHtml = `
            <option value="Weekly" ${curExp === "Weekly" ? "selected" : ""}>Weekly</option>
            <option value="Next Weekly" ${curExp === "Next Weekly" ? "selected" : ""}>Next Weekly</option>
            <option value="Monthly" ${curExp === "Monthly" ? "selected" : ""}>Monthly</option>
        `;
    }

    const hasMult = (l.multiple_strike_selection === "500/100 multiple" || l.multiple_strike_selection === "500_100_multiple");

    const isSell = (String(l.side || "Sell").toLowerCase() === "sell");
    const dynHedgeChecked = document.getElementById("cfgToggleDynamicHedges") ? document.getElementById("cfgToggleDynamicHedges").checked : true;
    const hCfg = l.associated_hedge || {};
    const hEnabled = (hCfg.enabled !== undefined) ? !!hCfg.enabled : (isSell && dynHedgeChecked);
    const hInit = parseInt(hCfg.initial_otm, 10) || 20;
    const hThresh = parseInt(hCfg.max_distance_threshold, 10) || 22;
    const hReset = parseInt(hCfg.reset_otm, 10) || 20;

    const stratTgtAdv = document.getElementById("cfgTargetOrderPlacement")?.value;
    const stratSlAdv = document.getElementById("cfgSlOrderPlacement")?.value;
    const effectiveTgtPlacement = (stratTgtAdv === "Place in advance") ? "Place in advance" : (l.target_order_placement || stratTgtAdv || "Place in advance");
    const isTgtAdv = (effectiveTgtPlacement === "Place in advance" || effectiveTgtPlacement === "With Broker" || effectiveTgtPlacement === "BROKER");
    const effectiveSlPlacement = (stratSlAdv === "Place in advance") ? "Place in advance" : (l.sl_order_placement || stratSlAdv || "Place in advance");
    const isSlAdv = (effectiveSlPlacement === "Place in advance" || effectiveSlPlacement === "With Broker" || effectiveSlPlacement === "BROKER");

    const tr = document.createElement("tr");
    tr.id = rowId;
    tr.className = "straddle-leg-builder-row";
    tr.innerHTML = `
        <td class="leg-col-handle" style="text-align:center; cursor:grab; color:#64748b; font-size:14px; user-select:none;" title="Drag to reorder leg">☰</td>
        <td class="leg-col-side">
            <select class="straddle-select leg-side" onchange="onLegSideChanged('${rowId}', this.value)">
                <option value="Buy" ${l.side === "Buy" ? "selected" : ""}>Buy</option>
                <option value="Sell" ${l.side === "Sell" ? "selected" : ""}>Sell</option>
            </select>
        </td>
        <td class="leg-col-prod">
            <select class="straddle-select leg-product">
                <option value="NRML" ${l.product === "NRML" ? "selected" : ""}>NRML</option>
                <option value="MIS" ${l.product === "MIS" ? "selected" : ""}>MIS</option>
            </select>
        </td>
        <td class="leg-col-strike">
            <div style="display:flex; align-items:center; gap:3px;">
                <div class="straddle-stepper-box" style="height:28px;">
                    <select class="straddle-select leg-strike straddle-stepper-input" style="min-width:85px; border:none; background:transparent; font-size:11.5px; font-weight:700; padding:2px 4px; color:#f8fafc;" onchange="syncStrategyLegChange('${rowId}')">
                        ${strikeOptionsHtml}
                    </select>
                    <div class="straddle-stepper-arrows" style="width:16px;">
                        <button type="button" class="strike-step-btn" onclick="stepLegStrikeSelect('${rowId}', -1)" title="Prev Strike">▲</button>
                        <button type="button" class="strike-step-btn" onclick="stepLegStrikeSelect('${rowId}', 1)" title="Next Strike">▼</button>
                    </div>
                </div>
                <input type="hidden" class="leg-multiple-strike" value="${l.multiple_strike_selection || 'None'}">
                <button type="button" class="straddle-btn btn-outline leg-strike-cfg-btn" style="padding:2px 6px; font-size:11px;" title="Strike & 500/100 Multiple Settings" onclick="openLegSettingsModal('${rowId}')">⚙️</button>
                <span class="leg-mult-badge" style="display:${hasMult ? 'inline-block' : 'none'}; font-size:9px; font-weight:700; color:#38bdf8; background:rgba(56,189,248,0.15); border:1px solid rgba(56,189,248,0.3); border-radius:3px; padding:1px 3px;" title="500/100 Multiple Strike Active">500/100</span>
            </div>
        </td>
        <td class="leg-col-type">
            <select class="straddle-select leg-type" onchange="syncStrategyLegChange('${rowId}')">
                <option value="CE" ${l.option_type === "CE" ? "selected" : ""}>CE</option>
                <option value="PE" ${l.option_type === "PE" ? "selected" : ""}>PE</option>
            </select>
        </td>
        <td class="leg-col-lots">
            <div class="straddle-stepper-box" style="height:28px;">
                <input type="number" class="straddle-input leg-lots straddle-stepper-input" style="width:38px; border:none; background:transparent; font-size:12px; font-weight:700; text-align:center; padding:2px 2px; color:#f8fafc;" value="${l.lots || 1}" min="1" max="500" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
                <div class="straddle-stepper-arrows" style="width:16px;">
                    <button type="button" class="lot-step-btn" onclick="stepLegLotsInput('${rowId}', 1)" title="Lots +">▲</button>
                    <button type="button" class="lot-step-btn" onclick="stepLegLotsInput('${rowId}', -1)" title="Lots -">▼</button>
                </div>
            </div>
        </td>
        <td class="leg-col-hedge">
            <div class="leg-hedge-container" style="display:${isSell ? 'flex' : 'none'}; align-items:center; gap:3px;">
                <input type="checkbox" class="leg-hedge-enabled" ${hEnabled ? 'checked' : ''} onchange="toggleLegHedgeRow('${rowId}', this.checked)" title="Enable Associated Dynamic Hedge for margin reduction">
                <button type="button" class="straddle-btn btn-outline leg-hedge-btn" style="padding:2px 5px; font-size:10px; font-weight:700; color:${hEnabled ? '#38bdf8' : '#64748b'}; border-color:${hEnabled ? '#38bdf8' : '#334155'}; background:${hEnabled ? 'rgba(56,189,248,0.1)' : 'transparent'}; white-space:nowrap;" onclick="openLegSettingsModal('${rowId}')" title="Configure Initial OTM, Shift Threshold and Reset OTM">
                    ${hEnabled ? `OTM ${hInit} (≤${hThresh})` : 'Hedge Off'}
                </button>
                <input type="hidden" class="leg-hedge-initial-otm" value="${hInit}">
                <input type="hidden" class="leg-hedge-max-thresh" value="${hThresh}">
                <input type="hidden" class="leg-hedge-reset-otm" value="${hReset}">
            </div>
            <div class="leg-hedge-na" style="display:${isSell ? 'none' : 'block'}; text-align:center; color:#475569; font-size:11px; font-weight:600;">—</div>
        </td>
        <td class="leg-col-target">
            <div style="display:flex; flex-direction:column; gap:3px;">
                <div style="display:flex; gap:2px;">
                    <select class="straddle-select leg-tgt-type" style="width:85px;" onchange="syncLegRowDisabledFields(this.closest('tr')); syncStrategyLegChange('${rowId}');">
                        ${tgtOptionsHtml}
                    </select>
                    <input type="number" class="straddle-input leg-tgt-val" style="width:50px;" value="${l.target_value !== undefined ? l.target_value : 45}" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
                </div>
                <select class="straddle-select leg-tgt-placement" style="font-size:10px; padding:1px 3px; background:#0b1120; border-color:#334155; color:${isTgtAdv ? '#10b981' : '#94a3b8'}; font-weight:700;" onchange="this.style.color = (this.value === 'Place in advance' ? '#10b981' : '#94a3b8'); syncStrategyLegChange('${rowId}');" title="Target Order Placement: Keep with self (monitored by software) vs Place with broker (advance LIMIT order on exchange)">
                    <option value="Keep with self" ${!isTgtAdv ? "selected" : ""}>💻 With Self</option>
                    <option value="Place in advance" ${isTgtAdv ? "selected" : ""}>🏦 With Broker (Limit)</option>
                </select>
            </div>
        </td>
        <td class="leg-col-tgt-reexec">
            <div style="display:flex; gap:2px;">
                <select class="straddle-select leg-tgt-re-type" style="width:120px;" onchange="syncLegRowDisabledFields(this.closest('tr')); syncStrategyLegChange('${rowId}');">
                    ${tgtReOptionsHtml}
                </select>
                <input type="number" class="straddle-input leg-tgt-re-cnt" style="width:40px;" value="${(l.target_re_execute_count !== undefined && l.target_re_execute_count !== null) ? l.target_re_execute_count : 6}" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
            </div>
        </td>
        <td class="leg-col-sl">
            <div style="display:flex; flex-direction:column; gap:3px;">
                <div style="display:flex; gap:2px;">
                    <select class="straddle-select leg-sl-type" style="width:80px;" onchange="syncLegRowDisabledFields(this.closest('tr')); syncStrategyLegChange('${rowId}');">
                        ${slOptionsHtml}
                    </select>
                    <input type="number" class="straddle-input leg-sl-val" style="width:50px;" value="${l.sl_value !== undefined ? l.sl_value : 66}" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
                </div>
                <select class="straddle-select leg-sl-placement" style="font-size:10px; padding:1px 3px; background:#0b1120; border-color:#334155; color:${isSlAdv ? '#10b981' : '#94a3b8'}; font-weight:700;" onchange="this.style.color = (this.value === 'Place in advance' ? '#10b981' : '#94a3b8'); syncStrategyLegChange('${rowId}');" title="Stoploss Order Placement: Keep with self (monitored by software) vs Place with broker (advance SL-L order on exchange)">
                    <option value="Keep with self" ${!isSlAdv ? "selected" : ""}>💻 With Self</option>
                    <option value="Place in advance" ${isSlAdv ? "selected" : ""}>🏦 With Broker (SL-L)</option>
                </select>
            </div>
        </td>
        <td class="leg-col-tsl">
            <div style="display:flex; flex-direction:column; gap:3px;">
                <select class="straddle-select leg-tsl-type" style="width:82px;" onchange="syncLegRowDisabledFields(this.closest('tr')); syncStrategyLegChange('${rowId}')">
                    <option value="None" ${l.tsl_type === "None" ? "selected" : ""}>None</option>
                    <option value="TSL: %" ${l.tsl_type === "TSL: %" ? "selected" : ""}>TSL: %</option>
                    <option value="TSL: pts" ${l.tsl_type === "TSL: pts" ? "selected" : ""}>TSL: pts</option>
                </select>
                <div class="leg-tsl-inputs" style="display:${(l.tsl_type && l.tsl_type !== 'None') ? 'flex' : 'none'}; gap:2px;" title="Lock (Reach) profit &amp; Trail step">
                    <input type="number" class="straddle-input leg-tsl-profit" style="width:38px; font-size:10px; padding:2px; text-align:center;" placeholder="Lock" value="${l.tsl_profit_pts !== undefined ? l.tsl_profit_pts : 10}" title="Reach/Lock Profit Value" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
                    <input type="number" class="straddle-input leg-tsl-trail" style="width:38px; font-size:10px; padding:2px; text-align:center;" placeholder="Trail" value="${l.tsl_trail_pts !== undefined ? l.tsl_trail_pts : 5}" title="Trail Step Value" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
                </div>
            </div>
        </td>
        <td class="leg-col-sl-reexec">
            <div style="display:flex; gap:2px;">
                <select class="straddle-select leg-sl-re-type" style="width:120px;" onchange="syncLegRowDisabledFields(this.closest('tr')); syncStrategyLegChange('${rowId}');">
                    ${slReOptionsHtml}
                </select>
                <input type="number" class="straddle-input leg-sl-re-cnt" style="width:40px;" value="${(l.sl_re_execute_count !== undefined && l.sl_re_execute_count !== null) ? l.sl_re_execute_count : 1}" oninput="syncStrategyLegChange('${rowId}')" onchange="syncStrategyLegChange('${rowId}')">
            </div>
        </td>
        <td class="leg-col-action">
            <select class="straddle-select leg-action" style="width:118px;" onchange="syncStrategyLegChange('${rowId}')">
                <option value="Square off leg" ${l.action === "Square off leg" ? "selected" : ""}>Square off leg</option>
                <option value="Square off all" ${l.action === "Square off all" ? "selected" : ""}>Square off all</option>
            </select>
        </td>
        <td class="leg-col-underlying">
            <select class="straddle-select leg-underlying" onchange="syncStrategyLegChange('${rowId}')">
                ${undOptionsHtml}
            </select>
        </td>
        <td class="leg-col-expiry">
            <select class="straddle-select leg-expiry" onchange="syncStrategyLegChange('${rowId}')">
                ${expOptionsHtml}
            </select>
        </td>
        <td class="leg-col-actions" style="text-align:center; position:sticky; right:0; background:#0c101c; z-index:4;">
            <button type="button" class="straddle-btn btn-danger" style="padding:2px 7px; font-size:11px; font-weight:700;" onclick="removeStraddleLegRow('${rowId}')" title="Delete leg">✕</button>
        </td>
    `;
    tbody.appendChild(tr);
    syncLegRowDisabledFields(tr);
    if (typeof setupLegRowDragAndDrop === "function") {
        setupLegRowDragAndDrop(tr);
    }
    if (typeof updateLegsCountBadge === "function") {
        updateLegsCountBadge();
    }
    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
        scheduleAutoMultiplierLiveRecalc();
    }
    if (!leg) {
        if (typeof syncStrategyLegChange === "function") {
            syncStrategyLegChange(rowId);
        }
        if (typeof markStraddleSettingsDirty === "function") {
            markStraddleSettingsDirty();
        }
    }
}

function removeStraddleLegRow(rowId) {
    const tr = document.getElementById(rowId);
    if (tr) {
        const tbody = tr.parentElement;
        const rowIdx = tbody ? Array.from(tbody.children).indexOf(tr) : -1;
        tr.remove();
        if (window._straddleState?.activeStrategy?.legs) {
            const legs = window._straddleState.activeStrategy.legs;
            const idx = legs.findIndex(l => l.id === rowId);
            if (idx >= 0) {
                legs.splice(idx, 1);
            } else if (rowIdx >= 0 && rowIdx < legs.length) {
                legs.splice(rowIdx, 1);
            }
        }
        if (typeof updateLegsCountBadge === "function") {
            updateLegsCountBadge();
        }
        if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
            scheduleAutoMultiplierLiveRecalc();
        }
        if (typeof markStraddleSettingsDirty === "function") {
            markStraddleSettingsDirty();
        }
    }
}

function syncLegRowDisabledFields(tr) {
    if (!tr) return;

    // 1. Target
    const tgtTypeSel = tr.querySelector(".leg-tgt-type");
    const tgtValInput = tr.querySelector(".leg-tgt-val");
    if (tgtTypeSel && tgtValInput) {
        const isNone = (tgtTypeSel.value || "").toLowerCase() === "none";
        tgtValInput.disabled = isNone;
        tgtValInput.classList.toggle("input-disabled-muted", isNone);
        if (isNone) {
            tgtValInput.setAttribute("title", "Target type is set to None. Select a target mode (%, pts) to enter a target value.");
        } else {
            tgtValInput.removeAttribute("title");
        }
    }

    // 2. Target Re-execution
    const tgtReSel = tr.querySelector(".leg-tgt-re-type");
    const tgtReInput = tr.querySelector(".leg-tgt-re-cnt");
    if (tgtReSel && tgtReInput) {
        const isNone = (tgtReSel.value || "").toUpperCase() === "NONE";
        tgtReInput.disabled = isNone;
        tgtReInput.classList.toggle("input-disabled-muted", isNone);
        if (isNone) {
            tgtReInput.setAttribute("title", "Target Re-execution is NONE. Choose a re-execution mode to enter execution count.");
        } else {
            tgtReInput.removeAttribute("title");
        }
    }

    // 3. Stoploss
    const slTypeSel = tr.querySelector(".leg-sl-type");
    const slValInput = tr.querySelector(".leg-sl-val");
    if (slTypeSel && slValInput) {
        const isNone = (slTypeSel.value || "").toLowerCase() === "none";
        slValInput.disabled = isNone;
        slValInput.classList.toggle("input-disabled-muted", isNone);
        if (isNone) {
            slValInput.setAttribute("title", "Stoploss type is set to None. Select a stoploss mode (%, pts) to enter a stoploss value.");
        } else {
            slValInput.removeAttribute("title");
        }
    }

    // 4. Stoploss Re-execution
    const slReSel = tr.querySelector(".leg-sl-re-type");
    const slReInput = tr.querySelector(".leg-sl-re-cnt");
    if (slReSel && slReInput) {
        const isNone = (slReSel.value || "").toUpperCase() === "NONE";
        slReInput.disabled = isNone;
        slReInput.classList.toggle("input-disabled-muted", isNone);
        if (isNone) {
            slReInput.setAttribute("title", "Stoploss Re-execution is NONE. Choose a re-execution mode to enter execution count.");
        } else {
            slReInput.removeAttribute("title");
        }
    }

    // 5. Trailing Stoploss inputs
    const tslTypeSel = tr.querySelector(".leg-tsl-type");
    const tslInputsDiv = tr.querySelector(".leg-tsl-inputs");
    if (tslTypeSel && tslInputsDiv) {
        const isNone = (tslTypeSel.value || "").toLowerCase() === "none";
        tslInputsDiv.style.display = isNone ? "none" : "flex";
    }
}
window.syncLegRowDisabledFields = syncLegRowDisabledFields;

// Controls bar Lots Multiplier stepper
function stepFormMultiplier(delta) {
    const el = document.getElementById("cfgMultiplier");
    if (!el) return;
    let val = parseInt(el.value, 10) || 1;
    val = Math.max(1, Math.min(100, val + delta));
    el.value = val;
    if (typeof onFormManualMultiplierChange === "function") {
        onFormManualMultiplierChange(val);
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.stepFormMultiplier = stepFormMultiplier;

// Live Legs Count Badge
function updateLegsCountBadge() {
    const badge = document.getElementById("cfgLegsCountBadge");
    if (!badge) return;
    const rows = document.querySelectorAll("#straddleLegsBuilderBody tr.straddle-leg-builder-row");
    const count = rows.length;
    let buyCount = 0;
    let sellCount = 0;
    rows.forEach(r => {
        const side = (r.querySelector(".leg-side")?.value || "Sell").toUpperCase();
        if (side === "BUY") buyCount++;
        else sellCount++;
    });
    if (count === 0) {
        badge.textContent = "0 Legs";
        badge.style.color = "#94a3b8";
        badge.style.background = "rgba(148,163,184,0.1)";
        badge.style.borderColor = "rgba(148,163,184,0.2)";
    } else {
        const parts = [];
        if (sellCount > 0) parts.push(`${sellCount}S`);
        if (buyCount > 0) parts.push(`${buyCount}B`);
        const breakdown = parts.length > 0 ? ` (${parts.join("+")})` : "";
        badge.textContent = `${count} Leg${count > 1 ? "s" : ""}${breakdown}`;
        badge.style.color = "#38bdf8";
        badge.style.background = "rgba(56,189,248,0.12)";
        badge.style.borderColor = "rgba(56,189,248,0.25)";
    }
}
window.updateLegsCountBadge = updateLegsCountBadge;

// One-click Add Straddle Pair (1 CE + 1 PE)
function addStraddleStraddlePair() {
    const curUnd = document.getElementById("cfgStrategyUnderlying")?.value || window._straddleState?.activeStrategy?.underlying || "NIFTY";
    const tgtPlacement = document.getElementById("cfgTargetOrderPlacement")?.value || "Place in advance";
    const slPlacement = document.getElementById("cfgSlOrderPlacement")?.value || "Place in advance";

    addStraddleLegRow({
        side: "Sell",
        product: "NRML",
        strike_selection: "ATM",
        multiple_strike_selection: "None",
        option_type: "CE",
        lots: 1,
        associated_hedge: { enabled: true, initial_otm: 20, max_distance_threshold: 22, reset_otm: 20, exit_with_parent: true },
        target_type: "TGT: %",
        target_value: 45.0,
        target_order_placement: tgtPlacement,
        target_re_execute_type: "RE EXECUTE",
        target_re_execute_count: 6,
        sl_type: "SL: %",
        sl_value: 66.0,
        sl_order_placement: slPlacement,
        tsl_type: "None",
        tsl_profit_pts: 0.0,
        tsl_trail_pts: 0.0,
        sl_re_execute_type: "RE EXECUTE",
        sl_re_execute_count: 1,
        action: "Square off leg",
        underlying: curUnd,
        expiry: "Weekly"
    });

    addStraddleLegRow({
        side: "Sell",
        product: "NRML",
        strike_selection: "ATM",
        multiple_strike_selection: "None",
        option_type: "PE",
        lots: 1,
        associated_hedge: { enabled: true, initial_otm: 20, max_distance_threshold: 22, reset_otm: 20, exit_with_parent: true },
        target_type: "TGT: %",
        target_value: 45.0,
        target_order_placement: tgtPlacement,
        target_re_execute_type: "RE EXECUTE",
        target_re_execute_count: 6,
        sl_type: "SL: %",
        sl_value: 66.0,
        sl_order_placement: slPlacement,
        tsl_type: "None",
        tsl_profit_pts: 0.0,
        tsl_trail_pts: 0.0,
        sl_re_execute_type: "RE EXECUTE",
        sl_re_execute_count: 1,
        action: "Square off leg",
        underlying: curUnd,
        expiry: "Weekly"
    });
    updateLegsCountBadge();
}
window.addStraddleStraddlePair = addStraddleStraddlePair;

// HTML5 Drag and Drop for Leg Row Reordering
function setupLegRowDragAndDrop(tr) {
    if (!tr) return;
    const handle = tr.querySelector(".leg-col-handle");
    if (handle) {
        handle.addEventListener("mousedown", () => {
            tr.setAttribute("draggable", "true");
        });
        handle.addEventListener("mouseup", () => {
            tr.setAttribute("draggable", "false");
        });
    }

    tr.addEventListener("dragstart", (e) => {
        tr.classList.add("leg-row-dragging");
        e.dataTransfer.effectAllowed = "move";
        e.dataTransfer.setData("text/plain", tr.id);
        window._draggedLegRow = tr;
    });

    tr.addEventListener("dragend", () => {
        tr.classList.remove("leg-row-dragging");
        tr.setAttribute("draggable", "false");
        document.querySelectorAll("#straddleLegsBuilderBody tr").forEach(r => {
            r.classList.remove("leg-drag-over-top", "leg-drag-over-bottom");
        });
        window._draggedLegRow = null;
        reorderStrategyLegsFromDom();
    });

    tr.addEventListener("dragover", (e) => {
        e.preventDefault();
        e.dataTransfer.dropEffect = "move";
        if (!window._draggedLegRow || window._draggedLegRow === tr) return;
        const rect = tr.getBoundingClientRect();
        const midY = rect.top + rect.height / 2;
        if (e.clientY < midY) {
            tr.classList.add("leg-drag-over-top");
            tr.classList.remove("leg-drag-over-bottom");
        } else {
            tr.classList.add("leg-drag-over-bottom");
            tr.classList.remove("leg-drag-over-top");
        }
    });

    tr.addEventListener("dragleave", () => {
        tr.classList.remove("leg-drag-over-top", "leg-drag-over-bottom");
    });

    tr.addEventListener("drop", (e) => {
        e.preventDefault();
        tr.classList.remove("leg-drag-over-top", "leg-drag-over-bottom");
        const dragged = window._draggedLegRow;
        if (!dragged || dragged === tr) return;
        const tbody = tr.parentElement;
        if (!tbody) return;
        const rect = tr.getBoundingClientRect();
        const midY = rect.top + rect.height / 2;
        if (e.clientY < midY) {
            tbody.insertBefore(dragged, tr);
        } else {
            tbody.insertBefore(dragged, tr.nextSibling);
        }
        reorderStrategyLegsFromDom();
    });
}
window.setupLegRowDragAndDrop = setupLegRowDragAndDrop;

function reorderStrategyLegsFromDom() {
    if (!window._straddleState?.activeStrategy) return;
    const currentLegs = window._straddleState.activeStrategy.legs || [];
    const newLegs = [];
    const rows = document.querySelectorAll("#straddleLegsBuilderBody tr.straddle-leg-builder-row");
    rows.forEach(r => {
        const match = currentLegs.find(l => l.id === r.id);
        if (match) {
            newLegs.push(match);
        } else {
            if (typeof syncStrategyLegChange === "function") syncStrategyLegChange(r.id);
            const found = (window._straddleState.activeStrategy.legs || []).find(l => l.id === r.id);
            if (found) newLegs.push(found);
        }
    });
    window._straddleState.activeStrategy.legs = newLegs;
    updateLegsCountBadge();
}
window.reorderStrategyLegsFromDom = reorderStrategyLegsFromDom;

function onToggleMtmSection(enabled) {
    const sec = document.getElementById("sec_builder_mtm");
    if (enabled && sec && sec.style.display === "none") {
        sec.style.display = "block";
        const ch = document.getElementById("chevron_sec_builder_mtm");
        if (ch) ch.textContent = "▼";
    }
    // If user re-enables the MTM section and both sub-toggles were OFF, turn them both ON so boxes are active
    if (enabled) {
        const tgtToggle = document.getElementById("cfgToggleMtmTarget");
        const slToggle = document.getElementById("cfgToggleMtmSl");
        if (tgtToggle && slToggle && !tgtToggle.checked && !slToggle.checked) {
            tgtToggle.checked = true;
            slToggle.checked = true;
        }
    }
    syncMtmFieldStates();
}
window.onToggleMtmSection = onToggleMtmSection;

function onToggleMtmSubTarget(enabled) {
    syncMtmFieldStates();
}
window.onToggleMtmSubTarget = onToggleMtmSubTarget;

function onToggleMtmSubSl(enabled) {
    syncMtmFieldStates();
}
window.onToggleMtmSubSl = onToggleMtmSubSl;

function getActiveStrategyLots() {
    const active = window._straddleState?.activeStrategy || {};
    const multType = (document.getElementById("grpAutoMultiplier")?.style.display !== "none") ? "AUTO" : "MANUAL";
    let mult = 1;
    if (multType === "MANUAL") {
        mult = parseInt(document.getElementById("cfgMultiplier")?.value) || 1;
    } else {
        mult = parseInt(active.effective_multiplier || active.multiplier || 1) || 1;
    }
    let maxLegLots = 1;
    const trs = document.querySelectorAll("#straddleLegsBuilderBody tr");
    if (trs.length > 0) {
        trs.forEach(tr => {
            const side = tr.querySelector(".leg-side")?.value || "Sell";
            if (String(side).toUpperCase() === "SELL") {
                const l = parseInt(tr.querySelector(".leg-lots")?.value) || 1;
                if (l > maxLegLots) maxLegLots = l;
            }
        });
    } else if (active.legs && active.legs.length > 0) {
        active.legs.forEach(l => {
            const side = l.side || "Sell";
            if (String(side).toUpperCase() === "SELL") {
                const lots = parseInt(l.lots) || 1;
                if (lots > maxLegLots) maxLegLots = lots;
            }
        });
    }
    return Math.max(1, mult * maxLegLots);
}
window.getActiveStrategyLots = getActiveStrategyLots;

function setMtmCalcMode(mode) {
    const isPerLot = (mode === "Per Lot");
    const btnOverall = document.getElementById("btnMtmModeOverall");
    const btnPerLot = document.getElementById("btnMtmModePerLot");
    if (btnOverall && btnPerLot) {
        if (isPerLot) {
            btnPerLot.className = "straddle-btn active-toggle";
            btnOverall.className = "straddle-btn btn-outline";
        } else {
            btnOverall.className = "straddle-btn active-toggle";
            btnPerLot.className = "straddle-btn btn-outline";
        }
    }

    const lots = getActiveStrategyLots();
    const ind = document.getElementById("mtmLotsIndicator");
    if (ind) {
        if (isPerLot) {
            ind.innerHTML = `(MTM Per Lot active: <b>${lots} lot(s)</b> &bull; Targets, Lock & Trail and SL values scale by <b>&times;${lots}</b>)`;
            ind.style.color = "#38bdf8";
        } else {
            ind.innerHTML = `(Overall strategy basket mode active: values apply directly to total basket)`;
            ind.style.color = "#94a3b8";
        }
    }

    // Dynamic labels
    const unitText = isPerLot ? "(₹ / lot)" : "(₹)";
    const reachLbl = document.getElementById("lblMtmTargetReach");
    if (reachLbl) reachLbl.textContent = `If profit reaches ${unitText}:`;
    const lockLbl = document.getElementById("lblMtmTargetLock");
    if (lockLbl) lockLbl.textContent = `Lock profit ${unitText}:`;
    const stepLbl = document.getElementById("lblMtmTargetTrailStep");
    if (stepLbl) stepLbl.textContent = `For every increase in profit by ${unitText}:`;
    const trailLbl = document.getElementById("lblMtmTargetTrailVal");
    if (trailLbl) trailLbl.textContent = `Trail profit by ${unitText}:`;
    const tgtValLbl = document.getElementById("lblMtmTargetVal");
    if (tgtValLbl) tgtValLbl.textContent = `Value ${isPerLot ? '(₹ / lot / %)' : '(₹ / %)'}:`;
    const slValLbl = document.getElementById("lblMtmSlVal");
    if (slValLbl) slValLbl.textContent = `Value ${isPerLot ? '(₹ / lot / %)' : '(₹ / %)'}:`;

    if (window._straddleState?.activeStrategy) {
        window._straddleState.activeStrategy.mtm_calc_mode = mode;
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.setMtmCalcMode = setMtmCalcMode;

function syncNoReEntryAfterState() {
    const toggle = document.getElementById("cfgToggleNoReEntryAfter");
    const group = document.getElementById("groupNoReEntryAfterTime");
    const isEnabled = toggle ? toggle.checked : false;
    if (group) {
        group.style.opacity = isEnabled ? "1" : "0.5";
        const inputs = group.querySelectorAll("input, button");
        inputs.forEach(el => {
            if (el.id !== "cfgNoReEntryAfterTime") {
                el.disabled = !isEnabled;
            }
        });
    }
}
window.syncNoReEntryAfterState = syncNoReEntryAfterState;

function syncMtmFieldStates() {
    const secToggle = document.getElementById("cfgToggleMtmSection");
    const isSecEnabled = secToggle ? secToggle.checked : true;
    const secBody = document.getElementById("sec_builder_mtm");
    const secBadge = document.getElementById("badge_mtm_section");

    if (secBadge) {
        secBadge.textContent = isSecEnabled ? "ENABLED" : "DISABLED";
        secBadge.className = "section-status-badge " + (isSecEnabled ? "badge-active" : "badge-disabled");
    }

    if (secBody) {
        secBody.classList.toggle("section-muted-locked", !isSecEnabled);
    }

    // Refresh MTM Calc mode indicator & labels
    const isPerLot = document.getElementById("btnMtmModePerLot")?.classList.contains("active-toggle");
    setMtmCalcMode(isPerLot ? "Per Lot" : "Overall");

    // Target Box
    const cardTgt = document.getElementById("card_mtm_target");
    const tgtSubToggle = document.getElementById("cfgToggleMtmTarget");
    const isTgtSubEnabled = isSecEnabled && (tgtSubToggle ? tgtSubToggle.checked : true);
    const tgtBadge = document.getElementById("badge_mtm_sub_target");

    if (tgtBadge) {
        tgtBadge.textContent = isTgtSubEnabled ? "ACTIVE" : "MUTED";
        tgtBadge.style.color = isTgtSubEnabled ? "#38bdf8" : "#64748b";
    }
    if (cardTgt) {
        cardTgt.classList.toggle("card-muted-locked", !isTgtSubEnabled);
    }

    const tgtTypeSel = document.getElementById("cfgMtmTargetType");
    const tgtType = tgtTypeSel ? tgtTypeSel.value : "Lock and Trail";
    const tgtRe = document.getElementById("cfgMtmTargetReEntry");
    const tgtAction = document.getElementById("cfgMtmTargetAction");

    if (tgtTypeSel) tgtTypeSel.disabled = !isTgtSubEnabled;
    if (tgtRe) tgtRe.disabled = !isTgtSubEnabled;
    if (tgtAction) tgtAction.disabled = !isTgtSubEnabled;
    if (tgtSubToggle) tgtSubToggle.disabled = !isSecEnabled;

    const grpLockTrail = document.getElementById("group_mtm_lock_and_trail");
    const boxReach = document.getElementById("box_mtm_reach");
    const boxLock = document.getElementById("box_mtm_lock");
    const boxTrailStep = document.getElementById("box_mtm_trail_step");
    const boxTrailVal = document.getElementById("box_mtm_trail_val");
    const grpFixedTgt = document.getElementById("group_mtm_fixed_target");

    const reachInp = document.getElementById("cfgMtmTargetReach");
    const lockInp = document.getElementById("cfgMtmTargetLock");
    const stepInp = document.getElementById("cfgMtmTargetTrailStep");
    const valInp = document.getElementById("cfgMtmTargetTrailVal");
    const fixedValInp = document.getElementById("cfgMtmTargetValue");

    if (tgtType === "Lock and Trail") {
        if (grpLockTrail) grpLockTrail.style.display = "block";
        if (boxReach) boxReach.style.display = "block";
        if (boxLock) boxLock.style.display = "block";
        if (boxTrailStep) boxTrailStep.style.display = "block";
        if (boxTrailVal) boxTrailVal.style.display = "block";
        if (grpFixedTgt) grpFixedTgt.style.display = "none";

        if (reachInp) reachInp.disabled = !isTgtSubEnabled;
        if (lockInp) lockInp.disabled = !isTgtSubEnabled;
        if (stepInp) stepInp.disabled = !isTgtSubEnabled;
        if (valInp) valInp.disabled = !isTgtSubEnabled;
    } else if (tgtType === "Lock Profit") {
        if (grpLockTrail) grpLockTrail.style.display = "block";
        if (boxReach) boxReach.style.display = "block";
        if (boxLock) boxLock.style.display = "block";
        if (boxTrailStep) boxTrailStep.style.display = "none";
        if (boxTrailVal) boxTrailVal.style.display = "none";
        if (grpFixedTgt) grpFixedTgt.style.display = "none";

        if (reachInp) reachInp.disabled = !isTgtSubEnabled;
        if (lockInp) lockInp.disabled = !isTgtSubEnabled;
    } else if (tgtType === "Trail Profit") {
        if (grpLockTrail) grpLockTrail.style.display = "block";
        if (boxReach) boxReach.style.display = "none";
        if (boxLock) boxLock.style.display = "none";
        if (boxTrailStep) boxTrailStep.style.display = "block";
        if (boxTrailVal) boxTrailVal.style.display = "block";
        if (grpFixedTgt) grpFixedTgt.style.display = "none";

        if (stepInp) stepInp.disabled = !isTgtSubEnabled;
        if (valInp) valInp.disabled = !isTgtSubEnabled;
    } else if (tgtType === "None") {
        if (grpLockTrail) grpLockTrail.style.display = "none";
        if (grpFixedTgt) grpFixedTgt.style.display = "none";
    } else {
        // Fixed Amount / Fixed %
        if (grpLockTrail) grpLockTrail.style.display = "none";
        if (grpFixedTgt) grpFixedTgt.style.display = "block";
        if (fixedValInp) fixedValInp.disabled = !isTgtSubEnabled;
    }

    // Stoploss Box
    const cardSl = document.getElementById("card_mtm_sl");
    const slSubToggle = document.getElementById("cfgToggleMtmSl");
    const isSlSubEnabled = isSecEnabled && (slSubToggle ? slSubToggle.checked : true);
    const slBadge = document.getElementById("badge_mtm_sub_sl");

    if (slBadge) {
        slBadge.textContent = isSlSubEnabled ? "ACTIVE" : "MUTED";
        slBadge.style.color = isSlSubEnabled ? "#f43f5e" : "#64748b";
    }
    if (cardSl) {
        cardSl.classList.toggle("card-muted-locked", !isSlSubEnabled);
    }

    const slTypeSel = document.getElementById("cfgMtmSlType");
    const slVal = document.getElementById("cfgMtmSlValue");
    const slAction = document.getElementById("cfgMtmSlAction");

    if (slTypeSel) slTypeSel.disabled = !isSlSubEnabled;
    if (slAction) slAction.disabled = !isSlSubEnabled;
    if (slSubToggle) slSubToggle.disabled = !isSecEnabled;

    if (slVal) {
        const isSlNone = !isSlSubEnabled || (slTypeSel && slTypeSel.value === "None");
        slVal.disabled = isSlNone;
        slVal.classList.toggle("input-disabled-muted", isSlNone);
        if (isSlNone) {
            slVal.setAttribute("title", "MTM Stoploss is disabled or set to None.");
        } else {
            slVal.removeAttribute("title");
        }
    }
}
window.syncMtmFieldStates = syncMtmFieldStates;

function onTogglePremSection(enabled) {
    const sec = document.getElementById("sec_builder_premium");
    if (enabled && sec && sec.style.display === "none") {
        sec.style.display = "block";
        const ch = document.getElementById("chevron_sec_builder_premium");
        if (ch) ch.textContent = "▼";
    }
    syncPremiumMatchingFieldStates();
}
window.onTogglePremSection = onTogglePremSection;

function selectPremMatchCard(radId) {
    const secToggle = document.getElementById("cfgTogglePremSection");
    if (secToggle && !secToggle.checked) {
        secToggle.checked = true;
        onTogglePremSection(true);
    }
    const rad = document.getElementById(radId);
    if (rad) {
        rad.checked = true;
        syncPremiumMatchingFieldStates();
    }
}
window.selectPremMatchCard = selectPremMatchCard;

function setPremDiffPreset(val, type) {
    selectPremMatchCard('radPremMaxDiff');
    const input = document.getElementById("cfgPremMaxDiffVal");
    if (input) {
        input.value = val;
        input.focus();
    }
    if (type && document.getElementById("cfgPremMaxDiffType")) {
        document.getElementById("cfgPremMaxDiffType").value = type;
    }
    syncPremiumMatchingFieldStates();
}
window.setPremDiffPreset = setPremDiffPreset;

function setPremCloseToPreset(val) {
    selectPremMatchCard('radPremCloseTo');
    const input = document.getElementById("cfgPremCloseToVal");
    if (input) {
        input.value = val;
        input.focus();
    }
    syncPremiumMatchingFieldStates();
}
window.setPremCloseToPreset = setPremCloseToPreset;

function setPremRangePreset(min, max) {
    selectPremMatchCard('radPremRange');
    const minInput = document.getElementById("cfgPremRangeMin");
    const maxInput = document.getElementById("cfgPremRangeMax");
    if (minInput) minInput.value = min;
    if (maxInput) {
        maxInput.value = max;
        maxInput.focus();
    }
    syncPremiumMatchingFieldStates();
}
window.setPremRangePreset = setPremRangePreset;

function setCombinedPremPreset(val) {
    const secToggle = document.getElementById("cfgTogglePremSection");
    if (secToggle && !secToggle.checked) {
        secToggle.checked = true;
        onTogglePremSection(true);
    }
    const combToggle = document.getElementById("cfgToggleCombinedPrem");
    if (combToggle) combToggle.checked = true;
    const input = document.getElementById("cfgCombinedPremVal");
    if (input) {
        input.value = val;
        input.focus();
    }
    syncPremiumMatchingFieldStates();
}
window.setCombinedPremPreset = setCombinedPremPreset;

function syncPremiumMatchingFieldStates() {
    const secToggle = document.getElementById("cfgTogglePremSection");
    const isSecEnabled = secToggle ? secToggle.checked : false;
    const secBody = document.getElementById("sec_builder_premium");
    const secBadge = document.getElementById("badge_prem_section");
    const ruleBadge = document.getElementById("badgePremActiveRule");
    const prevBadge = document.getElementById("prevPremModeBadge");
    const prevFormula = document.getElementById("prevPremFormulaText");
    const prevCadence = document.getElementById("prevPremCadenceText");

    if (secBadge) {
        secBadge.textContent = isSecEnabled ? "ENABLED" : "DISABLED";
        secBadge.className = "section-status-badge " + (isSecEnabled ? "badge-active" : "badge-disabled");
    }

    if (secBody) {
        secBody.classList.toggle("section-muted-locked", !isSecEnabled);
    }

    const radNone = document.getElementById("radPremNone");
    const radMaxDiff = document.getElementById("radPremMaxDiff");
    const radCloseTo = document.getElementById("radPremCloseTo");
    const radRange = document.getElementById("radPremRange");

    const cardNone = document.getElementById("cardPremNone");
    const cardMaxDiff = document.getElementById("cardPremMaxDiff");
    const cardCloseTo = document.getElementById("cardPremCloseTo");
    const cardRange = document.getElementById("cardPremRange");

    const diffVal = document.getElementById("cfgPremMaxDiffVal");
    const diffType = document.getElementById("cfgPremMaxDiffType");
    const closeToVal = document.getElementById("cfgPremCloseToVal");
    const rangeMin = document.getElementById("cfgPremRangeMin");
    const rangeMax = document.getElementById("cfgPremRangeMax");

    const combToggle = document.getElementById("cfgToggleCombinedPrem");
    const combType = document.getElementById("cfgCombinedPremType");
    const combVal = document.getElementById("cfgCombinedPremVal");
    const combBadge = document.getElementById("badgeCombinedPrem");
    const combFreq = document.getElementById("cfgPremFrequency");

    // All controls disabled if section is disabled
    [radNone, radMaxDiff, radCloseTo, radRange, combToggle].forEach(ctrl => {
        if (ctrl) ctrl.disabled = !isSecEnabled;
    });

    // Update Card Selected classes
    const isNone = radNone && radNone.checked;
    const isDiff = radMaxDiff && radMaxDiff.checked;
    const isClose = radCloseTo && radCloseTo.checked;
    const isRng = radRange && radRange.checked;

    if (cardNone) cardNone.classList.toggle("card-selected", isNone && isSecEnabled);
    if (cardMaxDiff) cardMaxDiff.classList.toggle("card-selected", isDiff && isSecEnabled);
    if (cardCloseTo) cardCloseTo.classList.toggle("card-selected", isClose && isSecEnabled);
    if (cardRange) cardRange.classList.toggle("card-selected", isRng && isSecEnabled);

    if (!isSecEnabled) {
        [diffVal, diffType, closeToVal, rangeMin, rangeMax, combType, combVal, combFreq].forEach(el => {
            if (el) {
                el.disabled = true;
                el.classList.add("input-disabled-muted");
            }
        });
        if (ruleBadge) {
            ruleBadge.textContent = "Disabled (Standard Immediate Entry)";
            ruleBadge.style.color = "#64748b";
            ruleBadge.style.background = "rgba(100,116,139,0.12)";
            ruleBadge.style.borderColor = "rgba(100,116,139,0.3)";
        }
        if (prevBadge) {
            prevBadge.textContent = "Section Disabled";
            prevBadge.style.color = "#64748b";
            prevBadge.style.background = "rgba(100,116,139,0.15)";
        }
        if (prevFormula) {
            prevFormula.textContent = "Strike Selection: Configured Leg Strikes (ATM/OTM) | Entry: Immediate at Start Time (No condition waiting)";
        }
        if (prevCadence) {
            prevCadence.textContent = "⏱️ Status: Strategy will enter immediately at start_time without premium matching.";
        }
        if (combBadge) {
            combBadge.textContent = "Off";
            combBadge.style.color = "#64748b";
            combBadge.style.background = "rgba(255,255,255,0.05)";
        }
        return;
    }

    // Section is enabled: enable fields according to selected radio and checkbox
    if (diffVal) {
        diffVal.disabled = !isDiff;
        diffVal.classList.toggle("input-disabled-muted", !isDiff);
    }
    if (diffType) {
        diffType.disabled = !isDiff;
        diffType.classList.toggle("input-disabled-muted", !isDiff);
    }

    if (closeToVal) {
        closeToVal.disabled = !isClose;
        closeToVal.classList.toggle("input-disabled-muted", !isClose);
    }

    if (rangeMin) {
        rangeMin.disabled = !isRng;
        rangeMin.classList.toggle("input-disabled-muted", !isRng);
    }
    if (rangeMax) {
        rangeMax.disabled = !isRng;
        rangeMax.classList.toggle("input-disabled-muted", !isRng);
    }

    const isCombActive = combToggle && combToggle.checked;
    if (combType) {
        combType.disabled = !isCombActive;
        combType.classList.toggle("input-disabled-muted", !isCombActive);
    }
    if (combVal) {
        combVal.disabled = !isCombActive;
        combVal.classList.toggle("input-disabled-muted", !isCombActive);
    }
    if (combFreq) {
        combFreq.disabled = false;
        combFreq.classList.remove("input-disabled-muted");
    }

    const cValNum = parseFloat(combVal?.value) || 0;
    const cTypeStr = combType?.value || "Lower than";
    const cTypeOp = cTypeStr.includes("Lower") ? "≤" : "≥";
    if (combBadge) {
        if (isCombActive && cValNum > 0) {
            combBadge.textContent = `Active (${cTypeOp} ₹${cValNum})`;
            combBadge.style.color = "#38bdf8";
            combBadge.style.background = "rgba(56,189,248,0.15)";
        } else if (isCombActive) {
            combBadge.textContent = "Active (Set Threshold)";
            combBadge.style.color = "#f59e0b";
            combBadge.style.background = "rgba(245,158,11,0.15)";
        } else {
            combBadge.textContent = "Off";
            combBadge.style.color = "#64748b";
            combBadge.style.background = "rgba(255,255,255,0.05)";
        }
    }

    // Dynamic Live Blueprint & Status text
    let modeText = "No Matching (Immediate Entry)";
    let formulaText = "Strike Selection: Configured ATM/OTM Leg Strikes | Entry: Immediate at Start Time";
    let badgeColor = "#10b981";

    if (isDiff) {
        const dVal = parseFloat(diffVal?.value) || 0;
        const dUnit = (diffType?.value === "Percentage") ? "%" : "₹";
        modeText = `Max Difference (≤ ${dUnit}${dVal})`;
        formulaText = `🎯 Rule: |CE_LTP - PE_LTP| ≤ ${dUnit}${dVal} | Checks primary CE vs PE sell legs`;
        badgeColor = "#38bdf8";
    } else if (isClose) {
        const cVal = parseFloat(closeToVal?.value) || 0;
        modeText = `Close to Target (₹${cVal} ±10%)`;
        formulaText = `🎯 Rule: |CE_LTP - ${cVal}| ≤ 10% AND |PE_LTP - ${cVal}| ≤ 10% (Target: ₹${cVal})`;
        badgeColor = "#f59e0b";
    } else if (isRng) {
        const rMin = parseFloat(rangeMin?.value) || 0;
        const rMax = parseFloat(rangeMax?.value) || 0;
        modeText = `Premium Range [₹${rMin} - ₹${rMax}]`;
        formulaText = `🎯 Rule: ₹${rMin} ≤ CE_LTP, PE_LTP ≤ ₹${rMax} | Both legs must be within bracket`;
        badgeColor = "#a855f7";
    }

    if (isCombActive && cValNum > 0) {
        formulaText += ` | 🔗 Combined Filter: (CE + PE) ${cTypeOp} ₹${cValNum}`;
    }

    if (ruleBadge) {
        ruleBadge.textContent = modeText;
        ruleBadge.style.color = badgeColor;
        ruleBadge.style.background = badgeColor === "#10b981" ? "rgba(16,185,129,0.1)" : (badgeColor === "#38bdf8" ? "rgba(56,189,248,0.15)" : (badgeColor === "#f59e0b" ? "rgba(245,158,11,0.15)" : "rgba(168,85,247,0.15)"));
        ruleBadge.style.borderColor = badgeColor;
    }

    if (prevBadge) {
        prevBadge.textContent = modeText;
        prevBadge.style.color = badgeColor;
        prevBadge.style.background = badgeColor === "#10b981" ? "rgba(16,185,129,0.15)" : (badgeColor === "#38bdf8" ? "rgba(56,189,248,0.15)" : (badgeColor === "#f59e0b" ? "rgba(245,158,11,0.15)" : "rgba(168,85,247,0.15)"));
    }

    if (prevFormula) {
        prevFormula.textContent = formulaText;
    }

    if (prevCadence) {
        const freqVal = combFreq?.value || "LTP (Continuous Monitoring)";
        prevCadence.textContent = `⏱️ Frequency: ${freqVal} | Monitored Legs: Primary Sell CE & PE | Auto-Entry Trigger on condition match`;
    }
}
window.syncPremiumMatchingFieldStates = syncPremiumMatchingFieldStates;


function onStrategyUnderlyingChange(newUnd) {
    if (!newUnd) return;
    const und = String(newUnd).toUpperCase().trim();
    const isComm = ["CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM", "SILVER", "SILVERM", "COPPER"].includes(und);

    // 1. Update in-memory active strategy underlying and its legs
    if (window._straddleState && window._straddleState.activeStrategy) {
        window._straddleState.activeStrategy.underlying = und;
        if (Array.isArray(window._straddleState.activeStrategy.legs)) {
            window._straddleState.activeStrategy.legs.forEach(leg => {
                leg.underlying = und;
            });
        }
    }

    // 2. Ensure header dropdown displays chosen underlying
    const stratUndEl = document.getElementById("cfgStrategyUnderlying");
    if (stratUndEl && stratUndEl.value !== und) {
        stratUndEl.value = und;
    }

    // 3. Update all existing leg rows in the legs table
    const trs = document.querySelectorAll("#straddleLegsBuilderBody tr");
    trs.forEach(tr => {
        const undSel = tr.querySelector(".leg-underlying");
        if (undSel) {
            undSel.value = und;
        }

        const expSel = tr.querySelector(".leg-expiry");
        if (expSel) {
            const curExp = expSel.value;
            if (isComm) {
                expSel.innerHTML = `
                    <option value="Current Month" ${curExp === "Current Month" || curExp === "Monthly" ? "selected" : ""}>Current Month</option>
                    <option value="Next Month" ${curExp === "Next Month" || curExp === "Next Monthly" ? "selected" : ""}>Next Month</option>
                    <option value="Far Month" ${curExp === "Far Month" ? "selected" : ""}>Far Month</option>
                `;
            } else {
                expSel.innerHTML = `
                    <option value="Weekly" ${curExp === "Weekly" ? "selected" : ""}>Weekly</option>
                    <option value="Next Weekly" ${curExp === "Next Weekly" ? "selected" : ""}>Next Weekly</option>
                    <option value="Monthly" ${curExp === "Monthly" ? "selected" : ""}>Monthly</option>
                `;
            }
        }
    });

    // Also update any other leg underlying selects with class .leg-underlying-select
    document.querySelectorAll(".leg-underlying-select").forEach(sel => {
        sel.value = und;
    });

    // 4. Auto-adjust start/end times if they were at defaults
    const startEl = document.getElementById("cfgStartTime");
    const endEl = document.getElementById("cfgEndTime");
    if (isComm) {
        if (startEl && (startEl.value === "09:15:56" || startEl.value === "09:15:00")) {
            setStraddleTimeInputValue("cfgStartTime", "09:00:00");
        }
        if (endEl && (endEl.value === "15:29:50" || endEl.value === "15:30:00")) {
            setStraddleTimeInputValue("cfgEndTime", "23:30:00");
        }
    } else {
        if (startEl && startEl.value === "09:00:00") {
            setStraddleTimeInputValue("cfgStartTime", "09:15:56");
        }
        if (endEl && (endEl.value === "23:30:00" || endEl.value === "23:55:00")) {
            setStraddleTimeInputValue("cfgEndTime", "15:29:50");
        }
    }

    // 5. Refresh live margin preview and auto multiplier preview
    updateFormAutoMultiplierPreview();
    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
        scheduleAutoMultiplierLiveRecalc();
    }
    if (typeof renderStrategyMarginEstimate === "function") {
        renderStrategyMarginEstimate();
    }
}
window.onStrategyUnderlyingChange = onStrategyUnderlyingChange;

function setUnderlyingPriceSource(source) {
    ["Spot", "Futures", "Synthetic Futures"].forEach(s => {
        const id = s === "Spot" ? "btnUndSpot" : (s === "Futures" ? "btnUndFutures" : "btnUndSynthetic");
        const btn = document.getElementById(id);
        if (btn) btn.classList.toggle("active-toggle", s === source);
    });
    if (window._straddleState?.activeStrategy) {
        window._straddleState.activeStrategy.underlying_price_source = source;
    }
    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.setUnderlyingPriceSource = setUnderlyingPriceSource;

function setStraddleTradeType(type) {
    document.getElementById("btnTradeIntraday")?.classList.toggle("active-toggle", type === "Intraday");
    document.getElementById("btnTradePositional")?.classList.toggle("active-toggle", type === "Positional");
    if (window._straddleState?.activeStrategy) {
        window._straddleState.activeStrategy.trade_type = type;
    }
    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.setStraddleTradeType = setStraddleTradeType;

// Section 2: Strategy Toggles Handler
function onStrategyToggleChanged(key, isChecked) {
    if (!window._straddleState?.activeStrategy) return;
    const strat = window._straddleState.activeStrategy;
    if (!strat.toggles) strat.toggles = {};
    strat.toggles[key] = isChecked;

    if (key === "move_sl_to_cost") strat.move_sl_to_cost = isChecked;
    if (key === "wait_and_trade") strat.wait_and_trade = isChecked;
    if (key === "re_entry_active") {
        strat.re_entry_enabled = isChecked;
        strat.toggles.re_entry_active = isChecked;
    }
    if (key === "range_breakout") strat.range_breakout = isChecked;
    if (key === "dynamic_hedging") {
        strat.dynamic_hedging = isChecked;
        strat.toggles.dynamic_hedging = isChecked;
        document.querySelectorAll("#straddleLegsBuilderBody tr.straddle-leg-builder-row").forEach(tr => {
            const side = (tr.querySelector(".leg-side")?.value || "Sell").toUpperCase();
            if (side === "SELL") {
                const chk = tr.querySelector(".leg-hedge-enabled");
                if (chk) {
                    chk.checked = isChecked;
                    if (typeof toggleLegHedgeRow === "function") toggleLegHedgeRow(tr.id, isChecked);
                }
            }
        });
    }

    const toggleIdMap = {
        'move_sl_to_cost': 'cfgToggleMoveSlToCost',
        'wait_and_trade': 'cfgToggleWaitAndTrade',
        're_entry_active': 'cfgToggleReEntry',
        'range_breakout': 'cfgToggleRangeBreakout',
        'dynamic_hedging': 'cfgToggleDynamicHedges'
    };
    const inputEl = document.getElementById(toggleIdMap[key]);
    if (inputEl) {
        inputEl.closest(".straddle-toggle-chip")?.classList.toggle("chip-checked", isChecked);
    }

    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.onStrategyToggleChanged = onStrategyToggleChanged;

// Section 2: Header Live Badges
function updateTimingsHeaderBadge() {
    const summaryBadge = document.getElementById("badge_timings_summary");
    const countBadge = document.getElementById("badge_active_toggles_count");

    if (summaryBadge) {
        let und = "Spot";
        if (document.getElementById("btnUndFutures")?.classList.contains("active-toggle")) und = "Futures";
        else if (document.getElementById("btnUndSynthetic")?.classList.contains("active-toggle")) und = "Synthetic";

        const tradeType = document.getElementById("btnTradePositional")?.classList.contains("active-toggle") ? "Positional" : "Intraday";

        const startTime = document.getElementById("cfgStartTime")?.value || "09:15:56";
        const endTime = document.getElementById("cfgEndTime")?.value || "15:29:50";

        summaryBadge.textContent = `${und} • ${tradeType} • ${startTime.slice(0, 5)} - ${endTime.slice(0, 5)}`;
    }

    if (countBadge) {
        const toggles = [
            document.getElementById("cfgToggleMoveSlToCost")?.checked,
            document.getElementById("cfgToggleWaitAndTrade")?.checked,
            document.getElementById("cfgToggleReEntry")?.checked,
            document.getElementById("cfgToggleRangeBreakout")?.checked,
            document.getElementById("cfgToggleDynamicHedges")?.checked
        ];
        const activeCount = toggles.filter(Boolean).length;
        countBadge.textContent = `${activeCount} Active Toggle${activeCount !== 1 ? "s" : ""}`;
        if (activeCount > 0) {
            countBadge.style.color = "#10b981";
            countBadge.style.background = "rgba(16,185,129,0.12)";
            countBadge.style.borderColor = "rgba(16,185,129,0.25)";
        } else {
            countBadge.style.color = "#94a3b8";
            countBadge.style.background = "rgba(148,163,184,0.1)";
            countBadge.style.borderColor = "rgba(148,163,184,0.2)";
        }
    }
}
window.updateTimingsHeaderBadge = updateTimingsHeaderBadge;

// Quick Time Presets
function setTimePreset(baseId, timeStr) {
    if (typeof setStraddleTimeInputValue === "function") {
        setStraddleTimeInputValue(baseId, timeStr);
    }
    if (typeof syncTimeSegmentsToMain === "function") {
        syncTimeSegmentsToMain(baseId);
    }
    if (window._straddleState?.activeStrategy) {
        if (baseId === "cfgStartTime") window._straddleState.activeStrategy.start_time = timeStr;
        if (baseId === "cfgEndTime") window._straddleState.activeStrategy.end_time = timeStr;
        if (baseId === "cfgNoReEntryAfterTime") {
            if (!window._straddleState.activeStrategy.no_reentry_after) window._straddleState.activeStrategy.no_reentry_after = {};
            window._straddleState.activeStrategy.no_reentry_after.time = timeStr;
        }
    }
    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.setTimePreset = setTimePreset;

// Execution Days of Week Pills
function toggleScheduleDay(day) {
    const btn = document.querySelector(`.day-pill-btn[data-day="${day}"]`);
    if (btn) {
        btn.classList.toggle("active");
    }
    syncScheduleDaysToActiveStrategy();
}
window.toggleScheduleDay = toggleScheduleDay;

function setAllScheduleDays(enableAll) {
    document.querySelectorAll(".day-pill-btn").forEach(btn => {
        btn.classList.toggle("active", !!enableAll);
    });
    syncScheduleDaysToActiveStrategy();
}
window.setAllScheduleDays = setAllScheduleDays;

function syncBuilderAdvanceSlToTable(val) {
    const isAdv = (val === "Place in advance");
    const activeStrat = window._straddleState?.activeStrategy;
    if (!activeStrat) return;
    const sid = activeStrat.strategy_id;
    if (!activeStrat.advanced) activeStrat.advanced = {};
    activeStrat.advanced.sl_order_placement = val;
    if (activeStrat.advanced_settings) activeStrat.advanced_settings.sl_order_placement = val;

    // Sync to allStrategies
    if (sid && Array.isArray(window._straddleState?.allStrategies)) {
        const found = window._straddleState.allStrategies.find(s => s.strategy_id === sid);
        if (found) {
            if (!found.advanced) found.advanced = {};
            found.advanced.sl_order_placement = val;
            if (found.advanced_settings) found.advanced_settings.sl_order_placement = val;
            if (Array.isArray(found.legs)) {
                found.legs.forEach(l => { l.sl_order_placement = val; });
            }
        }
    }

    // Sync switch in table row
    const advSwitch = document.querySelector(`input[data-adv-sl-sid="${sid}"]`);
    if (advSwitch) {
        advSwitch.checked = isAdv;
        const slider = advSwitch.nextElementSibling;
        if (slider) slider.style.background = isAdv ? "#10b981" : "";
    }
}
window.syncBuilderAdvanceSlToTable = syncBuilderAdvanceSlToTable;

function syncScheduleDaysToActiveStrategy() {
    if (!window._straddleState?.activeStrategy) return;
    const days = [];
    document.querySelectorAll(".day-pill-btn.active").forEach(btn => {
        const d = btn.getAttribute("data-day");
        if (d) days.push(d);
    });
    const finalDays = days.length > 0 ? days : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
    window._straddleState.activeStrategy.schedule_days = finalDays;

    const sid = window._straddleState.activeStrategy.strategy_id;
    if (sid && Array.isArray(window._straddleState?.allStrategies)) {
        const found = window._straddleState.allStrategies.find(s => s.strategy_id === sid);
        if (found) {
            found.schedule_days = [...finalDays];
        }
    }
    if (typeof renderStraddleAlgosTable === "function") {
        renderStraddleAlgosTable(false);
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.syncScheduleDaysToActiveStrategy = syncScheduleDaysToActiveStrategy;

function applyStraddleBulkEdit(action) {
    if (!action) return;
    if (action === "BOTH_BROKER") {
        document.querySelectorAll(".leg-sl-placement").forEach(el => { el.value = "Place in advance"; el.style.color = "#10b981"; });
        document.querySelectorAll(".leg-tgt-placement").forEach(el => { el.value = "Place in advance"; el.style.color = "#10b981"; });
        const slOrderEl = document.getElementById("cfgSlOrderPlacement");
        if (slOrderEl) slOrderEl.value = "Place in advance";
        const tgtOrderEl = document.getElementById("cfgTargetOrderPlacement");
        if (tgtOrderEl) tgtOrderEl.value = "Place in advance";
        syncBuilderAdvanceSlToTable("Place in advance");
    } else if (action === "BOTH_SELF") {
        document.querySelectorAll(".leg-sl-placement").forEach(el => { el.value = "Keep with self"; el.style.color = "#94a3b8"; });
        document.querySelectorAll(".leg-tgt-placement").forEach(el => { el.value = "Keep with self"; el.style.color = "#94a3b8"; });
        const slOrderEl = document.getElementById("cfgSlOrderPlacement");
        if (slOrderEl) slOrderEl.value = "Keep with self";
        const tgtOrderEl = document.getElementById("cfgTargetOrderPlacement");
        if (tgtOrderEl) tgtOrderEl.value = "Keep with self";
        syncBuilderAdvanceSlToTable("Keep with self");
    } else if (action === "SL_BROKER") {
        document.querySelectorAll(".leg-sl-placement").forEach(el => { el.value = "Place in advance"; el.style.color = "#10b981"; });
        const slOrderEl = document.getElementById("cfgSlOrderPlacement");
        if (slOrderEl) slOrderEl.value = "Place in advance";
        syncBuilderAdvanceSlToTable("Place in advance");
    } else if (action === "SL_SELF") {
        document.querySelectorAll(".leg-sl-placement").forEach(el => { el.value = "Keep with self"; el.style.color = "#94a3b8"; });
        const slOrderEl = document.getElementById("cfgSlOrderPlacement");
        if (slOrderEl) slOrderEl.value = "Keep with self";
        syncBuilderAdvanceSlToTable("Keep with self");
    } else if (action === "TGT_BROKER") {
        document.querySelectorAll(".leg-tgt-placement").forEach(el => { el.value = "Place in advance"; el.style.color = "#10b981"; });
        const tgtOrderEl = document.getElementById("cfgTargetOrderPlacement");
        if (tgtOrderEl) tgtOrderEl.value = "Place in advance";
    } else if (action === "TGT_SELF") {
        document.querySelectorAll(".leg-tgt-placement").forEach(el => { el.value = "Keep with self"; el.style.color = "#94a3b8"; });
        const tgtOrderEl = document.getElementById("cfgTargetOrderPlacement");
        if (tgtOrderEl) tgtOrderEl.value = "Keep with self";
    } else if (action === "HEDGE_ON") {
        document.querySelectorAll("#straddleLegsBuilderBody tr.straddle-leg-builder-row").forEach(tr => {
            const side = tr.querySelector(".leg-side")?.value || "Sell";
            if (side.toUpperCase() === "SELL") {
                const chk = tr.querySelector(".leg-hedge-enabled");
                if (chk) {
                    chk.checked = true;
                    if (typeof toggleLegHedgeRow === "function") toggleLegHedgeRow(tr.id, true);
                }
            }
        });
    } else if (action === "HEDGE_OFF") {
        document.querySelectorAll("#straddleLegsBuilderBody tr.straddle-leg-builder-row").forEach(tr => {
            const chk = tr.querySelector(".leg-hedge-enabled");
            if (chk) {
                chk.checked = false;
                if (typeof toggleLegHedgeRow === "function") toggleLegHedgeRow(tr.id, false);
            }
        });
    } else if (action === "LOTS_1") {
        document.querySelectorAll(".leg-lots").forEach(el => el.value = 1);
    } else if (action === "LOTS_2") {
        document.querySelectorAll(".leg-lots").forEach(el => el.value = 2);
    } else if (action === "LOTS_5") {
        document.querySelectorAll(".leg-lots").forEach(el => el.value = 5);
    } else if (action === "ACTION_SQUARE_LEG") {
        document.querySelectorAll(".leg-action").forEach(el => el.value = "Square off leg");
    } else if (action === "ACTION_SQUARE_ALL") {
        document.querySelectorAll(".leg-action").forEach(el => el.value = "Square off all");
    } else if (action === "REEXEC_NONE") {
        document.querySelectorAll(".leg-tgt-re-type").forEach(el => el.value = "NONE");
        document.querySelectorAll(".leg-sl-re-type").forEach(el => el.value = "NONE");
    } else if (action === "REEXEC_DEFAULT") {
        document.querySelectorAll(".leg-tgt-re-type").forEach(el => el.value = "RE EXECUTE");
        document.querySelectorAll(".leg-tgt-re-cnt").forEach(el => el.value = 6);
        document.querySelectorAll(".leg-sl-re-type").forEach(el => el.value = "RE EXECUTE");
        document.querySelectorAll(".leg-sl-re-cnt").forEach(el => el.value = 1);
    } else if (action === "TGT_45") {
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
    document.querySelectorAll("#straddleLegsBuilderBody tr").forEach(tr => {
        syncLegRowDisabledFields(tr);
        if (typeof syncStrategyLegChange === "function") syncStrategyLegChange(tr.id);
    });
    if (typeof updateLegsCountBadge === "function") updateLegsCountBadge();
    const bulkSelect = document.getElementById("cfgBulkEdit");
    if (bulkSelect) bulkSelect.value = "";
}

window._lastSecToggleTs = window._lastSecToggleTs || {};


// -------------------------------------------------------------
// 🎯 ALGOTEST & STOCKMOCK BACKTEST PARITY CONTROLS & HANDLERS
// -------------------------------------------------------------
function onRefPriceModeChange() {
    const modeEl = document.getElementById("cfgRefPriceMode");
    const mode = modeEl ? modeEl.value : "ENTRY_PRICE";
    const grpBmTime = document.getElementById("grpBenchmarkTime");
    const bmInput = document.getElementById("cfgBenchmarkTime");
    const badgeEl = document.getElementById("badgeRefModeParity");
    const helpEl = document.getElementById("cfgRefPriceModeHelp");

    if (mode === "SPECIFIC_TIME") {
        if (grpBmTime) grpBmTime.style.opacity = "1";
        if (bmInput) bmInput.disabled = false;
        if (badgeEl) {
            badgeEl.textContent = "Benchmark Minute Parity";
            badgeEl.style.color = "#38bdf8";
            badgeEl.style.background = "rgba(56,189,248,0.12)";
            badgeEl.style.borderColor = "rgba(56,189,248,0.3)";
        }
        if (helpEl) helpEl.textContent = "Locks SL & Target % to the exact 1-min candle close of the specified benchmark time.";
    } else if (mode === "ENTRY_CANDLE_CLOSE") {
        if (grpBmTime) grpBmTime.style.opacity = "0.45";
        if (bmInput) bmInput.disabled = true;
        if (badgeEl) {
            badgeEl.textContent = "Entry Candle Parity";
            badgeEl.style.color = "#38bdf8";
            badgeEl.style.background = "rgba(56,189,248,0.12)";
            badgeEl.style.borderColor = "rgba(56,189,248,0.3)";
        }
        if (helpEl) helpEl.textContent = "Calculates SL & Target % from the 1-minute candle close of the entry minute (AlgoTest default).";
    } else if (mode === "PREV_CANDLE_CLOSE") {
        if (grpBmTime) grpBmTime.style.opacity = "0.45";
        if (bmInput) bmInput.disabled = true;
        if (badgeEl) {
            badgeEl.textContent = "T-1 Close (0ms Parity)";
            badgeEl.style.color = "#a855f7";
            badgeEl.style.background = "rgba(168,85,247,0.12)";
            badgeEl.style.borderColor = "rgba(168,85,247,0.3)";
        }
        if (helpEl) helpEl.textContent = "Uses the just-completed prior 1-min candle close. 100% known at entry time with zero delay.";
    } else if (mode === "ENTRY_CANDLE_OPEN") {
        if (grpBmTime) grpBmTime.style.opacity = "0.45";
        if (bmInput) bmInput.disabled = true;
        if (badgeEl) {
            badgeEl.textContent = "Entry Open Parity";
            badgeEl.style.color = "#06b6d4";
            badgeEl.style.background = "rgba(6,182,212,0.12)";
            badgeEl.style.borderColor = "rgba(6,182,212,0.3)";
        }
        if (helpEl) helpEl.textContent = "Uses the open price of the entry minute bar. Available on first tick of the entry minute.";
    } else if (mode === "UNDERLYING_CLOSE") {
        if (grpBmTime) grpBmTime.style.opacity = "0.45";
        if (bmInput) bmInput.disabled = true;
        if (badgeEl) {
            badgeEl.textContent = "Underlying Spot Parity";
            badgeEl.style.color = "#f59e0b";
            badgeEl.style.background = "rgba(245,158,11,0.12)";
            badgeEl.style.borderColor = "rgba(245,158,11,0.3)";
        }
        if (helpEl) helpEl.textContent = "Tracks underlying spot index candle close for SL and Target parity.";
    } else {
        if (grpBmTime) grpBmTime.style.opacity = "0.45";
        if (bmInput) bmInput.disabled = true;
        if (badgeEl) {
            badgeEl.textContent = "Standard Broker Fill";
            badgeEl.style.color = "#10b981";
            badgeEl.style.background = "rgba(16,185,129,0.12)";
            badgeEl.style.borderColor = "rgba(16,185,129,0.3)";
        }
        if (helpEl) helpEl.textContent = "Calculates SL and Target directly from actual broker executed fill price.";
    }
    updateBenchmarkPreview();
}
window.onRefPriceModeChange = onRefPriceModeChange;

function setBenchmarkTimePreset(timeStr) {
    const input = document.getElementById("cfgBenchmarkTime");
    if (input) {
        input.value = timeStr;
    }
    const modeEl = document.getElementById("cfgRefPriceMode");
    if (modeEl && modeEl.value !== "SPECIFIC_TIME") {
        modeEl.value = "SPECIFIC_TIME";
        onRefPriceModeChange();
    } else {
        updateBenchmarkPreview();
    }
}
window.setBenchmarkTimePreset = setBenchmarkTimePreset;

function validateBenchmarkTimeInput(el) {
    if (!el) return;
    let v = el.value.trim();
    const match = v.match(/^([0-1]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?$/);
    if (!match) {
        if (typeof showStraddleToast === "function") {
            showStraddleToast("Invalid time format. Please use HH:MM:SS (e.g. 09:16:00)", "warning", 3000);
        }
        el.value = "09:16:00";
    } else if (!match[3]) {
        el.value = `${match[1].padStart(2, "0")}:${match[2]}:00`;
    }
    updateBenchmarkPreview();
}
window.validateBenchmarkTimeInput = validateBenchmarkTimeInput;

function updateBenchmarkPreview() {
    const mode = document.getElementById("cfgRefPriceMode")?.value || "ENTRY_PRICE";
    const bmTime = document.getElementById("cfgBenchmarkTime")?.value || "09:16:00";
    const scope = document.getElementById("cfgBenchmarkScope")?.value || "INITIAL_ONLY";
    const interim = document.getElementById("cfgBenchmarkInterimMode")?.value || "FILL_PRICE";
    const maxDev = parseFloat(document.getElementById("cfgBenchmarkMaxDevPct")?.value) || 0;
    
    const badgeEl = document.getElementById("prevRefModeBadge");
    const formulaEl = document.getElementById("prevFormulaText");
    const timingEl = document.getElementById("prevTimingText");

    if (!formulaEl || !timingEl) return;

    const bmDisplay = bmTime.length > 5 ? bmTime.substring(0, 5) : bmTime;

    if (mode === "ENTRY_PRICE") {
        if (badgeEl) badgeEl.textContent = "Standard Fill Mode";
        formulaEl.innerHTML = "Target = Broker Traded Fill × (1 - Target %) &nbsp;|&nbsp; Stoploss = Broker Traded Fill × (1 + SL %)";
        timingEl.innerHTML = "⏱️ <strong>Zero Latency:</strong> SL & Target placed immediately with broker upon entry. No waiting.";
    } else if (mode === "PREV_CANDLE_CLOSE") {
        if (badgeEl) badgeEl.textContent = "T-1 Candle Close (Instant)";
        formulaEl.innerHTML = "Target = Prev 1-Min Candle Close × (1 - Target %) &nbsp;|&nbsp; Stoploss = Prev 1-Min Close × (1 + SL %)";
        timingEl.innerHTML = "⚡ <strong>0ms Delay:</strong> Prior minute bar is already finalized at entry. Instant orders with zero naked risk.";
    } else if (mode === "ENTRY_CANDLE_OPEN") {
        if (badgeEl) badgeEl.textContent = "Entry Candle Open";
        formulaEl.innerHTML = "Target = Entry Minute Open × (1 - Target %) &nbsp;|&nbsp; Stoploss = Entry Minute Open × (1 + SL %)";
        timingEl.innerHTML = "⚡ <strong>Instant:</strong> Set at first tick of the entry minute. Matches AlgoTest open price backtest.";
    } else if (mode === "ENTRY_CANDLE_CLOSE") {
        if (badgeEl) badgeEl.textContent = "Entry Minute Close Parity";
        formulaEl.innerHTML = "Target = Entry Minute Close × (1 - Target %) &nbsp;|&nbsp; Stoploss = Entry Minute Close × (1 + SL %)";
        let interimText = interim === "WAIT_CLOSE" ? "Orders deferred until candle closes" : (interim === "CANDLE_OPEN" ? "Interim protection using candle open" : "Interim protection active with fill price");
        let devText = maxDev > 0 ? ` (Max wick deviation capped at ${maxDev}%)` : "";
        timingEl.innerHTML = `⏳ <strong>Candle Close Wait:</strong> Entry candle finalizes at MM:00. ${interimText}${devText}.`;
    } else if (mode === "SPECIFIC_TIME") {
        if (badgeEl) badgeEl.textContent = `Benchmark @ ${bmDisplay}`;
        formulaEl.innerHTML = `Target = ${bmDisplay} Candle Close × (1 - Target %) &nbsp;|&nbsp; Stoploss = ${bmDisplay} Close × (1 + SL %)`;
        let interimText = interim === "WAIT_CLOSE" ? "No resting orders until benchmark minute closes" : (interim === "CANDLE_OPEN" ? "Interim protection using candle open" : "Interim fill price protects open position");
        timingEl.innerHTML = `🎯 <strong>Benchmark Locking:</strong> Locks reference at ${bmDisplay}. ${interimText}.`;
    } else if (mode === "UNDERLYING_CLOSE") {
        if (badgeEl) badgeEl.textContent = "Spot Index Close";
        formulaEl.innerHTML = "Target / SL calibrated against Underlying Spot Index 1-minute close.";
        timingEl.innerHTML = "📊 <strong>Spot Parity:</strong> Movements tracked against the Cash Index underlying.";
    }
}
window.updateBenchmarkPreview = updateBenchmarkPreview;



// -------------------------------------------------------------
// ⚡ RE-EXECUTION, RE-ENTRY & MARGIN SAFETY GUARDS CONTROLLERS
// -------------------------------------------------------------
function onReExecuteSettingsChange() {
    const methodEl = document.getElementById("cfgReExecuteMethod");
    const method = methodEl ? methodEl.value : "Immediate";
    const delayEl = document.getElementById("cfgReExecuteOrderDelaySec");
    const delaySec = parseInt(delayEl ? delayEl.value : 0, 10) || 0;
    const badgeEl = document.getElementById("badgeReExecuteStatus");
    const helpEl = document.getElementById("cfgReExecuteMethodHelp");

    if (badgeEl) {
        if (method.includes("Candle Close")) {
            badgeEl.textContent = delaySec > 0 ? `Candle Close :00 (+${delaySec}s Delay)` : "Candle Close :00 (Stockmock)";
            badgeEl.style.color = "#f59e0b";
            badgeEl.style.background = "rgba(245,158,11,0.12)";
            badgeEl.style.borderColor = "rgba(245,158,11,0.3)";
        } else if (method.includes("Next Minute Open")) {
            badgeEl.textContent = delaySec > 0 ? `Next Min :01 (+${delaySec}s Delay)` : "Next Minute Open (:01s)";
            badgeEl.style.color = "#38bdf8";
            badgeEl.style.background = "rgba(56,189,248,0.12)";
            badgeEl.style.borderColor = "rgba(56,189,248,0.3)";
        } else if (method.includes("Complete 1-Minute")) {
            badgeEl.textContent = `1-Min Cooldown (${60 + delaySec}s Delay)`;
            badgeEl.style.color = "#a855f7";
            badgeEl.style.background = "rgba(168,85,247,0.12)";
            badgeEl.style.borderColor = "rgba(168,85,247,0.3)";
        } else {
            badgeEl.textContent = delaySec > 0 ? `Immediate (+${delaySec}s Delay)` : "Immediate (0ms Delay)";
            badgeEl.style.color = "#10b981";
            badgeEl.style.background = "rgba(16,185,129,0.12)";
            badgeEl.style.borderColor = "rgba(16,185,129,0.3)";
        }
    }

    if (helpEl) {
        if (method.includes("Candle Close")) {
            helpEl.textContent = "Waits for the active 1-minute candle to complete, punching the re-entry order at :00s of the next minute (Stockmock parity).";
        } else if (method.includes("Next Minute Open")) {
            helpEl.textContent = "Punches the re-entry order at :01s of the next minute, ensuring the opening quote is fully established.";
        } else if (method.includes("Complete 1-Minute")) {
            helpEl.textContent = "Applies a strict 60-second cooldown period from the moment of exit before re-executing.";
        } else {
            helpEl.textContent = "Dispatches re-entry order instantaneously (0ms delay) the moment a leg's SL or Target is breached.";
        }
    }

    updateReExecutePreview();
}
window.onReExecuteSettingsChange = onReExecuteSettingsChange;

function setReExecuteDelayPreset(sec) {
    const delayEl = document.getElementById("cfgReExecuteOrderDelaySec");
    if (delayEl) {
        delayEl.value = sec;
    }
    onReExecuteSettingsChange();
}
window.setReExecuteDelayPreset = setReExecuteDelayPreset;

function updateReExecutePreview() {
    const method = document.getElementById("cfgReExecuteMethod")?.value || "Immediate";
    const delaySec = parseInt(document.getElementById("cfgReExecuteOrderDelaySec")?.value || 0, 10) || 0;
    const filter = document.getElementById("cfgReExecuteTriggerFilter")?.value || "BOTH";
    const graceSec = document.getElementById("cfgMarginExitGraceSec")?.value || "15";
    const autoRel = document.getElementById("cfgAutoReleaseMarginReEntry")?.checked !== false;
    const autoSl = document.getElementById("cfgAutoHandlingSl")?.checked !== false;
    const autoExit = !!document.getElementById("cfgAutoExitMargin")?.checked;

    const badgeEl = document.getElementById("prevReExecuteBadge");
    const summaryEl = document.getElementById("prevReExecuteSummaryText");
    const timingEl = document.getElementById("prevReExecuteTimingText");

    if (!summaryEl || !timingEl) return;

    let filterLabel = filter === "STOPLOSS_ONLY" ? "SL Only" : (filter === "TARGET_ONLY" ? "Target Only" : "Both SL & Target");
    let marginReliefLabel = autoRel ? "Margin Relief: Active" : "Margin Relief: Off";
    let slDesyncLabel = autoSl ? "SL Desync Guard: Active" : "SL Desync Guard: Off";
    let autoExitLabel = autoExit ? `Margin Auto-Exit: ${graceSec}s Grace` : "Margin Auto-Exit: Disabled";

    let timingStr = "";
    if (method.includes("Candle Close")) {
        if (badgeEl) badgeEl.textContent = "Candle Close Re-Entry";
        timingStr = `⏳ <strong>Candle Close Timing:</strong> Waits until next minute :00${delaySec > 0 ? ` + ${delaySec}s delay` : ''} before re-entering.`;
    } else if (method.includes("Next Minute Open")) {
        if (badgeEl) badgeEl.textContent = "Next Minute Open";
        timingStr = `⏱️ <strong>Next Minute Open:</strong> Enters at :01s of next minute${delaySec > 0 ? ` + ${delaySec}s delay` : ''}.`;
    } else if (method.includes("Complete 1-Minute")) {
        if (badgeEl) badgeEl.textContent = "1-Min Cooldown";
        timingStr = `⏱️ <strong>Cooldown:</strong> Exact ${60 + delaySec}s wait from exit before re-entering.`;
    } else {
        if (badgeEl) badgeEl.textContent = "Immediate Execution";
        timingStr = `⏱️ <strong>Zero Latency:</strong> Dispatched immediately (${delaySec}s delay) upon leg exit.`;
    }

    summaryEl.innerHTML = `Mode: ${method.split(' ')[0]} (${delaySec}s delay) | Scope: ${filterLabel} | ${marginReliefLabel} | ${slDesyncLabel} | ${autoExitLabel}`;
    timingEl.innerHTML = timingStr;
}
window.updateReExecutePreview = updateReExecutePreview;


function toggleAlgoBuilderSection(secId, forceState = null) {
    const sec = document.getElementById(secId);
    const chevron = document.getElementById("chevron_" + secId);
    if (!sec) return;

    // Debounce rapid duplicate event dispatches (e.g. inline onclick + event listeners within 250ms)
    const now = Date.now();
    if (forceState === null && window._lastSecToggleTs[secId] && (now - window._lastSecToggleTs[secId] < 250)) {
        return;
    }
    window._lastSecToggleTs[secId] = now;

    // Check if section is currently hidden
    const isHidden = sec.classList.contains("collapsed") ||
        sec.style.display === "none" ||
        (!sec.classList.contains("expanded") && window.getComputedStyle(sec).display === "none");

    const shouldExpand = (forceState !== null) ? !!forceState : isHidden;

    if (shouldExpand) {
        sec.classList.remove("collapsed");
        sec.classList.add("expanded");
        sec.style.setProperty("display", "block", "important");
        if (chevron) chevron.textContent = "▼";
    } else {
        sec.classList.remove("expanded");
        sec.classList.add("collapsed");
        sec.style.setProperty("display", "none", "important");
        if (chevron) chevron.textContent = "▶";
    }
}
window.toggleAlgoBuilderSection = toggleAlgoBuilderSection;

function toggleAllAlgoBuilderSections(forceExpand = null) {
    const secIds = [
        'sec_builder_legs',
        'sec_builder_timings',
        'sec_builder_mtm',
        'sec_builder_premium',
        'sec_builder_execution',
        'sec_builder_parity',
        'sec_builder_reentry'
    ];
    const firstSec = document.getElementById(secIds[0]);
    const isFirstExpanded = firstSec ? (firstSec.classList.contains("expanded") && firstSec.style.display !== "none") : true;
    const shouldExpand = (forceExpand !== null) ? !!forceExpand : !isFirstExpanded;

    secIds.forEach(id => {
        toggleAlgoBuilderSection(id, shouldExpand);
    });

    const btn = document.getElementById("btnToggleAllAlgoSections");
    if (btn) {
        btn.textContent = shouldExpand ? "Collapse All" : "Expand All";
    }
}
window.toggleAllAlgoBuilderSections = toggleAllAlgoBuilderSections;

function initCollapsibleAccordionHeaders() {
    document.querySelectorAll(".straddle-collapsible-header").forEach(header => {
        header.style.cursor = "pointer";
    });
}
window.initCollapsibleAccordionHeaders = initCollapsibleAccordionHeaders;

let _timeStepHoldTimer = null;
let _timeStepHoldInterval = null;

function startTimeStepHold(baseId, unit, delta) {
    stepStraddleTimePart(baseId, unit, delta);
    stopTimeStepHold();
    _timeStepHoldTimer = setTimeout(() => {
        _timeStepHoldInterval = setInterval(() => {
            stepStraddleTimePart(baseId, unit, delta);
        }, 85);
    }, 280);
}
window.startTimeStepHold = startTimeStepHold;

function stopTimeStepHold() {
    if (_timeStepHoldTimer) { clearTimeout(_timeStepHoldTimer); _timeStepHoldTimer = null; }
    if (_timeStepHoldInterval) { clearInterval(_timeStepHoldInterval); _timeStepHoldInterval = null; }
}
window.stopTimeStepHold = stopTimeStepHold;

// Global mouseup / touchend safety
window.addEventListener("mouseup", stopTimeStepHold);
window.addEventListener("touchend", stopTimeStepHold);

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
    if (window._straddleState?.activeStrategy) {
        if (baseId === "cfgStartTime") window._straddleState.activeStrategy.start_time = finalStr;
        if (baseId === "cfgEndTime") window._straddleState.activeStrategy.end_time = finalStr;
        if (baseId === "cfgNoReEntryAfterTime") {
            if (!window._straddleState.activeStrategy.no_reentry_after) window._straddleState.activeStrategy.no_reentry_after = {};
            window._straddleState.activeStrategy.no_reentry_after.time = finalStr;
        }
    }
    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }
    if (typeof markStraddleSettingsDirty === "function") {
        markStraddleSettingsDirty();
    }
}
window.stepStraddleTimePart = stepStraddleTimePart;

function initStraddleTimeSteppers() {
    document.querySelectorAll(".straddle-time-stepper-group .straddle-stepper-box").forEach(box => {
        const input = box.querySelector(".straddle-stepper-input");
        if (!input || box._timeInitDone) return;
        box._timeInitDone = true;

        const id = input.id || "";
        const m = id.match(/^(cfg\w+)_(hh|mm|ss)$/);
        if (!m) return;
        const baseId = m[1];
        const unit = m[2];

        // 1. Mouse wheel scroll (Scroll up: +1, Scroll down: -1)
        box.addEventListener("wheel", (e) => {
            e.preventDefault();
            stepStraddleTimePart(baseId, unit, e.deltaY < 0 ? 1 : -1);
        }, { passive: false });

        // 2. Keyboard Arrow Up / Down keys
        input.addEventListener("keydown", (e) => {
            if (e.key === "ArrowUp") {
                e.preventDefault();
                stepStraddleTimePart(baseId, unit, 1);
            } else if (e.key === "ArrowDown") {
                e.preventDefault();
                stepStraddleTimePart(baseId, unit, -1);
            }
        });

        // 3. Auto pad to 2 digits on blur
        input.addEventListener("blur", () => {
            let num = parseInt(input.value, 10);
            if (isNaN(num)) num = 0;
            const maxVal = (unit === "hh") ? 23 : 59;
            num = Math.min(maxVal, Math.max(0, num));
            input.value = String(num).padStart(2, "0");
            syncTimeSegmentsToMain(baseId);
        });

        // 4. Continuous auto-step on hold
        const btns = box.querySelectorAll(".time-step-btn");
        if (btns.length >= 2) {
            const upBtn = btns[0];
            const downBtn = btns[1];

            const attachHold = (btn, delta) => {
                let holdTimer = null;
                let holdInterval = null;
                const stopHold = () => {
                    if (holdTimer) { clearTimeout(holdTimer); holdTimer = null; }
                    if (holdInterval) { clearInterval(holdInterval); holdInterval = null; }
                };

                btn.addEventListener("mousedown", (e) => {
                    if (e.button !== 0) return;
                    stopHold();
                    holdTimer = setTimeout(() => {
                        holdInterval = setInterval(() => {
                            stepStraddleTimePart(baseId, unit, delta);
                        }, 80);
                    }, 300);
                });

                btn.addEventListener("mouseup", stopHold);
                btn.addEventListener("mouseleave", stopHold);
                window.addEventListener("mouseup", stopHold);
            };

            attachHold(upBtn, 1);
            attachHold(downBtn, -1);
        }
    });
}
window.initStraddleTimeSteppers = initStraddleTimeSteppers;


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
    const finalStr = `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    const main = document.getElementById(baseId);
    if (main) main.value = finalStr;
    if (window._straddleState?.activeStrategy) {
        if (baseId === "cfgStartTime") window._straddleState.activeStrategy.start_time = finalStr;
        if (baseId === "cfgEndTime") window._straddleState.activeStrategy.end_time = finalStr;
        if (baseId === "cfgNoReEntryAfterTime") {
            if (!window._straddleState.activeStrategy.no_reentry_after) window._straddleState.activeStrategy.no_reentry_after = {};
            window._straddleState.activeStrategy.no_reentry_after.time = finalStr;
        }
    }
    if (typeof updateTimingsHeaderBadge === "function") {
        updateTimingsHeaderBadge();
    }
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
    // 1. Immediately synchronize any active stepper time inputs to main inputs
    if (typeof syncTimeSegmentsToMain === "function") {
        syncTimeSegmentsToMain("cfgStartTime");
        syncTimeSegmentsToMain("cfgEndTime");
        syncTimeSegmentsToMain("cfgNoReEntryAfterTime");
    }

    const active = window._straddleState.activeStrategy || {};
    const inputSid = document.getElementById("cfgStrategyId")?.value;
    const inputName = document.getElementById("cfgStrategyName")?.value;

    let undSource = "Spot";
    if (document.getElementById("btnUndFutures")?.classList.contains("active-toggle")) {
        undSource = "Futures";
    } else if (document.getElementById("btnUndSynthetic")?.classList.contains("active-toggle")) {
        undSource = "Synthetic Futures";
    }

    const stratUnd = (document.getElementById("cfgStrategyUnderlying")?.value || active.underlying || "NIFTY").toUpperCase();
    const stratExecMode = document.getElementById("cfgStrategyExecutionMode")?.value || active.execution_mode || "LIVE_BROKER";
    const stratShadowPaper = document.getElementById("cfgStrategyShadowPaper") ? document.getElementById("cfgStrategyShadowPaper").checked : (active.shadow_paper_trading !== false);

    const multParsed = parseInt(document.getElementById("cfgMultiplier")?.value, 10);
    const cushionParsed = parseFloat(document.getElementById("cfgCushionMargin")?.value);
    const mtmTgtVal = parseFloat(document.getElementById("cfgMtmTargetValue")?.value);
    const mtmSlVal = parseFloat(document.getElementById("cfgMtmSlValue")?.value);

    const strategy = {
        execution_mode: stratExecMode,
        shadow_paper_trading: stratShadowPaper,
        strategy_id: inputSid || active.strategy_id || ("strat_" + Date.now()),
        name: (inputName && inputName.trim()) ? inputName.trim() : (active.name || "Custom Straddle Strategy"),
        underlying: stratUnd,
        enabled: active.enabled !== false,
        multiplier_type: (document.getElementById("grpAutoMultiplier")?.style.display !== "none") ? "AUTO" : "MANUAL",
        multiplier: (!isNaN(multParsed) && multParsed > 0) ? multParsed : 1,
        cushion_margin: !isNaN(cushionParsed) ? Math.max(0, cushionParsed) : 15000.0,
        segment: document.getElementById("cfgSegment")?.value || "Options",
        start_time: document.getElementById("cfgStartTime")?.value || "09:15:56",
        end_time: document.getElementById("cfgEndTime")?.value || "15:29:50",
        move_sl_to_cost: document.getElementById("cfgToggleMoveSlToCost")?.checked,
        wait_and_trade: document.getElementById("cfgToggleWaitAndTrade")?.checked,
        re_entry_enabled: document.getElementById("cfgToggleReEntry")?.checked,
        range_breakout: document.getElementById("cfgToggleRangeBreakout")?.checked,
        dynamic_hedging: document.getElementById("cfgToggleDynamicHedges") ? document.getElementById("cfgToggleDynamicHedges").checked : true,
        toggles: {
            move_sl_to_cost: !!document.getElementById("cfgToggleMoveSlToCost")?.checked,
            wait_and_trade: !!document.getElementById("cfgToggleWaitAndTrade")?.checked,
            re_entry_active: !!document.getElementById("cfgToggleReEntry")?.checked,
            range_breakout: !!document.getElementById("cfgToggleRangeBreakout")?.checked,
            dynamic_hedging: document.getElementById("cfgToggleDynamicHedges") ? !!document.getElementById("cfgToggleDynamicHedges").checked : true
        },
        underlying_price_source: undSource,
        trade_type: document.getElementById("btnTradePositional")?.classList.contains("active-toggle") ? "Positional" : "Intraday",
        schedule_days: (() => {
            const activeDays = [];
            document.querySelectorAll(".day-pill-btn.active").forEach(btn => {
                const d = btn.getAttribute("data-day");
                if (d) activeDays.push(d);
            });
            if (activeDays.length > 0) return activeDays;
            return (active && Array.isArray(active.schedule_days) && active.schedule_days.length > 0) ? active.schedule_days : ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"];
        })(),
        mtm_enabled: !!document.getElementById("cfgToggleMtmSection")?.checked,
        mtm_calc_mode: document.getElementById("btnMtmModePerLot")?.classList.contains("active-toggle") ? "Per Lot" : "Overall",
        mtm_target: {
            enabled: !!(document.getElementById("cfgToggleMtmSection")?.checked && document.getElementById("cfgToggleMtmTarget")?.checked),
            type: document.getElementById("cfgMtmTargetType")?.value || "Lock and Trail",
            profit_reaches: !isNaN(parseFloat(document.getElementById("cfgMtmTargetReach")?.value)) ? parseFloat(document.getElementById("cfgMtmTargetReach")?.value) : 0.0,
            lock_profit: !isNaN(parseFloat(document.getElementById("cfgMtmTargetLock")?.value)) ? parseFloat(document.getElementById("cfgMtmTargetLock")?.value) : 0.0,
            trail_step: !isNaN(parseFloat(document.getElementById("cfgMtmTargetTrailStep")?.value)) ? parseFloat(document.getElementById("cfgMtmTargetTrailStep")?.value) : 0.0,
            trail_val: !isNaN(parseFloat(document.getElementById("cfgMtmTargetTrailVal")?.value)) ? parseFloat(document.getElementById("cfgMtmTargetTrailVal")?.value) : 0.0,
            value: !isNaN(mtmTgtVal) ? mtmTgtVal : 10000.0,
            re_entry: document.getElementById("cfgMtmTargetReEntry")?.value || "None",
            action: document.getElementById("cfgMtmTargetAction")?.value || "Square off all positions"
        },
        mtm_stoploss: {
            enabled: !!(document.getElementById("cfgToggleMtmSection")?.checked && document.getElementById("cfgToggleMtmSl")?.checked),
            type: document.getElementById("cfgMtmSlType")?.value || "None",
            value: !isNaN(mtmSlVal) ? mtmSlVal : -5000.0,
            action: document.getElementById("cfgMtmSlAction")?.value || "Square off all positions"
        },
        mtm_sl: {
            enabled: !!(document.getElementById("cfgToggleMtmSection")?.checked && document.getElementById("cfgToggleMtmSl")?.checked),
            type: document.getElementById("cfgMtmSlType")?.value || "None",
            value: !isNaN(mtmSlVal) ? mtmSlVal : -5000.0,
            action: document.getElementById("cfgMtmSlAction")?.value || "Square off all positions"
        },
        no_reentry_after: {
            enabled: !!document.getElementById("cfgToggleNoReEntryAfter")?.checked,
            time: document.getElementById("cfgNoReEntryAfterTime")?.value || "15:00:00"
        },
        premium_matching: {
            enabled: !!document.getElementById("cfgTogglePremSection")?.checked,
            mode: document.querySelector("input[name='prem_match_mode']:checked")?.value || "No matching of premiums",
            max_difference: !isNaN(parseFloat(document.getElementById("cfgPremMaxDiffVal")?.value)) ? parseFloat(document.getElementById("cfgPremMaxDiffVal")?.value) : 0.0,
            max_difference_type: document.getElementById("cfgPremMaxDiffType")?.value || "Points",
            close_to: !isNaN(parseFloat(document.getElementById("cfgPremCloseToVal")?.value)) ? parseFloat(document.getElementById("cfgPremCloseToVal")?.value) : 0.0,
            range_min: !isNaN(parseFloat(document.getElementById("cfgPremRangeMin")?.value)) ? parseFloat(document.getElementById("cfgPremRangeMin")?.value) : 0.0,
            range_max: !isNaN(parseFloat(document.getElementById("cfgPremRangeMax")?.value)) ? parseFloat(document.getElementById("cfgPremRangeMax")?.value) : 0.0,
            combined_premium_type: document.getElementById("cfgToggleCombinedPrem")?.checked ? (document.getElementById("cfgCombinedPremType")?.value || "Lower than") : "None",
            combined_premium_value: !isNaN(parseFloat(document.getElementById("cfgCombinedPremVal")?.value)) ? parseFloat(document.getElementById("cfgCombinedPremVal")?.value) : 0.0,
            condition_frequency: document.getElementById("cfgPremFrequency")?.value || "LTP (Continuous Monitoring)"
        },
        advanced_settings: {
            entry_order_type: document.getElementById("cfgEntryOrderType")?.value || "Limit (L) / Stoploss Limit (SL-L)",
            hedge_order_type: document.getElementById("cfgHedgeOrderType")?.value || "Marketable Limit",
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
            sl_order_placement: document.getElementById("cfgSlOrderPlacement")?.value || "Place in advance",
            target_order_placement: document.getElementById("cfgTargetOrderPlacement")?.value || "Place in advance",
            sl_placement_delay_sec: isNaN(parseInt(document.getElementById("cfgSlPlacementDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgSlPlacementDelaySec")?.value, 10),
            mtm_sl_delay_sec: isNaN(parseInt(document.getElementById("cfgMtmSlDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgMtmSlDelaySec")?.value, 10),
            re_execute_method: document.getElementById("cfgReExecuteMethod")?.value || "Immediate",
            re_execute_order_delay_sec: isNaN(parseInt(document.getElementById("cfgReExecuteOrderDelaySec")?.value, 10)) ? 0 : parseInt(document.getElementById("cfgReExecuteOrderDelaySec")?.value, 10),
            re_execute_trigger_filter: document.getElementById("cfgReExecuteTriggerFilter")?.value || "BOTH",
            margin_error_grace_sec: parseInt(document.getElementById("cfgMarginExitGraceSec")?.value, 10) || 15,
            re_entry_placement: document.getElementById("cfgReEntryPlacement")?.value || "Place in advance",
            re_cost_activation: document.getElementById("cfgReCostActivation")?.value || "Immediate",
            no_reentry_after_enabled: !!document.getElementById("cfgToggleNoReEntryAfter")?.checked,
            no_reentry_after_time: document.getElementById("cfgNoReEntryAfterTime")?.value || "15:00:00",
            re_entry_before_time: document.getElementById("cfgNoReEntryAfterTime")?.value || "15:00:00",
            auto_handling_sl_cancellation: document.getElementById("cfgAutoHandlingSl")?.checked !== false,
            auto_exit_margin_error: !!document.getElementById("cfgAutoExitMargin")?.checked,
            auto_exit_on_margin_error: !!document.getElementById("cfgAutoExitMargin")?.checked,
            auto_release_margin_reentry: document.getElementById("cfgAutoReleaseMarginReEntry") ? document.getElementById("cfgAutoReleaseMarginReEntry").checked : true,
            reference_price_mode: document.getElementById("cfgRefPriceMode")?.value || "ENTRY_PRICE",
            benchmark_time: document.getElementById("cfgBenchmarkTime")?.value || "09:16:00",
            benchmark_scope: document.getElementById("cfgBenchmarkScope")?.value || (document.getElementById("cfgBenchmarkInitialOnly")?.checked ? "INITIAL_ONLY" : "ALL_ENTRIES"),
            benchmark_initial_only: (document.getElementById("cfgBenchmarkScope")?.value || (document.getElementById("cfgBenchmarkInitialOnly")?.checked ? "INITIAL_ONLY" : "ALL_ENTRIES")) === "INITIAL_ONLY",
            benchmark_interim_mode: document.getElementById("cfgBenchmarkInterimMode")?.value || "FILL_PRICE",
            max_benchmark_deviation_pct: parseFloat(document.getElementById("cfgBenchmarkMaxDevPct")?.value) || 0,
            benchmark_broker_action: document.getElementById("cfgBenchmarkBrokerAction")?.value || "AUTO_MODIFY"
        },
        advanced: null,
        legs: []
    };
    strategy.advanced_settings = Object.assign(
        {},
        strategy.advanced_settings,
        (typeof serializeAutoSizingSettings === "function") ? serializeAutoSizingSettings() : {}
    );
    strategy.advanced = Object.assign({}, strategy.advanced_settings);

    const trs = document.querySelectorAll("#straddleLegsBuilderBody tr");
    if (trs.length === 0 && Array.isArray(active.legs) && active.legs.length > 0) {
        strategy.legs = JSON.parse(JSON.stringify(active.legs));
    } else {
        const dynHedgeChecked = document.getElementById("cfgToggleDynamicHedges") ? document.getElementById("cfgToggleDynamicHedges").checked : true;
        trs.forEach(tr => {
            const legSide = tr.querySelector(".leg-side")?.value || "Sell";
            const isSellLeg = String(legSide).toUpperCase() === "SELL";
            const hChk = tr.querySelector(".leg-hedge-enabled");
            const isHedgeOn = isSellLeg && (dynHedgeChecked || (hChk ? hChk.checked : true));
            const hInitVal = parseInt(tr.querySelector(".leg-hedge-initial-otm")?.value, 10) || 20;
            const hThreshVal = parseInt(tr.querySelector(".leg-hedge-max-thresh")?.value, 10) || 22;
            const hResetVal = parseInt(tr.querySelector(".leg-hedge-reset-otm")?.value, 10) || 20;

            const effectiveTgtPlacement = tr.querySelector(".leg-tgt-placement")?.value || strategy.advanced_settings.target_order_placement || "Place in advance";
            const effectiveSlPlacement = tr.querySelector(".leg-sl-placement")?.value || strategy.advanced_settings.sl_order_placement || "Place in advance";

            const tgtValParsed = parseFloat(tr.querySelector(".leg-tgt-val")?.value);
            const tgtReCntParsed = parseInt(tr.querySelector(".leg-tgt-re-cnt")?.value, 10);
            const slValParsed = parseFloat(tr.querySelector(".leg-sl-val")?.value);
            const slReCntParsed = parseInt(tr.querySelector(".leg-sl-re-cnt")?.value, 10);
            const lotsParsed = parseInt(tr.querySelector(".leg-lots")?.value, 10);

            const tslVal = tr.querySelector(".leg-tsl-type")?.value || "None";
            const tslProfitVal = parseFloat(tr.querySelector(".leg-tsl-profit")?.value);
            const tslTrailVal = parseFloat(tr.querySelector(".leg-tsl-trail")?.value);

            strategy.legs.push({
                id: tr.id,
                side: legSide,
                product: tr.querySelector(".leg-product")?.value,
                strike_selection: tr.querySelector(".leg-strike")?.value,
                multiple_strike_selection: tr.querySelector(".leg-multiple-strike")?.value || "None",
                option_type: tr.querySelector(".leg-type")?.value,
                lots: (!isNaN(lotsParsed) && lotsParsed > 0) ? lotsParsed : 1,
                associated_hedge: {
                    enabled: isHedgeOn,
                    initial_otm: hInitVal,
                    max_distance_threshold: hThreshVal,
                    reset_otm: hResetVal,
                    exit_with_parent: true
                },
                target_type: tr.querySelector(".leg-tgt-type")?.value,
                target_value: !isNaN(tgtValParsed) ? tgtValParsed : 45.0,
                target_order_placement: effectiveTgtPlacement,
                target_re_execute_type: tr.querySelector(".leg-tgt-re-type")?.value,
                target_re_execute_count: !isNaN(tgtReCntParsed) ? tgtReCntParsed : 6,
                sl_type: tr.querySelector(".leg-sl-type")?.value,
                sl_value: !isNaN(slValParsed) ? slValParsed : 66.0,
                sl_order_placement: effectiveSlPlacement,
                tsl_type: tslVal,
                tsl_profit_pts: !isNaN(tslProfitVal) ? tslProfitVal : 0.0,
                tsl_trail_pts: !isNaN(tslTrailVal) ? tslTrailVal : 0.0,
                sl_re_execute_type: tr.querySelector(".leg-sl-re-type")?.value,
                sl_re_execute_count: !isNaN(slReCntParsed) ? slReCntParsed : 1,
                action: tr.querySelector(".leg-action")?.value,
                underlying: stratUnd,
                expiry: tr.querySelector(".leg-expiry")?.value
            });
        });
    }

    return strategy;
}

async function saveStraddleStrategySettings(options = {}) {
    const isSilent = !!(options && options.silent);
    const strat = serializeCurrentStrategySettings();
    if (!isSilent) {
        showStraddleToast(`💾 Saving strategy '${strat.name}'...`, "info", 1500);
    }

    // Optimistically update local state for instant responsiveness
    if (window._straddleState) {
        if (!window._straddleState.allStrategies) {
            window._straddleState.allStrategies = [];
        }
        const idx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === strat.strategy_id || s.name === strat.name);
        if (idx >= 0) {
            const existing = window._straddleState.allStrategies[idx];
            strat.strategy_id = existing.strategy_id;
            strat.positions = existing.positions || [];
            strat.closed_positions = existing.closed_positions || [];
            strat.status = existing.status || strat.status || "READY";
            strat.mtm = existing.mtm || 0.0;
            strat.group_mtm = existing.group_mtm || 0.0;
            strat.re_execution_counts = existing.re_execution_counts || strat.re_execution_counts;
            strat.broker_margin_per_lot = existing.broker_margin_per_lot || strat.broker_margin_per_lot;
            strat.effective_multiplier = existing.effective_multiplier || strat.effective_multiplier;
            window._straddleState.allStrategies[idx] = strat;
        } else {
            // New strategy or renamed strategy: ensure it's added to allStrategies array immediately!
            if (window._straddleState.activeStrategy && (window._straddleState.activeStrategy.strategy_id === strat.strategy_id || window._straddleState.activeStrategy.name === strat.name)) {
                strat.positions = window._straddleState.activeStrategy.positions || [];
                strat.closed_positions = window._straddleState.activeStrategy.closed_positions || [];
                strat.status = window._straddleState.activeStrategy.status || strat.status || "READY";
                strat.mtm = window._straddleState.activeStrategy.mtm || 0.0;
                strat.group_mtm = window._straddleState.activeStrategy.group_mtm || 0.0;
                strat.re_execution_counts = window._straddleState.activeStrategy.re_execution_counts || strat.re_execution_counts;
                strat.broker_margin_per_lot = window._straddleState.activeStrategy.broker_margin_per_lot;
                strat.effective_multiplier = window._straddleState.activeStrategy.effective_multiplier;
            } else {
                if (!strat.positions) strat.positions = [];
                if (!strat.closed_positions) strat.closed_positions = [];
                if (!strat.status) strat.status = "READY";
                if (strat.mtm === undefined) strat.mtm = 0.0;
            }
            window._straddleState.allStrategies.push(strat);
        }
        window._straddleState.activeStrategy = strat;
        window._straddleState.activeStrategyId = strat.strategy_id;
        renderStraddleAlgosTable(true);
    }

    try {
        // Never send stale browser positions/closed_positions when saving settings;
        // runtime positions are strictly owned by the backend engine.
        const stratPayload = { ...strat };
        delete stratPayload.positions;
        delete stratPayload.closed_positions;
        const resp = await fetch("/api/straddle/strategy/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy: stratPayload })
        });
        const res = await resp.json();
        if (res.success) {
            window._straddleSettingsDirty = false;
            if (!isSilent) {
                showStraddleToast(`💾 Strategy '${strat.name}' saved successfully!`, "success", 2000);
            }
            if (res.strategy) {
                const saved = res.strategy;
                if (window._straddleState) {
                    window._straddleState.activeStrategy = saved;
                    window._straddleState.activeStrategyId = saved.strategy_id;
                    if (window._straddleState.allStrategies) {
                        const sIdx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === saved.strategy_id || s.name === saved.name);
                        if (sIdx >= 0) {
                            window._straddleState.allStrategies[sIdx] = saved;
                        } else {
                            window._straddleState.allStrategies.push(saved);
                        }
                    }
                }
                try {
                    localStorage.setItem("straddle_cached_strategies", JSON.stringify(window._straddleState.allStrategies));
                    localStorage.setItem("straddle_active_strategy_id", saved.strategy_id);
                } catch (_) {}
                if (saved.broker_margin_per_lot) {
                    window._formSimulatedMarginPerLot = saved.broker_margin_per_lot;
                    updateFormAutoMultiplierPreview(saved.broker_margin_per_lot);
                }
            }
            await fetchStraddleState();
            renderStraddleAlgosTable(true);
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        } else {
            if (!isSilent) {
                showStraddleToast("Error saving: " + (res.error || "Failed"), "error", 3500);
            }
        }
    } catch (e) {
        if (!isSilent) {
            showStraddleToast("Network error: " + e.message, "error", 3500);
        }
    }
}

async function saveAsNewStraddlePreset() {
    const name = prompt("Enter name for this new Strategy Preset:", "Custom Preset " + Date.now().toString().slice(-4));
    if (!name) return;
    const strat = serializeCurrentStrategySettings();
    strat.strategy_id = "strat_" + Date.now();
    strat.name = name;
    showStraddleToast(`📄 Creating preset '${name}'...`, "info", 1500);

    try {
        // Never send stale browser positions/closed_positions when saving settings;
        // runtime positions are strictly owned by the backend engine.
        const stratPayload = { ...strat };
        delete stratPayload.positions;
        delete stratPayload.closed_positions;
        const resp = await fetch("/api/straddle/strategy/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy: stratPayload })
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
    showStraddleToast(`💾 Saving settings and punching orders for '${strat.name}'...`, "info", 4000);

    const btn = document.getElementById("btnApplyAndStartTrade");
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = `⏳ Executing Trade...`;
    }

    const errBanner = document.getElementById("stratSettingsErrorBanner");
    if (errBanner) {
        errBanner.style.display = "none";
        errBanner.textContent = "";
    }

    try {
        // Step 1: Save settings FIRST and ensure it succeeds
        const saveResp = await fetch("/api/straddle/strategy/save", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy: strat })
        });
        const saveRes = await saveResp.json();
        if (!saveRes.success) {
            showStraddleToast(`❌ Failed to save settings: ${saveRes.error || "Unknown error"}`, "error", 5000);
            if (errBanner) {
                errBanner.textContent = "Failed to save settings: " + (saveRes.error || "Unknown error");
                errBanner.style.display = "block";
            }
            if (btn) { btn.disabled = false; btn.innerHTML = `▶ Apply & Start Trade`; }
            return;
        }

        // Step 2: Now start the algo with fresh settings and wait for order execution outcome
        if (window._straddleState?.activeStrategy) {
            window._straddleState.activeStrategy.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
            updateStraddleReexecBadges([]);
        }
        const startResp = await fetch("/api/straddle/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strat.strategy_id, force_fresh: true })
        });
        const startRes = await startResp.json();

        await Promise.all([fetchStraddleState(), loadStraddleOpenPositions()]);
        updateStraddleReexecBadges(window._straddleState?.activeStrategy?.positions || []);
        renderStraddleAlgosTable(true);

        if (startRes.success) {
            showStraddleToast(`🚀 Trade Started! ${startRes.filled_count ? startRes.filled_count + ' leg(s) filled on Kotak Neo.' : 'Orders punched.'}`, "success", 4000);
            toggleStraddleSettingsPanel(false);
        } else {
            const errMsg = startRes.error || "Broker rejected orders";
            showStraddleToast(`❌ Order Placement Failed: ${errMsg}`, "error", 7000);
            if (errBanner) {
                errBanner.textContent = `❌ Order Placement Failed: ${errMsg}`;
                errBanner.style.display = "block";
            }
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 5000);
        if (errBanner) {
            errBanner.textContent = "Error: " + e.message;
            errBanner.style.display = "block";
        }
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = `▶ Apply & Start Trade`;
        }
    }
}

async function onStraddleStrategySelect(stratId) {
    if (!stratId) return;
    try {
        // Auto-save pending changes on previous strategy before switching
        if (window._straddleSettingsDirty && typeof saveStraddleStrategySettings === "function") {
            try {
                await saveStraddleStrategySettings({ silent: true });
            } catch (_) {}
        }

        // 1. Immediately find target strategy in allStrategies
        let strat = (window._straddleState?.allStrategies || []).find(s => s.strategy_id === stratId || s.name === stratId);

        // 2. Synchronously update activeStrategy and activeStrategyId
        window._straddleState.activeStrategyId = strat ? strat.strategy_id : stratId;
        if (strat) {
            window._straddleState.activeStrategy = strat;
            window._straddleState.positions = strat.positions || [];
        }

        // 3. Immediately update header title and dropdown element
        const titleEl = document.getElementById("straddleTerminalStrategyTitle");
        if (titleEl && strat) {
            titleEl.textContent = strat.name || strat.strategy_id;
        }
        const undBadge = document.getElementById("straddleTerminalUnderlyingBadge");
        if (undBadge && strat) {
            undBadge.textContent = canonicalUnderlying(strat.underlying || "NIFTY");
        }

        const sel = document.getElementById("straddleStrategySelect");
        if (sel && sel.value !== (strat ? strat.strategy_id : stratId)) {
            sel.value = strat ? strat.strategy_id : stratId;
        }

        // 4. Immediately update settings form, positions, action bar, and alert banner
        if (strat) {
            populateStraddleSettingsForm(strat);
            renderStraddlePositions(strat.positions || [], strat.group_mtm || 0.0, strat.mtm || 0.0);
            if (typeof updateTerminalActionBar === "function") {
                updateTerminalActionBar(strat);
            }
            if (typeof renderStraddleMarginAutoExitAlert === "function") {
                renderStraddleMarginAutoExitAlert(strat);
            }
            if (typeof renderStraddleHeader === "function") {
                renderStraddleHeader({
                    ...window._straddleState,
                    active_strategy: strat
                });
            }
        }

        // 5. Notify backend to persist active strategy
        const resp = await fetch("/api/straddle/strategy/set_active", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: strat ? strat.strategy_id : stratId })
        });
        const res = await resp.json();
        if (res && res.strategy) {
            window._straddleState.activeStrategy = res.strategy;
            window._straddleState.activeStrategyId = res.strategy.strategy_id;
            if (window._straddleState.allStrategies) {
                const idx = window._straddleState.allStrategies.findIndex(s => s.strategy_id === res.strategy.strategy_id);
                if (idx >= 0) window._straddleState.allStrategies[idx] = res.strategy;
            }
            if (window._straddleState.currentSubView === "terminal") {
                const settingsPanel = document.getElementById("straddleSettingsPanel");
                const hasFocusInForm = settingsPanel && settingsPanel.contains(document.activeElement);
                if (!hasFocusInForm) {
                    populateStraddleSettingsForm(res.strategy);
                }
                renderStraddlePositions(res.strategy.positions || [], res.strategy.group_mtm || 0.0, res.strategy.mtm || 0.0);
            }
        }
    } catch (e) {
        console.error("Error setting active strategy:", e);
    }
}

async function straddleStartAlgo() {
    const panel = document.getElementById("straddleSettingsPanel");
    const isOpen = panel && !panel.classList.contains("hidden") && panel.style.display !== "none";
    if (isOpen) {
        return saveAndStartStraddleTrade();
    }

    const stratId = window._straddleState?.activeStrategyId;
    if (!stratId) {
        showStraddleToast("⚠️ No active algorithm selected to start", "warning", 3000);
        return;
    }

    const btn = document.getElementById("btnStraddleStartAlgo");
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = "⏳ Starting...";
    }

    showStraddleToast("🚀 Dispatching strategy orders to broker...", "info", 3500);
    if (window._straddleState?.activeStrategy) {
        window._straddleState.activeStrategy.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
        updateStraddleReexecBadges([]);
    }
    try {
        const resp = await fetch("/api/straddle/start", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: stratId, force_fresh: true })
        });
        const res = await resp.json();
        await Promise.all([fetchStraddleState(), loadStraddleOpenPositions()]);
        updateStraddleReexecBadges(window._straddleState?.activeStrategy?.positions || []);
        renderStraddleAlgosTable(true);

        if (res.success) {
            showStraddleToast(`✅ Algorithm started! ${res.filled_count ? res.filled_count + ' leg(s) filled.' : 'Orders punched.'}`, "success", 4000);
        } else {
            showStraddleToast(`❌ Order Placement Failed: ${res.error || "Broker rejected orders"}`, "error", 7000);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 4000);
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = "▶ Start Algo";
        }
    }
}

async function straddleResetActiveStrategy() {
    return resetCurrentStraddleAlgo();
}
window.straddleResetActiveStrategy = straddleResetActiveStrategy;

async function clearStraddleClosedAndExpiredPositions() {
    showStraddleToast("🧹 Clearing closed & expired positions...", "info", 1500);
    try {
        const strat = window._straddleState?.activeStrategy;
        const targetSid = strat?.strategy_id || window._straddleState?.activeStrategyId || "";
        const resp = await fetch("/api/straddle/positions/clear_closed", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ strategy_id: targetSid })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(`✅ Cleared ${res.cleared_count || 0} closed position(s).`, "success", 2000);
            if (window._straddleState?.activeStrategy) {
                window._straddleState.activeStrategy.closed_positions = [];
                const openLegs = (window._straddleState.activeStrategy.positions || []).filter(p => p.status === "OPEN" || p.status === "LIVE OPEN");
                window._straddleState.activeStrategy.positions = openLegs;
                if (openLegs.length === 0) {
                    window._straddleState.activeStrategy.re_execution_counts = { ce_sl: 0, ce_target: 0, pe_sl: 0, pe_target: 0 };
                    updateStraddleReexecBadges([]);
                }
            }
            window._straddleState.closed_positions = [];
            window._straddleState.closedPositionsCache = [];
            await loadStraddleOpenPositions();
            await fetchStraddleState();
            updateStraddleReexecBadges(window._straddleState?.activeStrategy?.positions || []);
        } else {
            showStraddleToast("Error: " + (res.error || "Failed"), "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Network error: " + e.message, "error", 3000);
    }
}

async function straddleTerminateAlgo() {
    if (!confirm("Pause/Terminate algorithm execution? (Note: Existing open broker positions will remain open without algorithmic SL/TP. Use 'Square Off All & Terminate' to exit positions).")) return;
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
            if (typeof renderStraddlePositions === "function") {
                renderStraddlePositions();
            }
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3000);
    }
}

async function straddleSquareOffAllAndTerminate() {
    let openPositionsCount = 0;
    const countedIds = new Set();

    // 1. Check open positions cache
    if (Array.isArray(window._openPositionsCache)) {
        window._openPositionsCache.forEach(p => {
            if (p && (p.status === "OPEN" || p.status === "LIVE OPEN" || p.status === "ENTRY_FILLED")) {
                const pid = p.symbol || p.order_id || p.id || JSON.stringify(p);
                if (!countedIds.has(pid)) {
                    countedIds.add(pid);
                    openPositionsCount++;
                }
            }
        });
    }

    // 2. Check active strategy positions
    const posList = window._straddleState?.positions || window._straddleState?.activeStrategy?.positions || [];
    if (Array.isArray(posList)) {
        posList.forEach(p => {
            if (p && (p.status === "OPEN" || p.status === "LIVE OPEN" || p.status === "ENTRY_FILLED")) {
                const pid = p.symbol || p.order_id || p.id || JSON.stringify(p);
                if (!countedIds.has(pid)) {
                    countedIds.add(pid);
                    openPositionsCount++;
                }
            }
        });
    }

    // 3. Check all other strategies in platform
    if (Array.isArray(window._straddleState?.allStrategies)) {
        window._straddleState.allStrategies.forEach(s => {
            if (Array.isArray(s.positions)) {
                s.positions.forEach(p => {
                    if (p && (p.status === "OPEN" || p.status === "LIVE OPEN" || p.status === "ENTRY_FILLED")) {
                        const pid = p.symbol || p.order_id || p.id || JSON.stringify(p);
                        if (!countedIds.has(pid)) {
                            countedIds.add(pid);
                            openPositionsCount++;
                        }
                    }
                });
            }
        });
    }

    const hasOpen = openPositionsCount > 0;

    if (!hasOpen) {
        if (!confirm("No open positions are currently active. Do you want to stop/terminate all running algorithms?")) {
            return;
        }
    } else {
        if (!confirm(`🚨 EMERGENCY ACTION:\nSquare off ALL ${openPositionsCount} open position(s) across all strategies and terminate algorithm execution immediately?`)) {
            return;
        }
    }
    showStraddleToast("⚠️ Executing Square-Off & Termination...", "warning", 2000);

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

function updateExecutionModeUI(mode) {
    const isPaper = (mode === "PAPER_TRADING");

    // Topbar Pill Button
    const pillBtn = document.getElementById("topbarModePillBtn");
    const pillIcon = document.getElementById("topbarModePillIcon");
    const pillText = document.getElementById("topbarModePillText");
    const resetBtn = document.getElementById("topbarResetPaperBtn");

    if (pillBtn && pillText && pillIcon) {
        if (isPaper) {
            pillIcon.textContent = "🧪";
            pillText.textContent = "PAPER TRADING";
            pillBtn.style.background = "rgba(168,85,247,0.15)";
            pillBtn.style.borderColor = "#a855f7";
            pillBtn.style.color = "#c084fc";
            pillBtn.title = "🧪 Paper Simulation Mode Active (Click to switch to Live Broker)";
            if (resetBtn) resetBtn.style.display = "inline-flex";
        } else {
            pillIcon.textContent = "🟢";
            pillText.textContent = "LIVE BROKER";
            pillBtn.style.background = "rgba(16,185,129,0.12)";
            pillBtn.style.borderColor = "#10b981";
            pillBtn.style.color = "#10b981";
            pillBtn.title = "🟢 Kotak Neo Live Broker Connected (Click to switch to Paper Trading)";
            if (resetBtn) resetBtn.style.display = "none";
        }
    }

    // Settings Dropdown Mode Label
    const topbarLabel = document.getElementById("topbarModeLabel");
    if (topbarLabel) {
        topbarLabel.textContent = isPaper ? "Paper Simulation" : "Live Broker";
        topbarLabel.style.color = isPaper ? "#c084fc" : "#10b981";
    }

    // Terminal / Open Positions Mode Badges
    const brokerText = document.getElementById("openPositionsBrokerText");
    if (brokerText) {
        brokerText.textContent = isPaper ? "🧪 Paper Trading (Simulation)" : "🟢 Kotak Neo Live Broker";
        brokerText.style.color = isPaper ? "#c084fc" : "#38bdf8";
    }
    const modeText = document.getElementById("straddleModeText");
    const modeDot = document.getElementById("straddleModeDot");
    if (modeText) {
        modeText.textContent = isPaper ? "🧪 Paper Trading (Simulation)" : "🟢 Kotak Neo Live Broker";
        modeText.style.color = isPaper ? "#c084fc" : "#38bdf8";
    }
    if (modeDot) {
        modeDot.style.background = isPaper ? "#a855f7" : "#10b981";
        modeDot.style.boxShadow = isPaper ? "0 0 8px #a855f7" : "0 0 8px #10b981";
    }
}
window.updateExecutionModeUI = updateExecutionModeUI;

async function straddleToggleExecutionMode() {
    const curMode = window._straddleState?.execution_mode || "LIVE_BROKER";
    const nextMode = (curMode === "LIVE_BROKER") ? "PAPER_TRADING" : "LIVE_BROKER";
    const confirmMsg = (nextMode === "PAPER_TRADING")
        ? "Switch to 🧪 PAPER TRADING mode?\n\n• Live market ticks and quotes will be used for execution.\n• Orders, Stoplosses, and Targets will simulate in high fidelity.\n• Zero real money or broker orders will be placed."
        : "Switch to 🟢 LIVE BROKER mode?\n\n• Real orders will be routed directly to Kotak Neo Trade API.\n• Actual margin, real capital, and live positions will be affected.";

    if (!confirm(confirmMsg)) return;

    try {
        const resp = await fetch("/api/straddle/toggle_mode", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ mode: nextMode })
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast(res.message || `Switched to ${nextMode}`, "success", 3000);
            if (window._straddleState) window._straddleState.execution_mode = nextMode;
            updateExecutionModeUI(nextMode);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to switch mode", "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error toggling mode: " + e.message, "error", 3000);
    }
}
window.straddleToggleExecutionMode = straddleToggleExecutionMode;
window.toggleStraddleExecutionMode = straddleToggleExecutionMode;

let _isBrokerConnected = true;

function updateBrokerConnectionUI(connected) {
    _isBrokerConnected = !!connected;

    // Topbar badge & button in dropdown
    const connBadge = document.getElementById("kotakBrokerConnBadge");
    const connBtn = document.getElementById("btnToggleBrokerConnection");
    const connBtnText = document.getElementById("btnToggleBrokerConnectionText");
    const topAlert = document.getElementById("topbarBrokerDisconnectedAlert");

    // Modal badge & button
    const modalBadge = document.getElementById("modalKotakBrokerConnBadge");
    const modalBtn = document.getElementById("btnModalToggleBrokerConnection");

    if (_isBrokerConnected) {
        if (connBadge) {
            connBadge.textContent = "CONNECTED";
            connBadge.style.background = "rgba(16,185,129,0.15)";
            connBadge.style.color = "#10b981";
            connBadge.style.borderColor = "#059669";
        }
        if (connBtn) {
            connBtn.style.background = "rgba(239,68,68,0.15)";
            connBtn.style.color = "#ef4444";
            connBtn.style.borderColor = "#ef4444";
        }
        if (connBtnText) connBtnText.textContent = "Disconnect from Kotak Neo";
        if (topAlert) topAlert.style.display = "none";

        if (modalBadge) {
            modalBadge.textContent = "CONNECTED";
            modalBadge.style.background = "rgba(16,185,129,0.15)";
            modalBadge.style.color = "#10b981";
            modalBadge.style.borderColor = "#059669";
        }
        if (modalBtn) {
            modalBtn.className = "straddle-btn btn-danger";
            modalBtn.textContent = "Disconnect from Kotak";
        }
    } else {
        if (connBadge) {
            connBadge.textContent = "DISCONNECTED";
            connBadge.style.background = "rgba(239,68,68,0.2)";
            connBadge.style.color = "#f87171";
            connBadge.style.borderColor = "#ef4444";
        }
        if (connBtn) {
            connBtn.style.background = "rgba(16,185,129,0.2)";
            connBtn.style.color = "#34d399";
            connBtn.style.borderColor = "#10b981";
        }
        if (connBtnText) connBtnText.textContent = "Connect to Kotak Neo";
        if (topAlert) topAlert.style.display = "inline-flex";

        if (modalBadge) {
            modalBadge.textContent = "DISCONNECTED (BLOCKED)";
            modalBadge.style.background = "rgba(239,68,68,0.2)";
            modalBadge.style.color = "#f87171";
            modalBadge.style.borderColor = "#ef4444";
        }
        if (modalBtn) {
            modalBtn.className = "straddle-btn btn-success";
            modalBtn.textContent = "Connect to Kotak Neo";
        }
    }
}
window.updateBrokerConnectionUI = updateBrokerConnectionUI;

async function toggleBrokerConnection(event) {
    if (event && event.stopPropagation) event.stopPropagation();

    const targetState = !_isBrokerConnected;
    const confirmMsg = targetState
        ? "Connect platform to Kotak Neo?\n\nLive orders will now be permitted to route to Kotak Neo."
        : "⚠️ ARE YOU SURE YOU WANT TO DISCONNECT FROM KOTAK NEO?\n\nWhile disconnected, UNDER NO CIRCUMSTANCES will any order (entries, SL, targets, re-entries, square-offs) be sent to Kotak Neo.\n\nYou can safely make code updates or monitor the market with zero risk of rogue orders.";

    if (!confirm(confirmMsg)) return;

    try {
        const resp = await fetch("/api/straddle/broker/connection", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ connected: targetState })
        });
        const res = await resp.json();
        if (res.success) {
            updateBrokerConnectionUI(res.connected);
            showStraddleToast(
                res.connected
                    ? "⚡ Kotak Neo CONNECTED: Live orders enabled."
                    : "🔴 Kotak Neo DISCONNECTED: All orders strictly blocked.",
                res.connected ? "success" : "warning",
                3500
            );
            if (typeof fetchStraddleState === "function") await fetchStraddleState();
        } else {
            showStraddleToast(res.error || "Failed to update connection", "error", 3500);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3000);
    }
}
window.toggleBrokerConnection = toggleBrokerConnection;

async function resetPaperTradingAccount() {
    if (!confirm("Reset Paper Trading account?\n\nThis will clear all simulated paper positions and clear the paper orderbook.")) return;
    try {
        const resp = await fetch("/api/straddle/paper/reset", {
            method: "POST",
            headers: { "Content-Type": "application/json" }
        });
        const res = await resp.json();
        if (res.success) {
            showStraddleToast("🧪 Paper trading account reset successfully.", "success", 2500);
            await fetchStraddleState();
            await loadStraddleOpenPositions();
        } else {
            showStraddleToast(res.error || "Failed to reset paper account", "error", 3000);
        }
    } catch (e) {
        showStraddleToast("Error: " + e.message, "error", 3000);
    }
}
window.resetPaperTradingAccount = resetPaperTradingAccount;


// =========================================================================
// OPTION CHAIN INTERACTIVE MATRIX (EXPRESSIVE, COLOURFUL + GREEKS & PCR)
// =========================================================================

let _optChainTickerInterval = null;
let _isLoadingOptionChain = false;
window._optChainAbortController = null;
window._lastRenderedOptionChainKey = null;

function startOptionChainTicker() {
    stopOptionChainTicker();
    _optChainTickerInterval = setInterval(() => {
        const modal = document.getElementById("straddleOptionChainModal");
        const isOpen = modal && !modal.classList.contains("hidden") && (modal.style.display !== "none") && (getComputedStyle(modal).display !== "none");
        if (isOpen) {
            loadStraddleOptionChain(null, true);
        } else {
            stopOptionChainTicker();
        }
    }, 1500); // 1.5s smooth, reliable real-time ticks
}

function stopOptionChainTicker() {
    if (_optChainTickerInterval) {
        clearInterval(_optChainTickerInterval);
        _optChainTickerInterval = null;
    }
    if (window._optChainAbortController) {
        try { window._optChainAbortController.abort(); } catch (_) { }
        window._optChainAbortController = null;
    }
}

const STRADDLE_STRIKE_STEPS = {
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
    "COPPER": 10
};

const STRADDLE_LOT_SIZES = {
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
    "COPPER": 2500
};

function formatOptionOi(oi) {
    if (!oi || isNaN(oi)) return "0";
    const num = Number(oi);
    if (num >= 10000000) return (num / 10000000).toFixed(2) + "Cr";
    if (num >= 100000) return (num / 100000).toFixed(1) + "L";
    if (num >= 1000) return (num / 1000).toFixed(1) + "k";
    return String(num);
}
window.formatOptionOi = formatOptionOi;

window._optChainViewMode = localStorage.getItem("straddle_opt_view_mode") || "standard";

function setOptChainViewMode(mode) {
    window._optChainViewMode = mode;
    try {
        localStorage.setItem("straddle_opt_view_mode", mode);
    } catch (_) { }

    const btnStd = document.getElementById("btnOptViewStandard");
    const btnGrk = document.getElementById("btnOptViewGreeks");
    if (btnStd) {
        btnStd.classList.toggle("active", mode === "standard");
        btnStd.style.background = (mode === "standard") ? "#1e293b" : "transparent";
        btnStd.style.color = (mode === "standard") ? "#38bdf8" : "#94a3b8";
        btnStd.style.fontWeight = (mode === "standard") ? "800" : "600";
    }
    if (btnGrk) {
        btnGrk.classList.toggle("active", mode === "greeks");
        btnGrk.style.background = (mode === "greeks") ? "#1e293b" : "transparent";
        btnGrk.style.color = (mode === "greeks") ? "#38bdf8" : "#94a3b8";
        btnGrk.style.fontWeight = (mode === "greeks") ? "800" : "600";
    }

    const thCeCol1 = document.getElementById("thCeCol1");
    const thCeCol2 = document.getElementById("thCeCol2");
    const thPeCol1 = document.getElementById("thPeCol1");
    const thPeCol2 = document.getElementById("thPeCol2");

    if (mode === "greeks") {
        if (thCeCol1) { thCeCol1.textContent = "IV %"; thCeCol1.style.color = "#38bdf8"; }
        if (thCeCol2) { thCeCol2.textContent = "DELTA"; thCeCol2.style.color = "#f8fafc"; }
        if (thPeCol2) { thPeCol2.textContent = "DELTA"; thPeCol2.style.color = "#f8fafc"; }
        if (thPeCol1) { thPeCol1.textContent = "IV %"; thPeCol1.style.color = "#38bdf8"; }
    } else {
        if (thCeCol1) { thCeCol1.textContent = "OI (L)"; thCeCol1.style.color = "#c084fc"; }
        if (thCeCol2) { thCeCol2.textContent = "CHG %"; thCeCol2.style.color = "#94a3b8"; }
        if (thPeCol2) { thPeCol2.textContent = "CHG %"; thPeCol2.style.color = "#94a3b8"; }
        if (thPeCol1) { thPeCol1.textContent = "OI (L)"; thPeCol1.style.color = "#c084fc"; }
    }

    loadStraddleOptionChain(null, false);
}
window.setOptChainViewMode = setOptChainViewMode;

function openOptionChainForActiveStrategy() {
    const stratUnd = document.getElementById("cfgStrategyUnderlying")?.value || 
                     window._straddleState?.activeStrategy?.underlying || 
                     window._activeGlobalUnderlying || "NIFTY";
    openStraddleOptionChainModal(stratUnd, false);
}
window.openOptionChainForActiveStrategy = openOptionChainForActiveStrategy;

function addOptionChainLegToStrategy(strike, optType, side = "Sell", symbol = "", ltp = 0) {
    const activeStrat = window._straddleState?.activeStrategy;
    if (!activeStrat) {
        showStraddleToast("⚠️ No active strategy selected in Algo. Please select or create a strategy first.", "warning", 3000);
        return;
    }
    const und = window._straddleState.optionChainUnderlying || activeStrat.underlying || "NIFTY";
    const rowId = "leg_" + Date.now() + "_" + Math.floor(Math.random() * 1000);
    
    const legObj = {
        id: rowId,
        side: (side && side.toUpperCase() === "BUY") ? "Buy" : "Sell",
        product: "NRML",
        strike_selection: String(strike),
        multiple_strike_selection: "None",
        option_type: (optType || "CE").toUpperCase(),
        lots: 1,
        associated_hedge: {
            enabled: false,
            initial_otm: 20,
            max_distance_threshold: 22,
            reset_otm: 20,
            exit_with_parent: true
        },
        target_type: "TGT: %",
        target_value: 45.0,
        target_re_execute_type: "RE EXECUTE",
        target_re_execute_count: 6,
        sl_type: "SL: %",
        sl_value: 66.0,
        sl_order_placement: document.getElementById("cfgSlOrderPlacement")?.value || "Place in advance",
        target_order_placement: document.getElementById("cfgTargetOrderPlacement")?.value || "Place in advance",
        tsl_type: "None",
        tsl_profit_pts: 0.0,
        tsl_trail_pts: 0.0,
        sl_re_execute_type: "RE EXECUTE",
        sl_re_execute_count: 1,
        action: "Square off leg",
        underlying: und,
        expiry: "Weekly"
    };

    if (!Array.isArray(activeStrat.legs)) {
        activeStrat.legs = [];
    }
    activeStrat.legs.push(legObj);

    if (typeof addStraddleLegRow === "function") {
        addStraddleLegRow(legObj);
    }
    if (typeof updateLegsCountBadge === "function") {
        updateLegsCountBadge();
    }
    if (typeof saveActiveStraddleStrategy === "function") {
        saveActiveStraddleStrategy(true);
    }

    showStraddleToast(`⚡ Added ${und} ${strike} ${optType} (${side}) into Algo Strategy "${activeStrat.name || activeStrat.strategy_id}"!`, "success", 2500);
}
window.addOptionChainLegToStrategy = addOptionChainLegToStrategy;

function getLotSizeForUnderlying(und) {
    if (!und) return 65;
    const u = String(und).toUpperCase();
    return STRADDLE_LOT_SIZES[u] || 65;
}
window.getLotSizeForUnderlying = getLotSizeForUnderlying;

function getStrikeStepForUnderlying(und) {
    if (!und) return 50;
    const u = String(und).toUpperCase();
    return STRADDLE_STRIKE_STEPS[u] || 50;
}
window.getStrikeStepForUnderlying = getStrikeStepForUnderlying;

function toggleOptionChainInsights(forceState = null) {
    const bar = document.getElementById("optChainInsightsBar");
    const arrow = document.getElementById("optChainInsightsArrow");
    const badge = document.getElementById("optChainInsightsBadge") || document.getElementById("optChainInsightsStateBadge");
    if (!bar) return;

    let isCollapsed;
    if (forceState !== null) {
        isCollapsed = !!forceState;
    } else {
        const currentlyVisible = (bar.style.display !== "none" && !bar.classList.contains("hidden") && window.getComputedStyle(bar).display !== "none");
        isCollapsed = currentlyVisible;
    }

    if (isCollapsed) {
        bar.style.setProperty("display", "none", "important");
        bar.classList.add("hidden");
        if (arrow) arrow.textContent = "▶";
        if (badge) {
            badge.textContent = "Show Details ▼";
            badge.style.color = "#38bdf8";
        }
    } else {
        bar.style.setProperty("display", "flex", "important");
        bar.classList.remove("hidden");
        if (arrow) arrow.textContent = "▼";
        if (badge) {
            badge.textContent = "Hide Details ▲";
            badge.style.color = "#94a3b8";
        }
    }

    try {
        localStorage.setItem("straddle_opt_chain_insights_collapsed", isCollapsed ? "true" : "false");
    } catch (e) { }
}
window.toggleOptionChainInsights = toggleOptionChainInsights;

async function openStraddleOptionChainModal(undParam, forceBasketMode = false) {
    const modal = document.getElementById("straddleOptionChainModal");
    if (!modal) return;
    modal.classList.remove("hidden");
    modal.style.setProperty("display", "flex", "important");

    // Force Basket Mode ON or OFF depending on entrypoint
    setOptionChainBasketMode(Boolean(forceBasketMode));

    // Sync View Mode (Standard vs Greeks) button states and headers
    const currentViewMode = window._optChainViewMode || localStorage.getItem("straddle_opt_view_mode") || "standard";
    const btnStd = document.getElementById("btnOptViewStandard");
    const btnGrk = document.getElementById("btnOptViewGreeks");
    if (btnStd) {
        btnStd.classList.toggle("active", currentViewMode === "standard");
        btnStd.style.background = (currentViewMode === "standard") ? "#1e293b" : "transparent";
        btnStd.style.color = (currentViewMode === "standard") ? "#38bdf8" : "#94a3b8";
        btnStd.style.fontWeight = (currentViewMode === "standard") ? "800" : "600";
    }
    if (btnGrk) {
        btnGrk.classList.toggle("active", currentViewMode === "greeks");
        btnGrk.style.background = (currentViewMode === "greeks") ? "#1e293b" : "transparent";
        btnGrk.style.color = (currentViewMode === "greeks") ? "#38bdf8" : "#94a3b8";
        btnGrk.style.fontWeight = (currentViewMode === "greeks") ? "800" : "600";
    }
    const thCeCol1 = document.getElementById("thCeCol1");
    const thCeCol2 = document.getElementById("thCeCol2");
    const thPeCol1 = document.getElementById("thPeCol1");
    const thPeCol2 = document.getElementById("thPeCol2");
    if (currentViewMode === "greeks") {
        if (thCeCol1) { thCeCol1.textContent = "IV %"; thCeCol1.style.color = "#38bdf8"; }
        if (thCeCol2) { thCeCol2.textContent = "DELTA"; thCeCol2.style.color = "#f8fafc"; }
        if (thPeCol2) { thPeCol2.textContent = "DELTA"; thPeCol2.style.color = "#f8fafc"; }
        if (thPeCol1) { thPeCol1.textContent = "IV %"; thPeCol1.style.color = "#38bdf8"; }
    } else {
        if (thCeCol1) { thCeCol1.textContent = "OI (L)"; thCeCol1.style.color = "#c084fc"; }
        if (thCeCol2) { thCeCol2.textContent = "CHG %"; thCeCol2.style.color = "#94a3b8"; }
        if (thPeCol2) { thPeCol2.textContent = "CHG %"; thPeCol2.style.color = "#94a3b8"; }
        if (thPeCol1) { thPeCol1.textContent = "OI (L)"; thPeCol1.style.color = "#c084fc"; }
    }

    // Collapsed by default unless user explicitly expanded
    const savedCollapsed = localStorage.getItem("straddle_opt_chain_insights_collapsed");
    const shouldCollapse = (savedCollapsed === null || savedCollapsed === "true");
    toggleOptionChainInsights(shouldCollapse);

    const undSel = document.getElementById("optChainUnderlyingSelect");
    const targetUnd = undParam || (forceBasketMode ? null : window._activeGlobalUnderlying) || undSel?.value || window._straddleState.activeStrategy?.underlying || "NIFTY";
    if (undSel) undSel.value = targetUnd;

    await onChangeOptChainUnderlying(targetUnd);
    startOptionChainTicker();
}

function closeStraddleOptionChainModal() {
    const modal = document.getElementById("straddleOptionChainModal");
    if (modal) {
        modal.classList.add("hidden");
        modal.style.setProperty("display", "none", "important");
    }
    stopOptionChainTicker();
}

async function onChangeOptChainUnderlying(und, skipGlobalSync = false) {
    if (!und) return;
    const cleanUnd = String(und).toUpperCase().trim();
    window._straddleState.optionChainUnderlying = cleanUnd;
    stopOptionChainTicker();
    _isLoadingOptionChain = false;

    // Sync global active asset across platform without recursive re-triggers
    if (!skipGlobalSync && window._activeGlobalUnderlying !== cleanUnd) {
        window._activeGlobalUnderlying = cleanUnd;
        try { localStorage.setItem("straddle_active_underlying", cleanUnd); } catch (_) { }
        const topName = document.getElementById("topbarAssetName");
        if (topName) {
            topName.textContent = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[cleanUnd]) ? COMMODITY_DISPLAY_NAMES[cleanUnd] : cleanUnd;
            topName.style.color = (typeof COMMODITY_COLOR_MAP !== "undefined" && COMMODITY_COLOR_MAP[cleanUnd]) ? COMMODITY_COLOR_MAP[cleanUnd] : "#38bdf8";
        }
        const topDot = document.getElementById("topbarAssetDot");
        if (topDot) {
            topDot.style.background = (typeof COMMODITY_COLOR_MAP !== "undefined" && COMMODITY_COLOR_MAP[cleanUnd]) ? COMMODITY_COLOR_MAP[cleanUnd] : "#38bdf8";
        }
    }

    const undSel = document.getElementById("optChainUnderlyingSelect");
    if (undSel && undSel.value !== cleanUnd) {
        undSel.value = cleanUnd;
    }

    const tbody = document.getElementById("straddleOptionChainBody");
    if (tbody) {
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center; padding:30px; color:#94a3b8;"><span style="font-size:16px;">⏳</span> Loading ${cleanUnd} Option Depth & Greeks...</td></tr>`;
    }

    try {
        const resp = await fetch(`/api/straddle/expiries?underlying=${encodeURIComponent(cleanUnd)}`);
        const data = await resp.json();
        let targetExpMs = null;
        if (data.success && data.expiries && data.expiries.length > 0) {
            const sel = document.getElementById("optChainExpirySelect");
            if (sel) {
                sel.innerHTML = "";
                data.expiries.forEach(e => {
                    const opt = document.createElement("option");
                    opt.value = e.expiry_ms;
                    opt.textContent = e.formatted_label + " (" + e.tag + ")";
                    sel.appendChild(opt);
                });
                sel.value = data.expiries[0].expiry_ms;
            }
            targetExpMs = data.expiries[0].expiry_ms;
        }
        await loadStraddleOptionChain(targetExpMs, false);
    } catch (e) {
        console.error("Error loading expiries for underlying:", e);
        await loadStraddleOptionChain(null, false);
    } finally {
        startOptionChainTicker();
    }
}

// =========================================================================
// BASKET MODE CONTROLLER FOR OPTION CHAIN (KOTAK NEO SCREENSHOT 5 GRADE)
// =========================================================================

window._optionChainBasketMode = false;
window._chainSelectedLegs = {};
window._basketOpeningForOptionChain = null;

function setOptionChainBasketMode(isMode) {
    window._optionChainBasketMode = Boolean(isMode);
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
        thCe.style.width = isMode ? "136px" : "104px";
        thCe.className = "opt-action-col";
    }
    if (thPe) {
        thPe.textContent = isMode ? "SELECT" : "ACTION";
        thPe.style.display = "";
        thPe.style.width = isMode ? "136px" : "104px";
        thPe.className = "opt-action-col";
    }
    const colgroup = document.getElementById("optChainColgroup");
    if (colgroup && colgroup.children && colgroup.children.length >= 9) {
        colgroup.children[0].style.width = isMode ? "136px" : "104px";
        colgroup.children[8].style.width = isMode ? "136px" : "104px";
    }
    if (thCeSuper) thCeSuper.setAttribute("colspan", "4");
    if (thPeSuper) thPeSuper.setAttribute("colspan", "4");
    if (botBar) botBar.style.display = isMode ? "flex" : "none";

    updateOptionChainBasketSummaryBar();
}
window.setOptionChainBasketMode = setOptionChainBasketMode;

function openOptionChainForActiveBasket() {
    window._basketOpeningForOptionChain = window._activeBasketId || "basket_default";
    const basketModal = document.getElementById("modalBasketOrders");
    if (basketModal) basketModal.style.display = "none";
    stopBasketQuoteTicker();

    // Priority 1: Check what is actively selected in the dropdown
    const undSelectEl = document.getElementById("basketUnderlyingSelect");
    const selectedUnd = undSelectEl ? undSelectEl.value : null;

    // Priority 2: Check current active basket in storage
    const list = getSavedBasketsList();
    const current = list.find(b => b.id === window._activeBasketId);

    // Authoritative underlying for this basket
    const basketUnderlying = selectedUnd || current?.underlying || window._activeGlobalUnderlying || "NIFTY";

    // Synchronize to active basket object and persistent storage
    if (current && current.underlying !== basketUnderlying) {
        current.underlying = basketUnderlying;
        saveBasketsList(list);
    }

    // Open option chain modal with forceBasketMode = true
    openStraddleOptionChainModal(basketUnderlying, true);

    showStraddleToast(`Select option legs for "${current?.name || 'Current Basket'}" (${COMMODITY_DISPLAY_NAMES[basketUnderlying] || basketUnderlying})`, "info", 2500);
}
window.openOptionChainForActiveBasket = openOptionChainForActiveBasket;

function toggleOptionChainBasketMode() {
    const newMode = !window._optionChainBasketMode;
    setOptionChainBasketMode(newMode);
    loadStraddleOptionChain();
}
window.toggleOptionChainBasketMode = toggleOptionChainBasketMode;

function toggleTopbarSettingsDropdown(event) {
    if (event && event.stopPropagation) event.stopPropagation();
    const dd = document.getElementById("topbarSettingsDropdown");
    const btn = document.getElementById("btnTopbarSettingsToggle");
    if (!dd) return;
    const isShown = dd.style.display === "block" || (window.getComputedStyle && window.getComputedStyle(dd).display === "block");
    dd.style.display = isShown ? "none" : "block";
    if (btn) btn.classList.toggle("open", !isShown);
    if (!isShown && typeof closeTopbarAssetMenu === "function") {
        closeTopbarAssetMenu();
    }
}
window.toggleTopbarSettingsDropdown = toggleTopbarSettingsDropdown;

function closeTopbarSettingsDropdown() {
    const dd = document.getElementById("topbarSettingsDropdown");
    const btn = document.getElementById("btnTopbarSettingsToggle");
    if (dd) dd.style.display = "none";
    if (btn) btn.classList.remove("open");
}
window.closeTopbarSettingsDropdown = closeTopbarSettingsDropdown;

document.addEventListener("click", function (e) {
    const wrap = document.getElementById("topbarSettingsMenuWrap");
    const dd = document.getElementById("topbarSettingsDropdown");
    if (dd && wrap && !wrap.contains(e.target)) {
        closeTopbarSettingsDropdown();
    }
});
window.toggleOptionChainBasketMode = toggleOptionChainBasketMode;

function onToggleChainBasketLeg(strike, optType, side, symbol, token, ltp, lotSize, expDate, isChecked) {
    const key = `${strike}_${optType}_${side}`;
    const und = window._straddleState?.optionChainUnderlying || "NIFTY";
    const resolvedLot = Number(lotSize) > 0 ? Number(lotSize) : getLotSizeForUnderlying(und);
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
            } catch (e) { }
        }
    } else {
        delete window._chainSelectedLegs[key];
        // Directly remove from window._basketOrders
        if (Array.isArray(window._basketOrders)) {
            window._basketOrders = window._basketOrders.filter(o => !(Number(o.strike) === resolvedStrike && o.opt_type === optType && o.side === side));
            try {
                localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
            } catch (e) { }
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
    const targetBasketId = window._basketOpeningForOptionChain || window._activeBasketId || "basket_default";

    // Switch to target basket if needed
    const list = getSavedBasketsList();
    let currentBasket = list.find(b => b.id === targetBasketId);
    if (!currentBasket) {
        currentBasket = list[0] || { id: "basket_default", name: "DEFAULT BASKET", orders: [], underlying: und };
    }
    window._activeBasketId = currentBasket.id;
    if (!Array.isArray(window._basketOrders)) window._basketOrders = [];

    let addedCount = 0;
    keys.forEach(k => {
        const leg = window._chainSelectedLegs[k];
        const resolvedLot = Number(leg.lot_size) > 0 ? Number(leg.lot_size) : getLotSizeForUnderlying(und);
        const resolvedStrike = Number(leg.strike) || 0;
        const resolvedPrice = Number(leg.ltp) || 0.05;

        const exists = window._basketOrders.some(o => Number(o.strike) === resolvedStrike && o.opt_type === leg.opt_type && o.side === leg.side);
        if (!exists) {
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
        }
    });

    currentBasket.orders = window._basketOrders;
    currentBasket.underlying = und;
    saveBasketsList(list);

    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify(window._basketOrders));
    } catch (e) { }

    window._chainSelectedLegs = {};
    window._basketOpeningForOptionChain = null;
    updateOptionChainBasketSummaryBar();
    updateBasketBadges();

    // Close option chain and reopen the basket modal
    closeStraddleOptionChainModal();
    openBasketOrderModal();
    showStraddleToast(`🧺 Added ${addedCount} leg(s) into "${currentBasket.name}"!`, "success", 2500);
}
window.addSelectedLegsToBasketModal = addSelectedLegsToBasketModal;

async function loadStraddleOptionChain(expiryMs, isBackgroundUpdate = false) {
    if (isBackgroundUpdate && _isLoadingOptionChain) return;
    _isLoadingOptionChain = true;

    const undSel = document.getElementById("optChainUnderlyingSelect");
    const und = undSel?.value || window._straddleState?.optionChainUnderlying || "NIFTY";

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
        tbody.innerHTML = `<tr><td colspan="9" style="text-align:center; padding:30px; color:#94a3b8;"><span style="font-size:16px;">⏳</span> Loading Real-Time Option Depth & Greeks...</td></tr>`;
    }

    try {
        let url = `/api/straddle/option_chain?underlying=${und}&strike_range=${strikeRange}`;
        if (exp) url += `&expiry_ms=${exp}`;

        // Only abort previous in-flight request if user triggered an explicit new foreground action
        if (!isBackgroundUpdate) {
            if (window._optChainAbortController) {
                try { window._optChainAbortController.abort(); } catch (_) { }
            }
            window._optChainAbortController = new AbortController();
        } else if (!window._optChainAbortController) {
            window._optChainAbortController = new AbortController();
        }

        const resp = await fetch(url, { signal: window._optChainAbortController?.signal });
        const data = await resp.json();
        if (data.success && data.data) {
            const chain = data.data;

            // Sync expiry dropdown if backend resolved a valid expiry for this underlying
            if (expSel && chain.selected_expiry_ms && expSel.value != chain.selected_expiry_ms) {
                expSel.value = chain.selected_expiry_ms;
            }

            if (!chain.spot_price && chain.spot) chain.spot_price = chain.spot;

            // Spot outlier anomaly filter (ignore >40% sub-second bounce)
            const spotKey = `${und}`;
            if (chain.spot_price > 0) {
                if (window._lastOptionChainSpotMap && window._lastOptionChainSpotMap[spotKey] > 0) {
                    const prevSpot = window._lastOptionChainSpotMap[spotKey];
                    if (Math.abs(chain.spot_price - prevSpot) / prevSpot > 0.40) {
                        console.warn(`[OptionChain] Ignoring anomalous spot spike: ${chain.spot_price} vs prev ${prevSpot}`);
                        chain.spot_price = prevSpot;
                    } else {
                        window._lastOptionChainSpotMap[spotKey] = chain.spot_price;
                    }
                } else {
                    if (!window._lastOptionChainSpotMap) window._lastOptionChainSpotMap = {};
                    window._lastOptionChainSpotMap[spotKey] = chain.spot_price;
                }
            }

            const currentExpDateStr = chain.expiry_date || (expSel?.selectedOptions && expSel.selectedOptions[0] ? expSel.selectedOptions[0].text : "Weekly");

            // Update Header Spot Ticker
            if (spotLabel) {
                spotLabel.textContent = `${chain.underlying}: ₹${chain.spot_price.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
            }

            // Sync Tradeable Only Checkbox state
            const chkTradeable = document.getElementById("chkOptTradeableOnly");
            if (chkTradeable) {
                if (typeof window._optChainTradeableOnly === "undefined") {
                    window._optChainTradeableOnly = (localStorage.getItem("straddle_opt_tradeable_only") === "true");
                }
                chkTradeable.checked = !!window._optChainTradeableOnly;
            }

            // Update Market Insights Ribbon
            const spotEl = document.getElementById("optInsightSpot");
            if (spotEl) spotEl.textContent = `₹${chain.spot_price.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })} (ATM: ${chain.atm_strike})`;

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
            const isGreeks = (window._optChainViewMode === "greeks");

            // Sync Header Colspan & Visibility
            const thCe = document.getElementById("thCeActionHeader");
            const thPe = document.getElementById("thPeActionHeader");
            if (thCe) {
                thCe.textContent = isBasketMode ? "SELECT" : "ACTION";
                thCe.style.width = isBasketMode ? "136px" : "104px";
            }
            if (thPe) {
                thPe.textContent = isBasketMode ? "SELECT" : "ACTION";
                thPe.style.width = isBasketMode ? "136px" : "104px";
            }

            // Filter rows based on "Tradeable Only" switch
            let renderRows = chain.rows || [];
            if (window._optChainTradeableOnly) {
                renderRows = renderRows.filter(r => {
                    const hasCe = r.ce && (Number(r.ce.ltp) > 0 || Number(r.ce.oi) > 0 || Number(r.ce.volume) > 0);
                    const hasPe = r.pe && (Number(r.pe.ltp) > 0 || Number(r.pe.oi) > 0 || Number(r.pe.volume) > 0);
                    return hasCe || hasPe;
                });
            }

            // Calculate max single Open Interest across visible chain to scale Angel One purple OI bars
            let maxSingleOi = 1;
            renderRows.forEach(r => {
                const cOi = r.ce ? (Number(r.ce.oi) || 0) : 0;
                const pOi = r.pe ? (Number(r.pe.oi) || 0) : 0;
                if (cOi > maxSingleOi) maxSingleOi = cOi;
                if (pOi > maxSingleOi) maxSingleOi = pOi;
            });

            const currentKey = `${und}_${exp}_${strikeRange}_${isBasketMode}_${!!window._optChainTradeableOnly}_${isGreeks ? 'greeks' : 'standard'}`;
            const spotP = chain.spot_price || 0;

            // IN-PLACE DOM PATCHING: If the structure hasn't changed, update text and flash ticks without destroying DOM
            if (isBackgroundUpdate && window._lastRenderedOptionChainKey === currentKey && tbody.dataset.renderedKey === currentKey) {
                const dividerRow = document.getElementById("optSpotDividerRow");
                const dividerBadge = document.getElementById("optSpotDividerBadge");
                if (dividerBadge && spotP > 0) {
                    dividerBadge.textContent = spotP.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                }

                let firstRowAfterSpot = null;

                renderRows.forEach(r => {
                    const strike = r.strike_price;
                    const rowEl = document.getElementById(`optRow_${strike}`);
                    if (!rowEl) return;

                    if (!firstRowAfterSpot && spotP > 0 && strike >= spotP) {
                        firstRowAfterSpot = rowEl;
                    }

                    // Update CE Col 1 (OI or IV)
                    const ceOiCell = document.getElementById(`ceOi_${strike}`);
                    if (ceOiCell && r.ce) {
                        const ceOiVal = r.ce.oi ? Number(r.ce.oi) : 0;
                        const ceIvVal = (r.ce.iv !== undefined && r.ce.iv !== null) ? Number(r.ce.iv) : 0;
                        const oiSpan = ceOiCell.querySelector(".opt-oi-val");
                        const oiBar = ceOiCell.querySelector(".opt-oi-purple-bar-ce");
                        if (isGreeks) {
                            if (oiSpan) {
                                oiSpan.textContent = ceIvVal > 0 ? (ceIvVal > 1 ? `${ceIvVal.toFixed(1)}%` : `${(ceIvVal * 100).toFixed(1)}%`) : "--";
                                oiSpan.style.color = "#38bdf8";
                                oiSpan.style.fontWeight = "700";
                            }
                            if (oiBar) oiBar.style.width = "0%";
                        } else {
                            if (oiSpan) {
                                oiSpan.textContent = formatOptionOi(ceOiVal);
                                oiSpan.style.color = "#cbd5e1";
                                oiSpan.style.fontWeight = "400";
                            }
                            const ceOiPct = Math.min(100, Math.max(0, (ceOiVal / maxSingleOi) * 100));
                            if (oiBar) oiBar.style.width = `${ceOiPct}%`;
                        }
                    }

                    // Update CE Col 2 (Chg% or Delta)
                    const ceChgEl = document.getElementById(`ceChg_${strike}`);
                    if (ceChgEl && r.ce) {
                        if (isGreeks) {
                            const dVal = (r.ce.delta !== undefined && r.ce.delta !== null) ? Number(r.ce.delta) : 0;
                            ceChgEl.textContent = dVal !== 0 ? dVal.toFixed(2) : "--";
                            ceChgEl.style.color = "#f8fafc";
                        } else {
                            const chgPctVal = Number(r.ce.net_change_pct) || 0;
                            const sign = chgPctVal > 0 ? "+" : "";
                            ceChgEl.textContent = `${sign}${chgPctVal.toFixed(2)}%`;
                            ceChgEl.style.color = chgPctVal > 0 ? "#10b981" : (chgPctVal < 0 ? "#ef4444" : "#94a3b8");
                        }
                    }

                    // Update CE LTP, Tick Flash & ITM styling
                    const ceLtpCell = document.getElementById(`ceLtp_${strike}`);
                    if (ceLtpCell) {
                        const ceHasPrice = r.ce && Number(r.ce.ltp) > 0;
                        const ceLtpNum = ceHasPrice ? Number(r.ce.ltp) : 0;
                        const prevLtp = parseFloat(ceLtpCell.getAttribute("data-ltp") || "0");
                        const ceLtpText = ceHasPrice ? `₹${ceLtpNum.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "--";

                        const ltpSpan = ceLtpCell.querySelector(".opt-ltp-val");
                        if (ltpSpan) ltpSpan.textContent = ceLtpText;

                        const diff = Math.abs(ceLtpNum - prevLtp);
                        if (diff > 0.001) {
                            ceLtpCell.setAttribute("data-ltp", ceLtpNum);
                            if (prevLtp > 0 && ceLtpNum > 0 && diff >= 0.05) {
                                const isUp = ceLtpNum > prevLtp;
                                ceLtpCell.classList.remove("opt-tick-flash-up", "opt-tick-flash-down");
                                ceLtpCell.classList.add(isUp ? "opt-tick-flash-up" : "opt-tick-flash-down");
                                setTimeout(() => {
                                    if (ceLtpCell) ceLtpCell.classList.remove("opt-tick-flash-up", "opt-tick-flash-down");
                                }, 250);
                            }
                        }

                        const isCeItm = strike < spotP;
                        ceLtpCell.classList.toggle("opt-chain-itm-ce", isCeItm);
                    }

                    // Update PE LTP, Tick Flash & ITM styling
                    const peLtpCell = document.getElementById(`peLtp_${strike}`);
                    if (peLtpCell) {
                        const peHasPrice = r.pe && Number(r.pe.ltp) > 0;
                        const peLtpNum = peHasPrice ? Number(r.pe.ltp) : 0;
                        const prevLtp = parseFloat(peLtpCell.getAttribute("data-ltp") || "0");
                        const peLtpText = peHasPrice ? `₹${peLtpNum.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "--";

                        const ltpSpan = peLtpCell.querySelector(".opt-ltp-val");
                        if (ltpSpan) ltpSpan.textContent = peLtpText;

                        const diff = Math.abs(peLtpNum - prevLtp);
                        if (diff > 0.001) {
                            peLtpCell.setAttribute("data-ltp", peLtpNum);
                            if (prevLtp > 0 && peLtpNum > 0 && diff >= 0.05) {
                                const isUp = peLtpNum > prevLtp;
                                peLtpCell.classList.remove("opt-tick-flash-up", "opt-tick-flash-down");
                                peLtpCell.classList.add(isUp ? "opt-tick-flash-up" : "opt-tick-flash-down");
                                setTimeout(() => {
                                    if (peLtpCell) peLtpCell.classList.remove("opt-tick-flash-up", "opt-tick-flash-down");
                                }, 250);
                            }
                        }

                        const isPeItm = strike > spotP;
                        peLtpCell.classList.toggle("opt-chain-itm-pe", isPeItm);
                    }

                    // Update PE Col 2 (Chg% or Delta)
                    const peChgEl = document.getElementById(`peChg_${strike}`);
                    if (peChgEl && r.pe) {
                        if (isGreeks) {
                            const dVal = (r.pe.delta !== undefined && r.pe.delta !== null) ? Number(r.pe.delta) : 0;
                            peChgEl.textContent = dVal !== 0 ? dVal.toFixed(2) : "--";
                            peChgEl.style.color = "#f8fafc";
                        } else {
                            const chgPctVal = Number(r.pe.net_change_pct) || 0;
                            const sign = chgPctVal > 0 ? "+" : "";
                            peChgEl.textContent = `${sign}${chgPctVal.toFixed(2)}%`;
                            peChgEl.style.color = chgPctVal > 0 ? "#10b981" : (chgPctVal < 0 ? "#ef4444" : "#94a3b8");
                        }
                    }

                    // Update PE Col 1 (OI or IV)
                    const peOiCell = document.getElementById(`peOi_${strike}`);
                    if (peOiCell && r.pe) {
                        const peOiVal = r.pe.oi ? Number(r.pe.oi) : 0;
                        const peIvVal = (r.pe.iv !== undefined && r.pe.iv !== null) ? Number(r.pe.iv) : 0;
                        const oiSpan = peOiCell.querySelector(".opt-oi-val");
                        const oiBar = peOiCell.querySelector(".opt-oi-purple-bar-pe");
                        if (isGreeks) {
                            if (oiSpan) {
                                oiSpan.textContent = peIvVal > 0 ? (peIvVal > 1 ? `${peIvVal.toFixed(1)}%` : `${(peIvVal * 100).toFixed(1)}%`) : "--";
                                oiSpan.style.color = "#38bdf8";
                                oiSpan.style.fontWeight = "700";
                            }
                            if (oiBar) oiBar.style.width = "0%";
                        } else {
                            if (oiSpan) {
                                oiSpan.textContent = formatOptionOi(peOiVal);
                                oiSpan.style.color = "#cbd5e1";
                                oiSpan.style.fontWeight = "400";
                            }
                            const peOiPct = Math.min(100, Math.max(0, (peOiVal / maxSingleOi) * 100));
                            if (oiBar) oiBar.style.width = `${peOiPct}%`;
                        }
                    }
                });

                // Move divider row smoothly if spot crossed strike boundary
                if (dividerRow && firstRowAfterSpot && dividerRow.nextElementSibling !== firstRowAfterSpot) {
                    tbody.insertBefore(dividerRow, firstRowAfterSpot);
                } else if (dividerRow && !firstRowAfterSpot && tbody.lastElementChild !== dividerRow) {
                    tbody.appendChild(dividerRow);
                }

                updateOptionChainBasketSummaryBar();
                return;
            }

            // FULL RENDER (First load or when parameters/mode change)
            let rowsHtml = "";
            let spotDividerInserted = false;

            renderRows.forEach(r => {
                const isAtm = r.is_atm;
                const strike = r.strike_price;
                const isCeItm = strike < spotP;
                const isPeItm = strike > spotP;

                // Center Spot Price Divider Line (Spans all 9 columns)
                if (!spotDividerInserted && spotP > 0 && strike >= spotP) {
                    rowsHtml += `
                        <tr class="opt-spot-divider-row" id="optSpotDividerRow">
                            <td colspan="9">
                                <div class="opt-spot-divider-line">
                                    <span class="opt-spot-divider-badge" id="optSpotDividerBadge">${spotP.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                                </div>
                            </td>
                        </tr>
                    `;
                    spotDividerInserted = true;
                }

                const rowClass = isAtm ? "opt-chain-atm-row" : "";
                const ceCellClass = isCeItm ? "opt-chain-itm-ce" : "";
                const peCellClass = isPeItm ? "opt-chain-itm-pe" : "";

                // CE Data & Formatting
                const ceHasPrice = r.ce && Number(r.ce.ltp) > 0;
                const ceLtpNum = ceHasPrice ? Number(r.ce.ltp) : 0;
                const ceLtpText = ceHasPrice ? `₹${ceLtpNum.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "--";
                const ceChgPct = r.ce ? (Number(r.ce.net_change_pct) || 0) : 0;
                const ceChgSign = ceChgPct > 0 ? "+" : "";
                const ceChgColor = ceChgPct > 0 ? "#10b981" : (ceChgPct < 0 ? "#ef4444" : "#94a3b8");
                const ceChgText = ceChgPct !== 0 ? `${ceChgSign}${ceChgPct.toFixed(2)}%` : "0.00%";
                const ceSym = r.ce ? (r.ce.trading_symbol || `${und} ${strike} CE`) : `${und} ${strike} CE`;
                const ceKey = r.ce ? (r.ce.instrument_key || "") : "";
                const ceLot = r.ce ? (r.ce.lot_size || getLotSizeForUnderlying(und)) : getLotSizeForUnderlying(und);
                const ceChg = r.ce ? (r.ce.net_change || 0) : 0;
                const ceOi = r.ce ? (Number(r.ce.oi) || 0) : 0;
                const ceOiPct = Math.min(100, Math.max(0, (ceOi / maxSingleOi) * 100));
                const ceOiFormatted = formatOptionOi(ceOi);
                const ceIvVal = (r.ce && r.ce.iv !== undefined && r.ce.iv !== null) ? Number(r.ce.iv) : 0;
                const ceIvText = ceIvVal > 0 ? (ceIvVal > 1 ? `${ceIvVal.toFixed(1)}%` : `${(ceIvVal * 100).toFixed(1)}%`) : "--";
                const ceDeltaVal = (r.ce && r.ce.delta !== undefined && r.ce.delta !== null) ? Number(r.ce.delta) : 0;
                const ceDeltaText = ceDeltaVal !== 0 ? ceDeltaVal.toFixed(2) : "--";

                // PE Data & Formatting
                const peHasPrice = r.pe && Number(r.pe.ltp) > 0;
                const peLtpNum = peHasPrice ? Number(r.pe.ltp) : 0;
                const peLtpText = peHasPrice ? `₹${peLtpNum.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : "--";
                const peChgPct = r.pe ? (Number(r.pe.net_change_pct) || 0) : 0;
                const peChgSign = peChgPct > 0 ? "+" : "";
                const peChgColor = peChgPct > 0 ? "#10b981" : (peChgPct < 0 ? "#ef4444" : "#94a3b8");
                const peChgText = peChgPct !== 0 ? `${peChgSign}${peChgPct.toFixed(2)}%` : "0.00%";
                const peSym = r.pe ? (r.pe.trading_symbol || `${und} ${strike} PE`) : `${und} ${strike} PE`;
                const peKey = r.pe ? (r.pe.instrument_key || "") : "";
                const peLot = r.pe ? (r.pe.lot_size || getLotSizeForUnderlying(und)) : getLotSizeForUnderlying(und);
                const peChg = r.pe ? (r.pe.net_change || 0) : 0;
                const peOi = r.pe ? (Number(r.pe.oi) || 0) : 0;
                const peOiPct = Math.min(100, Math.max(0, (peOi / maxSingleOi) * 100));
                const peOiFormatted = formatOptionOi(peOi);
                const peIvVal = (r.pe && r.pe.iv !== undefined && r.pe.iv !== null) ? Number(r.pe.iv) : 0;
                const peIvText = peIvVal > 0 ? (peIvVal > 1 ? `${peIvVal.toFixed(1)}%` : `${(peIvVal * 100).toFixed(1)}%`) : "--";
                const peDeltaVal = (r.pe && r.pe.delta !== undefined && r.pe.delta !== null) ? Number(r.pe.delta) : 0;
                const peDeltaText = peDeltaVal !== 0 ? peDeltaVal.toFixed(2) : "--";

                // Checkbox state in Basket Mode
                const ceBuyKey = `${strike}_CE_BUY`;
                const ceSellKey = `${strike}_CE_SELL`;
                const peBuyKey = `${strike}_PE_BUY`;
                const peSellKey = `${strike}_PE_SELL`;

                const isCeBuyChecked = !!(window._chainSelectedLegs[ceBuyKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "CE" && o.side === "BUY"));
                const isCeSellChecked = !!(window._chainSelectedLegs[ceSellKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "CE" && o.side === "SELL"));
                const isPeBuyChecked = !!(window._chainSelectedLegs[peBuyKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "PE" && o.side === "BUY"));
                const isPeSellChecked = !!(window._chainSelectedLegs[peSellKey] || (window._basketOrders || []).some(o => Number(o.strike) === strike && o.opt_type === "PE" && o.side === "SELL"));

                const ceActionCell = isBasketMode ? `
                    <td class="opt-action-col" style="text-align:center; padding:2px 2px; white-space:nowrap; width:136px;">
                        <div class="chain-tick-wrap" style="display:inline-flex; align-items:center; gap:2px;">
                            <label class="chain-tick-btn tick-buy ${isCeBuyChecked ? 'checked' : ''}" style="padding:1px 2px; font-size:9.5px;" title="Tick to BUY CE in basket">
                                <input type="checkbox" ${isCeBuyChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'CE', 'BUY', '${ceSym}', '${ceKey}', ${ceLtpNum}, ${ceLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isCeBuyChecked ? '☑' : '☐'}</span><span class="tick-lbl">B</span>
                            </label>
                            <label class="chain-tick-btn tick-sell ${isCeSellChecked ? 'checked' : ''}" style="padding:1px 2px; font-size:9.5px;" title="Tick to SELL CE in basket">
                                <input type="checkbox" ${isCeSellChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'CE', 'SELL', '${ceSym}', '${ceKey}', ${ceLtpNum}, ${ceLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isCeSellChecked ? '☑' : '☐'}</span><span class="tick-lbl">S</span>
                            </label>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#38bdf8 !important; border:1px solid rgba(56,189,248,0.4) !important;" title="Candlestick Chart: ${ceSym}" onclick="event.stopPropagation(); openActiveAssetChartModal('${ceSym}')">📈</button>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#fbbf24 !important; border:1px solid rgba(251,191,36,0.4) !important;" title="Market Depth: ${ceSym}" onclick="event.stopPropagation(); window.openMarketDepthModal('${ceSym}', '${ceKey}', ${ceLtpNum}, '${und}', ${strike}, 'CE')">📑</button>
                        </div>
                    </td>
                ` : `
                    <td class="opt-action-col" style="text-align:center; padding:2px 2px; white-space:nowrap; width:104px;">
                        <div class="chain-quick-trade-wrap" style="display:inline-flex; align-items:center; gap:2px;">
                            <button type="button" class="chain-quick-trade-btn chain-btn-buy" title="Direct Trade: BUY ${ceSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${ceSym}', '${ceKey}', ${strike}, 'CE', 'BUY', ${ceLtpNum}, ${ceLot}, ${ceChg}, ${ceChgPct})">B</button>
                            <button type="button" class="chain-quick-trade-btn chain-btn-sell" title="Direct Trade: SELL ${ceSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${ceSym}', '${ceKey}', ${strike}, 'CE', 'SELL', ${ceLtpNum}, ${ceLot}, ${ceChg}, ${ceChgPct})">S</button>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#38bdf8 !important; border:1px solid rgba(56,189,248,0.4) !important;" title="Candlestick Chart: ${ceSym}" onclick="event.stopPropagation(); openActiveAssetChartModal('${ceSym}')">📈</button>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#fbbf24 !important; border:1px solid rgba(251,191,36,0.4) !important;" title="Market Depth: ${ceSym}" onclick="event.stopPropagation(); window.openMarketDepthModal('${ceSym}', '${ceKey}', ${ceLtpNum}, '${und}', ${strike}, 'CE')">📑</button>
                        </div>
                    </td>
                `;

                const peActionCell = isBasketMode ? `
                    <td class="opt-action-col" style="text-align:center; padding:2px 2px; white-space:nowrap; width:136px;">
                        <div class="chain-tick-wrap" style="display:inline-flex; align-items:center; gap:2px;">
                            <label class="chain-tick-btn tick-buy ${isPeBuyChecked ? 'checked' : ''}" style="padding:1px 2px; font-size:9.5px;" title="Tick to BUY PE in basket">
                                <input type="checkbox" ${isPeBuyChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'PE', 'BUY', '${peSym}', '${peKey}', ${peLtpNum}, ${peLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isPeBuyChecked ? '☑' : '☐'}</span><span class="tick-lbl">B</span>
                            </label>
                            <label class="chain-tick-btn tick-sell ${isPeSellChecked ? 'checked' : ''}" style="padding:1px 2px; font-size:9.5px;" title="Tick to SELL PE in basket">
                                <input type="checkbox" ${isPeSellChecked ? 'checked' : ''} onchange="onToggleChainBasketLeg(${strike}, 'PE', 'SELL', '${peSym}', '${peKey}', ${peLtpNum}, ${peLot}, '${currentExpDateStr}', this.checked); this.parentElement.classList.toggle('checked', this.checked); this.parentElement.querySelector('.tick-box').textContent = this.checked ? '☑' : '☐'">
                                <span class="tick-box">${isPeSellChecked ? '☑' : '☐'}</span><span class="tick-lbl">S</span>
                            </label>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#38bdf8 !important; border:1px solid rgba(56,189,248,0.4) !important;" title="Candlestick Chart: ${peSym}" onclick="event.stopPropagation(); openActiveAssetChartModal('${peSym}')">📈</button>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#fbbf24 !important; border:1px solid rgba(251,191,36,0.4) !important;" title="Market Depth: ${peSym}" onclick="event.stopPropagation(); window.openMarketDepthModal('${peSym}', '${peKey}', ${peLtpNum}, '${und}', ${strike}, 'PE')">📑</button>
                        </div>
                    </td>
                ` : `
                    <td class="opt-action-col" style="text-align:center; padding:2px 2px; white-space:nowrap; width:104px;">
                        <div class="chain-quick-trade-wrap" style="display:inline-flex; align-items:center; gap:2px;">
                            <button type="button" class="chain-quick-trade-btn chain-btn-buy" title="Direct Trade: BUY ${peSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${peSym}', '${peKey}', ${strike}, 'PE', 'BUY', ${peLtpNum}, ${peLot}, ${peChg}, ${peChgPct})">B</button>
                            <button type="button" class="chain-quick-trade-btn chain-btn-sell" title="Direct Trade: SELL ${peSym}" onclick="event.stopPropagation(); openStraddleDirectOrderModal('${peSym}', '${peKey}', ${strike}, 'PE', 'SELL', ${peLtpNum}, ${peLot}, ${peChg}, ${peChgPct})">S</button>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#38bdf8 !important; border:1px solid rgba(56,189,248,0.4) !important;" title="Candlestick Chart: ${peSym}" onclick="event.stopPropagation(); openActiveAssetChartModal('${peSym}')">📈</button>
                            <button type="button" class="chain-quick-trade-btn" style="background:#0f172a !important; color:#fbbf24 !important; border:1px solid rgba(251,191,36,0.4) !important;" title="Market Depth: ${peSym}" onclick="event.stopPropagation(); window.openMarketDepthModal('${peSym}', '${peKey}', ${peLtpNum}, '${und}', ${strike}, 'PE')">📑</button>
                        </div>
                    </td>
                `;

                rowsHtml += `
                    <tr id="optRow_${strike}" class="${rowClass}">
                        ${ceActionCell}

                        <!-- CE COL 1: OI (L) with Purple Depth Bar OR IV % in Greeks Mode -->
                        <td id="ceOi_${strike}" class="ce-oi-col" style="position:relative; text-align:right; font-family:var(--font-mono); font-size:11px; padding:2px 6px; color:#cbd5e1;" title="${isGreeks ? 'CE Implied Volatility: ' + ceIvText : 'CE Open Interest: ' + ceOiFormatted}">
                            <span class="opt-oi-val" style="${isGreeks ? 'color:#38bdf8; font-weight:700;' : ''}">${isGreeks ? ceIvText : ceOiFormatted}</span>
                            <div class="opt-oi-purple-bar-ce" style="width:${isGreeks ? 0 : ceOiPct}%;"></div>
                        </td>

                        <!-- CE COL 2: CHG % OR DELTA in Greeks Mode -->
                        <td id="ceChg_${strike}" class="ce-chg-col" style="text-align:right; font-family:var(--font-mono); font-weight:700; font-size:11px; padding:2px 5px; color:${isGreeks ? '#f8fafc' : ceChgColor};">
                            <span class="opt-chg-val">${isGreeks ? ceDeltaText : ceChgText}</span>
                        </td>

                        <!-- CE LTP with ITM shading & tick flash -->
                        <td id="ceLtp_${strike}" data-ltp="${ceLtpNum}" class="${ceCellClass}" style="position:relative; text-align:right; font-family:var(--font-mono); color:#2dd4bf; font-weight:800; font-size:11.5px; padding:2px 6px; cursor:pointer;" onclick="openStraddleDirectOrderModal('${ceSym}', '${ceKey}', ${strike}, 'CE', 'BUY', ${ceLtpNum}, ${ceLot}, ${ceChg}, ${ceChgPct})" title="Click to trade ${ceSym}">
                            <span class="opt-ltp-val">${ceLtpText}</span>
                        </td>

                        <!-- STRIKE PRICE (Center) -->
                        <td style="text-align:center; padding:2px 2px; width:72px; background:#0b1120; border-left:1px solid rgba(255,255,255,0.08); border-right:1px solid rgba(255,255,255,0.08); font-family:var(--font-mono); font-weight:800; color:#f8fafc; font-size:12px;">
                            ${strike}
                        </td>

                        <!-- PE LTP with ITM shading & tick flash -->
                        <td id="peLtp_${strike}" data-ltp="${peLtpNum}" class="${peCellClass}" style="position:relative; text-align:left; font-family:var(--font-mono); color:#fb7185; font-weight:800; font-size:11.5px; padding:2px 6px; cursor:pointer;" onclick="openStraddleDirectOrderModal('${peSym}', '${peKey}', ${strike}, 'PE', 'BUY', ${peLtpNum}, ${peLot}, ${peChg}, ${peChgPct})" title="Click to trade ${peSym}">
                            <span class="opt-ltp-val">${peLtpText}</span>
                        </td>

                        <!-- PE COL 2: CHG % OR DELTA in Greeks Mode -->
                        <td id="peChg_${strike}" class="pe-chg-col" style="text-align:left; font-family:var(--font-mono); font-weight:700; font-size:11px; padding:2px 5px; color:${isGreeks ? '#f8fafc' : peChgColor};">
                            <span class="opt-chg-val">${isGreeks ? peDeltaText : peChgText}</span>
                        </td>

                        <!-- PE COL 1: OI (L) with Purple Depth Bar OR IV % in Greeks Mode -->
                        <td id="peOi_${strike}" class="pe-oi-col" style="position:relative; text-align:left; font-family:var(--font-mono); font-size:11px; padding:2px 6px; color:#cbd5e1;" title="${isGreeks ? 'PE Implied Volatility: ' + peIvText : 'PE Open Interest: ' + peOiFormatted}">
                            <span class="opt-oi-val" style="${isGreeks ? 'color:#38bdf8; font-weight:700;' : ''}">${isGreeks ? peIvText : peOiFormatted}</span>
                            <div class="opt-oi-purple-bar-pe" style="width:${isGreeks ? 0 : peOiPct}%;"></div>
                        </td>

                        ${peActionCell}
                    </tr>
                `;
            });

            // If divider hasn't been drawn (spot price higher than all strikes), append at bottom
            if (!spotDividerInserted && spotP > 0) {
                rowsHtml += `
                    <tr class="opt-spot-divider-row" id="optSpotDividerRow">
                        <td colspan="9">
                            <div class="opt-spot-divider-line">
                                <span class="opt-spot-divider-badge" id="optSpotDividerBadge">${spotP.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</span>
                            </div>
                        </td>
                    </tr>
                `;
            }

            if (!renderRows || renderRows.length === 0) {
                rowsHtml = `<tr><td colspan="9" style="text-align:center; padding:30px; color:#94a3b8;"><span style="font-size:16px;">⚠️</span> No active option contracts found for ${und} (${currentExpDateStr}). Try selecting a different expiry.</td></tr>`;
            }

            tbody.innerHTML = rowsHtml;
            tbody.dataset.renderedKey = currentKey;
            window._lastRenderedOptionChainKey = currentKey;
            updateOptionChainBasketSummaryBar();

            // Smoothly auto-scroll to ATM divider line on initial or user-triggered renders
            if (!isBackgroundUpdate) {
                setTimeout(() => {
                    const divider = document.getElementById("optSpotDividerRow") || document.querySelector(".opt-chain-atm-row");
                    const container = document.querySelector(".opt-chain-scroll-body");
                    if (divider && container) {
                        const containerRect = container.getBoundingClientRect();
                        const dividerRect = divider.getBoundingClientRect();
                        const offset = (dividerRect.top - containerRect.top) - (container.clientHeight / 2) + (dividerRect.height / 2);
                        container.scrollTo({ top: container.scrollTop + offset, behavior: "smooth" });
                    }
                }, 100);
            }
        }
    } catch (e) {
        if (e.name === 'AbortError') return;
        if (!isBackgroundUpdate) {
            tbody.innerHTML = `<tr><td colspan="9" style="text-align:center; color:#f43f5e; padding:30px;">Error loading Option Matrix: ${e.message}</td></tr>`;
        }
    } finally {
        _isLoadingOptionChain = false;
    }
}

function toggleOptTradeableOnly(isChecked) {
    window._optChainTradeableOnly = !!isChecked;
    try {
        localStorage.setItem("straddle_opt_tradeable_only", isChecked ? "true" : "false");
    } catch (e) { }
    loadStraddleOptionChain(null, true);
}
window.toggleOptTradeableOnly = toggleOptTradeableOnly;

// =========================================================================
// REAL-TIME CONTINUOUS MARKET DEPTH (Level 2 Quotes - 5 Bids & 5 Asks)
// =========================================================================
window._activeDepthParams = null;
window._marketDepthTimer = null;
window._lastDepthLtp = 0;
let _isPollingDepth = false;

function openMarketDepthModal(sym, key, ltp, underlying, strike, optType) {
    const modal = document.getElementById("modalMarketDepth");
    if (modal) {
        modal.classList.remove("hidden");
        modal.style.display = "flex";
        modal.style.zIndex = "200000";
        modal.style.setProperty("display", "flex", "important");
        modal.style.setProperty("z-index", "200000", "important");
    }

    const und = underlying || (window._straddleState && window._straddleState.optionChainUnderlying) || "NIFTY";
    const resolvedSym = sym && sym.length > 3 ? sym : `${und} ${strike} ${optType}`;
    const resolvedLtp = Number(ltp) || 0;
    const resolvedOptType = (optType || (resolvedSym.includes("PE") ? "PE" : "CE")).toUpperCase();

    window._activeDepthParams = {
        symbol: resolvedSym,
        instrument_key: key || "",
        ltp: resolvedLtp,
        underlying: und,
        strike: Number(strike) || 0,
        opt_type: resolvedOptType
    };
    window._marketDepthActiveSymbol = resolvedSym;
    window._lastDepthLtp = resolvedLtp;

    if (!modal) return;

    // Set initial display headers
    const symEl = document.getElementById("depthContractSymbol");
    if (symEl) symEl.textContent = resolvedSym;

    const badgeEl = document.getElementById("depthOptionBadge");
    if (badgeEl) {
        if (resolvedOptType === "PE") {
            badgeEl.textContent = "PUT";
            badgeEl.style.background = "#881337";
            badgeEl.style.color = "#fb7185";
        } else {
            badgeEl.textContent = "CALL";
            badgeEl.style.background = "#065f46";
            badgeEl.style.color = "#34d399";
        }
    }

    const ltpEl = document.getElementById("depthLtpDisplay");
    if (ltpEl) {
        ltpEl.textContent = resolvedLtp > 0 ? `₹${resolvedLtp.toFixed(2)}` : "₹--";
    }

    // Show modal cleanly
    modal.classList.remove("hidden");
    modal.style.setProperty("display", "flex", "important");
    modal.style.setProperty("z-index", "200000", "important");

    // Clear existing timer if any
    if (window._marketDepthTimer) {
        clearInterval(window._marketDepthTimer);
        window._marketDepthTimer = null;
    }

    // Fetch depth immediately
    pollMarketDepthContinuous();

    // Start continuous real-time depth polling interval (every 600ms)
    window._marketDepthTimer = setInterval(pollMarketDepthContinuous, 600);
}
window.openMarketDepthModal = openMarketDepthModal;

function closeMarketDepthModal() {
    if (window._marketDepthTimer) {
        clearInterval(window._marketDepthTimer);
        window._marketDepthTimer = null;
    }
    const modal = document.getElementById("modalMarketDepth");
    if (modal) {
        modal.classList.add("hidden");
        modal.style.setProperty("display", "none", "important");
    }
    window._activeDepthParams = null;
}
window.closeMarketDepthModal = closeMarketDepthModal;

async function pollMarketDepthContinuous() {
    if (!window._activeDepthParams || _isPollingDepth) return;
    const p = window._activeDepthParams;
    _isPollingDepth = true;

    try {
        const url = `/api/straddle/market_depth?symbol=${encodeURIComponent(p.symbol)}&instrument_key=${encodeURIComponent(p.instrument_key || '')}&underlying=${encodeURIComponent(p.underlying || '')}&strike=${p.strike || 0}&opt_type=${encodeURIComponent(p.opt_type || '')}&ltp=${p.ltp || 0}`;
        const resp = await fetch(url);
        if (!resp.ok) return;
        const data = await resp.json();
        if (!data || data.status !== "success") return;

        // If user switched or closed modal while request was inflight, discard
        if (!window._activeDepthParams || window._activeDepthParams.symbol !== p.symbol) return;

        const liveLtp = Number(data.ltp) || p.ltp || 0;
        window._lastDepthLtp = liveLtp;

        // Reflect authentic broker market status
        const modalEl = document.getElementById("modalMarketDepth");
        if (modalEl) {
            const statusBadge = modalEl.querySelector(".live-tick-pulse")?.parentElement;
            if (statusBadge) {
                if (data.market_status === "CLOSED") {
                    statusBadge.innerHTML = `<span style="width:6px; height:6px; background:#f59e0b; border-radius:50%; display:inline-block;"></span><span style="color:#fbbf24; font-size:10px; font-weight:800; letter-spacing:0.4px;">MARKET CLOSED (PREV CLOSE)</span>`;
                    statusBadge.style.background = "rgba(245,158,11,0.12)";
                    statusBadge.style.borderColor = "rgba(245,158,11,0.35)";
                } else {
                    statusBadge.innerHTML = `<span class="live-tick-pulse" style="width:6px; height:6px; background:#10b981; border-radius:50%; display:inline-block;"></span><span style="color:#10b981; font-size:10px; font-weight:800; letter-spacing:0.4px;">LIVE DEPTH</span>`;
                    statusBadge.style.background = "rgba(16,185,129,0.1)";
                    statusBadge.style.borderColor = "rgba(16,185,129,0.3)";
                }
            }
        }

        // Update Header LTP & Change
        const ltpEl = document.getElementById("depthLtpDisplay");
        if (ltpEl && liveLtp > 0) {
            ltpEl.textContent = `₹${liveLtp.toFixed(2)}`;
        }

        const chgEl = document.getElementById("depthChgDisplay");
        if (chgEl) {
            const netChg = Number(data.net_change) || 0;
            const netPct = Number(data.net_change_pct) || 0;
            const sign = netChg >= 0 ? "+" : "";
            chgEl.textContent = `${sign}${netChg.toFixed(2)} (${sign}${netPct.toFixed(2)}%)`;
            chgEl.style.color = netChg > 0 ? "#34d399" : (netChg < 0 ? "#fb7185" : "#94a3b8");
        }

        // Update OHLC & Volume Stats
        const setStat = (id, val, prefix = "") => {
            const el = document.getElementById(id);
            if (el) el.textContent = val > 0 ? `${prefix}${val.toLocaleString("en-IN")}` : "--";
        };
        setStat("depthStatOpen", data.open, "₹");
        setStat("depthStatHigh", data.high, "₹");
        setStat("depthStatLow", data.low, "₹");
        setStat("depthStatClose", data.close, "₹");
        setStat("depthStatVolume", data.volume, "");

        // Render 5 Bids & 5 Asks rows
        const tbody = document.getElementById("depthTableBody");
        if (tbody) {
            let html = "";
            for (let i = 0; i < 5; i++) {
                const b = (data.bids && data.bids[i]) || { orders: 0, quantity: 0, price: 0 };
                const a = (data.asks && data.asks[i]) || { orders: 0, quantity: 0, price: 0 };
                html += `
                    <tr style="border-bottom:1px solid rgba(255,255,255,0.03); height:22px;">
                        <td style="text-align:left; color:#94a3b8; padding:3px 2px;">${b.orders > 0 ? b.orders : '-'}</td>
                        <td style="text-align:right; color:#cbd5e1; padding:3px 2px;">${b.quantity > 0 ? b.quantity.toLocaleString('en-IN') : '-'}</td>
                        <td style="text-align:right; color:#34d399; font-weight:700; padding:3px 4px;">${b.price > 0 ? b.price.toFixed(2) : '-'}</td>
                        <td style="text-align:left; color:#fb7185; font-weight:700; padding:3px 4px; border-left:1px solid rgba(255,255,255,0.08);">${a.price > 0 ? a.price.toFixed(2) : '-'}</td>
                        <td style="text-align:right; color:#cbd5e1; padding:3px 2px;">${a.quantity > 0 ? a.quantity.toLocaleString('en-IN') : '-'}</td>
                        <td style="text-align:right; color:#94a3b8; padding:3px 2px;">${a.orders > 0 ? a.orders : '-'}</td>
                    </tr>
                `;
            }
            tbody.innerHTML = html;
        }

        // Update Totals & Buy/Sell Ratio Bar
        const buyTotEl = document.getElementById("depthTotalBuyQty");
        if (buyTotEl) buyTotEl.textContent = (data.total_buy_qty || 0).toLocaleString("en-IN");

        const sellTotEl = document.getElementById("depthTotalSellQty");
        if (sellTotEl) sellTotEl.textContent = (data.total_sell_qty || 0).toLocaleString("en-IN");

        const buyPct = Math.max(0, Math.min(100, Number(data.buy_pct) || 50));
        const sellPct = Math.max(0, Math.min(100, Number(data.sell_pct) || 50));

        const buyLbl = document.getElementById("depthBuyPctLabel");
        if (buyLbl) buyLbl.textContent = `Buyers: ${buyPct.toFixed(1)}%`;

        const sellLbl = document.getElementById("depthSellPctLabel");
        if (sellLbl) sellLbl.textContent = `Sellers: ${sellPct.toFixed(1)}%`;

        const buyBar = document.getElementById("depthBuyProgressBar");
        if (buyBar) buyBar.style.width = `${buyPct}%`;

        const sellBar = document.getElementById("depthSellProgressBar");
        if (sellBar) sellBar.style.width = `${sellPct}%`;

    } catch (e) {
        console.warn("Market depth continuous update note:", e);
    } finally {
        _isPollingDepth = false;
    }
}
window.pollMarketDepthContinuous = pollMarketDepthContinuous;

function executeDepthQuickTrade(side) {
    if (!window._activeDepthParams) return;
    const p = window._activeDepthParams;
    const ltp = window._lastDepthLtp || p.ltp || 0;
    closeMarketDepthModal();
    if (typeof openStraddleDirectOrderModal === "function") {
        openStraddleDirectOrderModal(p.symbol, p.instrument_key || "", p.strike || 0, p.opt_type || "CE", side, ltp, 65, 0, 0);
    }
}
window.executeDepthQuickTrade = executeDepthQuickTrade;

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

    const undFromState = window._straddleState.optionChainUnderlying || "NIFTY";
    const allCommodityCandidates = [
        "NATGASMINI", "NATURALGAS", "CRUDEOILM", "CRUDEOIL",
        "SILVERM", "SILVER", "GOLDM", "GOLD", "COPPER",
        "MIDCPNIFTY", "BANKNIFTY", "FINNIFTY", "BANKEX", "SENSEX", "NIFTY"
    ];
    let und = undFromState;
    const symUpper = (symbol || "").toUpperCase();
    for (const c of allCommodityCandidates) {
        if (symUpper.includes(c)) {
            und = c;
            break;
        }
    }

    const COMMODITY_LOT_DEFAULTS = {
        "GOLD": 1, "GOLDM": 100, "SILVER": 30, "SILVERM": 5,
        "CRUDEOIL": 100, "CRUDEOILM": 10, "NATURALGAS": 1250, "NATGASMINI": 250,
        "COPPER": 2500, "NIFTY": 65, "BANKNIFTY": 15, "FINNIFTY": 65,
        "MIDCPNIFTY": 120, "SENSEX": 10, "BANKEX": 15
    };

    const resolvedLot = lotSize > 0 ? lotSize : (COMMODITY_LOT_DEFAULTS[und] || 65);
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
        lots: 1,
        quantity: resolvedLot,
        ltp: resolvedLtp,
        net_change: Number(netChange) || 0,
        net_change_pct: Number(netChangePct) || 0,
        underlying: und
    };

    // Update Symbol & Subtitle Header
    const titleEl = document.getElementById("directOrderSymbolTitle");
    if (titleEl) titleEl.textContent = resolvedSymbol;

    const subEl = document.getElementById("directOrderSubtitle");
    const seg = ["SENSEX", "BANKEX"].includes(und) ? "BFO" : 
                (["CRUDEOIL", "CRUDEOILM", "NATURALGAS", "NATGASMINI", "GOLD", "GOLDM", "SILVER", "SILVERM", "COPPER"].includes(und) ? "MCX" : "NFO");
    if (subEl) subEl.textContent = `${seg} • ${und} Strike ${strike} ${optType}`;

    // Update LTP & Change
    const ltpEl = document.getElementById("directOrderLtp");
    if (ltpEl) ltpEl.textContent = `₹${resolvedLtp.toFixed(2)}`;

    const chgEl = document.getElementById("directOrderChange");
    if (chgEl) {
        const sign = window._directOrderState.net_change >= 0 ? "+" : "";
        chgEl.textContent = `${sign}${window._directOrderState.net_change.toFixed(2)} (${sign}${window._directOrderState.net_change_pct.toFixed(2)}%)`;
        chgEl.style.color = window._directOrderState.net_change >= 0 ? "#10b981" : "#f43f5e";
    }

    // Set Lots & Quantity Input
    const lotsInput = document.getElementById("directOrderLotsInput");
    if (lotsInput) {
        lotsInput.value = 1;
        lotsInput.min = 1;
        lotsInput.step = 1;
    }
    const qtyInput = document.getElementById("directOrderQuantityInput");
    if (qtyInput) {
        qtyInput.value = resolvedLot;
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

function adjustOrderLots(step) {
    let currentLots = Number(document.getElementById("directOrderLotsInput")?.value) || window._directOrderState.lots || 1;
    let newLots = currentLots + step;
    if (newLots < 1) newLots = 1;

    const lot = window._directOrderState.lot_size || 1;
    const newQty = newLots * lot;

    const lotsInput = document.getElementById("directOrderLotsInput");
    if (lotsInput) lotsInput.value = newLots;

    const qtyInput = document.getElementById("directOrderQuantityInput");
    if (qtyInput) qtyInput.value = newQty;

    window._directOrderState.lots = newLots;
    window._directOrderState.quantity = newQty;

    updateOrderLotBadge();
    updateOrderMarginSummary();
}
window.adjustOrderLots = adjustOrderLots;

function onDirectOrderLotsChange() {
    let val = Number(document.getElementById("directOrderLotsInput")?.value) || 1;
    if (val < 1) val = 1;

    const lot = window._directOrderState.lot_size || 1;
    const newQty = val * lot;

    const qtyInput = document.getElementById("directOrderQuantityInput");
    if (qtyInput) qtyInput.value = newQty;

    window._directOrderState.lots = val;
    window._directOrderState.quantity = newQty;

    updateOrderLotBadge();
    updateOrderMarginSummary();
}
window.onDirectOrderLotsChange = onDirectOrderLotsChange;

function adjustOrderQuantity(stepLots) {
    adjustOrderLots(stepLots);
}

function onDirectOrderQuantityChange() {
    onDirectOrderLotsChange();
}

function updateOrderLotBadge() {
    const badge = document.getElementById("directOrderLotBadge");
    if (!badge) return;
    const lot = window._directOrderState.lot_size || 1;
    const lots = window._directOrderState.lots || Math.max(1, Math.round((window._directOrderState.quantity || lot) / lot));
    const qty = window._directOrderState.quantity || (lots * lot);
    const und = window._directOrderState.underlying || window._straddleState.optionChainUnderlying || "NIFTY";
    const unit = (und === "GOLD") ? "KGS" : ((und === "SILVER") ? "KG" : "Qty");
    badge.textContent = `1 lot = ${lot} ${unit} (${qty} Total Qty)`;
}

function syncDirectOrderFunds() {
    fetch("/api/straddle/margin")
        .then(r => r.json())
        .then(data => {
            const avail = (data && data.success && data.margin && data.margin.available_margin) ? data.margin.available_margin : 0.0;
            const el = document.getElementById("directOrderAvailMargin");
            if (el) el.textContent = `₹${avail.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        })
        .catch(() => { });
}

function updateOrderMarginSummary() {
    const reqEl = document.getElementById("directOrderReqMargin");
    if (!reqEl) return;

    const qty = Number(window._directOrderState.quantity) || window._directOrderState.lot_size || 1;
    const lots = window._directOrderState.lots || Math.max(1, Math.round(qty / (window._directOrderState.lot_size || 1)));
    const isBuy = window._directOrderState.side === "BUY";
    const priceVal = Number(document.getElementById("directOrderPriceInput")?.value) || window._directOrderState.ltp || 100.0;
    const und = window._directOrderState.underlying || window._straddleState.optionChainUnderlying || "NIFTY";
    const prod = window._directOrderState.product || "D";

    // Standard Kotak Neo calibrated SPAN + Exposure Margin per Lot
    const LOT_MARGIN_MAP = {
        "GOLD": 1815574.25,
        "GOLDM": 181557.42,
        "SILVER": 625000.0,
        "SILVERM": 104500.0,
        "CRUDEOIL": 210000.0,
        "CRUDEOILM": 21000.0,
        "NATURALGAS": 140000.0,
        "NATGASMINI": 28000.0,
        "COPPER": 285000.0,
        "NIFTY": 124746.08,
        "BANKNIFTY": 117500.0,
        "FINNIFTY": 108000.0,
        "MIDCPNIFTY": 102000.0,
        "SENSEX": 122000.0,
        "BANKEX": 114000.0
    };

    const prodMult = prod === "I" ? 0.75 : 1.0;

    if (isBuy) {
        const req = qty * Math.max(0.05, priceVal);
        reqEl.textContent = req >= 100000
            ? `₹ ${(req / 100000).toFixed(2)} L (₹${req.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })})`
            : `₹ ${req.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    } else {
        const baseMarginPerLot = LOT_MARGIN_MAP[und] || 124746.08;
        const req = baseMarginPerLot * lots * prodMult;
        reqEl.textContent = req >= 100000
            ? `₹ ${(req / 100000).toFixed(2)} L (₹${req.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })})`
            : `₹ ${req.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
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
                    ? `₹ ${(req / 100000).toFixed(2)} L (₹${req.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })})`
                    : `₹ ${req.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
            }
        })
        .catch(() => { });
}

async function submitStraddleDirectOrder() {
    const submitBtn = document.getElementById("directOrderSubmitBtn");
    const lots = Number(document.getElementById("directOrderLotsInput")?.value) || window._directOrderState.lots || 1;
    const lotSize = window._directOrderState.lot_size || 1;
    const qty = lots * lotSize;
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

// =========================================================================
// STRADDLE ALGORITHMIC EXECUTION AUDIT LOGS CONTROLLER (INSTITUTIONAL GRADE)
// =========================================================================

window._straddleAuditLogsFilter = "all"; // "all" | "actual" | "paper" | "trades"
window._straddleAuditSideFilter = "ALL"; // "ALL" | "BUY" | "SELL"
window._straddleAuditLevelFilter = "ALL"; // "ALL" | "INFO" | "SUCCESS" | "INFO_SUCCESS" | "WARNING" | "ERROR"
window._straddleAuditOptionFilter = "ALL"; // "ALL" | "CE" | "PE"
window._straddleAuditRoleFilter = "ALL"; // "ALL" | "MAIN" | "HEDGE"
window._straddleAuditTriggerFilter = "ALL"; // "ALL" | "TARGET" | "STOPLOSS" | "REENTRY"
window._straddleAuditStrategyFilter = "ALL"; // "ALL" | strategy_name
window._straddleAuditAssetFilter = "ALL"; // "ALL" | asset_name
window._straddleAuditSearchTerm = "";
window._straddleAuditQuickMode = "all";
window._straddleAuditFilterCollapsed = false;
window._straddleAuditLiveStreamActive = true;
window._auditLogsPollTimer = null;
window._cachedAuditLogsList = [];

function openStraddleLogsModal() {
    const modal = document.getElementById("straddleLogsModal");
    if (!modal) return;
    modal.style.display = "flex";
    _populateAuditStrategyOptions();
    renderStraddleAuditLogs();
    startAuditLiveStreamTimer();
}
window.openStraddleLogsModal = openStraddleLogsModal;

function closeStraddleLogsModal() {
    const modal = document.getElementById("straddleLogsModal");
    if (modal) modal.style.display = "none";
    stopAuditLiveStreamTimer();
}
window.closeStraddleLogsModal = closeStraddleLogsModal;

function startAuditLiveStreamTimer() {
    stopAuditLiveStreamTimer();
    if (!window._straddleAuditLiveStreamActive) return;
    window._auditLogsPollTimer = setInterval(() => {
        const modal = document.getElementById("straddleLogsModal");
        if (modal && modal.style.display === "flex") {
            renderStraddleAuditLogs(true);
        } else {
            stopAuditLiveStreamTimer();
        }
    }, 2500);
}

function stopAuditLiveStreamTimer() {
    if (window._auditLogsPollTimer) {
        clearInterval(window._auditLogsPollTimer);
        window._auditLogsPollTimer = null;
    }
}

function toggleAuditLiveStream() {
    window._straddleAuditLiveStreamActive = !window._straddleAuditLiveStreamActive;
    const dot = document.getElementById("auditLiveStreamDot");
    const txt = document.getElementById("auditLiveStreamText");
    const btn = document.getElementById("btnToggleAuditLiveStream");

    if (window._straddleAuditLiveStreamActive) {
        if (dot) {
            dot.style.background = "#10b981";
            dot.style.boxShadow = "0 0 6px #10b981";
        }
        if (txt) txt.textContent = "LIVE: ON";
        if (btn) {
            btn.style.color = "#34d399";
            btn.style.borderColor = "#10b981";
            btn.style.background = "rgba(16,185,129,0.12)";
        }
        startAuditLiveStreamTimer();
        renderStraddleAuditLogs(true);
    } else {
        if (dot) {
            dot.style.background = "#64748b";
            dot.style.boxShadow = "none";
        }
        if (txt) txt.textContent = "LIVE: PAUSED";
        if (btn) {
            btn.style.color = "#94a3b8";
            btn.style.borderColor = "rgba(255,255,255,0.15)";
            btn.style.background = "rgba(255,255,255,0.05)";
        }
        stopAuditLiveStreamTimer();
    }
}
window.toggleAuditLiveStream = toggleAuditLiveStream;

function downloadAuditLogsExcel() {
    window.location.href = "/api/straddle/download_audit_logs?format=xlsx";
}
window.downloadAuditLogsExcel = downloadAuditLogsExcel;

function downloadAuditLogsCsv() {
    window.location.href = "/api/straddle/download_audit_logs?format=csv";
}
window.downloadAuditLogsCsv = downloadAuditLogsCsv;

function toggleAuditFilterBar() {
    const pane = document.getElementById("auditCollapsibleFilterPane");
    const icon = document.getElementById("iconToggleAuditFilter");
    const text = document.getElementById("textToggleAuditFilter");
    if (!pane) return;
    const isHidden = (pane.style.display === "none");
    pane.style.display = isHidden ? "flex" : "none";
    window._straddleAuditFilterCollapsed = !isHidden;
    if (icon) icon.textContent = isHidden ? "🔼" : "🔽";
    if (text) text.textContent = isHidden ? "Hide" : "Filters";
}
window.toggleAuditFilterBar = toggleAuditFilterBar;

function _updateAuditSummaryLabel() {
    const label = document.getElementById("auditFilterSummaryLabel");
    const resetBtn = document.getElementById("btnResetAuditFilters");
    if (!label) return;
    const parts = [];
    if (window._straddleAuditStrategyFilter !== "ALL") parts.push(`Strat: ${window._straddleAuditStrategyFilter}`);
    if (window._straddleAuditAssetFilter !== "ALL") parts.push(`Asset: ${window._straddleAuditAssetFilter}`);
    if (window._straddleAuditLevelFilter !== "ALL") parts.push(`Level: ${window._straddleAuditLevelFilter}`);
    if (window._straddleAuditOptionFilter !== "ALL") parts.push(`Opt: ${window._straddleAuditOptionFilter}`);
    if (window._straddleAuditRoleFilter !== "ALL") parts.push(`Role: ${window._straddleAuditRoleFilter}`);
    if (window._straddleAuditTriggerFilter !== "ALL") parts.push(`Trig: ${window._straddleAuditTriggerFilter}`);
    if (window._straddleAuditSideFilter !== "ALL") parts.push(`Side: ${window._straddleAuditSideFilter}`);
    if (window._straddleAuditLogsFilter !== "all") parts.push(`Mode: ${window._straddleAuditLogsFilter.toUpperCase()}`);
    if (window._straddleAuditSearchTerm) parts.push(`Search: "${window._straddleAuditSearchTerm}"`);

    const hasFilters = parts.length > 0;
    label.textContent = hasFilters ? `Filters: ${parts.join(" • ")}` : "All Events";
    if (resetBtn) {
        resetBtn.style.display = hasFilters ? "inline-block" : "none";
    }
}

function resetAuditFilters() {
    window._straddleAuditLogsFilter = "all";
    window._straddleAuditSideFilter = "ALL";
    window._straddleAuditLevelFilter = "ALL";
    window._straddleAuditOptionFilter = "ALL";
    window._straddleAuditRoleFilter = "ALL";
    window._straddleAuditTriggerFilter = "ALL";
    window._straddleAuditStrategyFilter = "ALL";
    window._straddleAuditAssetFilter = "ALL";
    window._straddleAuditSearchTerm = "";
    window._straddleAuditQuickMode = "all";

    const sStrat = document.getElementById("selAuditStrategyFilter");
    const sAsset = document.getElementById("selAuditAssetFilter");
    const sLvl = document.getElementById("selAuditLevelFilter");
    const sOpt = document.getElementById("selAuditOptionFilter");
    const sRole = document.getElementById("selAuditRoleFilter");
    const sTrig = document.getElementById("selAuditTriggerFilter");
    const sSide = document.getElementById("selAuditSideFilter");
    const sMode = document.getElementById("selAuditModeFilter");
    const sSearch = document.getElementById("inputAuditSearch");

    if (sStrat) sStrat.value = "ALL";
    if (sAsset) sAsset.value = "ALL";
    if (sLvl) sLvl.value = "ALL";
    if (sOpt) sOpt.value = "ALL";
    if (sRole) sRole.value = "ALL";
    if (sTrig) sTrig.value = "ALL";
    if (sSide) sSide.value = "ALL";
    if (sMode) sMode.value = "all";
    if (sSearch) sSearch.value = "";

    document.querySelectorAll(".algo-filter-pill").forEach(p => {
        p.classList.toggle("active", p.id === "btnAuditQuickAll");
        if (p.id === "btnAuditQuickAll") {
            p.style.background = "#1e293b";
            p.style.color = "#f8fafc";
        } else {
            p.style.background = "";
            p.style.color = "";
        }
    });

    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.resetAuditFilters = resetAuditFilters;

function onAuditSearchInput(val) {
    window._straddleAuditSearchTerm = (val || "").trim().toLowerCase();
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.onAuditSearchInput = onAuditSearchInput;

function setStraddleAuditStrategyFilter(strat) {
    window._straddleAuditStrategyFilter = strat || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditStrategyFilter = setStraddleAuditStrategyFilter;

function setStraddleAuditAssetFilter(asset) {
    window._straddleAuditAssetFilter = asset || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditAssetFilter = setStraddleAuditAssetFilter;

function setStraddleAuditLevelFilter(lvl) {
    window._straddleAuditLevelFilter = lvl || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditLevelFilter = setStraddleAuditLevelFilter;

function setStraddleAuditOptionFilter(opt) {
    window._straddleAuditOptionFilter = opt || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditOptionFilter = setStraddleAuditOptionFilter;

function setStraddleAuditRoleFilter(role) {
    window._straddleAuditRoleFilter = role || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditRoleFilter = setStraddleAuditRoleFilter;

function setStraddleAuditTriggerFilter(trig) {
    window._straddleAuditTriggerFilter = trig || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditTriggerFilter = setStraddleAuditTriggerFilter;

function setStraddleAuditSideFilter(side) {
    window._straddleAuditSideFilter = side || "ALL";
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditSideFilter = setStraddleAuditSideFilter;

function setStraddleAuditFilter(filterType) {
    window._straddleAuditLogsFilter = filterType || "all";
    const selMode = document.getElementById("selAuditModeFilter");
    if (selMode) selMode.value = window._straddleAuditLogsFilter;
    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setStraddleAuditFilter = setStraddleAuditFilter;

function setAuditQuickFilter(type) {
    window._straddleAuditQuickMode = type || "all";

    document.querySelectorAll(".algo-filter-pill").forEach(btn => {
        btn.classList.remove("active");
        btn.style.background = "";
        btn.style.color = "";
    });

    const sLvl = document.getElementById("selAuditLevelFilter");
    const sTrig = document.getElementById("selAuditTriggerFilter");
    const sRole = document.getElementById("selAuditRoleFilter");
    const sMode = document.getElementById("selAuditModeFilter");

    if (type === "all") {
        const b = document.getElementById("btnAuditQuickAll");
        if (b) { b.classList.add("active"); b.style.background = "#1e293b"; b.style.color = "#f8fafc"; }
        window._straddleAuditLevelFilter = "ALL";
        window._straddleAuditTriggerFilter = "ALL";
        window._straddleAuditRoleFilter = "ALL";
        window._straddleAuditLogsFilter = "all";
        if (sLvl) sLvl.value = "ALL";
        if (sTrig) sTrig.value = "ALL";
        if (sRole) sRole.value = "ALL";
        if (sMode) sMode.value = "all";
    } else if (type === "trades") {
        const b = document.getElementById("btnAuditQuickTrades");
        if (b) { b.classList.add("active"); b.style.background = "rgba(56,189,248,0.3)"; b.style.color = "#38bdf8"; }
        window._straddleAuditLogsFilter = "trades";
        if (sMode) sMode.value = "all";
    } else if (type === "entries") {
        const b = document.getElementById("btnAuditQuickEntries");
        if (b) { b.classList.add("active"); b.style.background = "rgba(14,165,233,0.3)"; b.style.color = "#38bdf8"; }
        window._straddleAuditTriggerFilter = "ENTRY";
        if (sTrig) sTrig.value = "ENTRY";
    } else if (type === "targets") {
        const b = document.getElementById("btnAuditQuickTargets");
        if (b) { b.classList.add("active"); b.style.background = "rgba(16,185,129,0.3)"; b.style.color = "#10b981"; }
        window._straddleAuditTriggerFilter = "TARGET";
        if (sTrig) sTrig.value = "TARGET";
    } else if (type === "stoploss") {
        const b = document.getElementById("btnAuditQuickStoploss");
        if (b) { b.classList.add("active"); b.style.background = "rgba(244,63,94,0.3)"; b.style.color = "#f43f5e"; }
        window._straddleAuditTriggerFilter = "STOPLOSS";
        if (sTrig) sTrig.value = "STOPLOSS";
    } else if (type === "hedges") {
        const b = document.getElementById("btnAuditQuickHedges");
        if (b) { b.classList.add("active"); b.style.background = "rgba(245,158,11,0.3)"; b.style.color = "#fbbf24"; }
        window._straddleAuditRoleFilter = "HEDGE";
        if (sRole) sRole.value = "HEDGE";
    } else if (type === "errors") {
        const b = document.getElementById("btnAuditQuickErrors");
        if (b) { b.classList.add("active"); b.style.background = "rgba(239,68,68,0.35)"; b.style.color = "#fca5a5"; }
        window._straddleAuditLevelFilter = "ERROR";
        if (sLvl) sLvl.value = "ERROR";
    }

    _updateAuditSummaryLabel();
    renderStraddleAuditLogsFiltered();
}
window.setAuditQuickFilter = setAuditQuickFilter;

function _populateAuditStrategyOptions() {
    const sel = document.getElementById("selAuditStrategyFilter");
    if (!sel) return;

    const currentVal = window._straddleAuditStrategyFilter || "ALL";
    const strats = new Set();

    // From active state strategies (sync directly with algo strategies)
    const stateStrats = (window._straddleState && (window._straddleState.allStrategies || window._straddleState.strategies))
        || (window._straddleState && window._straddleState.state && (window._straddleState.state.all_strategies || window._straddleState.state.strategies))
        || [];
    if (Array.isArray(stateStrats)) {
        stateStrats.forEach(s => {
            const name = (typeof s === "object" && s) ? (s.name || s.strategy_name || s.strategy_id || s.id) : String(s);
            if (name) strats.add(String(name).trim());
        });
    } else if (stateStrats && typeof stateStrats === "object") {
        Object.values(stateStrats).forEach(s => {
            const name = (typeof s === "object" && s) ? (s.name || s.strategy_name || s.strategy_id || s.id) : String(s);
            if (name) strats.add(String(name).trim());
        });
    }

    // Also scan cached logs for any mentioned strategy names
    if (Array.isArray(window._cachedAuditLogsList)) {
        window._cachedAuditLogsList.forEach(l => {
            if (l && typeof l === "object") {
                if (l.strategy_name) strats.add(String(l.strategy_name).trim());
                const det = l.details;
                if (det && typeof det === "object" && det.strategy_name) strats.add(String(det.strategy_name).trim());
            }
        });
    }

    let opts = '<option value="ALL">All Strategies</option>';
    Array.from(strats).filter(Boolean).sort().forEach(sName => {
        const isSel = (sName === currentVal) ? 'selected' : '';
        opts += `<option value="${sName}" ${isSel}>${sName}</option>`;
    });
    sel.innerHTML = opts;
}

function _parseAuditLogBadges(l) {
    const msg = String(l.message || "");
    const msgU = msg.toUpperCase();
    const det = (l.details && typeof l.details === "object") ? l.details : {};

    // 1. Level Badge
    const lvl = String(l.level || "INFO").toUpperCase();
    let levelBadge = "";
    let lvlColor = "#38bdf8";
    let lvlBg = "rgba(15, 23, 42, 0.75)";
    let lvlBorder = "rgba(255, 255, 255, 0.08)";

    if (lvl === "SUCCESS" || msgU.includes("✅") || msgU.includes("SUCCESSFULLY")) {
        levelBadge = `<span style="background:rgba(16,185,129,0.2); border:1px solid rgba(16,185,129,0.5); color:#10b981; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">SUCCESS</span>`;
        lvlColor = "#10b981";
        lvlBg = "rgba(6, 78, 59, 0.15)";
        lvlBorder = "rgba(16, 185, 129, 0.4)";
    } else if (lvl === "ERROR" || msgU.includes("FAILED") || msgU.includes("REJECTED") || msgU.includes("ERROR")) {
        levelBadge = `<span style="background:rgba(239,68,68,0.22); border:1px solid rgba(239,68,68,0.55); color:#f87171; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">ERROR</span>`;
        lvlColor = "#f87171";
        lvlBg = "rgba(127, 29, 29, 0.2)";
        lvlBorder = "rgba(239, 68, 68, 0.45)";
    } else if (lvl === "WARNING" || lvl === "WARN" || msgU.includes("⚠️") || msgU.includes("MARGIN GUARD") || msgU.includes("BLOCKED")) {
        levelBadge = `<span style="background:rgba(245,158,11,0.2); border:1px solid rgba(245,158,11,0.55); color:#fbbf24; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">WARNING</span>`;
        lvlColor = "#fbbf24";
        lvlBg = "rgba(120, 53, 15, 0.18)";
        lvlBorder = "rgba(245, 158, 11, 0.4)";
    } else {
        levelBadge = `<span style="background:rgba(56,189,248,0.18); border:1px solid rgba(56,189,248,0.45); color:#38bdf8; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">INFO</span>`;
    }

    // 2. Mode Badge (Live vs Paper vs System)
    let tt = String(l.trade_type || "").toUpperCase();
    if (!tt || tt === "-") {
        if (msgU.includes("[PAPER") || msgU.includes("PAPER TRADE") || det.mode === "PAPER") {
            tt = "PAPER";
        } else if (anyMatch(msgU, ["ENTERED", "EXITED", "FILLED", "STOPLOSS", "TARGET", "RE-EXECUTE", "RE-ENTRY", "HEDGE", "MARGIN AUTO-RELEASE"])) {
            tt = "LIVE";
        } else {
            tt = "SYSTEM";
        }
    }
    let modeBadge = "";
    if (tt === "LIVE") {
        modeBadge = `<span style="background:rgba(16,185,129,0.18); border:1px solid rgba(16,185,129,0.5); color:#34d399; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">🟢 LIVE</span>`;
    } else if (tt === "PAPER") {
        modeBadge = `<span style="background:rgba(168,85,247,0.18); border:1px solid rgba(168,85,247,0.5); color:#c084fc; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">🟣 PAPER</span>`;
    }

    // 3. Strategy Name Badge
    let stratName = String(l.strategy_name || det.strategy_name || det.algo_name || det.strategy || "").trim();
    if (!stratName || stratName === "-") {
        const mTag = msg.match(/^\[([a-zA-Z0-9_\- ]+)\]/);
        const systemTags = [
            "algotest parity", "hedge retained", "re-entry #1", "re-entry #2", "re-entry #3",
            "re-entry #4", "re-entry #5", "re-entry", "margin auto-release", "auto-reconciled position",
            "fail-safe sweep", "order blocked", "lpp auto-clamp", "hedge auto square-off", "hedge exit",
            "move sl to cost", "basket margin priority", "advance orders blocked", "pre-flight",
            "paper benchmark", "paper"
        ];
        if (mTag && !systemTags.includes(mTag[1].toLowerCase().trim())) {
            stratName = mTag[1].trim();
        }
    }
    if (!stratName || stratName === "-") {
        const m = msg.match(/(?:strategy|algorithm|for|into strategy)\s+['"]([^'"]+)['"]/i);
        if (m) stratName = m[1].trim();
    }
    if (!stratName || stratName === "-") {
        const activeS = (window._straddleState && (window._straddleState.activeStrategy || (window._straddleState.state && window._straddleState.state.activeStrategy)));
        stratName = (activeS ? (activeS.name || activeS.strategy_id) : "") || "nifty";
    }
    let stratBadge = "";
    if (stratName && stratName !== "-") {
        stratBadge = `<span style="background:rgba(147,197,253,0.15); border:1px solid rgba(147,197,253,0.35); color:#93c5fd; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">⚡ ${stratName}</span>`;
    }

    // 4. Underlying Asset Badge
    let und = String(l.underlying || det.underlying || "").trim();
    if (!und || und === "-") {
        const candidates = ["NATGASMINI", "NATURALGAS", "CRUDEOILM", "CRUDEOIL", "SILVERM", "SILVER", "GOLDM", "GOLD", "COPPER", "MIDCPNIFTY", "BANKNIFTY", "FINNIFTY", "BANKEX", "SENSEX", "NIFTY"];
        for (const u of candidates) {
            if (msgU.includes(u)) { und = u; break; }
        }
    }
    if (!und || und === "-") {
        und = "NIFTY";
    }
    let undBadge = "";
    if (und && und !== "-") {
        undBadge = `<span style="background:rgba(56,189,248,0.12); border:1px solid rgba(56,189,248,0.3); color:#38bdf8; font-size:10px; font-weight:800; padding:1px 5px; border-radius:4px;">${canonicalUnderlying(und)}</span>`;
    }

    // 5. Leg Role: Main vs Hedge
    let role = String(l.role || det.role || "").toUpperCase().trim();
    if (!role || role === "-") {
        const isHedge = Boolean(
            l.is_associated_hedge || det.is_associated_hedge || det.is_hedge
            || anyMatch(msgU, [
                "HEDGE", "HEDGE SHIFT", "HEDGE CHECK", "HEDGE AUTO", "ORPHAN HEDGE",
                "BUY HEDGE", "BUY NEW HEDGE", "HEDGE RETAINED", "HEDGE AUTO-EXIT",
                "HEDGE EXIT", "HEDGE REALIGNED", "ASSOCIATED HEDGE"
            ])
            || (msgU.includes("ENTERED BUY") && (msgU.includes("TGT: 0") || msgU.includes("TGT: 0.0") || msgU.includes("SL: 0")))
        );
        const isMain = Boolean(
            anyMatch(msgU, [
                "MAIN", "SHORT STRADDLE", "ENTERED SELL", "SHORT RE-ENTRY", "MAIN LEG",
                "TARGET ORDER", "STOPLOSS ORDER", "TARGET HIT", "STOPLOSS HIT", "TRAILING SL",
                "MOVE SL TO COST", "TARGET CANCELLED", "SL CANCELLED", "RE-EXECUTE", "RE-ENTRY"
            ])
            || msgU.includes("ENTERED SELL")
        );
        if (isHedge) role = "HEDGE";
        else if (isMain) role = "MAIN";
        else role = "-";
    }
    let roleBadge = "";
    if (role === "HEDGE") {
        roleBadge = `<span style="background:rgba(245,158,11,0.18); border:1px solid rgba(245,158,11,0.5); color:#fbbf24; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">🛡️ HEDGE</span>`;
    } else if (role === "MAIN") {
        roleBadge = `<span style="background:rgba(139,92,246,0.18); border:1px solid rgba(139,92,246,0.5); color:#a78bfa; font-size:10px; font-weight:800; padding:1px 6px; border-radius:4px;">⚡ MAIN</span>`;
    }

    // 6. Transaction Side: BUY vs SELL
    let side = String(l.side || det.side || "").toUpperCase().trim();
    if (!side || side === "-") {
        if (anyMatch(msgU, ["ENTERED BUY", "BUY NEW HEDGE", "BUY HEDGE", "BUYING", "BUY ", "(EXIT BUY)"])) {
            side = "BUY";
        } else if (anyMatch(msgU, ["ENTERED SELL", "SELLING", "SHORT RE-ENTRY", "SELL ", "(EXIT SELL)"])) {
            side = "SELL";
        } else if (anyMatch(msgU, ["SQUARING OFF ASSOCIATED HEDGE", "HEDGE AUTO SQUARE-OFF", "HEDGE AUTO-EXIT", "HEDGE EXIT", "ORPHAN HEDGE AUTO-EXIT"])) {
            side = "SELL";
        } else if (anyMatch(msgU, ["TARGET ORDER", "STOPLOSS ORDER", "TARGET HIT", "STOPLOSS HIT"])) {
            side = (role === "MAIN") ? "BUY" : "SELL";
        } else if (anyMatch(msgU, ["RE-EXECUTE", "RE EXECUTE"])) {
            side = (role === "MAIN") ? "SELL" : "BUY";
        } else {
            side = "-";
        }
    }
    let sideBadge = "";
    if (side === "BUY") {
        sideBadge = `<span style="background:rgba(34,197,94,0.18); border:1px solid rgba(34,197,94,0.5); color:#4ade80; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">BUY</span>`;
    } else if (side === "SELL") {
        sideBadge = `<span style="background:rgba(249,115,22,0.18); border:1px solid rgba(249,115,22,0.5); color:#fb923c; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">SELL</span>`;
    }

    // 7. Option Type: CE vs PE
    let opt = String(l.option_type || det.option_type || "").toUpperCase().trim();
    if (!opt || opt === "-") {
        if (/\bCE\b/.test(msgU) || msgU.includes(" CALL ") || msgU.includes("CE]") || msgU.includes("(CE)")) {
            opt = "CE";
        } else if (/\bPE\b/.test(msgU) || msgU.includes(" PUT ") || msgU.includes("PE]") || msgU.includes("(PE)")) {
            opt = "PE";
        } else {
            opt = "-";
        }
    }
    let optBadge = "";
    if (opt === "CE") {
        optBadge = `<span style="background:rgba(2,132,199,0.18); border:1px solid rgba(2,132,199,0.5); color:#38bdf8; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">📈 CE</span>`;
    } else if (opt === "PE") {
        optBadge = `<span style="background:rgba(168,85,247,0.18); border:1px solid rgba(168,85,247,0.5); color:#c084fc; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">📉 PE</span>`;
    }

    // 8. Trigger / Action: ENTRY vs REENTRY vs TARGET vs STOPLOSS vs EXIT
    let trig = String(l.trigger || det.trigger || det.action || "").toUpperCase().trim();
    if (!trig || trig === "-") {
        const isReEntry = anyMatch(msgU, ["RE-EXECUTE", "RE EXECUTE", "RE-ENTRY", "RE ENTRY", "RE COST", "RE-COST", "TRIGGERING RE"]);
        const isEntry = anyMatch(msgU, ["ENTERED BUY", "ENTERED SELL", "ENTERED ", "INITIAL STRATEGY ENTRY"]);
        const isTarget = anyMatch(msgU, ["TARGET HIT", "TARGET REACHED", "PROFIT TARGET", "HIT TARGET", "(TARGET HIT)", "TARGET ORDER", "TARGET FILLED", "(TARGET)"]) && !isEntry;
        const isStoploss = anyMatch(msgU, ["STOPLOSS HIT", "SL HIT", "HIT SL", "STOP LOSS HIT", "SL TRIGGERED", "(STOPLOSS HIT)", "STOPLOSS ORDER", "STOPLOSS FILLED", "(STOPLOSS)"]) && !isEntry;
        const isExit = anyMatch(msgU, ["EXITED", "SQUARE OFF", "SQUARED OFF", "MANUAL EXIT", "EOD EXIT", "AUTO-RECONCILED POSITION", "HEDGE AUTO SQUARE-OFF", "HEDGE AUTO-EXIT", "ORPHAN HEDGE AUTO-EXIT", "HEDGE EXIT"]) && !isEntry && !isTarget && !isStoploss;

        if (isReEntry) trig = "REENTRY";
        else if (isEntry) trig = "ENTRY";
        else if (isTarget) trig = "TARGET";
        else if (isStoploss) trig = "STOPLOSS";
        else if (isExit) trig = "EXIT";
        else trig = "-";
    }

    let trigBadge = "";
    if (trig === "ENTRY") {
        trigBadge = `<span style="background:rgba(14,165,233,0.18); border:1px solid rgba(14,165,233,0.5); color:#38bdf8; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">📥 ENTRY</span>`;
    } else if (trig === "REENTRY") {
        trigBadge = `<span style="background:rgba(6,182,212,0.22); border:1px solid rgba(6,182,212,0.55); color:#06b6d4; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">🔄 RE-ENTRY</span>`;
    } else if (trig === "TARGET") {
        trigBadge = `<span style="background:rgba(16,185,129,0.22); border:1px solid rgba(16,185,129,0.55); color:#10b981; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">🎯 TARGET</span>`;
    } else if (trig === "STOPLOSS") {
        trigBadge = `<span style="background:rgba(244,63,94,0.22); border:1px solid rgba(244,63,94,0.55); color:#f43f5e; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">🛑 STOPLOSS</span>`;
    } else if (trig === "EXIT") {
        trigBadge = `<span style="background:rgba(244,63,94,0.18); border:1px solid rgba(244,63,94,0.45); color:#fb7185; font-size:10px; font-weight:900; padding:1px 6px; border-radius:4px;">⏹️ EXIT</span>`;
    }

    return {
        levelBadge, modeBadge, stratBadge, undBadge, roleBadge, sideBadge, optBadge, trigBadge,
        level: lvl, tradeType: tt, strategyName: stratName, underlying: und, role, side, optType: opt, trigger: trig,
        color: lvlColor, bg: lvlBg, border: lvlBorder, details: det
    };
}

function anyMatch(text, words) {
    for (let i = 0; i < words.length; i++) {
        if (text.includes(words[i])) return true;
    }
    return false;
}

function renderStraddleAuditLogsFiltered() {
    const container = document.getElementById("straddleAuditLogsContainer");
    if (!container || !window._cachedAuditLogsList) return;
    container.innerHTML = _buildAuditLogsHtml(window._cachedAuditLogsList);
}

function _buildAuditLogsHtml(logsList) {
    try {
        if (!logsList || !Array.isArray(logsList) || logsList.length === 0) {
            const badge = document.getElementById("auditLogCountBadge");
            if (badge) badge.textContent = "0 Events";
            return `<div style="color:#64748b; text-align:center; padding:30px; font-size:13px;">No audit events recorded yet.</div>`;
        }

        // Always clean out unnecessary focused underlying asset switch logs from display
        const validLogs = logsList.filter(l => {
            if (!l || typeof l !== "object") return false;
            const msg = String(l.message || "").toLowerCase();
            return !msg.includes("focused underlying switched to");
        });

        const filterType = window._straddleAuditLogsFilter || "all";
        const sideFilter = window._straddleAuditSideFilter || "ALL";
        const levelFilter = window._straddleAuditLevelFilter || "ALL";
        const optionFilter = window._straddleAuditOptionFilter || "ALL";
        const roleFilter = window._straddleAuditRoleFilter || "ALL";
        const triggerFilter = window._straddleAuditTriggerFilter || "ALL";
        const strategyFilter = window._straddleAuditStrategyFilter || "ALL";
        const assetFilter = window._straddleAuditAssetFilter || "ALL";
        const searchTerm = (window._straddleAuditSearchTerm || "").toLowerCase().trim();
        const quickMode = window._straddleAuditQuickMode || "all";

        const filtered = validLogs.filter(l => {
            const parsed = _parseAuditLogBadges(l);
            const msg = String(l.message || "").toLowerCase();

            // 1. Live Keyword Search
            if (searchTerm) {
                const detStr = (l.details && typeof l.details === "object") ? JSON.stringify(l.details).toLowerCase() : "";
                const stratLower = String(parsed.strategyName || "").toLowerCase();
                const undLower = String(parsed.underlying || "").toLowerCase();
                if (!msg.includes(searchTerm) && !detStr.includes(searchTerm) && !stratLower.includes(searchTerm) && !undLower.includes(searchTerm)) {
                    return false;
                }
            }

            // 2. Strategy Filter
            if (strategyFilter !== "ALL" && String(parsed.strategyName || "").trim() !== String(strategyFilter).trim()) {
                return false;
            }

            // 3. Asset Filter
            if (assetFilter !== "ALL" && String(parsed.underlying || "").toUpperCase() !== String(assetFilter).toUpperCase()) {
                return false;
            }

            // 4. Level Filter
            if (levelFilter === "INFO" && parsed.level !== "INFO") return false;
            if (levelFilter === "SUCCESS" && parsed.level !== "SUCCESS") return false;
            if (levelFilter === "INFO_SUCCESS" && parsed.level !== "INFO" && parsed.level !== "SUCCESS") return false;
            if (levelFilter === "WARNING" && parsed.level !== "WARNING" && parsed.level !== "WARN") return false;
            if (levelFilter === "ERROR" && parsed.level !== "ERROR") return false;

            // 5. Trade Mode Filter
            if (filterType === "actual" && parsed.tradeType !== "LIVE") return false;
            if (filterType === "paper" && parsed.tradeType !== "PAPER") return false;
            if (filterType === "trades" && parsed.tradeType !== "LIVE" && parsed.tradeType !== "PAPER") return false;

            // 6. Side Filter
            if (sideFilter !== "ALL" && parsed.side !== sideFilter) return false;

            // 7. Option Type Filter (CE / PE)
            if (optionFilter !== "ALL" && parsed.optType !== optionFilter) return false;

            // 8. Role Filter (Main / Hedge)
            if (roleFilter !== "ALL" && parsed.role !== roleFilter) return false;

            // 9. Trigger Filter
            if (triggerFilter === "TARGET" && parsed.trigger !== "TARGET") return false;
            if (triggerFilter === "STOPLOSS" && parsed.trigger !== "STOPLOSS") return false;
            if (triggerFilter === "REENTRY" && parsed.trigger !== "REENTRY") return false;
            if (triggerFilter === "ENTRY" && parsed.trigger !== "ENTRY") return false;
            if (triggerFilter === "EXIT" && parsed.trigger !== "EXIT") return false;

            // 10. Quick Filter Mode overrides
            if (quickMode === "trades" && parsed.tradeType !== "LIVE") return false;
            if (quickMode === "entries" && parsed.trigger !== "ENTRY") return false;
            if (quickMode === "targets" && parsed.trigger !== "TARGET") return false;
            if (quickMode === "stoploss" && parsed.trigger !== "STOPLOSS") return false;
            if (quickMode === "hedges" && parsed.role !== "HEDGE") return false;
            if (quickMode === "errors" && parsed.level !== "ERROR") return false;

            return true;
        });

        // Update real-time event counter badge
        const badge = document.getElementById("auditLogCountBadge");
        if (badge) {
            badge.textContent = `${filtered.length} / ${validLogs.length} Events`;
        }

        if (filtered.length === 0) {
            return `<div style="color:#64748b; text-align:center; padding:30px; font-size:12.5px;">No matching log events for current filter settings. <button type="button" class="straddle-btn btn-outline" style="padding:2px 8px; font-size:11px; margin-left:8px;" onclick="resetAuditFilters()">Reset Filters</button></div>`;
        }

        let html = "";
        filtered.forEach((l, idx) => {
            if (!l || typeof l !== "object") return;
            const parsed = _parseAuditLogBadges(l);
            const ts = String(l.timestamp || "");
            const dt = String(l.date || "");
            const dateStr = (dt && !ts.startsWith(dt)) ? `${dt} ` : "";
            const det = (l.details && typeof l.details === "object") ? l.details : {};
            const hasDet = Object.keys(det).length > 0;

            let detailsHtml = "";
            if (hasDet) {
                const chips = [];
                if (det.order_id) chips.push(`<span style="color:#94a3b8;">Order: <b style="color:#f8fafc;">${det.order_id}</b></span>`);
                if (det.price) chips.push(`<span style="color:#94a3b8;">Price: <b style="color:#38bdf8;">₹${det.price}</b></span>`);
                if (det.qty) chips.push(`<span style="color:#94a3b8;">Qty: <b style="color:#f8fafc;">${det.qty}</b></span>`);
                if (det.symbol) chips.push(`<span style="color:#cbd5e1;">Symbol: <b style="color:#cbd5e1;">${det.symbol}</b></span>`);
                if (det.error_code) chips.push(`<span style="color:#f87171;">Error: <b>${det.error_code}</b></span>`);
                if (det.reason) chips.push(`<span style="color:#fbbf24;">Reason: <b>${det.reason}</b></span>`);

                if (chips.length > 0) {
                    detailsHtml = `
                        <div style="margin-top:4px; padding:3px 8px; background:rgba(0,0,0,0.3); border-radius:4px; display:flex; flex-wrap:wrap; gap:8px; font-size:10.5px; border-left:2px solid ${parsed.color};">
                            ${chips.join("<span style='color:rgba(255,255,255,0.15);'>|</span>")}
                        </div>
                    `;
                }
            }

            html += `
                <div class="audit-log-card" style="padding:8px 12px; background:${parsed.bg}; border:1px solid ${parsed.border}; border-radius:6px; margin-bottom:4px; display:flex; flex-direction:column; gap:4px; transition:border-color 0.15s ease;">
                    <div class="audit-log-card-header" style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:6px;">
                        <div style="display:inline-flex; align-items:center; gap:6px; flex-wrap:wrap;">
                            <span style="color:#94a3b8; font-size:11px; white-space:nowrap; font-family:var(--font-mono); font-weight:700;">🕒 [${dateStr}${ts}]</span>
                            ${parsed.levelBadge}
                            ${parsed.stratBadge}
                            ${parsed.undBadge}
                        </div>
                        <div style="display:inline-flex; align-items:center; gap:4px; flex-wrap:wrap;">
                            ${parsed.modeBadge}
                            ${parsed.roleBadge}
                            ${parsed.sideBadge}
                            ${parsed.optBadge}
                            ${parsed.trigBadge}
                        </div>
                    </div>
                    <div style="color:#f1f5f9; font-size:12px; line-height:1.45; word-break:break-word; overflow-wrap:break-word;">
                        ${l.message || ''}
                    </div>
                    ${detailsHtml}
                </div>
            `;
        });
        return html;
    } catch (err) {
        console.error("Critical error in _buildAuditLogsHtml:", err);
        return `<div style="color:#f43f5e; text-align:center; padding:20px; font-size:12.5px;">Log display error: ${err.message}</div>`;
    }
}

function renderStraddleAuditLogs(isSilent = false) {
    const container = document.getElementById("straddleAuditLogsContainer");
    if (!container) return;

    // Instant optimistic render from in-memory cache or state so modal is NEVER stuck on "Fetching..."
    const cachedLogs = (window._cachedAuditLogsList && window._cachedAuditLogsList.length > 0)
        ? window._cachedAuditLogsList
        : ((window._straddleState && (window._straddleState.logs || (window._straddleState.state && window._straddleState.state.logs))) || []);

    if (cachedLogs && cachedLogs.length > 0) {
        window._cachedAuditLogsList = cachedLogs;
        _populateAuditStrategyOptions();
        try {
            container.innerHTML = _buildAuditLogsHtml(cachedLogs);
        } catch (err) {
            console.warn("Optimistic audit log render error:", err);
        }
    } else if (!isSilent) {
        container.innerHTML = `<div style="color:#64748b; text-align:center; padding:20px;">Fetching live audit logs...</div>`;
    }

    const abortCtrl = new AbortController();
    const timeoutId = setTimeout(() => abortCtrl.abort(), 20000);

    fetch("/api/straddle/logs?limit=1000", { signal: abortCtrl.signal })
        .then(r => {
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            return r.json();
        })
        .then(data => {
            clearTimeout(timeoutId);
            const logs = (data && data.success && Array.isArray(data.logs)) ? data.logs : [];
            // Filter out any lingering underlying asset switch logs
            const cleanLogs = logs.filter(l => l && !String(l.message || "").toLowerCase().includes("focused underlying switched to"));
            window._cachedAuditLogsList = cleanLogs;

            // Audio alert detection for freshly arriving audit log events
            if (!window._soundSeenLogKeys) window._soundSeenLogKeys = new Set();
            cleanLogs.forEach(l => {
                if (!l) return;
                const logKey = `${l.date || ''}_${l.timestamp || ''}_${l.message || ''}`;
                if (!window._soundSeenLogKeys.has(logKey)) {
                    window._soundSeenLogKeys.add(logKey);
                    if (window._soundInitialLogLoadDone && window.StraddleSound) {
                        const msgU = String(l.message || "").toUpperCase();
                        if (msgU.includes("TARGET HIT") || msgU.includes("PROFIT TARGET")) {
                            StraddleSound.play("target");
                        } else if (msgU.includes("STOPLOSS HIT") || msgU.includes("SL HIT") || msgU.includes("STOP LOSS HIT")) {
                            StraddleSound.play("stoploss");
                        } else if (l.level === "ERROR" || msgU.includes("REJECTED") || msgU.includes("FAILED")) {
                            StraddleSound.play("error");
                        }
                    }
                }
            });
            window._soundInitialLogLoadDone = true;

            _populateAuditStrategyOptions();
            try {
                container.innerHTML = _buildAuditLogsHtml(cleanLogs);
            } catch (renderErr) {
                console.error("Error rendering audit logs:", renderErr);
                container.innerHTML = `<div style="color:#f43f5e; text-align:center; padding:20px;">Error rendering logs: ${renderErr.message}</div>`;
            }
        })
        .catch(e => {
            clearTimeout(timeoutId);
            const fallback = window._cachedAuditLogsList || [];
            if (fallback && fallback.length > 0) {
                try {
                    container.innerHTML = _buildAuditLogsHtml(fallback);
                } catch (renderErr) {
                    container.innerHTML = `<div style="color:#f43f5e; text-align:center; padding:20px;">Error loading logs: ${e.message}</div>`;
                }
            } else if (!isSilent) {
                container.innerHTML = `<div style="color:#f43f5e; text-align:center; padding:20px;">Error loading logs: ${e.message}</div>`;
            }
        });
}
window.renderStraddleAuditLogs = renderStraddleAuditLogs;
// Window global bindings for Straddle error retry and modal controls
window.renderStraddlePositions = renderStraddlePositions;
window.onStraddlePositionCheckboxChange = onStraddlePositionCheckboxChange;
window.toggleSelectAllStraddleLegs = toggleSelectAllStraddleLegs;
window.retrySelectedStraddleLegs = retrySelectedStraddleLegs;
window.resetCurrentStraddleAlgo = resetCurrentStraddleAlgo;
window.openLegSettingsModal = openLegSettingsModal;
window.closeLegSettingsModal = closeLegSettingsModal;
window.saveLegSettingsModal = saveLegSettingsModal;
window.toggleLegModalHedgeFields = toggleLegModalHedgeFields;
window.onLegSideChanged = onLegSideChanged;
window.toggleLegHedgeRow = toggleLegHedgeRow;
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
window.setOptChainViewMode = setOptChainViewMode;
window.addOptionChainLegToStrategy = addOptionChainLegToStrategy;
window.openOptionChainForActiveStrategy = openOptionChainForActiveStrategy;
window.openStraddleLogsModal = openStraddleLogsModal;
window.closeStraddleLogsModal = closeStraddleLogsModal;
window.downloadAuditLogsExcel = downloadAuditLogsExcel;
window.renderStraddleAuditLogs = renderStraddleAuditLogs;
window.setStraddleAuditFilter = setStraddleAuditFilter;
window.setStraddleAuditSideFilter = setStraddleAuditSideFilter;

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
window.sortStraddlePositionsBy = sortStraddlePositionsBy;
window.setStraddlePositionsViewFilter = setStraddlePositionsViewFilter;
window.confirmSquareOffAllOpenPositions = confirmSquareOffAllOpenPositions;
window.clearStraddleClosedAndExpiredPositions = clearStraddleClosedAndExpiredPositions;
window.loadStraddleMargin = loadStraddleMargin;

// Live trade editing and error customization exports
window.autoFitErrorLegLots = autoFitErrorLegLots;
window.dismissErrorLeg = dismissErrorLeg;
window.retrySingleStraddleLegInline = retrySingleStraddleLegInline;
window.saveRunningPositionChanges = saveRunningPositionChanges;
window.markRunningLegChanged = markRunningLegChanged;
window.squareOffSingleLegIndex = squareOffSingleLegIndex;
window.stepLiveStrikePrice = stepLiveStrikePrice;
window.stepLiveLots = stepLiveLots;
window.stepErrorStrikePrice = stepErrorStrikePrice;
window.stepErrorLots = stepErrorLots;
window.preserveErrorLegChange = preserveErrorLegChange;
window.stepLegStrikeSelect = stepLegStrikeSelect;
window.stepLegLotsInput = stepLegLotsInput;
window.syncStrategyLegChange = syncStrategyLegChange;
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

    const chk = document.getElementById("adopt_is_hedge_checkbox");
    if (chk) chk.checked = false;
    onAdoptIsHedgeToggle(false);

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

function onAdoptIsHedgeToggle(checked) {
    const rulesContainer = document.getElementById("adopt_rules_container");
    const parentContainer = document.getElementById("adopt_hedge_parent_container");

    if (checked) {
        if (rulesContainer) rulesContainer.style.display = "none";
        if (parentContainer) parentContainer.style.display = "block";
        populateAdoptHedgeParentOptions();
    } else {
        if (rulesContainer) rulesContainer.style.display = "block";
        if (parentContainer) parentContainer.style.display = "none";
    }
}
window.onAdoptIsHedgeToggle = onAdoptIsHedgeToggle;

function populateAdoptHedgeParentOptions() {
    const stratId = document.getElementById("adopt_target_strat_select")?.value ||
        window._straddleState?.activeStrategy?.strategy_id ||
        window._straddleState?.activeStrategyId;
    const parentSelect = document.getElementById("adopt_hedge_parent_select");
    if (!parentSelect) return;

    parentSelect.innerHTML = `<option value="">-- Select Short Leg to Hedge --</option>`;
    const allStrats = window._straddleState?.allStrategies || [];
    const strat = allStrats.find(s => s.strategy_id === stratId) || window._straddleState?.activeStrategy;
    if (strat && Array.isArray(strat.positions)) {
        const shortLegs = strat.positions.filter(p => p.status === "OPEN" && String(p.side || "").toUpperCase() === "SELL");
        if (shortLegs.length === 0) {
            parentSelect.innerHTML += `<option value="" disabled>(No active Short legs found in this strategy)</option>`;
        } else {
            shortLegs.forEach(sp => {
                parentSelect.innerHTML += `<option value="${sp.id || sp.leg_id || sp.symbol}">${sp.symbol} (Short ${sp.quantity} qty @ ₹${Number(sp.avg_ent_price || 0).toFixed(2)})</option>`;
            });
        }
    }
}
window.populateAdoptHedgeParentOptions = populateAdoptHedgeParentOptions;

function onAdoptTargetStratChange(targetId) {
    if (targetId) {
        fetchAdoptablePositionsForStrategy(targetId);
        if (document.getElementById("adopt_is_hedge_checkbox")?.checked) {
            populateAdoptHedgeParentOptions();
        }
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
    const chk = document.getElementById("adopt_is_hedge_checkbox");
    if (chk) chk.checked = false;
    onAdoptIsHedgeToggle(false);
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

    const isHedge = document.getElementById("adopt_is_hedge_checkbox")?.checked || false;
    const parentLegId = document.getElementById("adopt_hedge_parent_select")?.value || "";

    if (isHedge && !parentLegId) {
        showStraddleToast("Please select which Short leg this adopted trade will hedge.", "error", 3000);
        return;
    }

    const tgtMode = document.getElementById("adopt_tgt_mode")?.value || "%";
    const tgtVal = parseFloat(document.getElementById("adopt_tgt_val")?.value) || 25;
    const slMode = document.getElementById("adopt_sl_mode")?.value || "%";
    const slVal = parseFloat(document.getElementById("adopt_sl_val")?.value) || 15;
    const tgtReType = document.getElementById("adopt_tgt_re_type")?.value || "RE EXECUTE";
    const tgtReCount = parseInt(document.getElementById("adopt_tgt_re_count")?.value, 10) || 0;
    const slReType = document.getElementById("adopt_sl_re_type")?.value || "RE EXECUTE";
    const slReCount = parseInt(document.getElementById("adopt_sl_re_count")?.value, 10) || 0;
    const tslProfit = parseFloat(document.getElementById("adopt_tsl_profit")?.value) || 0;
    const tslTrail = parseFloat(document.getElementById("adopt_tsl_trail")?.value) || 0;

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
                    product: pos.product || "NRML",
                    quantity: pos.quantity,
                    lots: pos.lots || 1,
                    lot_size: pos.lot_size || 65,
                    strike_price: pos.strike_price,
                    option_type: pos.option_type,
                    expiry_date: pos.expiry_date,
                    avg_price: pos.avg_price,
                    ltp: pos.ltp || pos.avg_price,
                    is_hedge: isHedge,
                    parent_leg_id: parentLegId,
                    target_mode: tgtMode,
                    target_val: tgtVal,
                    sl_mode: slMode,
                    sl_val: slVal,
                    target_re_execute_type: tgtReType,
                    target_re_execute_count: tgtReCount,
                    sl_re_execute_type: slReType,
                    sl_re_execute_count: slReCount,
                    tsl_profit_pts: tslProfit,
                    tsl_trail_pts: tslTrail,
                    is_adopted: true,
                    entry_reason: isHedge ? `Adopted Hedge from Kotak Neo (${pos.option_type || 'Hedge'})` : `Adopted from Kotak Neo [${pos.side || 'TRADE'} ${pos.option_type || ''}]`
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
// ANGELONE-GRADE MULTI-BASKET ORDERS SYSTEM
// =========================================================================

const STORAGE_SAVED_BASKETS = "straddle_saved_baskets";
const STORAGE_LAST_BASKET_ID = "straddle_last_active_basket_id";
const STORAGE_LEGACY_ORDERS = "straddle_basket_orders";
const STORAGE_LEGACY_NAME = "straddle_basket_name";

window._basketOrders = [];
window._activeBasketId = "basket_default";

function getSavedBasketsList() {
    let list = [];
    try {
        const raw = localStorage.getItem(STORAGE_SAVED_BASKETS);
        if (raw) {
            list = JSON.parse(raw);
        }
    } catch (e) {
        console.error("Error reading saved baskets:", e);
    }

    if (!Array.isArray(list) || list.length === 0) {
        // Check for legacy migration
        let legacyOrders = [];
        let legacyName = "DEFAULT BASKET";
        try {
            const rawLegacy = localStorage.getItem(STORAGE_LEGACY_ORDERS);
            if (rawLegacy) legacyOrders = JSON.parse(rawLegacy) || [];
            const rawName = localStorage.getItem(STORAGE_LEGACY_NAME);
            if (rawName) legacyName = rawName.toUpperCase();
        } catch (e) { }

        list = [
            {
                id: "basket_default",
                name: legacyName || "DEFAULT BASKET",
                orders: legacyOrders,
                updated_at: Date.now()
            }
        ];
        try {
            localStorage.setItem(STORAGE_SAVED_BASKETS, JSON.stringify(list));
        } catch (e) { }
    }
    return list;
}

function saveBasketsList(list) {
    try {
        localStorage.setItem(STORAGE_SAVED_BASKETS, JSON.stringify(list));
    } catch (e) {
        console.error("Failed saving baskets list:", e);
    }
}

function syncBasketDropdown() {
    const sel = document.getElementById("savedBasketsSelect");
    if (!sel) return;
    const list = getSavedBasketsList();
    sel.innerHTML = "";
    list.forEach(b => {
        const opt = document.createElement("option");
        opt.value = b.id;
        let count = Array.isArray(b.orders) ? b.orders.length : 0;
        if (b.id === window._activeBasketId && Array.isArray(window._basketOrders)) {
            count = window._basketOrders.length;
        }
        opt.textContent = `${b.name} (${count})`;
        if (b.id === window._activeBasketId) {
            opt.selected = true;
        }
        sel.appendChild(opt);
    });
}

function persistActiveBasketOrders() {
    try {
        localStorage.setItem(STORAGE_LEGACY_ORDERS, JSON.stringify(window._basketOrders || []));
    } catch (e) { }

    const list = getSavedBasketsList();
    const current = list.find(b => b.id === window._activeBasketId);
    if (current) {
        current.orders = JSON.parse(JSON.stringify(window._basketOrders || []));
        current.updated_at = Date.now();
        saveBasketsList(list);
    }
    syncBasketDropdown();
    updateBasketBadges();
}
window.persistActiveBasketOrders = persistActiveBasketOrders;

function loadActiveBasketFromStorage() {
    const list = getSavedBasketsList();
    let targetId = localStorage.getItem(STORAGE_LAST_BASKET_ID);
    let basket = list.find(b => b.id === targetId);
    if (!basket) {
        basket = list[0];
        targetId = basket ? basket.id : "basket_default";
    }
    window._activeBasketId = targetId;

    if (!basket.orders || basket.orders.length === 0) {
        try {
            const legacy = JSON.parse(localStorage.getItem(STORAGE_LEGACY_ORDERS) || "[]");
            if (Array.isArray(legacy) && legacy.length > 0) {
                basket.orders = legacy;
                saveBasketsList(list);
            }
        } catch (e) { }
    }

    window._basketOrders = Array.isArray(basket.orders) ? JSON.parse(JSON.stringify(basket.orders)) : [];

    // Set UI elements
    const nameEl = document.getElementById("basketModalName");
    if (nameEl) nameEl.innerText = basket.name || "DEFAULT BASKET";

    const undSel = document.getElementById("basketUnderlyingSelect");
    if (undSel) {
        undSel.value = basket.underlying || "NIFTY";
        const searchInput = document.getElementById("basketSearchInput");
        if (searchInput) searchInput.placeholder = `Search ${undSel.value} strikes (e.g. 23500, CE, PE)...`;
    }
    const badgeEl = document.getElementById("basketUnderlyingBadge");
    if (badgeEl) badgeEl.textContent = basket.underlying || "NIFTY";

    syncBasketDropdown();
    updateBasketBadges();
}

function onBasketUnderlyingChange(newUnd) {
    if (!newUnd) return;
    const cleanUnd = String(newUnd).toUpperCase().replace(/\s+/g, '');
    const list = getSavedBasketsList();
    const current = list.find(b => b.id === window._activeBasketId);
    if (current) {
        current.underlying = cleanUnd;
        current.orders = []; // Immediately empty the saved basket orders for this basket
        saveBasketsList(list);
    }
    // Immediately empty active in-memory basket orders and selections
    window._basketOrders = [];
    window._chainSelectedLegs = {};
    try {
        localStorage.setItem("straddle_basket_orders", JSON.stringify([]));
    } catch (e) { }

    const searchInput = document.getElementById("basketSearchInput");
    if (searchInput) {
        searchInput.placeholder = `Search ${cleanUnd} strikes (e.g. 23800, 17 Nov)...`;
    }
    loadBasketExpiryFilterOptions(cleanUnd);
    clearBasketSearch();
    renderBasketOrdersTable();
    updateBasketBadges();
    updateBasketMarginCalculation();
    updateBasketAutoName();
    showStraddleToast(`Switched underlying to ${cleanUnd}: basket emptied`, "info", 2000);
}
window.onBasketUnderlyingChange = onBasketUnderlyingChange;

function onSwitchSavedBasket(newBasketId) {
    if (!newBasketId || newBasketId === window._activeBasketId) return;
    // Auto-save current basket first so modifications are not lost
    saveCurrentBasket(false);

    const list = getSavedBasketsList();
    const target = list.find(b => b.id === newBasketId);
    if (!target) return;

    window._activeBasketId = target.id;
    window._basketOrders = Array.isArray(target.orders) ? JSON.parse(JSON.stringify(target.orders)) : [];
    localStorage.setItem(STORAGE_LAST_BASKET_ID, target.id);

    const nameEl = document.getElementById("basketModalName");
    if (nameEl) nameEl.innerText = target.name || "DEFAULT BASKET";

    const undSel = document.getElementById("basketUnderlyingSelect");
    if (undSel) {
        undSel.value = target.underlying || "NIFTY";
        const searchInput = document.getElementById("basketSearchInput");
        if (searchInput) searchInput.placeholder = `Search ${undSel.value} strikes (e.g. 23500, CE, PE)...`;
    }

    syncBasketDropdown();
    renderBasketOrdersTable();
    updateBasketBadges();
    updateBasketMarginCalculation();
    showStraddleToast(`Loaded basket: ${target.name}`, "info", 1500);
}

function createNewBasket() {
    // Save current active basket first
    saveCurrentBasket(false);

    const list = getSavedBasketsList();
    const newIdx = list.length + 1;
    const newId = "basket_" + Date.now();
    const newName = `BASKET ${newIdx}`;
    const undSel = document.getElementById("basketUnderlyingSelect");
    const currentUnd = undSel?.value || "NIFTY";

    const newBasket = {
        id: newId,
        name: newName,
        underlying: currentUnd,
        orders: [],
        updated_at: Date.now()
    };

    list.push(newBasket);
    saveBasketsList(list);

    window._activeBasketId = newId;
    window._basketOrders = [];
    localStorage.setItem(STORAGE_LAST_BASKET_ID, newId);

    const nameEl = document.getElementById("basketModalName");
    if (nameEl) nameEl.innerText = newName;

    syncBasketDropdown();
    renderBasketOrdersTable();
    updateBasketBadges();
    updateBasketMarginCalculation();
    showStraddleToast(`Created "${newName}". Add legs or rename it!`, "success", 2500);

    // Focus name for immediate user convenience
    if (nameEl) {
        setTimeout(() => {
            nameEl.focus();
        }, 100);
    }
}

function saveCurrentBasket(notify = false) {
    const list = getSavedBasketsList();
    let current = list.find(b => b.id === window._activeBasketId);
    const nameEl = document.getElementById("basketModalName");
    const resolvedName = (nameEl ? nameEl.innerText.trim().toUpperCase() : "") || "MY BASKET";
    const undSel = document.getElementById("basketUnderlyingSelect");
    const resolvedUnd = undSel?.value || current?.underlying || "NIFTY";

    if (!current) {
        current = {
            id: window._activeBasketId || ("basket_" + Date.now()),
            name: resolvedName,
            underlying: resolvedUnd,
            orders: window._basketOrders || [],
            updated_at: Date.now()
        };
        list.push(current);
        window._activeBasketId = current.id;
    } else {
        current.name = resolvedName;
        current.underlying = resolvedUnd;
        current.orders = window._basketOrders || [];
        current.updated_at = Date.now();
    }

    saveBasketsList(list);
    try {
        localStorage.setItem(STORAGE_LAST_BASKET_ID, window._activeBasketId);
        localStorage.setItem(STORAGE_LEGACY_ORDERS, JSON.stringify(window._basketOrders));
        localStorage.setItem(STORAGE_LEGACY_NAME, resolvedName);
    } catch (e) { }

    syncBasketDropdown();
    if (notify) {
        showStraddleToast(`💾 Basket "${resolvedName}" saved successfully!`, "success", 2000);
    }
}

function deleteCurrentBasket() {
    const list = getSavedBasketsList();
    if (list.length <= 1) {
        if (confirm("This is your only basket. Do you want to clear all orders in it?")) {
            window._basketOrders = [];
            saveCurrentBasket(false);
            renderBasketOrdersTable();
            updateBasketBadges();
            updateBasketMarginCalculation();
            showStraddleToast("Cleared all legs from basket.", "info", 2000);
        }
        return;
    }

    const current = list.find(b => b.id === window._activeBasketId);
    const basketName = current ? current.name : "this basket";
    if (!confirm(`Are you sure you want to delete "${basketName}"?`)) {
        return;
    }

    const filtered = list.filter(b => b.id !== window._activeBasketId);
    saveBasketsList(filtered);

    // Switch to first remaining basket
    const nextBasket = filtered[0];
    window._activeBasketId = nextBasket.id;
    window._basketOrders = Array.isArray(nextBasket.orders) ? JSON.parse(JSON.stringify(nextBasket.orders)) : [];
    localStorage.setItem(STORAGE_LAST_BASKET_ID, nextBasket.id);

    const nameEl = document.getElementById("basketModalName");
    if (nameEl) nameEl.innerText = nextBasket.name;

    syncBasketDropdown();
    renderBasketOrdersTable();
    updateBasketBadges();
    updateBasketMarginCalculation();
    showStraddleToast(`Deleted basket. Switched to "${nextBasket.name}".`, "info", 2000);
}

function onBasketNameManualEdit(newName) {
    const clean = (newName || "").trim().toUpperCase() || "MY BASKET";
    const nameEl = document.getElementById("basketModalName");
    if (nameEl) nameEl.innerText = clean;
    saveCurrentBasket(false);
}

function persistActiveBasketState() {
    saveCurrentBasket(false);
}

// Load default / last active basket on script execution
try {
    loadActiveBasketFromStorage();
} catch (e) { }

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
    const topbarBasketBadge = document.getElementById("topbarBasketCountBadge");
    if (topbarBasketBadge) {
        topbarBasketBadge.textContent = count;
        topbarBasketBadge.style.display = count > 0 ? "inline-block" : "none";
    }
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
                } catch (e) { }
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
    loadActiveBasketFromStorage();
    const multInput = document.getElementById("basketMultiplierInput");
    if (multInput) multInput.value = window._basketMultiplier || 1;
    updateBasketBadges();

    // On mobile, collapse Search & Presets header by default when legs exist so 4+ legs fit instantly
    if (window.innerWidth <= 768 && window._basketOrders && window._basketOrders.length > 0) {
        const row = document.getElementById("basketHeaderRow2");
        const btn = document.getElementById("btnToggleBasketSearchHeader");
        if (row) row.style.display = "none";
        if (btn) {
            btn.innerHTML = `<span>🔍</span> Search & Presets ▾`;
            btn.style.borderColor = "#475569";
            btn.style.color = "#cbd5e1";
        }
    }

    renderBasketOrdersTable();
    updateBasketMarginCalculation();
    loadBasketExpiryFilterOptions();
    startBasketQuoteTicker();
}

function toggleBasketSearchHeader() {
    const row = document.getElementById("basketHeaderRow2");
    const btn = document.getElementById("btnToggleBasketSearchHeader");
    if (!row) return;
    const isHidden = (row.style.display === "none");
    row.style.display = isHidden ? "flex" : "none";
    if (btn) {
        btn.innerHTML = isHidden ? `<span>🔍</span> Search & Presets ▴` : `<span>🔍</span> Search & Presets ▾`;
        btn.style.borderColor = isHidden ? "#38bdf8" : "#475569";
        btn.style.color = isHidden ? "#38bdf8" : "#cbd5e1";
    }
}
window.toggleBasketSearchHeader = toggleBasketSearchHeader;

function closeBasketOrderModal() {
    const modal = document.getElementById("modalBasketOrders");
    if (modal) modal.style.display = "none";
    stopBasketQuoteTicker();
    clearBasketSearch();
}

function addLegToBasket(sym, tok, strike, optType, side = "SELL", ltp = 0, lotSize = 0, exp = "", product = "CF") {
    const undSel = document.getElementById("basketUnderlyingSelect");
    const und = undSel?.value || window._straddleState?.optionChainUnderlying || "NIFTY";
    const resolvedLot = Number(lotSize) > 0 ? Number(lotSize) : getLotSizeForUnderlying(und);
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
    persistActiveBasketOrders();
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
        updateBasketAutoName();
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
                <td style="text-align:center; padding:6px 4px; width:34px;">
                    <input type="checkbox" ${o.selected ? "checked" : ""} onchange="toggleBasketOrderSelected(${idx}, this.checked)">
                </td>

                <!-- Scrip & Expiry (Merged for compact layout, no horizontal scrollbar) -->
                <td style="padding:6px 8px; min-width:180px;">
                    <div style="display:flex; align-items:center; gap:6px;">
                        <span class="basket-row-sym" style="font-weight:800; color:#f8fafc; font-size:12.5px; font-family:var(--font-mono);">${formatSymbolColorCoded(o.symbol)}</span>
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
                <td style="text-align:center; padding:6px 4px; width:54px;">
                    <button type="button" class="basket-action-btn ${actionClass}" title="Click to toggle BUY / SELL" onclick="toggleBasketOrderSide(${idx})" style="${actionStyle} width:32px; height:26px; border-radius:6px; font-weight:900; font-size:12px; cursor:pointer; display:inline-flex; align-items:center; justify-content:center;">
                        ${actionLabel}
                    </button>
                </td>

                <!-- Strike Price (Stepper) -->
                <td style="text-align:center; padding:6px 4px; width:115px;">
                    <div class="basket-stepper-box" style="display:inline-flex; align-items:center; background:#0c101a; border:1px solid #334155; border-radius:6px; overflow:hidden;">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderStrike(${idx}, -1)">▼</button>
                        <input type="number" class="basket-input-num" style="width:58px; background:transparent; border:none; text-align:center; color:#f8fafc; font-weight:800; font-family:var(--font-mono); font-size:12px; outline:none; -moz-appearance:textfield;" value="${o.strike}" onchange="onBasketOrderStrikeChange(${idx}, this.value)">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderStrike(${idx}, 1)">▲</button>
                    </div>
                </td>

                <!-- CE / PE Toggle -->
                <td style="text-align:center; padding:6px 4px; width:56px;">
                    <button type="button" class="basket-badge-type ${typeClass}" onclick="toggleBasketOrderOptType(${idx})" style="${typeStyle} padding:3px 8px; border-radius:6px; font-weight:800; font-size:11px; cursor:pointer; display:inline-flex; align-items:center; justify-content:center;">
                        ${o.opt_type}
                    </button>
                </td>

                <!-- Product (CF / MIS / NRML) -->
                <td style="text-align:center; padding:6px 4px; width:72px;">
                    <select onchange="onBasketOrderProductChange(${idx}, this.value)" style="background:#0b1329; border:1px solid #2563eb; color:#60a5fa; font-size:11px; font-weight:800; border-radius:5px; padding:3px 6px; outline:none; cursor:pointer;">
                        <option value="CF" ${o.product === 'CF' ? 'selected' : ''}>CF</option>
                        <option value="MIS" ${o.product === 'MIS' ? 'selected' : ''}>MIS</option>
                        <option value="NRML" ${o.product === 'NRML' ? 'selected' : ''}>NRML</option>
                    </select>
                </td>

                <!-- Quantity / Lots (Stepper) -->
                <td style="text-align:center; padding:6px 4px; width:96px;">
                    <div class="basket-stepper-box" style="display:inline-flex; align-items:center; background:#0c101a; border:1px solid #334155; border-radius:6px; overflow:hidden;">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderLots(${idx}, -1)">▼</button>
                        <input type="number" class="basket-input-num" min="1" style="width:38px; background:transparent; border:none; text-align:center; color:#f8fafc; font-weight:800; font-family:var(--font-mono); font-size:12px; outline:none; -moz-appearance:textfield;" value="${o.lots}" onchange="onBasketOrderLotsChange(${idx}, this.value)">
                        <button type="button" class="basket-stepper-btn" style="background:#1e293b; color:#cbd5e1; border:none; padding:3px 6px; cursor:pointer; font-size:10px;" onclick="stepBasketOrderLots(${idx}, 1)">▲</button>
                    </div>
                    <div class="basket-effective-qty-label" style="font-size:10.5px; color:#60a5fa; margin-top:2px; font-family:var(--font-mono); font-weight:700;">
                        ${effectiveQty} qty
                    </div>
                </td>

                <!-- Price / Price Type (LIMIT / MARKET) -->
                <td style="padding:6px 8px; width:180px;">
                    <div style="display:flex; align-items:center; gap:6px;">
                        <input type="number" step="0.05" min="0.05" class="basket-input-num basket-price-input" data-saved-price="${o.price || o.ltp || 0.05}" style="width:68px; text-align:right; padding-right:5px; background:#0c101a; border:1px solid #1e293b; border-radius:4px; font-weight:700; font-size:11px; ${isMarket ? 'opacity:0.6;' : ''}" value="${isMarket ? '0.00' : Number(o.price || 0.05).toFixed(2)}" ${isMarket ? 'disabled placeholder="MKT"' : ''} onchange="onBasketOrderPriceChange(${idx}, this.value)">
                        
                        <!-- Limit / Market Pill Toggle Switch -->
                        <div style="display:inline-flex; align-items:center; background:#070a13; border:1px solid #1e293b; border-radius:12px; padding:2px; cursor:pointer; flex-shrink:0; white-space:nowrap;" onclick="toggleBasketOrderType(${idx})" title="Click to toggle LIMIT / MARKET">
                            <span class="basket-pill-limit" style="font-size:9.5px; font-weight:800; padding:2px 6px; border-radius:10px; white-space:nowrap; transition:all 0.15s ease; ${!isMarket ? 'background:#2563eb; color:#fff;' : 'background:transparent; color:#64748b;'}">LIMIT</span>
                            <span class="basket-pill-market" style="font-size:9.5px; font-weight:800; padding:2px 6px; border-radius:10px; white-space:nowrap; transition:all 0.15s ease; ${isMarket ? 'background:#ea580c; color:#fff;' : 'background:transparent; color:#64748b;'}">MARKET</span>
                        </div>
                    </div>
                </td>

                <!-- Delete -->
                <td style="text-align:center; padding:6px 4px; width:36px;">
                    <button type="button" onclick="removeBasketOrder(${idx})" style="background:none; border:none; color:#ef4444; font-size:13px; cursor:pointer; padding:2px;" title="Delete order">✕</button>
                </td>
            </tr>
        `;
    });

    tbody.innerHTML = rowsHtml;
    updateBasketBadges();
    updateBasketAutoName();
}

function toggleSelectAllBasketOrders(checked) {
    window._basketOrders.forEach(o => o.selected = checked);
    persistActiveBasketOrders();
    renderBasketOrdersTable();
    updateBasketMarginCalculation();
}

function toggleBasketOrderSelected(idx, checked) {
    if (window._basketOrders[idx]) {
        window._basketOrders[idx].selected = checked;
        persistActiveBasketOrders();
        updateBasketBadges();
        updateBasketMarginCalculation();
    }
}

function toggleBasketOrderSide(idx) {
    if (window._basketOrders[idx]) {
        window._basketOrders[idx].side = (window._basketOrders[idx].side === "BUY") ? "SELL" : "BUY";
        persistActiveBasketOrders();
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

                persistActiveBasketOrders();

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
        persistActiveBasketOrders();
        renderBasketOrdersTable();
        fetchQuoteForBasketOrder(idx);
    }
}

function stepBasketOrderStrike(idx, step) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        const stepSize = getStrikeStepForUnderlying(item.underlying);
        item.strike = Math.max(0, item.strike + (step * stepSize));
        item.symbol = `${item.underlying} ${item.strike} ${item.opt_type}`;
        persistActiveBasketOrders();
        renderBasketOrdersTable();
        fetchQuoteForBasketOrder(idx);
    }
}

function onBasketOrderStrikeChange(idx, val) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        item.strike = Number(val) || item.strike;
        item.symbol = `${item.underlying} ${item.strike} ${item.opt_type}`;
        persistActiveBasketOrders();
        renderBasketOrdersTable();
        fetchQuoteForBasketOrder(idx);
    }
}

function stepBasketOrderLots(idx, step) {
    if (window._basketOrders[idx]) {
        const item = window._basketOrders[idx];
        item.lots = Math.max(1, item.lots + step);
        item.quantity = item.lots * item.lot_size;
        persistActiveBasketOrders();

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
        persistActiveBasketOrders();

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
        persistActiveBasketOrders();
    }
}

function toggleBasketOrderType(idx) {
    if (!window._basketOrders[idx]) return;
    const item = window._basketOrders[idx];
    const newType = (item.order_type === "LIMIT") ? "MARKET" : "LIMIT";
    item.order_type = newType;
    persistActiveBasketOrders();

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
        persistActiveBasketOrders();
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
        persistActiveBasketOrders();
        renderBasketOrdersTable();
        updateBasketMarginCalculation();
    }
}

function removeBasketOrder(idx) {
    if (window._basketOrders[idx]) {
        window._basketOrders.splice(idx, 1);
        persistActiveBasketOrders();
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
    } catch (e) { }
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
    } catch (e) { }
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
        const finalEl = document.getElementById("basketFinalMarginText");
        const availEl = document.getElementById("basketAvailableMarginText");
        const chargesEl = document.getElementById("basketTotalChargesText");
        const autoLotsEl = document.getElementById("basketAutoLotsCount");
        const shortfallEl = document.getElementById("basketMarginShortfallWarning");

        const selected = window._basketOrders.filter(o => o.selected);
        if (selected.length === 0) {
            if (reqEl) reqEl.textContent = "0.00";
            if (finalEl) finalEl.textContent = "0.00";
            if (chargesEl) chargesEl.textContent = "0.00";
            if (autoLotsEl) autoLotsEl.textContent = "1";
            if (shortfallEl) shortfallEl.style.display = "none";
            populateBasketChargesBreakdown(null);
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
                const reqVal = Number(data.required_margin || 0);
                const finalVal = Number(data.final_margin || reqVal);
                const availVal = Number(data.available_margin || 0);

                if (reqEl) reqEl.textContent = reqVal.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                if (finalEl) finalEl.textContent = finalVal.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                if (availEl) availEl.textContent = availVal.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
                if (chargesEl) chargesEl.textContent = Number(data.total_charges || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

                // Auto Lots calculation
                const maxSafeLots = Math.max(1, parseInt(data.max_tradeable_lots || 1, 10));
                window._basketMaxSafeLots = maxSafeLots;
                if (autoLotsEl) autoLotsEl.textContent = maxSafeLots;

                // Check margin shortfall against available funds
                if (shortfallEl) {
                    if (reqVal > availVal && availVal > 0) {
                        shortfallEl.style.display = "flex";
                    } else {
                        shortfallEl.style.display = "none";
                    }
                }

                // Populate Angel One exact charges breakdown popover
                if (data.charges_breakdown) {
                    populateBasketChargesBreakdown(data.charges_breakdown);
                }
            }
        } catch (e) {
            console.warn("Margin calculation update error:", e);
        }
    }, 150);
}

function autoCalculateBasketLots() {
    const safeLots = window._basketMaxSafeLots || 1;
    window._basketMultiplier = safeLots;
    const input = document.getElementById("basketMultiplierInput");
    if (input) input.value = safeLots;
    onBasketMultiplierChange(safeLots);
    showStraddleToast(`⚡ Auto Lots set to ${safeLots} lots based on Available Margin.`, "success", 2500);
}
window.autoCalculateBasketLots = autoCalculateBasketLots;

function populateBasketChargesBreakdown(b) {
    if (!b) return;
    const setT = (id, val) => {
        const el = document.getElementById(id);
        if (el) el.textContent = Number(val || 0).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    };
    setT("chgBreakdownBrokerage", b.brokerage);
    setT("chgBreakdownExternal", b.external_charges);
    setT("chgBreakdownExchange", b.exchange_txn_charges);
    setT("chgBreakdownStamp", b.stamp_duty);
    setT("chgBreakdownIpft", b.ipft_charges);
    setT("chgBreakdownSebi", b.sebi_fees);
    setT("chgBreakdownTaxes", b.taxes);
    setT("chgBreakdownStt", b.stt);
    setT("chgBreakdownGst", b.gst);
    setT("chgBreakdownTotal", b.total_charges);
}
window.populateBasketChargesBreakdown = populateBasketChargesBreakdown;

let _chargesPopoverTimer = null;
function showBasketChargesPopover() {
    clearTimeout(_chargesPopoverTimer);
    const pop = document.getElementById("basketChargesPopover");
    if (pop) pop.style.display = "block";
}
window.showBasketChargesPopover = showBasketChargesPopover;

function hideBasketChargesPopoverDelayed() {
    _chargesPopoverTimer = setTimeout(() => {
        const pop = document.getElementById("basketChargesPopover");
        if (pop) pop.style.display = "none";
    }, 300);
}
window.hideBasketChargesPopoverDelayed = hideBasketChargesPopoverDelayed;

function clearBasketChargesPopoverTimer() {
    clearTimeout(_chargesPopoverTimer);
}
window.clearBasketChargesPopoverTimer = clearBasketChargesPopoverTimer;

function toggleBasketChargesPopover(e) {
    if (e) e.stopPropagation();
    const pop = document.getElementById("basketChargesPopover");
    if (!pop) return;
    pop.style.display = (pop.style.display === "block") ? "none" : "block";
}
window.toggleBasketChargesPopover = toggleBasketChargesPopover;

// Close popover when clicking anywhere outside
document.addEventListener("click", function (e) {
    const pop = document.getElementById("basketChargesPopover");
    const icon = document.getElementById("basketChargesInfoIcon");
    const text = document.getElementById("basketTotalChargesText");
    if (pop && pop.style.display === "block") {
        if (!pop.contains(e.target) && e.target !== icon && e.target !== text && !text?.contains(e.target)) {
            pop.style.display = "none";
        }
    }
});

// =========================================================================
// BASKET AUTO-NAMING SYSTEM (Matches prompt: "ATM Straddle - 20OTM Hedge")
// =========================================================================

window._basketNameIsManual = false;

function generateBasketAutoName() {
    const orders = (window._basketOrders || []).filter(o => o.selected);
    if (!orders || orders.length === 0) return "MY BASKET";

    const und = orders[0].underlying || "NIFTY";
    const step = getStrikeStepForUnderlying(und);

    const sellLegs = orders.filter(o => String(o.side).toUpperCase().startsWith("S"));
    const buyLegs = orders.filter(o => String(o.side).toUpperCase().startsWith("B"));

    const sellCE = sellLegs.filter(o => o.opt_type === "CE");
    const sellPE = sellLegs.filter(o => o.opt_type === "PE");
    const buyCE = buyLegs.filter(o => o.opt_type === "CE");
    const buyPE = buyLegs.filter(o => o.opt_type === "PE");

    // Case 1: Short Straddle (1 CE Sell + 1 PE Sell at same strike)
    if (sellCE.length === 1 && sellPE.length === 1 && sellCE[0].strike === sellPE[0].strike) {
        const atm = sellCE[0].strike;
        // Check for OTM hedges (Iron Butterfly / Hedged Straddle)
        if (buyCE.length === 1 && buyPE.length === 1) {
            const ceOffset = buyCE[0].strike - atm;
            const peOffset = atm - buyPE[0].strike;
            const ceStrikes = Math.round(ceOffset / step);
            const peStrikes = Math.round(peOffset / step);

            if (ceStrikes > 0 && peStrikes > 0) {
                if (ceStrikes === peStrikes) {
                    return `ATM STRADDLE - ${ceStrikes}OTM HEDGE`;
                } else {
                    return `ATM STRADDLE - ${ceStrikes}CE/${peStrikes}PE HEDGE`;
                }
            }
        } else if (buyCE.length === 1 && buyPE.length === 0) {
            const ceOffset = buyCE[0].strike - atm;
            const ceStrikes = Math.round(ceOffset / step);
            return `ATM STRADDLE - ${ceStrikes}OTM CE HEDGE`;
        } else if (buyPE.length === 1 && buyCE.length === 0) {
            const peOffset = atm - buyPE[0].strike;
            const peStrikes = Math.round(peOffset / step);
            return `ATM STRADDLE - ${peStrikes}OTM PE HEDGE`;
        }
        return `ATM STRADDLE (${atm})`;
    }

    // Case 2: Short Strangle (1 CE Sell + 1 PE Sell at different strikes)
    if (sellCE.length === 1 && sellPE.length === 1 && sellCE[0].strike !== sellPE[0].strike) {
        const ceStk = sellCE[0].strike;
        const peStk = sellPE[0].strike;
        if (buyCE.length === 1 && buyPE.length === 1) {
            return `IRON CONDOR (${ceStk}CE/${peStk}PE)`;
        }
        return `STRANGLE (${ceStk}CE/${peStk}PE)`;
    }

    // Case 3: Spreads (1 Buy + 1 Sell)
    if (orders.length === 2 && buyLegs.length === 1 && sellLegs.length === 1) {
        const b = buyLegs[0];
        const s = sellLegs[0];
        if (b.opt_type === "CE" && s.opt_type === "CE") {
            return b.strike < s.strike ? `BULL CALL SPREAD (${b.strike}/${s.strike})` : `BEAR CALL SPREAD (${s.strike}/${b.strike})`;
        }
        if (b.opt_type === "PE" && s.opt_type === "PE") {
            return b.strike > s.strike ? `BEAR PUT SPREAD (${b.strike}/${s.strike})` : `BULL PUT SPREAD (${s.strike}/${b.strike})`;
        }
    }

    // Case 4: Single Leg
    if (orders.length === 1) {
        const o = orders[0];
        return `${und} ${o.strike} ${o.opt_type} ${o.side}`;
    }

    // Generic Multi-Leg Basket
    return `${und} ${orders.length}-LEG BASKET`;
}
window.generateBasketAutoName = generateBasketAutoName;

function updateBasketAutoName() {
    const nameEl = document.getElementById("basketModalName");
    const resetBtn = document.getElementById("btnResetBasketName");
    if (!nameEl) return;

    const list = getSavedBasketsList();
    const current = list.find(b => b.id === window._activeBasketId);
    if (current && current.name && current.name !== "DEFAULT BASKET" && !current.name.startsWith("BASKET ")) {
        nameEl.innerText = current.name;
        if (resetBtn) resetBtn.style.display = "inline-block";
        return;
    }

    if (window._basketNameIsManual) {
        if (resetBtn) resetBtn.style.display = "inline-block";
        return;
    }

    const autoName = current?.name || generateBasketAutoName();
    nameEl.innerText = autoName;
    if (resetBtn) resetBtn.style.display = "none";
}
window.updateBasketAutoName = updateBasketAutoName;

function onBasketNameManualEdit(newVal) {
    const clean = (newVal || "").trim().toUpperCase() || "MY BASKET";
    window._basketNameIsManual = true;
    const nameEl = document.getElementById("basketModalName");
    if (nameEl) nameEl.innerText = clean;
    const resetBtn = document.getElementById("btnResetBasketName");
    if (resetBtn) resetBtn.style.display = "inline-block";
    saveCurrentBasket(false);
}
window.onBasketNameManualEdit = onBasketNameManualEdit;

function resetBasketAutoName() {
    window._basketNameIsManual = false;
    const resetBtn = document.getElementById("btnResetBasketName");
    if (resetBtn) resetBtn.style.display = "none";
    updateBasketAutoName();
    showStraddleToast("Basket name reset to auto-generated strategy name", "info", 1500);
}
window.resetBasketAutoName = resetBasketAutoName;

// =========================================================================
// STRATEGY BUILDER MULTIPLIER MODE & CUSHION MARGIN HANDLERS
// =========================================================================

// Note: onStrategyUnderlyingChange is consolidated and defined in the Strategy Builder section.

function setFormMultiplierMode(mode, syncToServer = true) {
    const isManual = (mode === "MANUAL");
    const btnMan = document.getElementById("btnMultModeManual");
    const btnAuto = document.getElementById("btnMultModeAuto");
    const grpMan = document.getElementById("grpManualMultiplier");
    const grpAuto = document.getElementById("grpAutoMultiplier");

    if (btnMan) {
        btnMan.style.background = isManual ? "#2563eb" : "#1e293b";
        btnMan.style.color = isManual ? "#fff" : "#94a3b8";
    }
    if (btnAuto) {
        btnAuto.style.background = !isManual ? "#059669" : "#1e293b";
        btnAuto.style.color = !isManual ? "#fff" : "#94a3b8";
    }
    if (grpMan) grpMan.style.display = isManual ? "" : "none";
    if (grpAuto) grpAuto.style.display = !isManual ? "" : "none";

    updateFormAutoMultiplierPreview();
    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
        scheduleAutoMultiplierLiveRecalc();
    }
    if (typeof onAutoSizingFieldChange === "function") {
        onAutoSizingFieldChange();
    }

    const activeStrat = window._straddleState?.activeStrategy;
    if (activeStrat) {
        activeStrat.multiplier_type = mode;
        const sid = activeStrat.strategy_id;
        if (sid && syncToServer) {
            window._pendingMultiplierModes = window._pendingMultiplierModes || {};
            window._pendingMultiplierModes[String(sid).trim()] = { mode: mode, timestamp: Date.now() };
        }
        if (sid && Array.isArray(window._straddleState?.allStrategies)) {
            const found = window._straddleState.allStrategies.find(s => s.strategy_id === sid);
            if (found) {
                found.multiplier_type = mode;
            }
        }
        renderStraddleAlgosTable(true);

        if (syncToServer && activeStrat.strategy_id) {
            fetch("/api/straddle/strategy/update_multiplier", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    strategy_id: activeStrat.strategy_id,
                    multiplier_type: mode
                })
            }).then(r => r.json()).then(res => {
                if (window._pendingMultiplierModes) delete window._pendingMultiplierModes[String(activeStrat.strategy_id).trim()];
                if (res.success && res.strategy) {
                    Object.assign(activeStrat, res.strategy);
                    renderStraddleAlgosTable(true);
                }
            }).catch(e => console.warn(e));
        }
    }
}
window.setFormMultiplierMode = setFormMultiplierMode;

let _autoMultiplierRecalcTimer = null;
function scheduleAutoMultiplierLiveRecalc() {
    clearTimeout(_autoMultiplierRecalcTimer);
    _autoMultiplierRecalcTimer = setTimeout(async () => {
        try {
            const badge = document.getElementById("cfgAutoMultBadge");
            if (!badge) return;

            let strat = null;
            if (typeof serializeCurrentStrategySettings === "function") {
                try {
                    strat = serializeCurrentStrategySettings();
                } catch (e) {
                    strat = window._straddleState?.activeStrategy;
                }
            } else {
                strat = window._straddleState?.activeStrategy;
            }
            if (!strat) return;

            const und = (strat.underlying || document.getElementById("cfgStrategyUnderlying")?.value || "NIFTY").toUpperCase();
            const step = (und === "BANKNIFTY") ? 100 : ((und === "MIDCPNIFTY") ? 25 : 50);

            let spot = 25300.0;
            if (window._straddleState?.underlying_spot) {
                spot = parseFloat(window._straddleState.underlying_spot) || spot;
            }
            const atmStrike = Math.round(spot / step) * step;

            const simOrders = [];
            const legs = strat.legs || [];
            const dynHedgeChecked = strat.toggles?.dynamic_hedging || strat.dynamic_hedging || (document.getElementById("cfgToggleDynamicHedges") ? document.getElementById("cfgToggleDynamicHedges").checked : true);

            legs.forEach(leg => {
                const side = (leg.side || "Sell").toUpperCase();
                const optType = (leg.option_type || leg.type || "CE").toUpperCase();
                let strike = atmStrike;
                const sel = String(leg.strike_selection || "ATM STRIKE").toUpperCase();
                if (sel.includes("OTM") || sel.includes("ITM")) {
                    const match = sel.match(/(OTM|ITM)\s*(\d+)/i);
                    const offset = match ? parseInt(match[2], 10) : 0;
                    if (sel.includes("OTM")) {
                        strike = (optType === "CE") ? atmStrike + (offset * step) : atmStrike - (offset * step);
                    } else {
                        strike = (optType === "CE") ? atmStrike - (offset * step) : atmStrike + (offset * step);
                    }
                }

                const estPrice = Math.abs(strike - atmStrike) <= step ? 180.0 : (Math.abs(strike - atmStrike) <= 5 * step ? 50.0 : 10.0);
                const legLots = Math.max(1, parseInt(leg.lots, 10) || 1);
                simOrders.push({
                    underlying: und,
                    strike: strike,
                    opt_type: optType,
                    side: side,
                    lots: legLots,
                    price: estPrice,
                    selected: true
                });

                if (side === "SELL") {
                    const hCfg = leg.associated_hedge || {};
                    const isHedgeOn = dynHedgeChecked || !!hCfg.enabled;
                    if (isHedgeOn) {
                        const initOtm = parseInt(hCfg.initial_otm, 10) || 20;
                        const hStrike = (optType === "CE") ? (strike + initOtm * step) : (strike - initOtm * step);
                        simOrders.push({
                            underlying: und,
                            strike: hStrike,
                            opt_type: optType,
                            side: "BUY",
                            lots: legLots,
                            price: 6.0,
                            selected: true
                        });
                    }
                }
            });

            if (simOrders.length === 0) return;

            const resp = await fetch("/api/straddle/basket/margin", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    orders: simOrders,
                    multiplier: 1
                })
            });
            const data = await resp.json();
            if (data.success && data.required_margin > 0) {
                window._formSimulatedMarginPerLot = Number(data.required_margin);
                window._formSimulatedMarginUnderlying = und;
                updateFormAutoMultiplierPreview(Number(data.required_margin));
            }
        } catch (e) {
            console.warn("Live auto multiplier recalc error:", e);
        }
    }, 150);
}
window.scheduleAutoMultiplierLiveRecalc = scheduleAutoMultiplierLiveRecalc;

function updateFormAutoMultiplierPreview(overrideMargin) {
    const cushionVal = parseFloat(document.getElementById("cfgCushionMargin")?.value) || 0;
    const badge = document.getElementById("cfgAutoMultBadge");
    if (!badge) return;

    const strat = window._straddleState?.activeStrategy;
    const und = (document.getElementById("cfgStrategyUnderlying")?.value || strat?.underlying || "NIFTY").toUpperCase();
    const defaultMarginTable = {
        "NIFTY": 130750.0,
        "BANKNIFTY": 140000.0,
        "FINNIFTY": 120000.0,
        "MIDCPNIFTY": 110000.0,
        "SENSEX": 135000.0,
        "BANKEX": 135000.0,
        "CRUDEOIL": 240000.0,
        "CRUDEOILM": 24000.0,
        "NATURALGAS": 175000.0,
        "NATGASMINI": 45000.0,
        "GOLD": 450000.0,
        "GOLDM": 45000.0,
        "SILVER": 320000.0,
        "SILVERM": 65000.0,
        "COPPER": 220000.0
    };

    const dynHedgeChecked = document.getElementById("cfgToggleDynamicHedges") ? document.getElementById("cfgToggleDynamicHedges").checked : (strat?.dynamic_hedging || strat?.toggles?.dynamic_hedging);
    const hasLegHedges = !!document.querySelector(".leg-hedge-enabled:checked");
    const isHedged = (dynHedgeChecked !== false && hasLegHedges) || !!dynHedgeChecked;

    let defaultMargin = defaultMarginTable[und] || 130750.0;
    if (!isHedged && und === "NIFTY") {
        defaultMargin = 145492.0;
    }

    let brokerMargin = defaultMargin;
    if (typeof overrideMargin === "number" && overrideMargin > 0) {
        brokerMargin = overrideMargin;
        window._formSimulatedMarginPerLot = overrideMargin;
        window._formSimulatedMarginUnderlying = und;
    } else if (window._formSimulatedMarginPerLot && window._formSimulatedMarginUnderlying === und) {
        brokerMargin = window._formSimulatedMarginPerLot;
    } else if (strat && strat.broker_margin_per_lot && (strat.underlying || "NIFTY").toUpperCase() === und) {
        brokerMargin = strat.broker_margin_per_lot;
    }

    const totalPerLot = brokerMargin + cushionVal;

    let avail = 0.0;
    if (window._straddleState?.margin?.available_margin !== undefined && window._straddleState?.margin?.available_margin !== null) {
        avail = parseFloat(window._straddleState.margin.available_margin);
    } else if (window._straddleMargin?.available_margin !== undefined && window._straddleMargin?.available_margin !== null) {
        avail = parseFloat(window._straddleMargin.available_margin);
    } else if (window._straddleState?.margin?.available_cash !== undefined && window._straddleState?.margin?.available_cash !== null) {
        avail = parseFloat(window._straddleState.margin.available_cash);
    }

    const lots = totalPerLot > 0 ? Math.floor(avail / totalPerLot) : 0;

    if (lots > 0) {
        badge.innerHTML = `⚡ Auto: <b>${lots} Lots</b> (₹${(totalPerLot / 100000).toFixed(2)}L/lot)`;
        badge.style.color = "#38bdf8";
        badge.style.background = "rgba(56, 189, 248, 0.1)";
        badge.title = `Broker Required: ₹${brokerMargin.toLocaleString('en-IN', { maximumFractionDigits: 0 })} + Cushion: ₹${cushionVal.toLocaleString('en-IN', { maximumFractionDigits: 0 })} = ₹${totalPerLot.toLocaleString('en-IN', { maximumFractionDigits: 0 })} / lot\nLive Available Capital: ₹${avail.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} ➔ ${lots} Lots`;
    } else {
        badge.innerHTML = `⚡ Auto: <b>0 Lots</b> (₹${(totalPerLot / 100000).toFixed(2)}L/lot)`;
        badge.style.color = "#f87171";
        badge.style.background = "rgba(248, 113, 113, 0.1)";
        badge.title = `Live Available Capital (₹${avail.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}) is less than Required Margin per lot (₹${totalPerLot.toLocaleString('en-IN', { maximumFractionDigits: 0 })}). Minimum ₹${totalPerLot.toLocaleString('en-IN', { maximumFractionDigits: 0 })} needed for 1 lot.`;
    }
}
window.updateFormAutoMultiplierPreview = updateFormAutoMultiplierPreview;

function onFormManualMultiplierChange(val) {
    const activeStrat = window._straddleState?.activeStrategy;
    if (activeStrat && activeStrat.strategy_id) {
        onChangeAlgoMultiplier(activeStrat.strategy_id, val);
    }
}
window.onFormManualMultiplierChange = onFormManualMultiplierChange;

function onFormCushionMarginChange(val) {
    updateFormAutoMultiplierPreview();
    if (typeof scheduleAutoMultiplierLiveRecalc === "function") {
        scheduleAutoMultiplierLiveRecalc();
    }
    const activeStrat = window._straddleState?.activeStrategy;
    if (activeStrat && activeStrat.strategy_id) {
        onChangeAlgoCushionMargin(activeStrat.strategy_id, val);
    }
}
window.onFormCushionMarginChange = onFormCushionMarginChange;

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
            persistActiveBasketOrders();

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

function analyseBasketPayoff() {
    const selected = (window._basketOrders || []).filter(o => o.selected);
    if (selected.length === 0) {
        showStraddleToast("Add and select legs in the basket to analyse strategy payoff.", "warning", 2500);
        return;
    }

    const modal = document.getElementById("modalBasketPayoff");
    if (!modal) return;

    const mult = window._basketMultiplier || 1;
    const nameEl = document.getElementById("basketModalName");
    const basketTitle = (nameEl ? nameEl.innerText.trim() : "") || "STRATEGY BASKET";

    let netPremium = 0;
    let totalSellQty = 0;
    let totalBuyQty = 0;
    let sellLegs = [];
    let buyLegs = [];

    let legsHtml = "";
    selected.forEach(o => {
        const qty = o.lots * o.lot_size * mult;
        const prc = Number(o.price || o.ltp || 0.05);
        const flow = (o.side === "SELL") ? (prc * qty) : -(prc * qty);
        netPremium += flow;

        if (o.side === "SELL") {
            totalSellQty += qty;
            sellLegs.push(o);
        } else {
            totalBuyQty += qty;
            buyLegs.push(o);
        }

        const isSell = (o.side === "SELL");
        const flowColor = isSell ? "#10b981" : "#f43f5e";
        const flowSign = isSell ? "+" : "-";

        legsHtml += `
            <tr style="border-bottom:1px solid rgba(255,255,255,0.04);">
                <td style="padding:6px 10px;">
                    <span style="font-weight:900; font-size:10.5px; padding:2px 6px; border-radius:4px; ${isSell ? 'background:#dc2626; color:#fff;' : 'background:#059669; color:#fff;'}">${o.side}</span>
                </td>
                <td style="padding:6px 10px; font-weight:700; color:#f8fafc; font-family:var(--font-mono); font-size:11px;">
                    ${o.symbol} <span style="color:#64748b; font-size:10px;">(${o.expiry_date || 'Weekly'})</span>
                </td>
                <td style="padding:6px 10px; text-align:center; font-family:var(--font-mono); color:#cbd5e1;">${o.lots * mult}</td>
                <td style="padding:6px 10px; text-align:center; font-family:var(--font-mono); color:#cbd5e1;">${qty}</td>
                <td style="padding:6px 10px; text-align:right; font-family:var(--font-mono); color:#f8fafc;">₹${prc.toFixed(2)}</td>
                <td style="padding:6px 10px; text-align:right; font-family:var(--font-mono); font-weight:800; color:${flowColor};">${flowSign}₹${Math.abs(flow).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</td>
            </tr>
        `;
    });

    const isCredit = netPremium >= 0;
    const absNetPrem = Math.abs(netPremium);

    // Subtitle
    const subEl = document.getElementById("payoffStrategySubtitle");
    if (subEl) subEl.textContent = `${basketTitle} • ${selected.length} LEGS (${mult}x)`;

    // Net Premium
    const netPremEl = document.getElementById("payoffNetPremium");
    if (netPremEl) {
        netPremEl.textContent = `₹ ${absNetPrem.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        netPremEl.style.color = isCredit ? "#10b981" : "#f43f5e";
    }
    const netBadgeEl = document.getElementById("payoffNetPremiumBadge");
    if (netBadgeEl) {
        netBadgeEl.textContent = isCredit ? "Net Credit Received" : "Net Debit Paid";
        netBadgeEl.style.color = isCredit ? "#34d399" : "#fb7185";
    }

    // Max Profit & Max Loss
    const maxProfitEl = document.getElementById("payoffMaxProfit");
    const maxLossEl = document.getElementById("payoffMaxLoss");
    const riskTypeEl = document.getElementById("payoffRiskType");

    // Check if defined risk (Iron Condor / Spreads)
    const hasDefinedWings = buyLegs.length > 0 && buyLegs.length >= sellLegs.length;
    if (isCredit) {
        if (maxProfitEl) maxProfitEl.textContent = `₹ ${absNetPrem.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        if (hasDefinedWings) {
            let maxWingSpread = 0;
            sellLegs.forEach(s => {
                const sameTypeBuys = buyLegs.filter(b => b.opt_type === s.opt_type);
                sameTypeBuys.forEach(b => {
                    const width = Math.abs(b.strike - s.strike);
                    if (width > maxWingSpread) maxWingSpread = width;
                });
            });
            const lotSz = selected[0]?.lot_size || 65;
            const maxLossVal = maxWingSpread > 0 ? Math.max(0, (maxWingSpread * lotSz * mult) - absNetPrem) : absNetPrem * 2;
            if (maxLossEl) maxLossEl.textContent = `₹ ${maxLossVal.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
            if (riskTypeEl) {
                riskTypeEl.textContent = "Defined Risk (Hedged)";
                riskTypeEl.style.color = "#34d399";
            }
        } else {
            if (maxLossEl) maxLossEl.textContent = "Undefined (Hedged with SL)";
            if (riskTypeEl) {
                riskTypeEl.textContent = "Unlimited (SL Protected)";
                riskTypeEl.style.color = "#fbbf24";
            }
        }
    } else {
        if (maxLossEl) maxLossEl.textContent = `₹ ${absNetPrem.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        if (riskTypeEl) {
            riskTypeEl.textContent = "Defined Risk (Max Loss = Premium)";
            riskTypeEl.style.color = "#34d399";
        }
        if (maxProfitEl) maxProfitEl.textContent = hasDefinedWings ? `Defined Spread Profit` : `Unlimited Upside`;
    }

    // Breakevens
    const beEl = document.getElementById("payoffBreakevens");
    if (beEl) {
        const ceLegs = selected.filter(o => o.opt_type === "CE");
        const peLegs = selected.filter(o => o.opt_type === "PE");
        if (ceLegs.length > 0 && peLegs.length > 0) {
            const avgCeStrike = ceLegs.reduce((a, b) => a + Number(b.strike), 0) / ceLegs.length;
            const avgPeStrike = peLegs.reduce((a, b) => a + Number(b.strike), 0) / peLegs.length;
            const effLotSz = selected[0]?.lot_size || 65;
            const premPerUnit = absNetPrem / (effLotSz * mult);
            const lowerBE = Math.round(avgPeStrike - premPerUnit);
            const upperBE = Math.round(avgCeStrike + premPerUnit);
            beEl.textContent = `${lowerBE} / ${upperBE}`;
        } else {
            beEl.textContent = "N/A (Single Wing)";
        }
    }

    // Capital & Margin Metrics from Active Calculation
    const reqText = document.getElementById("basketRequiredMarginText")?.textContent || "0.00";
    const finalText = document.getElementById("basketFinalMarginText")?.textContent || "0.00";
    const chgText = document.getElementById("basketTotalChargesText")?.textContent || "0.00";

    const reqVal = parseFloat(reqText.replace(/,/g, '')) || 0;
    const finalVal = parseFloat(finalText.replace(/,/g, '')) || 0;
    const savings = Math.max(0, reqVal - finalVal);

    const payoffReqEl = document.getElementById("payoffReqMargin");
    if (payoffReqEl) payoffReqEl.textContent = `₹ ${reqText}`;

    const payoffFinalEl = document.getElementById("payoffFinalMargin");
    if (payoffFinalEl) payoffFinalEl.textContent = `₹ ${finalText}`;

    const payoffChgEl = document.getElementById("payoffCharges");
    if (payoffChgEl) payoffChgEl.textContent = `₹ ${chgText}`;

    const savingsBadge = document.getElementById("payoffHedgeSavingsBadge");
    if (savingsBadge) {
        savingsBadge.textContent = savings > 0 ? `Hedge Benefit: ₹ ${savings.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} Saved` : `Standard Margin`;
    }

    const romEl = document.getElementById("payoffRom");
    if (romEl) {
        const baseMargin = finalVal > 0 ? finalVal : reqVal;
        if (baseMargin > 0 && isCredit) {
            const romPct = (absNetPrem / baseMargin) * 100;
            romEl.textContent = `${romPct.toFixed(1)}%`;
        } else {
            romEl.textContent = "--";
        }
    }

    // Legs table
    const tableBody = document.getElementById("payoffLegsTableBody");
    if (tableBody) tableBody.innerHTML = legsHtml;

    modal.style.display = "flex";
}
window.analyseBasketPayoff = analyseBasketPayoff;

function closeBasketPayoffModal() {
    const modal = document.getElementById("modalBasketPayoff");
    if (modal) modal.style.display = "none";
}
window.closeBasketPayoffModal = closeBasketPayoffModal;

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
    const undSel = document.getElementById("basketUnderlyingSelect");
    const und = undSel?.value || window._straddleState?.optionChainUnderlying || "NIFTY";

    if (_basketSearchDebounceTimer) {
        clearTimeout(_basketSearchDebounceTimer);
    }

    // Show searching spinner
    drop.innerHTML = `<div style="padding:14px; color:#38bdf8; font-size:12px; text-align:center;"><span style="display:inline-block; animation:spin 1s linear infinite;">⏳</span> Searching strikes & expiries for <strong>${q}</strong>...</div>`;
    drop.style.display = "block";

    _basketSearchDebounceTimer = setTimeout(async () => {
        try {
            const expSel = document.getElementById("basketExpiryFilterSelect");
            const expVal = expSel ? expSel.value : "ALL";
            const expParam = (expVal && expVal !== "ALL") ? `&expiry=${encodeURIComponent(expVal)}` : "";
            const resp = await fetch(`/api/straddle/search_contracts?query=${encodeURIComponent(q)}&underlying=${encodeURIComponent(und)}&limit=40${expParam}`);
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
                                <span style="font-size:11px; font-weight:700; color:#38bdf8; background:rgba(56,189,248,0.12); border:1px solid rgba(56,189,248,0.35); border-radius:4px; padding:1px 6px; font-family:var(--font-mono);">${r.expiry_date || ''}</span>
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
document.addEventListener("click", function (e) {
    const searchBox = document.getElementById("basketSearchInput")?.closest("div");
    const drop = document.getElementById("basketSearchResultsDropdown");
    if (drop && drop.style.display !== "none") {
        if (!searchBox?.contains(e.target) && !drop.contains(e.target)) {
            drop.style.display = "none";
            stopBasketSearchQuoteTicker();
        }
    }
});

// =========================================================================
// BASKET PRESETS ENGINE (BUILT-IN STRATEGIES & CUSTOM USER SAVED PRESETS)
// =========================================================================

const STORAGE_CUSTOM_PRESETS = "straddle_custom_presets";

function getCustomBasketPresets() {
    try {
        const raw = localStorage.getItem(STORAGE_CUSTOM_PRESETS);
        if (raw) return JSON.parse(raw) || [];
    } catch (e) { }
    return [];
}
window.getCustomBasketPresets = getCustomBasketPresets;

function saveCustomBasketPresets(presets) {
    try {
        localStorage.setItem(STORAGE_CUSTOM_PRESETS, JSON.stringify(presets));
    } catch (e) { }
}

const BUILTIN_PRESETS = [
    {
        id: "iron_condor_hedged",
        name: "🛡️ Hedged Straddle / Iron Condor",
        desc: "Sell ATM CE+PE, Buy OTM Hedge CE+PE",
        tag: "Popular",
        legs: [
            { type: "CE", side: "SELL", offset_step: 0 },
            { type: "PE", side: "SELL", offset_step: 0 },
            { type: "CE", side: "BUY", offset_step: 4 },
            { type: "PE", side: "BUY", offset_step: -4 }
        ]
    },
    {
        id: "atm_straddle",
        name: "⚡ ATM Short Straddle",
        desc: "Sell ATM Call + Sell ATM Put",
        tag: "Standard",
        legs: [
            { type: "CE", side: "SELL", offset_step: 0 },
            { type: "PE", side: "SELL", offset_step: 0 }
        ]
    },
    {
        id: "otm1_strangle",
        name: "🎯 OTM 1 Short Strangle (Crude Favorite)",
        desc: "Sell OTM 1 Call + Sell OTM 1 Put",
        tag: "Crude",
        legs: [
            { type: "CE", side: "SELL", offset_step: 1 },
            { type: "PE", side: "SELL", offset_step: -1 }
        ]
    },
    {
        id: "otm2_strangle",
        name: "🎯 OTM 2 Short Strangle",
        desc: "Sell OTM 2 Call + Sell OTM 2 Put",
        tag: "Conservative",
        legs: [
            { type: "CE", side: "SELL", offset_step: 2 },
            { type: "PE", side: "SELL", offset_step: -2 }
        ]
    },
    {
        id: "otm3_strangle",
        name: "🎯 OTM 3 Short Strangle (Silver Favorite)",
        desc: "Sell OTM 3 Call + Sell OTM 3 Put",
        tag: "Silver",
        legs: [
            { type: "CE", side: "SELL", offset_step: 3 },
            { type: "PE", side: "SELL", offset_step: -3 }
        ]
    },
    {
        id: "bull_call_spread",
        name: "📈 Bull Call Spread",
        desc: "Buy ATM Call + Sell OTM 1 Call",
        tag: "Directional",
        legs: [
            { type: "CE", side: "BUY", offset_step: 0 },
            { type: "CE", side: "SELL", offset_step: 1 }
        ]
    },
    {
        id: "bear_put_spread",
        name: "📉 Bear Put Spread",
        desc: "Buy ATM Put + Sell OTM 1 Put",
        tag: "Directional",
        legs: [
            { type: "PE", side: "BUY", offset_step: 0 },
            { type: "PE", side: "SELL", offset_step: -1 }
        ]
    }
];

function toggleBasketPresetsDropdown(event) {
    if (event) event.stopPropagation();
    const drop = document.getElementById("basketPresetsDropdown");
    if (!drop) return;
    if (drop.style.display === "block") {
        drop.style.display = "none";
    } else {
        renderBasketPresetsDropdown();
        drop.style.display = "block";
    }
}
window.toggleBasketPresetsDropdown = toggleBasketPresetsDropdown;

// Close presets dropdown on outside click
document.addEventListener("click", function (e) {
    const drop = document.getElementById("basketPresetsDropdown");
    const btn = document.getElementById("btnBasketPresetsToggle");
    if (drop && drop.style.display === "block") {
        if (!drop.contains(e.target) && e.target !== btn && !btn?.contains(e.target)) {
            drop.style.display = "none";
        }
    }
});

function renderBasketPresetsDropdown() {
    const drop = document.getElementById("basketPresetsDropdown");
    if (!drop) return;

    const undSel = document.getElementById("basketUnderlyingSelect");
    const currentUnd = undSel?.value || "NIFTY";
    const customList = getCustomBasketPresets();

    let html = `
        <div class="preset-dropdown-header">
            ⚡ Quick Strategy Presets for ${currentUnd}
        </div>
    `;

    BUILTIN_PRESETS.forEach(p => {
        html += `
            <div class="preset-dropdown-item" onclick="applyBasketPreset('${p.id}')">
                <div style="flex:1;">
                    <div class="preset-item-title">${p.name} <span class="preset-item-tag">${p.tag}</span></div>
                    <div class="preset-item-desc">${p.desc}</div>
                </div>
                <span style="font-size:12px; color:#38bdf8; font-weight:800;">Apply ➔</span>
            </div>
        `;
    });

    if (customList.length > 0) {
        html += `
            <div class="preset-dropdown-header" style="border-top:1px solid rgba(255,255,255,0.08); margin-top:4px;">
                ⭐ My Saved Presets
            </div>
        `;
        customList.forEach(cp => {
            html += `
                <div class="preset-dropdown-item" onclick="applyCustomBasketPreset('${cp.id}')">
                    <div style="flex:1;">
                        <div class="preset-item-title">${cp.name} <span class="preset-item-tag" style="background:rgba(234,179,8,0.15); color:#facc15; border-color:rgba(234,179,8,0.3);">${cp.underlying || 'CUSTOM'}</span></div>
                        <div class="preset-item-desc">${cp.desc || `${cp.legs.length} legs`}</div>
                    </div>
                    <div style="display:flex; align-items:center; gap:8px;">
                        <span style="font-size:12px; color:#38bdf8; font-weight:800;">Load</span>
                        <span title="Delete Preset" style="color:#ef4444; cursor:pointer; font-size:12px; padding:2px 4px;" onclick="event.stopPropagation(); deleteCustomBasketPreset('${cp.id}')">🗑️</span>
                    </div>
                </div>
            `;
        });
    }

    drop.innerHTML = html;
}
window.renderBasketPresetsDropdown = renderBasketPresetsDropdown;

async function applyBasketPreset(presetId) {
    const drop = document.getElementById("basketPresetsDropdown");
    if (drop) drop.style.display = "none";

    const preset = BUILTIN_PRESETS.find(p => p.id === presetId);
    if (!preset) return;

    const undSel = document.getElementById("basketUnderlyingSelect");
    const und = undSel?.value || "NIFTY";
    const step = getStrikeStepForUnderlying(und);
    const lotSize = getLotSizeForUnderlying(und);

    showStraddleToast(`Resolving live ATM strike for ${und}...`, "info", 1500);

    try {
        const spotResp = await fetch(`/api/straddle/spot?underlying=${encodeURIComponent(und)}`);
        const spotData = await spotResp.json();
        let atm = spotData.atm || 0;
        let spot = spotData.spot || 0;

        if (atm <= 0 && spot > 0) {
            atm = Math.round(spot / step) * step;
        }
        if (atm <= 0) {
            atm = (und === "NIFTY" ? 22600 : (und === "BANKNIFTY" ? 48500 : (und === "CRUDEOIL" ? 6100 : (und === "SILVER" ? 85000 : 25000))));
        }

        // Fetch near expiry
        let nearExpiry = "Weekly";
        try {
            const expResp = await fetch(`/api/straddle/expiries?underlying=${encodeURIComponent(und)}`);
            const expData = await expResp.json();
            if (expData.success && expData.expiries && expData.expiries.length > 0) {
                nearExpiry = expData.expiries[0].formatted_label || expData.expiries[0].tag || "Weekly";
            }
        } catch (e) { }

        // For commodities in iron condor, adjust offset
        let legsConfig = JSON.parse(JSON.stringify(preset.legs));
        if (preset.id === "iron_condor_hedged") {
            const hedgeSteps = (und === "SILVER" || und === "SILVERM") ? 3 : ((und === "CRUDEOIL" || und === "CRUDEOILM") ? 2 : 4);
            legsConfig.forEach(l => {
                if (l.side === "BUY") {
                    l.offset_step = (l.type === "CE" ? hedgeSteps : -hedgeSteps);
                }
            });
        }

        const newOrders = [];
        legsConfig.forEach(legDef => {
            const strike = Math.round(atm + (legDef.offset_step * step));
            newOrders.push({
                id: "b_" + Date.now() + "_" + Math.floor(Math.random() * 10000),
                selected: true,
                underlying: und,
                symbol: `${und} ${strike} ${legDef.type}`,
                trading_symbol: `${und} ${strike} ${legDef.type}`,
                token: "",
                strike: strike,
                opt_type: legDef.type,
                side: legDef.side,
                expiry_date: nearExpiry,
                product: "CF",
                lots: 1,
                lot_size: lotSize,
                quantity: lotSize,
                price: 0.05,
                order_type: "LIMIT",
                ltp: 0.05,
                change: 0,
                change_pct: 0
            });
        });

        // Set to basket orders and save
        window._basketOrders = newOrders;
        const list = getSavedBasketsList();
        const current = list.find(b => b.id === window._activeBasketId);
        if (current) {
            current.orders = newOrders;
            current.underlying = und;
            saveBasketsList(list);
        }
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(newOrders));
        } catch (e) { }

        renderBasketOrdersTable();
        updateBasketBadges();
        updateBasketMarginCalculation();
        refreshAllBasketQuotes();
        showStraddleToast(`Applied preset "${preset.name}" at ATM ${atm}!`, "success", 2500);

    } catch (e) {
        showStraddleToast(`Error applying preset: ${e.message}`, "error", 3000);
    }
}
window.applyBasketPreset = applyBasketPreset;

function saveCurrentBasketAsPreset() {
    if (!window._basketOrders || window._basketOrders.length === 0) {
        showStraddleToast("Current basket has no orders to save as preset.", "warning", 2500);
        return;
    }

    const defaultName = generateBasketAutoName();
    const presetName = prompt("Enter a name for this Custom Strategy Preset:", defaultName);
    if (!presetName || !presetName.trim()) return;

    const undSel = document.getElementById("basketUnderlyingSelect");
    const und = undSel?.value || "NIFTY";
    const step = getStrikeStepForUnderlying(und);

    // Calculate reference strike from median strike
    const strikes = window._basketOrders.map(o => o.strike);
    const minStrike = Math.min(...strikes);
    const maxStrike = Math.max(...strikes);
    const refAtm = Math.round(((minStrike + maxStrike) / 2) / step) * step;

    const legs = window._basketOrders.map(o => ({
        type: o.opt_type,
        side: o.side,
        offset_step: Math.round((o.strike - refAtm) / step),
        lots: o.lots || 1,
        product: o.product || "CF"
    }));

    const newPreset = {
        id: "custom_" + Date.now(),
        name: presetName.trim(),
        desc: `${legs.length} legs (${legs.map(l => (l.side === "BUY" ? "+B " : "-S ") + (l.offset_step >= 0 ? "+" : "") + l.offset_step + l.type).join(", ")})`,
        underlying: und,
        legs: legs,
        created_at: Date.now()
    };

    const customList = getCustomBasketPresets();
    customList.push(newPreset);
    saveCustomBasketPresets(customList);

    showStraddleToast(`💾 Saved custom preset "${newPreset.name}"!`, "success", 2500);
    renderBasketPresetsDropdown();
}
window.saveCurrentBasketAsPreset = saveCurrentBasketAsPreset;

async function applyCustomBasketPreset(presetId) {
    const drop = document.getElementById("basketPresetsDropdown");
    if (drop) drop.style.display = "none";

    const customList = getCustomBasketPresets();
    const preset = customList.find(p => p.id === presetId);
    if (!preset) return;

    const undSel = document.getElementById("basketUnderlyingSelect");
    const und = preset.underlying || undSel?.value || "NIFTY";
    if (undSel) undSel.value = und;

    const step = getStrikeStepForUnderlying(und);
    const lotSize = getLotSizeForUnderlying(und);

    showStraddleToast(`Loading custom preset "${preset.name}" for ${und}...`, "info", 1500);

    try {
        const spotResp = await fetch(`/api/straddle/spot?underlying=${encodeURIComponent(und)}`);
        const spotData = await spotResp.json();
        let atm = spotData.atm || 0;
        let spot = spotData.spot || 0;
        if (atm <= 0 && spot > 0) atm = Math.round(spot / step) * step;
        if (atm <= 0) atm = (und === "NIFTY" ? 22600 : 25000);

        let nearExpiry = "Weekly";
        try {
            const expResp = await fetch(`/api/straddle/expiries?underlying=${encodeURIComponent(und)}`);
            const expData = await expResp.json();
            if (expData.success && expData.expiries && expData.expiries.length > 0) {
                nearExpiry = expData.expiries[0].formatted_label || expData.expiries[0].tag || "Weekly";
            }
        } catch (e) { }

        const newOrders = [];
        preset.legs.forEach(legDef => {
            const strike = Math.round(atm + (legDef.offset_step * step));
            newOrders.push({
                id: "b_" + Date.now() + "_" + Math.floor(Math.random() * 10000),
                selected: true,
                underlying: und,
                symbol: `${und} ${strike} ${legDef.type}`,
                trading_symbol: `${und} ${strike} ${legDef.type}`,
                token: "",
                strike: strike,
                opt_type: legDef.type,
                side: legDef.side,
                expiry_date: nearExpiry,
                product: legDef.product || "CF",
                lots: legDef.lots || 1,
                lot_size: lotSize,
                quantity: (legDef.lots || 1) * lotSize,
                price: 0.05,
                order_type: "LIMIT",
                ltp: 0.05,
                change: 0,
                change_pct: 0
            });
        });

        window._basketOrders = newOrders;
        const list = getSavedBasketsList();
        const current = list.find(b => b.id === window._activeBasketId);
        if (current) {
            current.orders = newOrders;
            current.underlying = und;
            saveBasketsList(list);
        }
        try {
            localStorage.setItem("straddle_basket_orders", JSON.stringify(newOrders));
        } catch (e) { }

        renderBasketOrdersTable();
        updateBasketBadges();
        updateBasketMarginCalculation();
        refreshAllBasketQuotes();
        showStraddleToast(`Loaded preset "${preset.name}" at ATM ${atm}!`, "success", 2500);
    } catch (e) {
        showStraddleToast(`Error loading preset: ${e.message}`, "error", 3000);
    }
}
window.applyCustomBasketPreset = applyCustomBasketPreset;

function deleteCustomBasketPreset(presetId) {
    let customList = getCustomBasketPresets();
    const target = customList.find(p => p.id === presetId);
    if (!target) return;
    if (!confirm(`Delete preset "${target.name}"?`)) return;

    customList = customList.filter(p => p.id !== presetId);
    saveCustomBasketPresets(customList);
    renderBasketPresetsDropdown();
    showStraddleToast(`Deleted preset "${target.name}".`, "info", 1500);
}
window.deleteCustomBasketPreset = deleteCustomBasketPreset;

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
window.getSavedBasketsList = getSavedBasketsList;
window.saveBasketsList = saveBasketsList;
window.syncBasketDropdown = syncBasketDropdown;
window.loadActiveBasketFromStorage = loadActiveBasketFromStorage;
window.onSwitchSavedBasket = onSwitchSavedBasket;
window.createNewBasket = createNewBasket;
window.deleteCurrentBasket = deleteCurrentBasket;
window.onBasketNameManualEdit = onBasketNameManualEdit;
window.persistActiveBasketState = persistActiveBasketState;
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
window.toggleBasketPresetsDropdown = toggleBasketPresetsDropdown;
window.renderBasketPresetsDropdown = renderBasketPresetsDropdown;
window.applyBasketPreset = applyBasketPreset;
window.saveCurrentBasketAsPreset = saveCurrentBasketAsPreset;
window.applyCustomBasketPreset = applyCustomBasketPreset;
window.deleteCustomBasketPreset = deleteCustomBasketPreset;
window.openOptionChainForActiveBasket = openOptionChainForActiveBasket;
window.toggleOptionChainInsights = toggleOptionChainInsights;
window.onBasketUnderlyingChange = onBasketUnderlyingChange;

// =========================================================================
// GLOBAL COMMODITY & NIFTY SELECTION & LIVE CANDLESTICK CHART CONTROLLER
// =========================================================================

window._activeGlobalUnderlying = "NIFTY";
window._activeChartTimeframe = "5m";
window._activeChartUnderlying = "NIFTY";
window._activeCandles = [];
window._chartCrosshairPos = null;
window._chartTickTimer = null;

const COMMODITY_DISPLAY_NAMES = {
    "NIFTY": "NIFTY 50",
    "BANKNIFTY": "BANKNIFTY",
    "FINNIFTY": "FINNIFTY",
    "MIDCPNIFTY": "MIDCPNIFTY",
    "SENSEX": "SENSEX",
    "BANKEX": "BANKEX",
    "CRUDEOIL": "Crude Oil",
    "NATURALGAS": "Natural Gas",
    "GOLD": "Gold",
    "SILVER": "Silver",
    "CRUDEOILM": "Crude Oil Mini",
    "NATGASMINI": "Nat Gas Mini",
    "GOLDM": "Gold Mini",
    "SILVERM": "Silver Mini",
    "COPPER": "Copper"
};

const COMMODITY_COLOR_MAP = {
    "NIFTY": "#38bdf8",
    "BANKNIFTY": "#818cf8",
    "FINNIFTY": "#a78bfa",
    "MIDCPNIFTY": "#2dd4bf",
    "SENSEX": "#f43f5e",
    "BANKEX": "#fb7185",
    "CRUDEOIL": "#f43f5e",
    "NATURALGAS": "#f59e0b",
    "GOLD": "#eab308",
    "SILVER": "#94a3b8",
    "CRUDEOILM": "#10b981",
    "NATGASMINI": "#ea580c",
    "GOLDM": "#84cc16",
    "SILVERM": "#8b5cf6",
    "COPPER": "#f97316"
};

function selectGlobalUnderlying(undKey) {
    if (!undKey) return;
    const cleanKey = canonicalUnderlying(undKey);
    window._activeGlobalUnderlying = cleanKey;
    try {
        localStorage.setItem("straddle_active_underlying", cleanKey);
    } catch (_) { }

    const dispName = COMMODITY_DISPLAY_NAMES[cleanKey] || cleanKey;
    const color = COMMODITY_COLOR_MAP[cleanKey] || "#38bdf8";

    // 0. Sync Topbar Asset Pill Button & Dropdown Items
    const topName = document.getElementById("topbarAssetName");
    if (topName) {
        topName.textContent = dispName;
        topName.style.color = color;
    }
    const topDot = document.getElementById("topbarAssetDot");
    if (topDot) {
        topDot.style.background = color;
        topDot.style.boxShadow = `0 0 10px ${color}`;
    }
    const topPillBtn = document.getElementById("btnTopbarAssetSelect");
    if (topPillBtn) {
        topPillBtn.style.borderColor = color;
    }
    const topChartLabel = document.getElementById("btnTopbarChartLabel");
    if (topChartLabel) {
        topChartLabel.textContent = `Chart (${cleanKey})`;
    }
    document.querySelectorAll(".asset-dropdown-item").forEach(item => {
        const isMatch = (item.getAttribute("data-und") === cleanKey);
        item.classList.toggle("active", isMatch);
    });

    // 1. Sync global top bar dropdown
    const globalSel = document.getElementById("globalUnderlyingSelect");
    if (globalSel && globalSel.value !== cleanKey) {
        globalSel.value = cleanKey;
    }

    // 2. Update Focused Asset Hero Card
    const nameEl = document.getElementById("activeUndName");
    if (nameEl) {
        nameEl.textContent = dispName;
        nameEl.style.color = color;
    }
    const dotEl = document.getElementById("activeUndDot");
    if (dotEl) {
        dotEl.style.background = color;
        dotEl.style.boxShadow = `0 0 10px ${color}`;
    }
    const cardEl = document.getElementById("activeUndCard");
    if (cardEl) {
        cardEl.style.borderColor = color;
    }

    // 3. Update legacy pills states if any exist
    const pills = document.querySelectorAll(".commodity-pill");
    pills.forEach(p => {
        const isMatch = (p.getAttribute("data-und") === cleanKey);
        p.classList.toggle("active", isMatch);
    });

    // 4. Update Header Spot Label & Ticker
    const labelEl = document.getElementById("straddleUnderlyingLabel");
    if (labelEl) {
        labelEl.textContent = `${dispName.toUpperCase()}:`;
        labelEl.style.color = color;
    }

    const chartBtnLabel = document.getElementById("btnActiveAssetChartLabel");
    if (chartBtnLabel) {
        chartBtnLabel.textContent = `Chart (${cleanKey})`;
    }

    // 5. Sync Option Chain read-only badge & underlying selector
    const optBadge = document.getElementById("optChainUnderlyingBadge");
    if (optBadge) {
        optBadge.textContent = dispName;
        optBadge.style.color = color;
    }
    const optUndSel = document.getElementById("optChainUnderlyingSelect");
    if (optUndSel && optUndSel.value !== cleanKey) {
        optUndSel.value = cleanKey;
        if (typeof onChangeOptChainUnderlying === "function") {
            const modal = document.getElementById("straddleOptionChainModal");
            if (modal && modal.style.display === "flex") {
                onChangeOptChainUnderlying(cleanKey);
            }
        }
    }

    // 6. Sync Basket read-only badge & underlying selector
    const basketBadge = document.getElementById("basketUnderlyingBadge");
    if (basketBadge) {
        basketBadge.textContent = dispName;
        basketBadge.style.color = color;
    }
    const basketUndSel = document.getElementById("basketUnderlyingSelect");
    if (basketUndSel && basketUndSel.value !== cleanKey) {
        basketUndSel.value = cleanKey;
        if (typeof onBasketUnderlyingChange === "function") {
            onBasketUnderlyingChange(cleanKey);
        }
    }

    // 7. Sync Algos table focused asset badge
    const algosBadge = document.getElementById("algosFocusedUnderlyingBadge");
    if (algosBadge) {
        algosBadge.textContent = `Focused: ${dispName}`;
        algosBadge.style.color = color;
    }

    // 8. Sync Algo builder modal default underlying
    const newAlgoUnd = document.getElementById("newAlgoUnderlying");
    if (newAlgoUnd) newAlgoUnd.value = cleanKey;

    // 9. If Live Chart modal is currently open, switch chart immediately
    const chartModal = document.getElementById("modalLiveChart");
    if (chartModal && chartModal.style.display === "flex") {
        openActiveAssetChartModal(cleanKey);
    }

    // 10. Filter Algos table for this active underlying
    renderStraddleAlgosTable(true);

    // 11. Notify Backend of focused active underlying (zero API contention)
    fetch("/api/straddle/set_active_underlying", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ underlying: cleanKey })
    }).catch(() => { });

    // 12. Fetch live tick immediately
    fetchLiveSpotTick();

    showStraddleToast(`Active Asset: ${dispName}`, "info", 1500);
}
window.selectGlobalUnderlying = selectGlobalUnderlying;

// Helper: Exchange Segment routing for Underlyings & Options
function getExchangeSegmentForUnderlying(und) {
    const u = String(und || "NIFTY").toUpperCase();
    if (u === "SENSEX" || u === "BANKEX") return "BFO";
    if (["NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY"].includes(u)) return "NSE_FO";
    return "MCX_FO";
}
window.getExchangeSegmentForUnderlying = getExchangeSegmentForUnderlying;

// Helper: Extract root underlying key from any underlying or option contract symbol
function extractRootUnderlying(symbol) {
    if (!symbol) return window._activeGlobalUnderlying || "NIFTY";
    const s = String(symbol).toUpperCase().replace(/\s+/g, "");
    const sorted = [
        "NATGASMINI", "CRUDEOILM", "MIDCPNIFTY", "NATURALGAS",
        "BANKNIFTY", "FINNIFTY", "CRUDEOIL", "SILVERM", "SENSEX",
        "BANKEX", "COPPER", "SILVER", "GOLDM", "NIFTY", "GOLD"
    ];
    for (const u of sorted) {
        if (s.startsWith(u)) return u;
    }
    return window._activeGlobalUnderlying || "NIFTY";
}
window.extractRootUnderlying = extractRootUnderlying;

function openActiveAssetChartModal(undParam) {
    const rawSymbol = undParam || window._activeGlobalUnderlying || "NIFTY";
    window._activeChartUnderlying = rawSymbol;
    const modal = document.getElementById("modalLiveChart");
    if (!modal) return;
    modal.classList.remove("hidden");
    modal.style.setProperty("display", "flex", "important");

    const rootUnd = extractRootUnderlying(rawSymbol);
    const dispName = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[rootUnd]) || rootUnd;
    const color = (typeof COMMODITY_COLOR_MAP !== "undefined" && COMMODITY_COLOR_MAP[rootUnd]) || "#38bdf8";

    // Synchronize interactive Asset Selector dropdown
    const selectEl = document.getElementById("chartAssetSelect");
    if (selectEl) {
        selectEl.value = rootUnd;
    }

    const titleEl = document.getElementById("chartAssetTitle");
    if (titleEl) {
        titleEl.textContent = `${dispName.toUpperCase()} LIVE CANDLESTICK CHART`;
    }
    const badgeEl = document.getElementById("chartActiveContractBadge");
    if (badgeEl) {
        badgeEl.textContent = rawSymbol.includes(" ") ? rawSymbol : `${dispName.toUpperCase()} NEAR MONTH`;
    }
    const dotEl = document.getElementById("chartAssetDot");
    if (dotEl) {
        dotEl.style.background = color;
        dotEl.style.boxShadow = `0 0 10px ${color}`;
    }

    const segEl = document.getElementById("chartExchangeSegInfo");
    if (segEl) {
        segEl.textContent = getExchangeSegmentForUnderlying(rootUnd);
    }

    updateChartIndicatorButtons();
    setupChartCanvasEvents();
    if (typeof applyChartHeaderCollapseState === "function") {
        applyChartHeaderCollapseState();
    }
    loadAndRenderCandleChart(rawSymbol, window._activeChartTimeframe || "5m");
    startChartTickStream();
}
window.openActiveAssetChartModal = openActiveAssetChartModal;

// In-Chart Asset Switcher handler
function onChartAssetChange(newUnd) {
    if (!newUnd) return;
    window._activeChartUnderlying = newUnd;

    // Reset and abort previous in-flight AI requests immediately
    if (window._kronosAbortController) {
        try { window._kronosAbortController.abort(); } catch (e) {}
        window._kronosAbortController = null;
    }
    if (window._strategistAbortController) {
        try { window._strategistAbortController.abort(); } catch (e) {}
        window._strategistAbortController = null;
    }
    window._kronosForecastData = null;
    window._aiStrategistData = null;
    if (typeof resetKronosForecastUI === "function") {
        resetKronosForecastUI();
    }
    if (typeof resetAIStrategistUI === "function") {
        resetAIStrategistUI();
    }

    if (typeof selectGlobalUnderlying === "function") {
        selectGlobalUnderlying(newUnd);
    }

    const dispName = (typeof COMMODITY_DISPLAY_NAMES !== "undefined" && COMMODITY_DISPLAY_NAMES[newUnd]) || newUnd;
    const color = (typeof COMMODITY_COLOR_MAP !== "undefined" && COMMODITY_COLOR_MAP[newUnd]) || "#38bdf8";

    const badgeEl = document.getElementById("chartActiveContractBadge");
    if (badgeEl) {
        badgeEl.textContent = `${dispName.toUpperCase()} NEAR MONTH`;
    }
    const dotEl = document.getElementById("chartAssetDot");
    if (dotEl) {
        dotEl.style.background = color;
        dotEl.style.boxShadow = `0 0 10px ${color}`;
    }
    const segEl = document.getElementById("chartExchangeSegInfo");
    if (segEl) {
        segEl.textContent = getExchangeSegmentForUnderlying(newUnd);
    }

    window._chartViewStart = null;
    window._chartPriceRange = null;

    loadAndRenderCandleChart(newUnd, window._activeChartTimeframe || "5m");
}
window.onChartAssetChange = onChartAssetChange;

// Direct Quick Order Execution from Chart Modal
function executeChartDirectOrder(side) {
    const symbol = window._activeContractSymbol || window._activeChartUnderlying || "NIFTY";
    const lastCandle = (window._activeCandles && window._activeCandles.length > 0) ? window._activeCandles[window._activeCandles.length - 1] : null;
    const ltp = lastCandle ? lastCandle.close : 0;

    let strike = 0;
    let optType = "CE";
    const m = symbol.match(/(\d+(?:\.\d+)?)\s*(CE|PE)/i);
    if (m) {
        strike = parseFloat(m[1]);
        optType = m[2].toUpperCase();
    } else if (lastCandle) {
        strike = Math.round(lastCandle.close);
    }

    if (typeof openStraddleDirectOrderModal === "function") {
        openStraddleDirectOrderModal(symbol, "", strike, optType, side, ltp);
    } else {
        showStraddleToast(`Direct order modal not available`, "warning", 2000);
    }
}
window.executeChartDirectOrder = executeChartDirectOrder;

// Add Active Chart Contract to Algo Strategy Builder
function addChartAssetToAlgoStrategy() {
    const symbol = window._activeContractSymbol || window._activeChartUnderlying || "NIFTY";
    const lastCandle = (window._activeCandles && window._activeCandles.length > 0) ? window._activeCandles[window._activeCandles.length - 1] : null;
    const ltp = lastCandle ? lastCandle.close : 0;
    let strike = 0;
    let optType = "CE";
    const m = symbol.match(/(\d+(?:\.\d+)?)\s*(CE|PE)/i);
    if (m) {
        strike = parseFloat(m[1]);
        optType = m[2].toUpperCase();
    } else if (lastCandle) {
        strike = Math.round(lastCandle.close);
    }

    if (typeof addOptionChainLegToStrategy === "function") {
        addOptionChainLegToStrategy(strike, optType, "Buy", symbol, ltp);
    } else {
        showStraddleToast(`Algo strategy leg builder not available`, "warning", 2000);
    }
}
window.addChartAssetToAlgoStrategy = addChartAssetToAlgoStrategy;

// Fullscreen Modal Toggle
window._chartIsFullscreen = false;
function toggleChartFullscreen() {
    window._chartIsFullscreen = !window._chartIsFullscreen;
    const modal = document.getElementById("modalLiveChart");
    if (!modal) return;
    const card = modal.querySelector(".straddle-modal-card");
    const icon = document.getElementById("iconChartFullscreen");
    const lbl = document.getElementById("lblChartFullscreen");
    const btn = document.getElementById("btnToggleChartFullscreen");

    if (window._chartIsFullscreen) {
        if (card) {
            card.style.setProperty("max-width", "99vw", "important");
            card.style.setProperty("width", "99vw", "important");
            card.style.setProperty("height", "98vh", "important");
            card.style.setProperty("max-height", "98vh", "important");
        }
        if (icon) icon.textContent = "🗗";
        if (lbl) lbl.textContent = "Restore";
        if (btn) {
            btn.style.borderColor = "#38bdf8";
            btn.style.color = "#38bdf8";
        }
    } else {
        if (card) {
            card.style.removeProperty("max-width");
            card.style.removeProperty("width");
            card.style.removeProperty("height");
            card.style.removeProperty("max-height");
        }
        if (icon) icon.textContent = "⛶";
        if (lbl) lbl.textContent = "Maximize";
        if (btn) {
            btn.style.removeProperty("border-color");
            btn.style.removeProperty("color");
        }
    }

    setTimeout(() => {
        applyChartHeaderCollapseState();
    }, 50);
}
window.toggleChartFullscreen = toggleChartFullscreen;

// Technical Indicators State & Math
window._chartActiveIndicators = {
    ema9: false,
    ema21: false,
    vwap: false,
    bb: false
};
try {
    const savedInds = localStorage.getItem("straddle_chart_indicators");
    if (savedInds) {
        window._chartActiveIndicators = Object.assign(window._chartActiveIndicators, JSON.parse(savedInds));
    }
} catch (e) { }

function toggleChartIndicator(ind) {
    if (!window._chartActiveIndicators) {
        window._chartActiveIndicators = { ema9: false, ema21: false, vwap: false, bb: false };
    }
    window._chartActiveIndicators[ind] = !window._chartActiveIndicators[ind];
    try {
        localStorage.setItem("straddle_chart_indicators", JSON.stringify(window._chartActiveIndicators));
    } catch (e) { }
    updateChartIndicatorButtons();
    renderCandlestickCanvas();
}
window.toggleChartIndicator = toggleChartIndicator;

function updateChartIndicatorButtons() {
    const inds = window._chartActiveIndicators || {};
    document.querySelectorAll(".chart-indicator-btn").forEach(btn => {
        const indName = btn.getAttribute("data-ind");
        if (indName) {
            btn.classList.toggle("active", !!inds[indName]);
        }
    });
}
window.updateChartIndicatorButtons = updateChartIndicatorButtons;

function calcChartEMA(candles, period) {
    const n = candles.length;
    const emaValues = new Array(n).fill(null);
    if (n < period) return emaValues;

    let sum = 0;
    for (let i = 0; i < period; i++) sum += candles[i].close;
    let prev = sum / period;
    emaValues[period - 1] = prev;

    const mult = 2 / (period + 1);
    for (let i = period; i < n; i++) {
        const val = (candles[i].close - prev) * mult + prev;
        emaValues[i] = val;
        prev = val;
    }
    return emaValues;
}

function calcChartVWAP(candles) {
    const n = candles.length;
    const vwapValues = new Array(n).fill(null);
    if (n === 0) return vwapValues;

    let cumVol = 0;
    let cumTypicalVol = 0;
    let lastDay = -1;

    for (let i = 0; i < n; i++) {
        const c = candles[i];
        const d = new Date(c.time * 1000);
        const day = d.getUTCDate();
        if (day !== lastDay) {
            cumVol = 0;
            cumTypicalVol = 0;
            lastDay = day;
        }
        const typical = (c.high + c.low + c.close) / 3;
        const vol = (c.volume && c.volume > 0) ? c.volume : 1;
        cumVol += vol;
        cumTypicalVol += typical * vol;
        vwapValues[i] = (cumVol > 0) ? (cumTypicalVol / cumVol) : typical;
    }
    return vwapValues;
}

function calcChartBollingerBands(candles, period = 20, mult = 2) {
    const n = candles.length;
    const upper = new Array(n).fill(null);
    const middle = new Array(n).fill(null);
    const lower = new Array(n).fill(null);
    if (n < period) return { upper, middle, lower };

    for (let i = period - 1; i < n; i++) {
        let sum = 0;
        for (let j = 0; j < period; j++) sum += candles[i - j].close;
        const mean = sum / period;
        let sumSqDiff = 0;
        for (let j = 0; j < period; j++) sumSqDiff += Math.pow(candles[i - j].close - mean, 2);
        const stdDev = Math.sqrt(sumSqDiff / period);
        middle[i] = mean;
        upper[i] = mean + mult * stdDev;
        lower[i] = mean - mult * stdDev;
    }
    return { upper, middle, lower };
}

function closeLiveChartModal() {
    const modal = document.getElementById("modalLiveChart");
    if (modal) {
        modal.classList.add("hidden");
        modal.style.setProperty("display", "none", "important");
    }
    stopChartTickStream();
}
window.closeLiveChartModal = closeLiveChartModal;

function switchChartTimeframe(tf) {
    window._activeChartTimeframe = tf;
    document.querySelectorAll(".chart-timeframe-btn").forEach(btn => {
        btn.classList.toggle("active", btn.getAttribute("data-tf") === tf);
    });

    // Cleanly cancel previous in-flight AI requests for old timeframe
    if (window._kronosAbortController) {
        try { window._kronosAbortController.abort(); } catch (e) {}
        window._kronosAbortController = null;
    }
    window._kronosForecastData = null;
    if (typeof resetKronosForecastUI === "function") {
        resetKronosForecastUI();
    }

    loadAndRenderCandleChart(window._activeChartUnderlying, tf);
}
window.switchChartTimeframe = switchChartTimeframe;

function reloadActiveCandlestickChart() {
    loadAndRenderCandleChart(window._activeChartUnderlying, window._activeChartTimeframe);
}
window.reloadActiveCandlestickChart = reloadActiveCandlestickChart;

function startChartTickStream() {
    stopChartTickStream();
    window._chartTickTimer = setInterval(() => {
        const modal = document.getElementById("modalLiveChart");
        if (modal && modal.style.display === "flex") {
            fetchLiveSpotTick();
        } else {
            stopChartTickStream();
        }
    }, 1000);
}

function stopChartTickStream() {
    if (window._chartTickTimer) {
        clearInterval(window._chartTickTimer);
        window._chartTickTimer = null;
    }
}

async function loadAndRenderCandleChart(und, tf) {
    const countEl = document.getElementById("chartCandleCountInfo");
    if (countEl) countEl.textContent = "Loading candles...";
    try {
        const resp = await fetch(`/api/straddle/chart_candles?underlying=${encodeURIComponent(und)}&timeframe=${encodeURIComponent(tf)}&range=1d`);
        const data = await resp.json();
        if (data && data.status === "success" && Array.isArray(data.candles)) {
            window._activeCandles = data.candles;
            window._activeContractSymbol = data.contract_symbol || und;
            window._activeExpiryDate = data.expiry_date || "";
            window._activeSegment = data.segment || getExchangeSegmentForUnderlying(und);

            // Reset viewport view start and manual price scale to auto-align to the latest candles
            window._chartViewStart = null;
            window._chartPriceRange = null;

            if (countEl) countEl.textContent = `${data.candles.length} candles (${tf})`;

            const badgeEl = document.getElementById("chartActiveContractBadge");
            if (badgeEl) {
                badgeEl.textContent = data.contract_symbol ? data.contract_symbol : `${und} NEAR MONTH`;
            }

            const segEl = document.getElementById("chartExchangeSegInfo");
            if (segEl) {
                segEl.textContent = window._activeSegment;
            }

            const ltpEl = document.getElementById("chartLtpDisplay");
            const curP = data.current_price || (data.candles.length ? data.candles[data.candles.length - 1].close : 0);
            if (ltpEl && curP > 0) ltpEl.textContent = "₹" + curP.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

            let maxH = 0, minL = Infinity;
            data.candles.forEach(c => {
                if (c.high > maxH) maxH = c.high;
                if (c.low < minL && c.low > 0) minL = c.low;
            });
            const highEl = document.getElementById("chartHighDisplay");
            const lowEl = document.getElementById("chartLowDisplay");
            if (highEl && maxH > 0) highEl.textContent = "₹" + maxH.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            if (lowEl && minL < Infinity) lowEl.textContent = "₹" + minL.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

            if (data.derivatives_intelligence) {
                window._activeDerivativesIntelligence = data.derivatives_intelligence;
                updateDerivativesUI(data.derivatives_intelligence);
            }

            renderCandlestickCanvas();
            if (window._kronosForecastActive) {
                triggerKronosForecast(false);
            }
        }
    } catch (e) {
        console.error("Error loading candles:", e);
        if (countEl) countEl.textContent = "Error loading candles";
    }
}

// Global Kronos AI Forecast & Derivatives State
window._kronosForecastActive = false;
window._kronosForecastHorizon = 24;
window._kronosForecastData = null;
window._kronosForecastLoading = false;
window._kronosAbortController = null;
window._kronosRequestId = 0;
window._activeDerivativesIntelligence = null;
window._aiStrategistActive = false;
window._aiStrategistData = null;
window._aiStrategistLoading = false;
window._strategistAbortController = null;
window._strategistRequestId = 0;

function resetKronosForecastUI() {
    const dirBadge = document.getElementById("kronosDirectionBadge");
    const chgPtsEl = document.getElementById("kronosChangePoints");
    const highEl = document.getElementById("kronosProjectedHigh");
    const lowEl = document.getElementById("kronosProjectedLow");
    const spreadEl = document.getElementById("kronosProjectedSpread");
    const edgeEl = document.getElementById("kronosStraddleEdgePill");
    const statusText = document.getElementById("kronosStatusText");

    if (statusText) statusText.textContent = `Horizon: ${window._kronosForecastHorizon || 24} candles • Ready`;
    if (dirBadge) {
        dirBadge.innerHTML = "<span>⚡</span> <span>CALCULATING...</span>";
        dirBadge.style.color = "#38bdf8";
        dirBadge.style.background = "rgba(56, 189, 248, 0.25)";
        dirBadge.style.border = "1px solid #38bdf8";
    }
    if (chgPtsEl) chgPtsEl.textContent = "(+0.00 pts)";
    if (highEl) highEl.textContent = "₹ --";
    if (lowEl) lowEl.textContent = "₹ --";
    if (spreadEl) spreadEl.textContent = "-- pts";
    if (edgeEl) {
        edgeEl.textContent = "THETA CORRIDOR EVALUATING...";
        edgeEl.style.background = "rgba(56, 189, 248, 0.15)";
        edgeEl.style.borderColor = "#38bdf8";
        edgeEl.style.color = "#38bdf8";
    }
}
window.resetKronosForecastUI = resetKronosForecastUI;

function resetAIStrategistUI() {
    const regimeEl = document.getElementById("aiStrategistRegime");
    const biasEl = document.getElementById("aiStrategistBias");
    const recEl = document.getElementById("aiStrategistRecommendation");
    const b1 = document.getElementById("aiStrategistBullet1");
    const b2 = document.getElementById("aiStrategistBullet2");
    const b3 = document.getElementById("aiStrategistBullet3");
    const srcBadge = document.getElementById("aiStrategistSourceBadge");

    if (srcBadge) srcBadge.textContent = "Institutional Quant Synthesis Engine";
    if (regimeEl) regimeEl.textContent = "Evaluating...";
    if (biasEl) biasEl.textContent = "Analyzing Greeks...";
    if (recEl) recEl.textContent = "Analyzing optimal structure...";
    if (b1) b1.innerHTML = "• <b>1. Execution:</b> Calculating strike placements...";
    if (b2) b2.innerHTML = "• <b>2. Risk / Stop:</b> Determining dynamic hedge boundary...";
    if (b3) b3.innerHTML = "• <b>3. Target:</b> Projecting theta harvest horizon...";
}
window.resetAIStrategistUI = resetAIStrategistUI;

function toggleKronosForecast() {
    window._kronosForecastActive = !window._kronosForecastActive;
    const btn = document.getElementById("btnToggleKronosForecast");
    const badge = document.getElementById("kronosBadgeState");
    const panel = document.getElementById("kronosForecastPanel");

    if (window._kronosForecastActive) {
        if (btn) btn.classList.add("btn-ai-kronos-active");
        if (badge) {
            badge.textContent = "ON";
            badge.style.background = "#a855f7";
            badge.style.boxShadow = "0 0 8px rgba(168, 85, 247, 0.8)";
        }
        if (panel) panel.style.display = "flex";
        triggerKronosForecast(true);
    } else {
        if (btn) btn.classList.remove("btn-ai-kronos-active");
        if (badge) {
            badge.textContent = "OFF";
            badge.style.background = "#475569";
            badge.style.boxShadow = "none";
        }
        if (panel) panel.style.display = "none";
        if (window._kronosAbortController) {
            try { window._kronosAbortController.abort(); } catch (e) {}
            window._kronosAbortController = null;
        }
        window._kronosForecastData = null;
        renderCandlestickCanvas();
    }
}
window.toggleKronosForecast = toggleKronosForecast;

function setKronosHorizon(horizon) {
    window._kronosForecastHorizon = parseInt(horizon) || 24;
    document.querySelectorAll(".kronos-horizon-btn").forEach(btn => {
        const h = parseInt(btn.getAttribute("data-horizon"));
        const isActive = (h === window._kronosForecastHorizon);
        btn.classList.toggle("active", isActive);
        btn.style.background = isActive ? "#a855f7" : "none";
        btn.style.color = isActive ? "#ffffff" : "#cbd5e1";
        btn.style.fontWeight = isActive ? "800" : "700";
    });
    const statusText = document.getElementById("kronosStatusText");
    if (statusText) {
        statusText.textContent = `Horizon: ${window._kronosForecastHorizon} candles • Recalculating...`;
    }
    if (window._kronosForecastActive) {
        triggerKronosForecast(true);
    }
}
window.setKronosHorizon = setKronosHorizon;

async function triggerKronosForecast(force = false) {
    if (!window._kronosForecastActive) return;
    const und = window._activeChartUnderlying || "NIFTY";
    const tf = window._activeChartTimeframe || "5m";
    const horizon = window._kronosForecastHorizon || 24;

    // Prevent duplicate simultaneous requests unless forced by user click
    if (window._kronosForecastLoading && !force) {
        return;
    }

    // Cancel any in-flight request
    if (window._kronosAbortController) {
        try { window._kronosAbortController.abort(); } catch (e) {}
    }
    window._kronosAbortController = new AbortController();
    const currentSignal = window._kronosAbortController.signal;
    const currentReqId = ++window._kronosRequestId;

    const statusBadge = document.getElementById("kronosStatusText");
    const dirBadge = document.getElementById("kronosDirectionBadge");
    if (statusBadge) {
        statusBadge.innerHTML = `<span class="live-tick-pulse" style="background:#a855f7; box-shadow:0 0 7px #a855f7;"></span> <span style="color:#d8b4fe;">Computing ${horizon}-Bar Path...</span>`;
    }
    if (dirBadge) {
        dirBadge.innerHTML = "<span>⚡</span> <span>CALCULATING...</span>";
        dirBadge.style.color = "#38bdf8";
        dirBadge.style.background = "rgba(56, 189, 248, 0.25)";
        dirBadge.style.border = "1px solid #38bdf8";
    }

    window._kronosForecastLoading = true;
    try {
        const timeoutId = setTimeout(() => {
            if (window._kronosAbortController && currentReqId === window._kronosRequestId) {
                try { window._kronosAbortController.abort(); } catch (e) {}
            }
        }, 12000);

        const url = `/api/straddle/kronos_forecast?underlying=${encodeURIComponent(und)}&timeframe=${encodeURIComponent(tf)}&horizon=${horizon}&force=${force ? '1' : '0'}&_t=${Date.now()}`;
        const resp = await fetch(url, { signal: currentSignal });
        clearTimeout(timeoutId);

        if (currentReqId !== window._kronosRequestId) return;

        if (!resp.ok) {
            throw new Error(`HTTP ${resp.status}`);
        }

        const data = await resp.json();
        if (currentReqId !== window._kronosRequestId) return;

        if (data && data.success && Array.isArray(data.candles)) {
            window._kronosForecastData = data;
            if (data.derivatives_intelligence) {
                window._activeDerivativesIntelligence = data.derivatives_intelligence;
                updateDerivativesUI(data.derivatives_intelligence);
            }
            updateKronosForecastUI(data);
            renderCandlestickCanvas();
        } else {
            const err = (data && data.error) ? data.error : "Unable to generate forecast";
            handleKronosForecastError(err);
        }
    } catch (e) {
        if (e.name === "AbortError" && currentReqId !== window._kronosRequestId) {
            return;
        }
        console.warn("Notice during Kronos forecast:", e);
        handleKronosForecastError(e.name === "AbortError" ? "Request Timeout" : "Sync notice");
    } finally {
        if (currentReqId === window._kronosRequestId) {
            window._kronosForecastLoading = false;
        }
    }
}
window.triggerKronosForecast = triggerKronosForecast;

function handleKronosForecastError(msg) {
    const statusBadge = document.getElementById("kronosStatusText");
    const dirBadge = document.getElementById("kronosDirectionBadge");

    if (statusBadge) {
        statusBadge.innerHTML = `<span style="color:#fb7185;">⚠️ ${msg || "Sync notice"} • <a href="javascript:void(0)" onclick="triggerKronosForecast(true)" style="color:#c084fc; font-weight:700; text-decoration:underline;">Retry</a></span>`;
    }
    if (dirBadge) {
        if (window._kronosForecastData && window._kronosForecastData.summary && window._kronosForecastData.summary.direction) {
            const dir = String(window._kronosForecastData.summary.direction).toUpperCase();
            dirBadge.innerHTML = `<span>⚡</span> <span>${dir} (CACHED)</span>`;
            dirBadge.style.color = "#fbbf24";
            dirBadge.style.background = "rgba(251, 191, 36, 0.2)";
            dirBadge.style.border = "1px solid #fbbf24";
        } else {
            dirBadge.innerHTML = `<span>⚠️</span> <span>PAUSED</span>`;
            dirBadge.style.color = "#fb7185";
            dirBadge.style.background = "rgba(244, 63, 94, 0.2)";
            dirBadge.style.border = "1px solid #f43f5e";
        }
    }
}

function updateKronosForecastUI(data) {
    if (!data) return;
    const summary = data.summary || {};

    const dirBadge = document.getElementById("kronosDirectionBadge");
    const chgPtsEl = document.getElementById("kronosChangePoints");
    const highEl = document.getElementById("kronosProjectedHigh");
    const lowEl = document.getElementById("kronosProjectedLow");
    const spreadEl = document.getElementById("kronosProjectedSpread");
    const edgeEl = document.getElementById("kronosStraddleEdgePill");
    const statusText = document.getElementById("kronosStatusText");

    // 1. Predicted Direction & Change Points
    if (dirBadge) {
        const dir = (summary.direction || "NEUTRAL").toUpperCase();
        let icon = summary.direction_icon || "⚡";
        let bg = "rgba(56, 189, 248, 0.25)";
        let border = "#38bdf8";
        let color = "#38bdf8";

        if (dir.includes("BULLISH")) {
            icon = "📈";
            bg = "rgba(16, 185, 129, 0.25)";
            border = "#10b981";
            color = "#34d399";
        } else if (dir.includes("BEARISH")) {
            icon = "📉";
            bg = "rgba(244, 63, 94, 0.25)";
            border = "#f43f5e";
            color = "#fb7185";
        }
        dirBadge.innerHTML = `<span>${icon}</span> <span>${dir}</span>`;
        dirBadge.style.background = bg;
        dirBadge.style.border = `1px solid ${border}`;
        dirBadge.style.color = color;
    }

    if (chgPtsEl) {
        const chg = summary.change_points !== undefined ? summary.change_points : 0;
        const sign = chg >= 0 ? "+" : "";
        chgPtsEl.textContent = `(${sign}${Number(chg).toFixed(2)} pts)`;
        chgPtsEl.style.color = chg >= 0 ? "#34d399" : "#fb7185";
    }

    // 2. Projected High, Low, and Spread
    if (highEl) {
        const maxH = summary.projected_high || (Array.isArray(data.candles) && data.candles.length ? Math.max(...data.candles.map(c => c.high)) : 0);
        if (maxH > 0) {
            highEl.textContent = "₹ " + Number(maxH).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
    }

    if (lowEl) {
        const minL = summary.projected_low || (Array.isArray(data.candles) && data.candles.length ? Math.min(...data.candles.map(c => c.low)) : 0);
        if (minL > 0) {
            lowEl.textContent = "₹ " + Number(minL).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        }
    }

    if (spreadEl) {
        const spread = summary.projected_range_pts || (summary.projected_high && summary.projected_low ? (summary.projected_high - summary.projected_low) : 0);
        spreadEl.textContent = `${Number(spread).toFixed(2)} pts`;
    }

    // 3. Straddle Edge Pill
    if (edgeEl) {
        const edge = data.straddle_edge || {};
        const assessment = edge.assessment || (summary.projected_range_pct !== undefined ? (summary.projected_range_pct <= 1.0 ? "HIGH THETA DECAY EDGE (Tight Corridor)" : (summary.projected_range_pct <= 2.2 ? "MODERATE VOLATILITY (Standard Range)" : "HIGH VOLATILITY EXPANSION WARNING")) : "THETA CORRIDOR EVALUATED");
        edgeEl.textContent = assessment;
        edgeEl.title = edge.recommendation || "Straddle Strategy Edge Evaluation";

        if (assessment.includes("HIGH THETA") || assessment.includes("OVERPRICED")) {
            edgeEl.style.background = "rgba(16, 185, 129, 0.2)";
            edgeEl.style.borderColor = "#10b981";
            edgeEl.style.color = "#34d399";
        } else if (assessment.includes("EXPANSION") || assessment.includes("UNDERPRICED") || assessment.includes("WARNING")) {
            edgeEl.style.background = "rgba(244, 63, 94, 0.2)";
            edgeEl.style.borderColor = "#f43f5e";
            edgeEl.style.color = "#fb7185";
        } else {
            edgeEl.style.background = "rgba(56, 189, 248, 0.15)";
            edgeEl.style.borderColor = "#38bdf8";
            edgeEl.style.color = "#38bdf8";
        }
    }

    // 4. Status and Drift
    if (statusText) {
        const drift = summary.change_percent !== undefined ? summary.change_percent : (summary.expected_return_pct || 0);
        const sign = drift > 0 ? "+" : "";
        statusText.textContent = `Horizon: ${window._kronosForecastHorizon || 24} candles • Drift: ${sign}${Number(drift).toFixed(2)}%`;
    }
}

function updateDerivativesUI(der) {
    if (!der) return;
    const mpEl = document.getElementById("lblMaxPainStrike");
    const cwEl = document.getElementById("lblCallWallStrike");
    const pwEl = document.getElementById("lblPutWallStrike");
    const pcrEl = document.getElementById("lblPcrRatio") || document.getElementById("lblDerivativesPcr");
    const atmEl = document.getElementById("lblAtmStraddlePremium");
    const misEl = document.getElementById("lblStraddleMispricingBadge");
    const mtfEl = document.getElementById("lblMtfTrendBadge");

    const maxPain = der.max_pain !== undefined ? der.max_pain : der.max_pain_strike;
    if (mpEl && maxPain) {
        mpEl.textContent = "₹ " + Number(maxPain).toLocaleString('en-IN');
    }

    const callWall = der.call_oi_wall !== undefined ? der.call_oi_wall : der.call_wall_strike;
    if (cwEl && callWall) {
        const spot = der.spot_price || (window._activeCandles && window._activeCandles.length ? window._activeCandles[window._activeCandles.length - 1].close : 0);
        const dist = spot > 0 ? (callWall - spot) : (der.call_wall_distance || der.call_wall_dist || 0);
        const distStr = dist > 0 ? ` (+${dist.toFixed(2)})` : (dist < 0 ? ` (${dist.toFixed(2)})` : "");
        cwEl.textContent = "₹ " + Number(callWall).toLocaleString('en-IN') + distStr;
    }

    const putWall = der.put_oi_wall !== undefined ? der.put_oi_wall : der.put_wall_strike;
    if (pwEl && putWall) {
        const spot = der.spot_price || (window._activeCandles && window._activeCandles.length ? window._activeCandles[window._activeCandles.length - 1].close : 0);
        const dist = spot > 0 ? (spot - putWall) : (der.put_wall_distance || der.put_wall_dist || 0);
        const distStr = dist > 0 ? ` (-${dist.toFixed(2)})` : (dist < 0 ? ` (+${Math.abs(dist).toFixed(2)})` : "");
        pwEl.textContent = "₹ " + Number(putWall).toLocaleString('en-IN') + distStr;
    }

    if (pcrEl && der.pcr !== undefined) {
        const sentiment = der.sentiment || der.pcr_sentiment || (der.pcr > 1.2 ? "Bullish" : (der.pcr < 0.8 ? "Bearish" : "Neutral"));
        pcrEl.textContent = `${Number(der.pcr).toFixed(2)} (${sentiment})`;
        pcrEl.style.color = der.sentiment_color || (der.pcr > 1.0 ? "#34d399" : (der.pcr < 0.8 ? "#fb7185" : "#38bdf8"));
    }

    if (atmEl && der.atm_straddle_premium) {
        const pct = der.atm_implied_move_pct ? ` (±${der.atm_implied_move_pct.toFixed(2)}%)` : (der.atm_straddle_pct ? ` (±${der.atm_straddle_pct.toFixed(2)}%)` : "");
        atmEl.textContent = "₹ " + Number(der.atm_straddle_premium).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + pct;
    }

    // Straddle Mispricing Badge
    if (misEl) {
        const badgeText = der.mispricing_badge || (der.straddle_edge && der.straddle_edge.verdict) || (der.mispricing_status ? `${der.mispricing_status} [${der.mispricing_ratio || 1.0}x]` : "BALANCED");
        const ratio = der.mispricing_ratio ? ` [${der.mispricing_ratio}x]` : "";
        const fullText = badgeText.includes("[") ? badgeText : `${badgeText}${ratio}`;
        misEl.textContent = fullText;

        const isOver = fullText.includes("OVERPRICED") || der.mispricing_status === "OVERPRICED";
        const isUnder = fullText.includes("UNDERPRICED") || der.mispricing_status === "UNDERPRICED";

        if (isOver) {
            misEl.className = "mispricing-badge-overpriced";
            misEl.style.background = "rgba(16, 185, 129, 0.2)";
            misEl.style.borderColor = "#10b981";
            misEl.style.color = "#34d399";
        } else if (isUnder) {
            misEl.className = "mispricing-badge-underpriced";
            misEl.style.background = "rgba(244, 63, 94, 0.2)";
            misEl.style.borderColor = "#f43f5e";
            misEl.style.color = "#fb7185";
        } else {
            misEl.className = "mispricing-badge-neutral";
            misEl.style.background = "rgba(56, 189, 248, 0.15)";
            misEl.style.borderColor = "#38bdf8";
            misEl.style.color = "#38bdf8";
        }
    }

    // MTF Trend Badge
    if (mtfEl) {
        const bias = der.mtf_bias || (der.mtf_confluence && der.mtf_confluence.bias) || "NEUTRAL";
        mtfEl.textContent = bias;
        if (bias.includes("BULLISH")) {
            mtfEl.style.background = "rgba(16, 185, 129, 0.2)";
            mtfEl.style.borderColor = "#10b981";
            mtfEl.style.color = "#34d399";
        } else if (bias.includes("BEARISH")) {
            mtfEl.style.background = "rgba(244, 63, 94, 0.2)";
            mtfEl.style.borderColor = "#f43f5e";
            mtfEl.style.color = "#fb7185";
        } else {
            mtfEl.style.background = "rgba(56, 189, 248, 0.15)";
            mtfEl.style.borderColor = "#38bdf8";
            mtfEl.style.color = "#38bdf8";
        }
    }
}

function toggleAIStrategist() {
    window._aiStrategistActive = !window._aiStrategistActive;
    const btn = document.getElementById("btnToggleAIStrategist");
    const badge = document.getElementById("aiStrategistBadgeState");
    const panel = document.getElementById("aiStrategistPanel");

    if (window._aiStrategistActive) {
        if (btn) btn.classList.add("btn-ai-strategist-active");
        if (badge) {
            badge.textContent = "OPEN";
            badge.style.background = "#10b981";
            badge.style.boxShadow = "0 0 8px rgba(16, 185, 129, 0.8)";
        }
        if (panel) panel.style.display = "flex";
        fetchAIStrategist(true);
    } else {
        if (btn) btn.classList.remove("btn-ai-strategist-active");
        if (badge) {
            badge.textContent = "READY";
            badge.style.background = "#475569";
            badge.style.boxShadow = "none";
        }
        if (panel) panel.style.display = "none";
        if (window._strategistAbortController) {
            try { window._strategistAbortController.abort(); } catch (e) {}
            window._strategistAbortController = null;
        }
        window._aiStrategistData = null;
    }
}
window.toggleAIStrategist = toggleAIStrategist;

async function fetchAIStrategist(force = false) {
    const und = window._activeChartUnderlying || "NIFTY";
    const tf = window._activeChartTimeframe || "5m";

    if (window._aiStrategistLoading && !force) {
        return;
    }

    if (window._strategistAbortController) {
        try { window._strategistAbortController.abort(); } catch (e) {}
    }
    window._strategistAbortController = new AbortController();
    const currentSignal = window._strategistAbortController.signal;
    const currentReqId = ++window._strategistRequestId;

    const regimeEl = document.getElementById("aiStrategistRegime");
    const biasEl = document.getElementById("aiStrategistBias");
    const recEl = document.getElementById("aiStrategistRecommendation");
    const b1 = document.getElementById("aiStrategistBullet1");
    const b2 = document.getElementById("aiStrategistBullet2");
    const b3 = document.getElementById("aiStrategistBullet3");
    const srcBadge = document.getElementById("aiStrategistSourceBadge");

    if (regimeEl) regimeEl.textContent = "Synthesizing Institutional Quant Playbook...";
    if (biasEl) biasEl.textContent = "Analyzing Greeks & Volatility...";
    if (recEl) recEl.textContent = "Analyzing optimal structure...";

    window._aiStrategistLoading = true;
    try {
        const timeoutId = setTimeout(() => {
            if (window._strategistAbortController && currentReqId === window._strategistRequestId) {
                try { window._strategistAbortController.abort(); } catch (e) {}
            }
        }, 12000);

        const url = `/api/straddle/ai_strategist?underlying=${encodeURIComponent(und)}&timeframe=${encodeURIComponent(tf)}&force=${force ? '1' : '0'}&_t=${Date.now()}`;
        const resp = await fetch(url, { signal: currentSignal });
        clearTimeout(timeoutId);

        if (currentReqId !== window._strategistRequestId) return;
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);

        const data = await resp.json();
        if (currentReqId !== window._strategistRequestId) return;

        if (data && data.success && data.report) {
            const r = data.report;
            window._aiStrategistData = r;

            if (srcBadge) {
                srcBadge.textContent = r.source || "Institutional Quant Synthesis Engine";
            }
            if (regimeEl) regimeEl.textContent = r.market_regime || "Normal Market Conditions";
            if (biasEl) biasEl.textContent = r.bias || "DELTA-NEUTRAL";
            if (recEl) recEl.textContent = r.strategy_recommendation || "Delta-Neutral Straddle / Strangle";

            const plans = r.execution_plan || [];
            if (b1) b1.innerHTML = plans[0] ? `• <b>${plans[0].title}:</b> ${plans[0].detail}` : "";
            if (b2) b2.innerHTML = plans[1] ? `• <b>${plans[1].title}:</b> ${plans[1].detail}` : "";
            if (b3) b3.innerHTML = plans[2] ? `• <b>${plans[2].title}:</b> ${plans[2].detail}` : "";

            if (data.derivatives) {
                window._activeDerivativesIntelligence = data.derivatives;
                updateDerivativesUI(data.derivatives);
                renderCandlestickCanvas();
            }
        } else {
            handleStrategistError((data && data.error) ? data.error : "Unable to generate playbook");
        }
    } catch (e) {
        if (e.name === "AbortError" && currentReqId !== window._strategistRequestId) {
            return;
        }
        console.warn("Notice during AI Strategist synthesis:", e);
        handleStrategistError(e.name === "AbortError" ? "Request Timeout" : "Sync notice");
    } finally {
        if (currentReqId === window._strategistRequestId) {
            window._aiStrategistLoading = false;
        }
    }
}
window.fetchAIStrategist = fetchAIStrategist;

function handleStrategistError(msg) {
    const regimeEl = document.getElementById("aiStrategistRegime");
    const biasEl = document.getElementById("aiStrategistBias");
    const recEl = document.getElementById("aiStrategistRecommendation");
    if (regimeEl) regimeEl.textContent = "Tactical Market Assessment";
    if (biasEl) biasEl.textContent = "DELTA-NEUTRAL";
    if (recEl) {
        recEl.innerHTML = `<span style="color:#fb7185;">Notice: ${msg || "Strategist paused"} • </span><a href="javascript:void(0)" onclick="fetchAIStrategist(true)" style="color:#6ee7b7; font-weight:700; text-decoration:underline;">Click to Re-Analyze</a>`;
    }
}

// Safe numerical clamp helper
function clamp(val, min, max) {
    return Math.max(min, Math.min(max, val));
}

// Global Viewport, Pan/Zoom & Price Scale State
window._chartZoomCount = 80; // default visible candles in viewport
window._chartViewStart = null; // null = right-aligned to newest candles
window._chartPriceRange = null; // null = automatic fit; object { min, max } when user drags/scales price
window._chartDragState = null;
window._chartIsDragging = false;
window._chartData = null;

function getValidForecastCandles() {
    if (!window._kronosForecastActive || !window._kronosForecastData || !Array.isArray(window._kronosForecastData.candles)) {
        return [];
    }
    const curAsset = window._activeChartUnderlying || "NIFTY";
    const curTf = window._activeChartTimeframe || "15m";
    const fAsset = window._kronosForecastData.underlying;
    const fTf = window._kronosForecastData.timeframe;
    if (fAsset && fAsset !== curAsset) return [];
    if (fTf && fTf !== curTf) return [];
    return window._kronosForecastData.candles;
}
window.getValidForecastCandles = getValidForecastCandles;

function chartZoomIn() {
    const validForecast = getValidForecastCandles();
    const totalBars = (window._activeCandles ? window._activeCandles.length : 0) + validForecast.length;
    if (totalBars <= 0) return;
    const currentZoom = window._chartZoomCount || 80;
    const newZoom = Math.max(15, Math.round(currentZoom * 0.75));
    if (newZoom !== currentZoom) {
        const currentStart = (window._chartViewStart !== null) ? window._chartViewStart : Math.max(0, totalBars - currentZoom + 4);
        const anchor = currentStart + (currentZoom * 0.85);
        window._chartZoomCount = newZoom;
        window._chartViewStart = Math.max(0, Math.min(totalBars - 5, anchor - (newZoom * 0.85)));
    }
    if (window._chartPriceRange && Number.isFinite(window._chartPriceRange.min) && Number.isFinite(window._chartPriceRange.max)) {
        const mid = (window._chartPriceRange.min + window._chartPriceRange.max) / 2;
        const halfSpan = (window._chartPriceRange.max - window._chartPriceRange.min) * 0.425;
        window._chartPriceRange = { min: mid - halfSpan, max: mid + halfSpan };
    }
    renderCandlestickCanvas();
}
window.chartZoomIn = chartZoomIn;

function chartZoomOut() {
    const validForecast = getValidForecastCandles();
    const totalBars = (window._activeCandles ? window._activeCandles.length : 0) + validForecast.length;
    if (totalBars <= 0) return;
    const currentZoom = window._chartZoomCount || 80;
    const maxZoomAllowed = Math.min(Math.max(30, totalBars + 6), 450);
    const newZoom = Math.min(maxZoomAllowed, Math.round(currentZoom * 1.33));
    if (newZoom !== currentZoom) {
        const currentStart = (window._chartViewStart !== null) ? window._chartViewStart : Math.max(0, totalBars - currentZoom + 4);
        const anchor = currentStart + (currentZoom * 0.85);
        window._chartZoomCount = newZoom;
        window._chartViewStart = Math.max(0, Math.min(totalBars - 5, anchor - (newZoom * 0.85)));
    }
    if (window._chartPriceRange && Number.isFinite(window._chartPriceRange.min) && Number.isFinite(window._chartPriceRange.max)) {
        const mid = (window._chartPriceRange.min + window._chartPriceRange.max) / 2;
        const halfSpan = (window._chartPriceRange.max - window._chartPriceRange.min) * 0.6;
        window._chartPriceRange = { min: mid - halfSpan, max: mid + halfSpan };
    }
    renderCandlestickCanvas();
}
window.chartZoomOut = chartZoomOut;

function chartResetZoom() {
    window._chartZoomCount = 80;
    window._chartViewStart = null;
    window._chartPriceRange = null;
    renderCandlestickCanvas();
}
window.chartResetZoom = chartResetZoom;
window.resetChartView = chartResetZoom;

function resetPriceScaleOnly() {
    window._chartPriceRange = null;
    renderCandlestickCanvas();
}
window.resetPriceScaleOnly = resetPriceScaleOnly;

function renderCandlestickCanvas() {
    const canvas = document.getElementById("liveCandlestickCanvas");
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    const container = canvas.parentElement;

    const dpr = window.devicePixelRatio || 1;
    const w = Math.floor(container.clientWidth || 1050);
    let h = window._chartCanvasHeight || (window._chartHeaderCollapsed ? 680 : 500);

    if (window._chartIsFullscreen) {
        const headerOffset = window._chartHeaderCollapsed ? 100 : 210;
        h = Math.max(450, window.innerHeight - headerOffset);
    }

    if (canvas.width !== Math.floor(w * dpr) || canvas.height !== Math.floor(h * dpr)) {
        canvas.width = Math.floor(w * dpr);
        canvas.height = Math.floor(h * dpr);
    }
    canvas.style.width = w + "px";
    canvas.style.height = h + "px";

    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.scale(dpr, dpr);

    // Deep modern dark background
    ctx.fillStyle = "#070a12";
    ctx.fillRect(0, 0, w, h);

    const baseCandles = window._activeCandles || [];
    const forecastCandles = getValidForecastCandles();

    // Combine base and forecast candles with explicit indexing
    const allCandles = [];
    baseCandles.forEach((c, idx) => {
        allCandles.push({ ...c, is_forecast: false, globalIdx: idx });
    });
    forecastCandles.forEach((c, idx) => {
        allCandles.push({ ...c, is_forecast: true, globalIdx: baseCandles.length + idx });
    });

    const totalBars = allCandles.length;
    if (totalBars === 0) {
        ctx.fillStyle = "#64748b";
        ctx.font = "14px -apple-system, BlinkMacSystemFont, sans-serif";
        ctx.textAlign = "center";
        ctx.fillText("No candlestick data available", w / 2, h / 2);
        return;
    }

    // Chart margins
    const margin = { top: 25, right: 88, bottom: 28, left: 16 };
    const chartW = w - margin.left - margin.right;
    const chartH = h - margin.top - margin.bottom;
    const volumeH = chartH * 0.16;
    const priceH = chartH - volumeH - 12;

    // Viewport windowing:
    const zoomCount = Math.max(15, Math.min(Math.min(totalBars + 5, 450), window._chartZoomCount || 80));
    window._chartZoomCount = zoomCount;

    const rightGap = 4;
    if (window._chartViewStart === null || window._chartViewStart === undefined) {
        window._chartViewStart = Math.max(0, totalBars - zoomCount + rightGap);
    }
    const maxStart = Math.max(0, totalBars - Math.max(5, Math.floor(zoomCount * 0.15)));
    const viewStart = Math.max(0, Math.min(maxStart, window._chartViewStart));
    window._chartViewStart = viewStart;
    const viewEnd = viewStart + zoomCount;

    const slotW = chartW / zoomCount;
    const candleW = Math.max(2, Math.min(26, Math.floor(slotW * 0.72)));

    // Visible slice for dynamic Y-axis auto-scaling
    const visStartIdx = Math.max(0, Math.floor(viewStart));
    const visEndIdx = Math.min(totalBars, Math.ceil(viewEnd) + 1);
    const visibleCandles = allCandles.slice(visStartIdx, visEndIdx);

    let minPrice = Infinity;
    let maxPrice = -Infinity;
    let maxVolume = 0;

    visibleCandles.forEach(c => {
        if (c.low && c.low < minPrice && c.low > 0) minPrice = c.low;
        if (c.high && c.high > maxPrice) maxPrice = c.high;
        if (c.volume > maxVolume) maxVolume = c.volume;
    });

    if (forecastCandles.length > 0 && window._kronosForecastData && window._kronosForecastData.volatility_corridor) {
        const vc = window._kronosForecastData.volatility_corridor;
        const u95 = vc.upper_95 || vc.upper || [];
        const l95 = vc.lower_95 || vc.lower || [];
        forecastCandles.forEach((c, fIdx) => {
            const globalIdx = baseCandles.length + fIdx;
            if (globalIdx >= visStartIdx && globalIdx <= visEndIdx) {
                if (u95[fIdx] && u95[fIdx].value > maxPrice) maxPrice = u95[fIdx].value;
                if (l95[fIdx] && l95[fIdx].value < minPrice) minPrice = l95[fIdx].value;
            }
        });
    }

    if (minPrice === Infinity || maxPrice === -Infinity || minPrice <= 0) {
        minPrice = 100;
        maxPrice = 110;
    }
    if (minPrice === maxPrice) {
        minPrice *= 0.99;
        maxPrice *= 1.01;
    }

    // 8% vertical padding on price scale
    const pricePadding = (maxPrice - minPrice) * 0.08;
    minPrice -= pricePadding;
    maxPrice += pricePadding;

    if (window._chartPriceRange && Number.isFinite(window._chartPriceRange.min) && Number.isFinite(window._chartPriceRange.max)) {
        minPrice = window._chartPriceRange.min;
        maxPrice = window._chartPriceRange.max;
    }

    if (minPrice >= maxPrice) {
        const mid = (minPrice + maxPrice) / 2 || 100;
        minPrice = mid * 0.95;
        maxPrice = mid * 1.05;
    }

    const priceRange = Math.max(maxPrice - minPrice, 0.001);

    // Save chart geometry & coordinate mapping for interactive events
    window._chartData = {
        priceMin: minPrice,
        priceMax: maxPrice,
        priceTop: margin.top,
        priceBottom: margin.top + priceH,
        padLeft: margin.left,
        padRight: margin.right,
        padBottom: margin.bottom,
        chartWidth: chartW,
        priceHeight: priceH,
        visibleStart: viewStart,
        visibleCount: zoomCount,
        width: w,
        height: h
    };

    const autoBtn = document.getElementById("chartAutoScaleBtn");
    if (autoBtn) {
        if (window._chartPriceRange) {
            autoBtn.classList.add("active");
            autoBtn.title = "Manual price scale active — Click to Reset (Auto)";
        } else {
            autoBtn.classList.remove("active");
            autoBtn.title = "Price scale is in Auto mode";
        }
    }

    // Horizontal Price Grid lines
    ctx.strokeStyle = "rgba(255, 255, 255, 0.04)";
    ctx.lineWidth = 1;
    ctx.fillStyle = "#64748b";
    ctx.font = "11px JetBrains Mono, monospace";
    ctx.textAlign = "left";

    const priceSteps = 6;
    for (let i = 0; i <= priceSteps; i++) {
        const p = minPrice + (priceRange * (i / priceSteps));
        const y = Math.floor(margin.top + priceH - (priceH * (i / priceSteps))) + 0.5;
        ctx.beginPath();
        ctx.moveTo(margin.left, y);
        ctx.lineTo(w - margin.right, y);
        ctx.stroke();

        ctx.fillText("₹" + p.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }), w - margin.right + 6, y + 4);
    }

    // Vertical Time Grid Lines and X-axis Labels (with bold day boundary separators)
    ctx.save();
    const labelStep = Math.max(5, Math.floor(zoomCount / 7));
    let lastRenderedDate = null;

    for (let i = visStartIdx; i < visEndIdx; i++) {
        const c = allCandles[i];
        if (!c) continue;
        const d = new Date(c.time * 1000);
        const dateStr = d.toLocaleDateString([], { month: 'short', day: 'numeric' });
        const isNewDay = (lastRenderedDate !== null && lastRenderedDate !== dateStr);
        lastRenderedDate = dateStr;

        if (i % labelStep === 0 || i === baseCandles.length - 1 || isNewDay) {
            const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
            if (x >= margin.left && x <= w - margin.right) {
                ctx.beginPath();
                if (isNewDay) {
                    ctx.strokeStyle = "rgba(255, 255, 255, 0.12)";
                    ctx.setLineDash([]);
                } else {
                    ctx.strokeStyle = "rgba(255, 255, 255, 0.035)";
                    ctx.setLineDash([2, 4]);
                }
                ctx.moveTo(Math.floor(x) + 0.5, margin.top);
                ctx.lineTo(Math.floor(x) + 0.5, h - margin.bottom);
                ctx.stroke();

                const timeStr = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
                if (isNewDay) {
                    ctx.fillStyle = "#38bdf8";
                    ctx.font = "bold 10px JetBrains Mono, monospace";
                    ctx.textAlign = "center";
                    ctx.fillText(dateStr, x, h - 8);
                } else {
                    ctx.fillStyle = c.is_forecast ? "#c084fc" : "#64748b";
                    ctx.font = "10px JetBrains Mono, monospace";
                    ctx.textAlign = "center";
                    ctx.fillText(timeStr, x, h - 8);
                }
            }
        }
    }
    ctx.restore();

    const COLOR_BULLISH = "#089981";
    const COLOR_BEARISH = "#f23645";

    // Clip rendering strictly to chart canvas region
    ctx.save();
    ctx.beginPath();
    ctx.rect(margin.left, margin.top - 12, chartW, chartH + 12);
    ctx.clip();

    // 1. Draw Monte Carlo Volatility Corridors if Kronos forecast is active
    if (forecastCandles.length > 0 && window._kronosForecastData && window._kronosForecastData.volatility_corridor) {
        const vc = window._kronosForecastData.volatility_corridor;
        const u95 = vc.upper_95 || [];
        const l95 = vc.lower_95 || [];
        if (u95.length === forecastCandles.length && l95.length === forecastCandles.length) {
            ctx.save();
            ctx.fillStyle = "rgba(168, 85, 247, 0.05)";
            ctx.strokeStyle = "rgba(168, 85, 247, 0.22)";
            ctx.lineWidth = 1.0;
            ctx.setLineDash([2, 4]);
            ctx.beginPath();
            for (let f = 0; f < forecastCandles.length; f++) {
                const idx = baseCandles.length + f;
                const fx = margin.left + ((idx - viewStart) * slotW) + (slotW / 2);
                const fy = margin.top + priceH - ((u95[f].value - minPrice) / priceRange * priceH);
                if (f === 0) ctx.moveTo(fx, fy);
                else ctx.lineTo(fx, fy);
            }
            for (let f = forecastCandles.length - 1; f >= 0; f--) {
                const idx = baseCandles.length + f;
                const fx = margin.left + ((idx - viewStart) * slotW) + (slotW / 2);
                const fy = margin.top + priceH - ((l95[f].value - minPrice) / priceRange * priceH);
                ctx.lineTo(fx, fy);
            }
            ctx.closePath();
            ctx.fill();
            ctx.stroke();
            ctx.restore();
        }

        const u80 = vc.upper_80 || vc.upper || [];
        const l80 = vc.lower_80 || vc.lower || [];
        if (u80.length === forecastCandles.length && l80.length === forecastCandles.length) {
            ctx.save();
            ctx.fillStyle = "rgba(168, 85, 247, 0.12)";
            ctx.strokeStyle = "rgba(168, 85, 247, 0.45)";
            ctx.lineWidth = 1.3;
            ctx.setLineDash([3, 3]);
            ctx.beginPath();
            for (let f = 0; f < forecastCandles.length; f++) {
                const idx = baseCandles.length + f;
                const fx = margin.left + ((idx - viewStart) * slotW) + (slotW / 2);
                const fy = margin.top + priceH - ((u80[f].value - minPrice) / priceRange * priceH);
                if (f === 0) ctx.moveTo(fx, fy);
                else ctx.lineTo(fx, fy);
            }
            for (let f = forecastCandles.length - 1; f >= 0; f--) {
                const idx = baseCandles.length + f;
                const fx = margin.left + ((idx - viewStart) * slotW) + (slotW / 2);
                const fy = margin.top + priceH - ((l80[f].value - minPrice) / priceRange * priceH);
                ctx.lineTo(fx, fy);
            }
            ctx.closePath();
            ctx.fill();
            ctx.stroke();
            ctx.restore();
        }
    }

    // 2. Draw Vertical Divider between Historical and Forecast Horizon
    if (forecastCandles.length > 0 && baseCandles.length > 0) {
        const divX = Math.floor(margin.left + ((baseCandles.length - viewStart) * slotW)) + 0.5;
        if (divX >= margin.left - 50 && divX <= w - margin.right + 50) {
            ctx.save();
            ctx.setLineDash([4, 4]);
            ctx.strokeStyle = "#a855f7";
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            ctx.moveTo(divX, margin.top);
            ctx.lineTo(divX, h - margin.bottom);
            ctx.stroke();

            ctx.fillStyle = "#a855f7";
            ctx.beginPath();
            ctx.roundRect(divX - 42, margin.top - 20, 84, 18, 4);
            ctx.fill();
            ctx.fillStyle = "#ffffff";
            ctx.font = "bold 9.5px JetBrains Mono, monospace";
            ctx.textAlign = "center";
            ctx.fillText("🔮 KRONOS AI", divX, margin.top - 7);
            ctx.restore();
        }
    }

    // 2b. Draw Technical Studies / Indicators on historical baseCandles
    if (baseCandles.length > 0 && window._chartActiveIndicators) {
        const inds = window._chartActiveIndicators;

        // A. Bollinger Bands (20, 2)
        if (inds.bb && baseCandles.length >= 5) {
            const bb = calcChartBollingerBands(baseCandles, 20, 2);
            ctx.save();
            ctx.fillStyle = "rgba(168, 85, 247, 0.08)";
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (bb.upper[i] !== null && bb.lower[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const yUp = margin.top + priceH - ((bb.upper[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, yUp); started = true; }
                    else { ctx.lineTo(x, yUp); }
                }
            }
            for (let i = baseCandles.length - 1; i >= 0; i--) {
                if (bb.upper[i] !== null && bb.lower[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const yLow = margin.top + priceH - ((bb.lower[i] - minPrice) / priceRange * priceH);
                    ctx.lineTo(x, yLow);
                }
            }
            ctx.closePath();
            ctx.fill();

            // Upper band
            ctx.strokeStyle = "rgba(168, 85, 247, 0.55)";
            ctx.lineWidth = 1.1;
            ctx.setLineDash([3, 3]);
            ctx.beginPath();
            started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (bb.upper[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const yUp = margin.top + priceH - ((bb.upper[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, yUp); started = true; }
                    else { ctx.lineTo(x, yUp); }
                }
            }
            ctx.stroke();

            // Lower band
            ctx.beginPath();
            started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (bb.lower[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const yLow = margin.top + priceH - ((bb.lower[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, yLow); started = true; }
                    else { ctx.lineTo(x, yLow); }
                }
            }
            ctx.stroke();

            // Middle band
            ctx.strokeStyle = "rgba(168, 85, 247, 0.35)";
            ctx.setLineDash([2, 4]);
            ctx.beginPath();
            started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (bb.middle[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const yMid = margin.top + priceH - ((bb.middle[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, yMid); started = true; }
                    else { ctx.lineTo(x, yMid); }
                }
            }
            ctx.stroke();
            ctx.restore();
        }

        // B. VWAP
        if (inds.vwap && baseCandles.length > 0) {
            const vwapVals = calcChartVWAP(baseCandles);
            ctx.save();
            ctx.strokeStyle = "#ec4899";
            ctx.lineWidth = 1.4;
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (vwapVals[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const y = margin.top + priceH - ((vwapVals[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, y); started = true; }
                    else { ctx.lineTo(x, y); }
                }
            }
            ctx.stroke();
            ctx.restore();
        }

        // C. EMA 21
        if (inds.ema21 && baseCandles.length >= 21) {
            const ema21Vals = calcChartEMA(baseCandles, 21);
            ctx.save();
            ctx.strokeStyle = "#f59e0b";
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (ema21Vals[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const y = margin.top + priceH - ((ema21Vals[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, y); started = true; }
                    else { ctx.lineTo(x, y); }
                }
            }
            ctx.stroke();
            ctx.restore();
        }

        // D. EMA 9
        if (inds.ema9 && baseCandles.length >= 9) {
            const ema9Vals = calcChartEMA(baseCandles, 9);
            ctx.save();
            ctx.strokeStyle = "#38bdf8";
            ctx.lineWidth = 1.5;
            ctx.beginPath();
            let started = false;
            for (let i = 0; i < baseCandles.length; i++) {
                if (ema9Vals[i] !== null) {
                    const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
                    const y = margin.top + priceH - ((ema9Vals[i] - minPrice) / priceRange * priceH);
                    if (!started) { ctx.moveTo(x, y); started = true; }
                    else { ctx.lineTo(x, y); }
                }
            }
            ctx.stroke();
            ctx.restore();
        }
    }

    // 3. Draw Candles & Volumes
    allCandles.forEach((c, i) => {
        const x = margin.left + ((i - viewStart) * slotW) + (slotW / 2);
        if (x < margin.left - slotW * 2 || x > w - margin.right + slotW * 2) return;

        const isUp = c.close >= c.open;
        const isForecast = !!c.is_forecast;

        let color = isUp ? COLOR_BULLISH : COLOR_BEARISH;
        if (isForecast) {
            color = isUp ? "#34d399" : "#fb7185";
        }

        const yOpen = margin.top + priceH - ((c.open - minPrice) / priceRange * priceH);
        const yClose = margin.top + priceH - ((c.close - minPrice) / priceRange * priceH);
        const yHigh = margin.top + priceH - ((c.high - minPrice) / priceRange * priceH);
        const yLow = margin.top + priceH - ((c.low - minPrice) / priceRange * priceH);

        // Wick
        const wickX = Math.floor(x) + 0.5;
        ctx.save();
        ctx.strokeStyle = color;
        ctx.lineWidth = isForecast ? 1.5 : 1.2;
        ctx.beginPath();
        ctx.moveTo(wickX, yHigh);
        ctx.lineTo(wickX, yLow);
        ctx.stroke();
        ctx.restore();

        // Body
        const bodyTop = Math.min(yOpen, yClose);
        const bodyH = Math.max(2, Math.abs(yClose - yOpen));
        const bodyLeft = Math.floor(x - (candleW / 2));

        if (isForecast) {
            ctx.save();
            ctx.fillStyle = isUp ? "rgba(16, 185, 129, 0.85)" : "rgba(244, 63, 94, 0.85)";
            ctx.fillRect(bodyLeft, bodyTop, Math.floor(candleW), bodyH);
            ctx.strokeStyle = isUp ? "#34d399" : "#fb7185";
            ctx.lineWidth = 1.2;
            ctx.strokeRect(bodyLeft, bodyTop, Math.floor(candleW), bodyH);
            ctx.restore();
        } else {
            ctx.fillStyle = color;
            ctx.fillRect(bodyLeft, bodyTop, Math.floor(candleW), bodyH);
        }

        // Volume bar
        if (maxVolume > 0 && c.volume > 0) {
            const vBarH = (c.volume / maxVolume) * volumeH;
            const yVol = margin.top + chartH - vBarH;
            if (isForecast) {
                ctx.fillStyle = isUp ? "rgba(52, 211, 153, 0.25)" : "rgba(251, 113, 133, 0.25)";
            } else {
                ctx.fillStyle = isUp ? "rgba(8, 153, 129, 0.35)" : "rgba(242, 54, 69, 0.35)";
            }
            ctx.fillRect(bodyLeft, yVol, Math.floor(candleW), vBarH);
        }
    });

    ctx.restore(); // Restore clip region

    // 4. Current Price Line on last historical candle
    if (baseCandles.length > 0) {
        const lastCandle = baseCandles[baseCandles.length - 1];
        const curP = lastCandle.close;
        const curY = margin.top + priceH - ((curP - minPrice) / priceRange * priceH);
        const lastIsUp = lastCandle.close >= lastCandle.open;
        const lastColor = lastIsUp ? COLOR_BULLISH : COLOR_BEARISH;

        if (curY >= margin.top && curY <= margin.top + priceH + 15) {
            ctx.save();
            ctx.setLineDash([4, 4]);
            ctx.strokeStyle = lastColor;
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.moveTo(margin.left, Math.floor(curY) + 0.5);
            ctx.lineTo(w - margin.right, Math.floor(curY) + 0.5);
            ctx.stroke();
            ctx.restore();

            ctx.fillStyle = lastColor;
            ctx.beginPath();
            ctx.roundRect(w - margin.right + 2, Math.floor(curY) - 10, margin.right - 4, 20, 4);
            ctx.fill();
            ctx.fillStyle = "#ffffff";
            ctx.font = "bold 11px JetBrains Mono, monospace";
            ctx.textAlign = "left";
            ctx.fillText("₹" + curP.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }), w - margin.right + 6, Math.floor(curY) + 4);
        }
    }

    // 5. Kronos Target Line if Forecast Active
    if (forecastCandles.length > 0 && window._kronosForecastData && window._kronosForecastData.summary) {
        const targetP = window._kronosForecastData.summary.final_projected_price;
        const targetY = margin.top + priceH - ((targetP - minPrice) / priceRange * priceH);

        if (targetY >= margin.top && targetY <= margin.top + priceH + 15) {
            const divX = Math.max(margin.left, Math.min(w - margin.right, margin.left + ((baseCandles.length - viewStart) * slotW)));
            ctx.save();
            ctx.setLineDash([3, 3]);
            ctx.strokeStyle = "#a855f7";
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.moveTo(divX, Math.floor(targetY) + 0.5);
            ctx.lineTo(w - margin.right, Math.floor(targetY) + 0.5);
            ctx.stroke();

            ctx.fillStyle = "#a855f7";
            ctx.beginPath();
            ctx.roundRect(w - margin.right + 2, Math.floor(targetY) - 10, margin.right - 4, 20, 4);
            ctx.fill();
            ctx.fillStyle = "#ffffff";
            ctx.font = "bold 10px JetBrains Mono, monospace";
            ctx.textAlign = "left";
            ctx.fillText("🎯 ₹" + targetP.toFixed(2), w - margin.right + 4, Math.floor(targetY) + 4);
            ctx.restore();
        }
    }

    // 6. Crosshair, Hover HUD & Stationary Status
    let targetCandle = null;
    let targetIdx = -1;
    let isHovering = false;
    let crosshairPrice = null;

    if (window._chartCrosshairPos && !window._chartIsDragging) {
        const mx = window._chartCrosshairPos.x;
        const my = window._chartCrosshairPos.y;

        if (mx >= margin.left && mx <= w - margin.right && my >= margin.top && my <= h - margin.bottom) {
            isHovering = true;
            ctx.save();
            ctx.setLineDash([2, 2]);
            ctx.strokeStyle = "rgba(255, 255, 255, 0.4)";
            ctx.lineWidth = 1;

            // Vertical line
            ctx.beginPath();
            ctx.moveTo(Math.floor(mx) + 0.5, margin.top);
            ctx.lineTo(Math.floor(mx) + 0.5, h - margin.bottom);
            ctx.stroke();

            // Horizontal line
            ctx.beginPath();
            ctx.moveTo(margin.left, Math.floor(my) + 0.5);
            ctx.lineTo(w - margin.right, Math.floor(my) + 0.5);
            ctx.stroke();
            ctx.restore();

            // Compute price under crosshair cursor and render on right price scale
            if (my >= margin.top && my <= margin.top + priceH + 6) {
                crosshairPrice = Math.max(minPrice, minPrice + ((margin.top + priceH - my) / priceH) * priceRange);
            }

            if (crosshairPrice !== null && crosshairPrice > 0) {
                const badgeY = Math.min(margin.top + priceH + 2, Math.max(margin.top - 2, my));
                const badgeX = w - margin.right + 2;
                const badgeW = margin.right - 4;
                const badgeH = 20;

                ctx.save();
                ctx.fillStyle = "#2563eb";
                ctx.strokeStyle = "#93c5fd";
                ctx.lineWidth = 1.2;
                ctx.beginPath();
                ctx.roundRect(badgeX, badgeY - 10, badgeW, badgeH, 4);
                ctx.fill();
                ctx.stroke();

                const fmtCrosshair = (crosshairPrice >= 1000) ?
                    crosshairPrice.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) :
                    crosshairPrice.toFixed(2);

                ctx.fillStyle = "#ffffff";
                ctx.font = "bold 11px JetBrains Mono, monospace";
                ctx.textAlign = "left";
                ctx.fillText("₹" + fmtCrosshair, badgeX + 5, badgeY + 4);
                ctx.restore();
            }

            // Hovered candle index in allCandles
            const hoveredRelSlot = (mx - margin.left) / slotW;
            const hIdx = Math.floor(viewStart + hoveredRelSlot);
            if (hIdx >= 0 && hIdx < allCandles.length) {
                targetCandle = allCandles[hIdx];
                targetIdx = hIdx;

                // Time badge on bottom X-axis
                const d = new Date(targetCandle.time * 1000);
                const tStr = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
                ctx.save();
                ctx.fillStyle = targetCandle.is_forecast ? "#7e22ce" : "#1e293b";
                ctx.strokeStyle = targetCandle.is_forecast ? "#c084fc" : "#64748b";
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.roundRect(mx - 28, h - margin.bottom + 3, 56, 18, 4);
                ctx.fill();
                ctx.stroke();
                ctx.fillStyle = "#ffffff";
                ctx.font = "bold 10px JetBrains Mono, monospace";
                ctx.textAlign = "center";
                ctx.fillText(tStr, mx, h - margin.bottom + 15);
                ctx.restore();
            }
        }
    }

    // If not hovering over a valid candle, default to the latest real historical candle
    if (!targetCandle) {
        if (baseCandles.length > 0) {
            targetCandle = baseCandles[baseCandles.length - 1];
            targetIdx = baseCandles.length - 1;
        } else if (allCandles.length > 0) {
            targetCandle = allCandles[allCandles.length - 1];
            targetIdx = allCandles.length - 1;
        }
    }

    if (targetCandle) {
        const openEl = document.getElementById("lblCandleOpen");
        const highEl = document.getElementById("lblCandleHigh");
        const lowEl = document.getElementById("lblCandleLow");
        const closeEl = document.getElementById("lblCandleClose");
        const chgEl = document.getElementById("lblCandleChg");
        const priceEl = document.getElementById("lblCursorPrice");
        const timeEl = document.getElementById("lblCandleTime");
        const indStripEl = document.getElementById("lblIndValuesStrip");

        if (openEl) openEl.textContent = "₹" + targetCandle.open.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        if (highEl) highEl.textContent = "₹" + targetCandle.high.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        if (lowEl) lowEl.textContent = "₹" + targetCandle.low.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
        if (closeEl) closeEl.textContent = "₹" + targetCandle.close.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

        const diff = targetCandle.close - targetCandle.open;
        const diffPct = targetCandle.open > 0 ? (diff / targetCandle.open * 100) : 0;
        const sign = diff >= 0 ? "+" : "";
        if (chgEl) {
            chgEl.textContent = `${sign}₹${Math.abs(diff).toFixed(2)} (${sign}${diffPct.toFixed(2)}%)`;
            chgEl.style.color = diff >= 0 ? "#10b981" : "#f43f5e";
        }

        if (priceEl) {
            const p = (isHovering && crosshairPrice !== null) ? crosshairPrice : (targetCandle ? targetCandle.close : 0);
            if (p > 0) {
                priceEl.textContent = "₹" + Number(p).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
            }
        }

        if (timeEl) {
            const d = new Date(targetCandle.time * 1000);
            const tStr = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', hour12: false });
            const dateStr = d.toLocaleDateString([], { month: 'short', day: 'numeric' });
            if (targetCandle.is_forecast) {
                timeEl.textContent = `🔮 FORECAST (${tStr})`;
                timeEl.style.color = "#c084fc";
            } else if (isHovering) {
                timeEl.textContent = `${dateStr} ${tStr}`;
                timeEl.style.color = "#94a3b8";
            } else {
                timeEl.textContent = `${dateStr} ${tStr} (LIVE)`;
                timeEl.style.color = "#34d399";
            }
        }

        // Indicator badges in legend
        if (indStripEl) {
            const badges = [];
            const inds = window._chartActiveIndicators || {};
            if (targetIdx >= 0 && targetIdx < baseCandles.length) {
                if (inds.ema9 && baseCandles.length >= 9) {
                    const ema9 = calcChartEMA(baseCandles, 9);
                    if (ema9[targetIdx] !== null) {
                        badges.push(`<span style="color:#38bdf8; font-weight:700;">EMA9: ₹${ema9[targetIdx].toFixed(2)}</span>`);
                    }
                }
                if (inds.ema21 && baseCandles.length >= 21) {
                    const ema21 = calcChartEMA(baseCandles, 21);
                    if (ema21[targetIdx] !== null) {
                        badges.push(`<span style="color:#f59e0b; font-weight:700;">EMA21: ₹${ema21[targetIdx].toFixed(2)}</span>`);
                    }
                }
                if (inds.vwap && baseCandles.length > 0) {
                    const vwap = calcChartVWAP(baseCandles);
                    if (vwap[targetIdx] !== null) {
                        badges.push(`<span style="color:#ec4899; font-weight:700;">VWAP: ₹${vwap[targetIdx].toFixed(2)}</span>`);
                    }
                }
                if (inds.bb && baseCandles.length >= 20) {
                    const bb = calcChartBollingerBands(baseCandles, 20, 2);
                    if (bb.upper[targetIdx] !== null && bb.lower[targetIdx] !== null) {
                        badges.push(`<span style="color:#a855f7; font-weight:700;">BB: [${bb.lower[targetIdx].toFixed(1)} - ${bb.upper[targetIdx].toFixed(1)}]</span>`);
                    }
                }
            }
            indStripEl.innerHTML = badges.join("<span style='color:rgba(255,255,255,0.2); margin:0 2px;'>|</span>");
        }
    }
}
window.renderCandlestickCanvas = renderCandlestickCanvas;

function updateLiveCandleTick(newPrice) {
    if (!newPrice || Number(newPrice) <= 0) return;
    const p = Number(newPrice);
    const candles = window._activeCandles;
    if (candles && candles.length > 0) {
        const last = candles[candles.length - 1];
        if (last.close > 0) {
            const ratioDiff = Math.abs(p - last.close) / last.close;
            if (ratioDiff > 0.35) {
                console.warn(`[Chart] Ignoring anomalous tick ₹${p} (last was ₹${last.close})`);
                return;
            }
        }
        last.close = p;
        if (p > last.high) last.high = p;
        if (p < last.low) last.low = p;

        const ltpEl = document.getElementById("chartLtpDisplay");
        if (ltpEl) ltpEl.textContent = "₹" + p.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

        renderCandlestickCanvas();
    }
}

function setupChartCanvasEvents() {
    const canvas = document.getElementById("liveCandlestickCanvas");
    if (!canvas || canvas._eventsSetup) return;
    canvas._eventsSetup = true;

    // 1. Mouse Wheel Zoom (Price Scale vs Chart Body)
    canvas.addEventListener("wheel", (e) => {
        e.preventDefault();
        const totalBars = (window._activeCandles ? window._activeCandles.length : 0) +
            ((window._kronosForecastActive && window._kronosForecastData && window._kronosForecastData.candles) ? window._kronosForecastData.candles.length : 0);
        if (totalBars <= 0) return;

        const cData = window._chartData;
        if (!cData) return;

        const rect = canvas.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;
        const isPriceScale = mouseX >= rect.width - cData.padRight && mouseY >= cData.priceTop && mouseY <= cData.priceBottom;
        const zoomIn = e.deltaY < 0;

        if (isPriceScale) {
            const currentMin = cData.priceMin;
            const currentMax = cData.priceMax;
            const priceSpan = Math.max(currentMax - currentMin, 0.001);
            const clampedY = clamp(mouseY, cData.priceTop, cData.priceBottom);
            const anchorPrice = currentMax - ((clampedY - cData.priceTop) / cData.priceHeight) * priceSpan;
            const priceZoomFactor = zoomIn ? 0.85 : 1.15;
            const nextSpan = Math.max(priceSpan * priceZoomFactor, 0.001);
            window._chartPriceRange = {
                min: anchorPrice - ((anchorPrice - currentMin) / priceSpan) * nextSpan,
                max: anchorPrice + ((currentMax - anchorPrice) / priceSpan) * nextSpan
            };
            renderCandlestickCanvas();
            return;
        }

        const chartW = cData.chartWidth;
        if (chartW <= 0) return;
        const clampedMouseX = Math.max(cData.padLeft, Math.min(rect.width - cData.padRight, mouseX));
        const normX = (clampedMouseX - cData.padLeft) / chartW;

        const currentZoom = window._chartZoomCount || 80;
        const currentStart = (window._chartViewStart !== null) ? window._chartViewStart : Math.max(0, totalBars - currentZoom + 4);

        const factor = zoomIn ? 0.82 : 1.22;
        const maxZoom = Math.min(Math.max(30, totalBars + 8), 450);
        const newZoom = Math.max(15, Math.min(maxZoom, Math.round(currentZoom * factor)));

        if (newZoom !== currentZoom) {
            const barUnderCursor = currentStart + (normX * currentZoom);
            let newStart = barUnderCursor - (normX * newZoom);
            const minView = -Math.max(140, Math.round(newZoom * 2.2));
            const maxView = totalBars + Math.round(newZoom * 1.5);
            window._chartZoomCount = newZoom;
            window._chartViewStart = clamp(newStart, minView, maxView);
        }

        if (window._chartPriceRange) {
            const currentMin = window._chartPriceRange.min;
            const currentMax = window._chartPriceRange.max;
            const priceSpan = Math.max(currentMax - currentMin, 0.001);
            const clampedY = clamp(mouseY, cData.priceTop, cData.priceBottom);
            const anchorPrice = currentMax - ((clampedY - cData.priceTop) / cData.priceHeight) * priceSpan;
            const priceZoomFactor = zoomIn ? 0.85 : 1.15;
            const nextSpan = Math.max(priceSpan * priceZoomFactor, 0.001);
            window._chartPriceRange = {
                min: anchorPrice - ((anchorPrice - currentMin) / priceSpan) * nextSpan,
                max: anchorPrice + ((currentMax - anchorPrice) / priceSpan) * nextSpan
            };
        }

        renderCandlestickCanvas();
    }, { passive: false });

    // 2. Pointer Down:
    canvas.addEventListener("pointerdown", (e) => {
        if (e.button !== 0) return;
        const totalBars = (window._activeCandles ? window._activeCandles.length : 0) +
            ((window._kronosForecastActive && window._kronosForecastData && window._kronosForecastData.candles) ? window._kronosForecastData.candles.length : 0);
        if (totalBars <= 0) return;

        const cData = window._chartData;
        if (!cData) return;

        const rect = canvas.getBoundingClientRect();
        const pointerX = e.clientX - rect.left;
        const pointerY = e.clientY - rect.top;

        const isPriceScale = pointerX >= rect.width - cData.padRight && pointerY >= cData.priceTop && pointerY <= cData.priceBottom;

        if (isPriceScale) {
            const relativeY = clamp((pointerY - cData.priceTop) / cData.priceHeight, 0, 1);
            const priceSpan = Math.max(cData.priceMax - cData.priceMin, 0.001);
            window._chartDragState = {
                mode: "price",
                startY: e.clientY,
                initialMin: cData.priceMin,
                initialMax: cData.priceMax,
                anchorPrice: cData.priceMax - relativeY * priceSpan
            };
            window._chartIsDragging = true;
            try { canvas.setPointerCapture(e.pointerId); } catch (err) { }
            canvas.classList.add("is-price-scaling");
            canvas.style.cursor = "ns-resize";
            e.preventDefault();
            return;
        }

        const currentZoom = window._chartZoomCount || 80;
        const currentStart = (window._chartViewStart !== null) ? window._chartViewStart : Math.max(0, totalBars - currentZoom + 4);

        window._chartDragState = {
            mode: "pan",
            startX: e.clientX,
            startY: e.clientY,
            initialStart: currentStart,
            initialPriceMin: cData.priceMin,
            initialPriceMax: cData.priceMax,
            priceHeight: cData.priceHeight,
            visibleCount: currentZoom,
            chartWidth: cData.chartWidth
        };
        window._chartIsDragging = true;
        try { canvas.setPointerCapture(e.pointerId); } catch (err) { }
        canvas.classList.add("is-dragging");
        canvas.style.cursor = "grabbing";
        e.preventDefault();
    });

    // 3. Pointer Move:
    canvas.addEventListener("pointermove", (e) => {
        const rect = canvas.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;
        const cData = window._chartData;

        if (window._chartDragState) {
            if (window._chartDragState.mode === "price") {
                const zoomFactor = clamp(Math.exp((e.clientY - window._chartDragState.startY) / 180), 0.08, 12);
                window._chartPriceRange = {
                    min: window._chartDragState.anchorPrice - (window._chartDragState.anchorPrice - window._chartDragState.initialMin) * zoomFactor,
                    max: window._chartDragState.anchorPrice + (window._chartDragState.initialMax - window._chartDragState.anchorPrice) * zoomFactor
                };
                window._chartCrosshairPos = null;
                renderCandlestickCanvas();
                return;
            }

            if (window._chartDragState.mode === "pan") {
                const totalBars = (window._activeCandles ? window._activeCandles.length : 0) +
                    ((window._kronosForecastActive && window._kronosForecastData && window._kronosForecastData.candles) ? window._kronosForecastData.candles.length : 0);
                const minViewStart = -Math.max(140, Math.round(window._chartDragState.visibleCount * 2.2));
                const maxViewStart = totalBars + Math.round(window._chartDragState.visibleCount * 1.5);
                const deltaX = e.clientX - window._chartDragState.startX;
                const candleShift = (deltaX / window._chartDragState.chartWidth) * window._chartDragState.visibleCount;

                window._chartViewStart = clamp(window._chartDragState.initialStart - candleShift, minViewStart, maxViewStart);

                const deltaY = e.clientY - window._chartDragState.startY;
                const verticalShift = (deltaY / window._chartDragState.priceHeight) * (window._chartDragState.initialPriceMax - window._chartDragState.initialPriceMin);

                if (Math.abs(deltaY) > 2) {
                    window._chartPriceRange = {
                        min: window._chartDragState.initialPriceMin + verticalShift,
                        max: window._chartDragState.initialPriceMax + verticalShift
                    };
                }

                window._chartCrosshairPos = null;
                renderCandlestickCanvas();
                return;
            }
        }

        if (cData) {
            const isPriceScale = mouseX >= rect.width - cData.padRight && mouseY >= cData.priceTop && mouseY <= cData.priceBottom;
            if (isPriceScale) {
                canvas.style.cursor = "ns-resize";
            } else {
                canvas.style.cursor = "crosshair";
            }
        }

        window._chartCrosshairPos = { x: mouseX, y: mouseY };
        renderCandlestickCanvas();
    });

    const endDrag = (e) => {
        if (window._chartDragState) {
            const wasPrice = window._chartDragState.mode === "price";
            window._chartDragState = null;
            window._chartIsDragging = false;
            canvas.classList.remove("is-dragging");
            canvas.classList.remove("is-price-scaling");
            try { if (e && e.pointerId !== undefined) canvas.releasePointerCapture(e.pointerId); } catch (err) { }
            canvas.style.cursor = wasPrice ? "ns-resize" : "crosshair";
            renderCandlestickCanvas();
        }
    };
    canvas.addEventListener("pointerup", endDrag);
    canvas.addEventListener("pointercancel", endDrag);

    // 4. Double-Click to Reset:
    canvas.addEventListener("dblclick", (e) => {
        e.preventDefault();
        const cData = window._chartData;
        const rect = canvas.getBoundingClientRect();
        const pointerX = e.clientX - rect.left;
        const padRight = (cData && cData.padRight) ? cData.padRight : 88;
        if (pointerX >= rect.width - padRight) {
            resetPriceScaleOnly();
        } else {
            chartResetZoom();
        }
    });

    // 5. Right-Click Context Menu to Reset Chart View
    canvas.addEventListener("contextmenu", (e) => {
        e.preventDefault();
        chartResetZoom();
    });

    // 6. Mouse Leave
    canvas.addEventListener("mouseleave", () => {
        window._chartCrosshairPos = null;
        renderCandlestickCanvas();
    });

    // 7. Touch Events for Full Mobile / Tablet Support
    let _touchStartPos = null;
    let _touchPanState = null;
    let _touchMoved = false;
    let _initialPinchDist = 0;
    let _initialPinchZoom = 80;
    let _initialPinchStart = 0;
    let _pinchCenterRatio = 0.5;

    canvas.addEventListener("touchstart", (e) => {
        const cData = window._chartData;
        if (!cData) return;
        const rect = canvas.getBoundingClientRect();

        if (e.touches.length === 2) {
            const t1 = e.touches[0];
            const t2 = e.touches[1];
            _initialPinchDist = Math.hypot(t1.clientX - t2.clientX, t1.clientY - t2.clientY);
            _initialPinchZoom = window._chartZoomCount || 80;
            _initialPinchStart = window._chartViewStart !== null ? window._chartViewStart : 0;
            const midX = ((t1.clientX + t2.clientX) / 2) - rect.left;
            _pinchCenterRatio = clamp((midX - cData.padLeft) / cData.chartWidth, 0, 1);
        } else if (e.touches.length === 1) {
            const t = e.touches[0];
            const pX = t.clientX - rect.left;
            const pY = t.clientY - rect.top;
            const isScale = pX >= rect.width - cData.padRight && pY >= cData.priceTop && pY <= cData.priceBottom;
            _touchStartPos = { x: t.clientX, y: t.clientY, time: Date.now() };
            _touchMoved = false;

            const totalBars = (window._activeCandles ? window._activeCandles.length : 0) +
                ((window._kronosForecastActive && window._kronosForecastData && window._kronosForecastData.candles) ? window._kronosForecastData.candles.length : 0);
            const currentZoom = window._chartZoomCount || 80;
            const currentStart = (window._chartViewStart !== null) ? window._chartViewStart : Math.max(0, totalBars - currentZoom + 4);

            _touchPanState = {
                startX: t.clientX,
                startY: t.clientY,
                initialStart: currentStart,
                initialMin: cData.priceMin,
                initialMax: cData.priceMax,
                anchorPrice: cData.priceMax - clamp((pY - cData.priceTop) / cData.priceHeight, 0, 1) * (cData.priceMax - cData.priceMin),
                isPriceScale: isScale,
                visibleCount: currentZoom,
                chartWidth: cData.chartWidth,
                priceHeight: cData.priceHeight
            };
        }
    }, { passive: false });

    canvas.addEventListener("touchmove", (e) => {
        const totalBars = (window._activeCandles ? window._activeCandles.length : 0) +
            ((window._kronosForecastActive && window._kronosForecastData && window._kronosForecastData.candles) ? window._kronosForecastData.candles.length : 0);

        if (e.touches.length === 2 && _initialPinchDist > 5) {
            e.preventDefault();
            const t1 = e.touches[0];
            const t2 = e.touches[1];
            const currentDist = Math.hypot(t1.clientX - t2.clientX, t1.clientY - t2.clientY);

            if (currentDist > 8) {
                const scale = currentDist / _initialPinchDist;
                const maxZoom = Math.min(Math.max(30, totalBars + 8), 450);
                const nextZoom = clamp(Math.round(_initialPinchZoom / scale), 15, maxZoom);
                const centerCandleIdx = _initialPinchStart + _pinchCenterRatio * _initialPinchZoom;
                const nextStart = Math.round(centerCandleIdx - _pinchCenterRatio * nextZoom);
                const minView = -Math.max(140, Math.round(nextZoom * 2.2));
                const maxView = totalBars + Math.round(nextZoom * 1.5);

                window._chartZoomCount = nextZoom;
                window._chartViewStart = clamp(nextStart, minView, maxView);
                renderCandlestickCanvas();
            }
        } else if (e.touches.length === 1 && _touchPanState) {
            const t = e.touches[0];
            const deltaX = t.clientX - _touchStartPos.x;
            const deltaY = t.clientY - _touchStartPos.y;

            if (Math.hypot(deltaX, deltaY) > 6) {
                _touchMoved = true;
                e.preventDefault();

                if (_touchPanState.isPriceScale) {
                    const zoomFactor = clamp(Math.exp(deltaY / 180), 0.08, 12);
                    window._chartPriceRange = {
                        min: _touchPanState.anchorPrice - (_touchPanState.anchorPrice - _touchPanState.initialMin) * zoomFactor,
                        max: _touchPanState.anchorPrice + (_touchPanState.initialMax - _touchPanState.anchorPrice) * zoomFactor
                    };
                } else {
                    const candleShift = (deltaX / _touchPanState.chartWidth) * _touchPanState.visibleCount;
                    const minView = -Math.max(140, Math.round(_touchPanState.visibleCount * 2.2));
                    const maxView = totalBars + Math.round(_touchPanState.visibleCount * 1.5);
                    window._chartViewStart = clamp(_touchPanState.initialStart - candleShift, minView, maxView);

                    if (Math.abs(deltaY) > 2) {
                        const verticalShift = (deltaY / _touchPanState.priceHeight) * (_touchPanState.initialPriceMax - _touchPanState.initialPriceMin);
                        window._chartPriceRange = {
                            min: _touchPanState.initialMin + verticalShift,
                            max: _touchPanState.initialPriceMax + verticalShift
                        };
                    }
                }
                renderCandlestickCanvas();
            }
        }
    }, { passive: false });

    canvas.addEventListener("touchend", (e) => {
        if (e.touches.length === 0) {
            _touchPanState = null;
            _initialPinchDist = 0;
        }
    });
    canvas.addEventListener("touchcancel", (e) => {
        _touchPanState = null;
        _initialPinchDist = 0;
    });

    // 8. Window Resize
    window.addEventListener("resize", () => {
        const modal = document.getElementById("modalLiveChart");
        if (modal && modal.style.display === "flex") {
            renderCandlestickCanvas();
        }
    });
}

// LIVE CHART MODAL HEADER COLLAPSE / EXPAND TOGGLE & CANVAS SCALING
window._chartHeaderCollapsed = false;
window._chartCanvasHeight = 500;

function toggleChartHeaderCollapse() {
    window._chartHeaderCollapsed = !window._chartHeaderCollapsed;
    try {
        localStorage.setItem("straddle_chart_header_collapsed", window._chartHeaderCollapsed ? "true" : "false");
    } catch (e) { }
    applyChartHeaderCollapseState();
}
window.toggleChartHeaderCollapse = toggleChartHeaderCollapse;

function applyChartHeaderCollapseState() {
    try {
        const saved = localStorage.getItem("straddle_chart_header_collapsed");
        if (saved !== null) {
            window._chartHeaderCollapsed = (saved === "true");
        }
    } catch (e) { }

    const isCollapsed = !!window._chartHeaderCollapsed;
    const controlsRow = document.getElementById("chartControlsRow");
    const strip = document.getElementById("chartDerivativesStrip");
    const kronosPanel = document.getElementById("kronosForecastPanel");
    const aiPanel = document.getElementById("aiStrategistPanel");
    const icon = document.getElementById("iconChartHeaderCollapse");
    const lbl = document.getElementById("lblChartHeaderCollapse");
    const btn = document.getElementById("btnToggleChartHeader");

    if (controlsRow) {
        controlsRow.style.display = isCollapsed ? "none" : "flex";
    }
    if (strip) {
        strip.style.display = isCollapsed ? "none" : "flex";
    }
    if (kronosPanel && isCollapsed) {
        kronosPanel.style.display = "none";
    }
    if (aiPanel && isCollapsed) {
        aiPanel.style.display = "none";
    }

    if (icon) {
        icon.textContent = isCollapsed ? "▼" : "▲";
    }
    if (lbl) {
        lbl.textContent = isCollapsed ? "Expand" : "Compact";
    }
    if (btn) {
        btn.title = isCollapsed ? "Expand topbar to show controls, derivatives intelligence & AI models" : "Collapse topbar to 1st row only and maximize chart space";
    }

    // Dynamic responsive height for candlestick canvas
    if (window._chartIsFullscreen) {
        const headerOffset = isCollapsed ? 100 : 210;
        window._chartCanvasHeight = Math.max(450, window.innerHeight - headerOffset);
    } else {
        window._chartCanvasHeight = isCollapsed ? 680 : 500;
    }

    const canvas = document.getElementById("liveCandlestickCanvas");
    if (canvas) {
        canvas.style.height = window._chartCanvasHeight + "px";
        if (typeof renderCandlestickCanvas === "function") {
            renderCandlestickCanvas();
        }
    }
}
window.applyChartHeaderCollapseState = applyChartHeaderCollapseState;

// Topbar Underlying Selector Dropdown Handlers
function toggleTopbarAssetMenu(event) {
    if (event) event.stopPropagation();
    const dd = document.getElementById("topbarAssetDropdown");
    const btn = document.getElementById("btnTopbarAssetSelect");
    if (!dd) return;
    const isShown = dd.style.display === "block";
    dd.style.display = isShown ? "none" : "block";
    if (btn) btn.classList.toggle("open", !isShown);
    if (!isShown && typeof closeTopbarSettingsDropdown === "function") {
        closeTopbarSettingsDropdown();
    }
}
window.toggleTopbarAssetMenu = toggleTopbarAssetMenu;

function closeTopbarAssetMenu() {
    const dd = document.getElementById("topbarAssetDropdown");
    const btn = document.getElementById("btnTopbarAssetSelect");
    if (dd) dd.style.display = "none";
    if (btn) btn.classList.remove("open");
}
window.closeTopbarAssetMenu = closeTopbarAssetMenu;

window.openBasketOrdersModal = openBasketOrderModal;

document.addEventListener("click", function (e) {
    const wrap = document.getElementById("topbarAssetSelectorWrap");
    if (wrap && !wrap.contains(e.target)) {
        closeTopbarAssetMenu();
    }
});

// =========================================================================
// BASKET EXPIRY FILTER CONTROLLER
// =========================================================================

async function loadBasketExpiryFilterOptions(undParam) {
    const und = undParam || window._activeGlobalUnderlying || "NIFTY";
    const sel = document.getElementById("basketExpiryFilterSelect");
    if (!sel) return;
    try {
        const resp = await fetch(`/api/straddle/expiries?underlying=${encodeURIComponent(und)}`);
        const data = await resp.json();
        const currentVal = sel.value;
        sel.innerHTML = '<option value="ALL">All Expiries</option>';
        if (data.success && data.expiries && data.expiries.length > 0) {
            data.expiries.forEach(e => {
                const opt = document.createElement("option");
                opt.value = e.expiry_ms;
                opt.textContent = `${e.formatted_label} (${e.tag})`;
                sel.appendChild(opt);
            });
            if (currentVal && Array.from(sel.options).some(o => o.value === currentVal)) {
                sel.value = currentVal;
            }
        }
    } catch (e) {
        console.error("Error loading basket expiries:", e);
    }
}
window.loadBasketExpiryFilterOptions = loadBasketExpiryFilterOptions;

function onBasketExpiryFilterChange(val) {
    const input = document.getElementById("basketSearchInput");
    if (input) {
        const q = input.value.trim();
        handleBasketSearchInput(q || "ATM");
    }
}
window.onBasketExpiryFilterChange = onBasketExpiryFilterChange;


// =========================================================================
// EXECUTION BENCHMARK (KOTAK NEO LIVE BROKER vs PARALLEL PAPER SIMULATION)
// =========================================================================

async function openExecutionBenchmarkModal() {
    const modal = document.getElementById("modalExecutionBenchmark");
    if (!modal) return;
    modal.style.display = "flex";
    await loadExecutionBenchmarkData();
}
window.openExecutionBenchmarkModal = openExecutionBenchmarkModal;

function closeExecutionBenchmarkModal() {
    const modal = document.getElementById("modalExecutionBenchmark");
    if (modal) modal.style.display = "none";
}
window.closeExecutionBenchmarkModal = closeExecutionBenchmarkModal;

async function loadExecutionBenchmarkData() {
    try {
        const activeSid = window._straddleState?.activeStrategyId || window._straddleState?.activeStrategy?.strategy_id || "";
        const url = activeSid ? `/api/straddle/benchmark?strategy_id=${encodeURIComponent(activeSid)}` : "/api/straddle/benchmark";
        const resp = await fetch(url);
        const res = await resp.json();
        if (res.success && res.benchmark) {
            renderExecutionBenchmarkData(res.benchmark);
        }
    } catch (e) {
        console.error("Error loading benchmark data:", e);
    }
}
window.loadExecutionBenchmarkData = loadExecutionBenchmarkData;

function renderExecutionBenchmarkData(bmk) {
    const elAvg = document.getElementById("bmkAvgLatency");
    const elFastSlow = document.getElementById("bmkFastSlowLatency");
    const elSlipPts = document.getElementById("bmkTotalSlippagePts");
    const elSlipInr = document.getElementById("bmkTotalSlippageInr");
    const elGrade = document.getElementById("bmkHealthGrade");
    const tbody = document.getElementById("bmkOrdersTbody");

    if (elAvg) elAvg.textContent = `${(bmk.avg_latency_ms || 0).toFixed(1)} ms`;
    if (elFastSlow) elFastSlow.textContent = `${(bmk.fastest_latency_ms || 0).toFixed(0)} ms / ${(bmk.slowest_latency_ms || 0).toFixed(0)} ms`;

    const slipPts = Number(bmk.total_slippage_pts || 0);
    const slipInr = Number(bmk.total_slippage_inr || 0);
    if (elSlipPts) {
        elSlipPts.textContent = `${slipPts >= 0 ? '+' : ''}${slipPts.toFixed(2)} pts`;
        elSlipPts.style.color = slipPts >= 0 ? "#10b981" : "#f43f5e";
    }
    if (elSlipInr) {
        elSlipInr.textContent = `₹${slipInr >= 0 ? '+' : ''}${slipInr.toFixed(2)}`;
        elSlipInr.style.color = slipInr >= 0 ? "#10b981" : "#f43f5e";
    }

    if (elGrade) {
        const g = bmk.health_grade || "GOOD";
        if (g === "ULTRA_FAST") {
            elGrade.textContent = "⚡ ULTRA-FAST (<50ms)";
            elGrade.style.color = "#38bdf8";
        } else if (g === "GOOD") {
            elGrade.textContent = "🟢 GOOD (50-100ms)";
            elGrade.style.color = "#10b981";
        } else if (g === "MODERATE") {
            elGrade.textContent = "🟡 MODERATE (100-200ms)";
            elGrade.style.color = "#fbbf24";
        } else {
            elGrade.textContent = "🔴 HIGH LATENCY (>200ms)";
            elGrade.style.color = "#f43f5e";
        }
    }

    if (!tbody) return;
    const orders = bmk.orders || [];
    if (orders.length === 0) {
        tbody.innerHTML = '<tr><td colspan="12" style="text-align:center; padding:28px; color:#64748b;">No benchmark executions recorded yet. Start live trading to observe parallel benchmark comparison.</td></tr>';
        return;
    }

    const _msColor = (msVal) => msVal <= 50 ? '#38bdf8' : (msVal <= 150 ? '#fbbf24' : '#f43f5e');

    let html = "";
    orders.slice().reverse().forEach(o => {
        const sPts = Number(o.slippage_pts || 0);
        const sInr = Number(o.slippage_inr || 0);
        const sColor = sPts >= 0 ? '#10b981' : '#f43f5e';
        const isFilled = o.status === "FILLED";
        const stColor = isFilled ? '#10b981' : '#f43f5e';
        const sideColor = String(o.side).toUpperCase() === 'BUY' ? '#38bdf8' : '#fb923c';

        const sigTime = o.signal_time || o.dispatch_time || '-';
        const bSentTime = o.broker_sent_time || o.dispatch_time || '-';
        const bSendMsVal = (o.broker_send_ms !== undefined && o.broker_send_ms !== null)
            ? Number(o.broker_send_ms)
            : Number(o.broker_ack_ms !== undefined ? o.broker_ack_ms : (o.broker_latency_ms || 0));
        const exFillTime = o.exchange_filled_time || o.broker_fill_time || '-';
        const exFillMsVal = (o.exchange_fill_ms !== undefined && o.exchange_fill_ms !== null)
            ? Number(o.exchange_fill_ms)
            : null;
        const exFillBracket = exFillMsVal !== null ? `<span style="color:${_msColor(exFillMsVal)}; font-weight:700;">(${exFillMsVal.toFixed(0)} ms)</span>` : '';

        html += `
            <tr style="border-bottom:1px solid rgba(255,255,255,0.05);">
                <td style="padding:6px 4px; color:#94a3b8; white-space:nowrap;">${sigTime}</td>
                <td style="padding:6px 4px; color:#cbd5e1; white-space:nowrap;">${bSentTime} <span style="color:${_msColor(bSendMsVal)}; font-weight:700;">(${bSendMsVal.toFixed(0)} ms)</span></td>
                <td style="padding:6px 4px; color:#cbd5e1; white-space:nowrap;">${exFillTime} ${exFillBracket}</td>
                <td style="padding:6px 4px; font-weight:700; color:#f8fafc;">${formatSymbolColorCoded(o.symbol)}</td>
                <td style="padding:6px 4px;"><span style="color:${sideColor}; font-weight:800;">${o.side}</span></td>
                <td style="padding:6px 4px; color:#cbd5e1;">${o.action || 'ENTRY'}</td>
                <td style="padding:6px 4px; text-align:right; color:#94a3b8;">₹${Number(o.paper_price || 0).toFixed(2)}</td>
                <td style="padding:6px 4px; text-align:right; font-weight:700; color:#f8fafc;">₹${Number(o.broker_price || 0).toFixed(2)}</td>
                <td style="padding:6px 4px; text-align:right; color:${_msColor(Number(o.broker_latency_ms || 0))}; font-weight:700;">${Number(o.broker_latency_ms || 0).toFixed(0)} ms</td>
                <td style="padding:6px 4px; text-align:right; color:${sColor}; font-weight:700;">${sPts >= 0 ? '+' : ''}${sPts.toFixed(2)}</td>
                <td style="padding:6px 4px; text-align:right; color:${sColor}; font-weight:700;">₹${sInr >= 0 ? '+' : ''}${sInr.toFixed(2)}</td>
                <td style="padding:6px 4px; text-align:center;"><span style="color:${stColor}; font-weight:800;">${o.status || 'FILLED'}</span></td>
            </tr>
        `;
    });
    tbody.innerHTML = html;
}

function downloadExecutionBenchmarkReport() {
    const activeSid = window._straddleState?.activeStrategyId || window._straddleState?.activeStrategy?.strategy_id || "";
    const url = activeSid ? `/api/straddle/download_benchmark_report?strategy_id=${encodeURIComponent(activeSid)}` : "/api/straddle/download_benchmark_report";
    window.location.href = url;
}
window.downloadExecutionBenchmarkReport = downloadExecutionBenchmarkReport;
