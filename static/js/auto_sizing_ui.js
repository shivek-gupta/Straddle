/**
 * UI State, Serialization & Live Status Badges for:
 * - Day-Lock Symmetry (+ Auto-Trim)
 * - Profit-Released Cushion
 * - T-10s Pre-Flight Margin Sync
 */
(function () {
    "use strict";

    function onAutoSizingFieldChange() {
        const isAutoMode = document.getElementById("grpAutoMultiplier")?.style.display !== "none";
        const dayLock = !!document.getElementById("cfgToggleDayLockSymmetry")?.checked;
        const autoTrim = document.getElementById("cfgToggleAutoTrimSymmetric");
        if (autoTrim) autoTrim.disabled = !dayLock;

        const bDayLock = document.getElementById("badgeDayLockStatus");
        if (bDayLock) {
            const active = window._straddleState?.activeStrategy;
            const lockedLots = active?.locked_day_lots;
            if (!isAutoMode && dayLock) {
                bDayLock.textContent = "AUTO MODE ONLY";
                bDayLock.style.color = "#fbbf24";
                bDayLock.style.background = "rgba(251,191,36,0.15)";
            } else if (dayLock) {
                bDayLock.textContent = lockedLots ? `LOCKED: ${lockedLots}x` : "ACTIVE";
                bDayLock.style.color = "#38bdf8";
                bDayLock.style.background = "rgba(56,189,248,0.15)";
            } else {
                bDayLock.textContent = "OFF";
                bDayLock.style.color = "#64748b";
                bDayLock.style.background = "rgba(100,116,139,0.15)";
            }
        }

        const profCush = !!document.getElementById("cfgToggleProfitCushion")?.checked;
        const trig = document.getElementById("cfgCushionReleaseTrigger")?.value || "MTM_THRESHOLD";
        const mtmEl = document.getElementById("cfgCushionReleaseMtmThreshold");
        const timeEl = document.getElementById("cfgCushionReleaseTime");
        const redEl = document.getElementById("cfgReducedCushionMargin");
        const trigEl = document.getElementById("cfgCushionReleaseTrigger");
        const latchEl = document.getElementById("cfgToggleCushionReleaseLatch");

        if (redEl) redEl.disabled = !profCush;
        if (trigEl) trigEl.disabled = !profCush;
        if (latchEl) latchEl.disabled = !profCush;
        if (mtmEl) mtmEl.disabled = !profCush || !["MTM_THRESHOLD", "BOTH_EITHER"].includes(trig);
        if (timeEl) timeEl.disabled = !profCush || trig !== "TIME_AFTER";

        const bCush = document.getElementById("badgeProfitCushionStatus");
        if (bCush) {
            const active = window._straddleState?.activeStrategy;
            if (!isAutoMode && profCush) {
                bCush.textContent = "AUTO MODE ONLY";
                bCush.style.color = "#fbbf24";
                bCush.style.background = "rgba(251,191,36,0.15)";
            } else if (profCush) {
                if (active?.cushion_released_active) {
                    bCush.textContent = `RELEASED (₹${redEl?.value || 3000})`;
                    bCush.style.color = "#10b981";
                    bCush.style.background = "rgba(16,185,129,0.18)";
                } else {
                    bCush.textContent = "ARMED";
                    bCush.style.color = "#10b981";
                    bCush.style.background = "rgba(16,185,129,0.12)";
                }
            } else {
                bCush.textContent = "OFF";
                bCush.style.color = "#64748b";
                bCush.style.background = "rgba(100,116,139,0.15)";
            }
        }

        if (typeof updateFormAutoMultiplierPreview === "function") {
            updateFormAutoMultiplierPreview();
        }

        const pfOn = !!document.getElementById("cfgTogglePreflightMarginSync")?.checked;
        const pfLead = document.getElementById("cfgPreflightLeadSec");
        const pfLive = document.getElementById("cfgTogglePreflightLiveQuotes");
        if (pfLead) pfLead.disabled = !pfOn;
        if (pfLive) pfLive.disabled = !pfOn;

        const bPf = document.getElementById("badgePreflightStatus");
        if (bPf) {
            if (pfOn) {
                bPf.textContent = `T-${pfLead?.value || 10}s ACTIVE`;
                bPf.style.color = "#f59e0b";
                bPf.style.background = "rgba(245,158,11,0.15)";
            } else {
                bPf.textContent = "OFF";
                bPf.style.color = "#64748b";
                bPf.style.background = "rgba(100,116,139,0.15)";
            }
        }
        syncSec8MultiplierBar();
    }

    function syncSec8MultiplierBar() {
        const isAutoMode = document.getElementById("grpAutoMultiplier")?.style.display !== "none";
        const btnMan = document.getElementById("sec8BtnMultManual");
        const btnAuto = document.getElementById("sec8BtnMultAuto");
        const grpMan = document.getElementById("sec8GrpManualMult");
        const grpAuto = document.getElementById("sec8GrpAutoCushion");

        if (btnMan) {
            btnMan.style.background = !isAutoMode ? "#2563eb" : "#161822";
            btnMan.style.color = !isAutoMode ? "#fff" : "#94a3b8";
        }
        if (btnAuto) {
            btnAuto.style.background = isAutoMode ? "#059669" : "#161822";
            btnAuto.style.color = isAutoMode ? "#fff" : "#94a3b8";
        }
        if (grpMan) grpMan.style.display = !isAutoMode ? "flex" : "none";
        if (grpAuto) grpAuto.style.display = isAutoMode ? "flex" : "none";

        const srcMult = document.getElementById("cfgMultiplier");
        const dstMult = document.getElementById("sec8MultiplierInput");
        if (srcMult && dstMult && document.activeElement !== dstMult) {
            dstMult.value = srcMult.value || 1;
        }

        const srcCush = document.getElementById("cfgCushionMargin");
        const dstCush = document.getElementById("sec8CushionMarginInput");
        if (srcCush && dstCush && document.activeElement !== dstCush) {
            dstCush.value = srcCush.value || 15000;
        }

        const srcBadge = document.getElementById("cfgAutoMultBadge");
        const dstBadge = document.getElementById("sec8AutoMultBadge");
        if (srcBadge && dstBadge) {
            dstBadge.innerHTML = srcBadge.innerHTML;
            dstBadge.style.color = srcBadge.style.color;
            dstBadge.style.background = srcBadge.style.background;
            dstBadge.title = srcBadge.title;
        }
    }

    function syncSec8ManualMult(val) {
        const num = Math.max(1, parseInt(val, 10) || 1);
        const srcMult = document.getElementById("cfgMultiplier");
        if (srcMult) srcMult.value = num;
        if (typeof onFormManualMultiplierChange === "function") {
            onFormManualMultiplierChange(num);
        }
        syncSec8MultiplierBar();
    }

    function syncSec8CushionMargin(val) {
        const num = Math.max(0, parseFloat(val) || 0);
        const srcCush = document.getElementById("cfgCushionMargin");
        if (srcCush) srcCush.value = num;
        if (typeof onFormCushionMarginChange === "function") {
            onFormCushionMarginChange(num);
        }
        setTimeout(syncSec8MultiplierBar, 10);
    }

    function populateAutoSizingForm(s) {
        const adv = s?.advanced_settings || s?.advanced || {};
        const elDayLock = document.getElementById("cfgToggleDayLockSymmetry");
        const elAutoTrim = document.getElementById("cfgToggleAutoTrimSymmetric");
        const elProfCush = document.getElementById("cfgToggleProfitCushion");
        const elRedCush = document.getElementById("cfgReducedCushionMargin");
        const elTrig = document.getElementById("cfgCushionReleaseTrigger");
        const elMtm = document.getElementById("cfgCushionReleaseMtmThreshold");
        const elTime = document.getElementById("cfgCushionReleaseTime");
        const elLatch = document.getElementById("cfgToggleCushionReleaseLatch");
        const elPfSync = document.getElementById("cfgTogglePreflightMarginSync");
        const elPfLead = document.getElementById("cfgPreflightLeadSec");
        const elPfLive = document.getElementById("cfgTogglePreflightLiveQuotes");

        if (elDayLock) elDayLock.checked = !!adv.day_lock_symmetry;
        if (elAutoTrim) elAutoTrim.checked = !!adv.auto_trim_symmetric;
        if (elProfCush) elProfCush.checked = !!adv.profit_released_cushion;
        if (elRedCush) elRedCush.value = (adv.reduced_cushion_margin !== undefined && adv.reduced_cushion_margin !== null) ? adv.reduced_cushion_margin : 3000;
        if (elTrig) elTrig.value = adv.cushion_release_trigger || "MTM_THRESHOLD";
        if (elMtm) elMtm.value = (adv.cushion_release_mtm_threshold !== undefined && adv.cushion_release_mtm_threshold !== null) ? adv.cushion_release_mtm_threshold : 100000;
        if (elTime) elTime.value = adv.cushion_release_time || "12:30:00";
        if (elLatch) elLatch.checked = (adv.cushion_release_latch !== false);
        if (elPfSync) elPfSync.checked = !!adv.preflight_margin_sync;
        if (elPfLead) elPfLead.value = (adv.preflight_lead_sec !== undefined && adv.preflight_lead_sec !== null) ? adv.preflight_lead_sec : 10;
        if (elPfLive) elPfLive.checked = (adv.preflight_use_live_quotes !== false);

        onAutoSizingFieldChange();
    }

    function serializeAutoSizingSettings() {
        const redParsed = parseFloat(document.getElementById("cfgReducedCushionMargin")?.value);
        const mtmParsed = parseFloat(document.getElementById("cfgCushionReleaseMtmThreshold")?.value);
        const leadParsed = parseInt(document.getElementById("cfgPreflightLeadSec")?.value, 10);
        return {
            day_lock_symmetry: !!document.getElementById("cfgToggleDayLockSymmetry")?.checked,
            auto_trim_symmetric: !!document.getElementById("cfgToggleAutoTrimSymmetric")?.checked,
            profit_released_cushion: !!document.getElementById("cfgToggleProfitCushion")?.checked,
            reduced_cushion_margin: !isNaN(redParsed) ? Math.max(0, redParsed) : 3000,
            cushion_release_trigger: document.getElementById("cfgCushionReleaseTrigger")?.value || "MTM_THRESHOLD",
            cushion_release_mtm_threshold: !isNaN(mtmParsed) ? Math.max(0, mtmParsed) : 100000,
            cushion_release_time: document.getElementById("cfgCushionReleaseTime")?.value || "12:30:00",
            cushion_release_latch: document.getElementById("cfgToggleCushionReleaseLatch")?.checked !== false,
            preflight_margin_sync: !!document.getElementById("cfgTogglePreflightMarginSync")?.checked,
            preflight_lead_sec: !isNaN(leadParsed) ? Math.max(3, Math.min(60, leadParsed)) : 10,
            preflight_use_live_quotes: document.getElementById("cfgTogglePreflightLiveQuotes")?.checked !== false
        };
    }

    window.onAutoSizingFieldChange = onAutoSizingFieldChange;
    window.populateAutoSizingForm = populateAutoSizingForm;
    window.serializeAutoSizingSettings = serializeAutoSizingSettings;
    window.syncSec8MultiplierBar = syncSec8MultiplierBar;
    window.syncSec8ManualMult = syncSec8ManualMult;
    window.syncSec8CushionMargin = syncSec8CushionMargin;
})();
