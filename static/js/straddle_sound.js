/**
 * Straddle Pro - High-Fidelity Audio Notification Engine
 * Zero-dependency Web Audio API synthesizer for Stoploss, Target, Errors, Order Fills, and Exits.
 * Works seamlessly in all modern browsers without external audio asset downloads.
 */

(function () {
    "use strict";

    const AudioContextClass = window.AudioContext || window.webkitAudioContext;

    // Default Configuration
    const DEFAULT_CONFIG = {
        enabled: true,
        volume: 0.85, // 0.0 to 1.0
        categories: {
            target: true,     // Target Hit / Win
            stoploss: true,   // Stop Loss Triggered
            error: true,      // Order Rejected / Error / Core Failure
            order: true,      // New Order Placed / Filled
            exit: true,       // Position Exited / Squared Off
            warning: true     // Unexpected Cancellation / Margin Warning
        }
    };

    // Load persisted settings or fallback
    function loadConfig() {
        try {
            const raw = localStorage.getItem("straddle_sound_config");
            if (raw) {
                const parsed = JSON.parse(raw);
                return {
                    enabled: parsed.enabled !== false,
                    volume: typeof parsed.volume === "number" ? Math.min(1, Math.max(0, parsed.volume)) : DEFAULT_CONFIG.volume,
                    categories: {
                        ...DEFAULT_CONFIG.categories,
                        ...(parsed.categories || {})
                    }
                };
            }
        } catch (e) {
            console.debug("[StraddleSound] Failed loading audio config:", e);
        }
        return { ...DEFAULT_CONFIG, categories: { ...DEFAULT_CONFIG.categories } };
    }

    function saveConfig(cfg) {
        try {
            localStorage.setItem("straddle_sound_config", JSON.stringify(cfg));
        } catch (e) {
            console.debug("[StraddleSound] Failed saving audio config:", e);
        }
    }

    class SoundManager {
        constructor() {
            this.config = loadConfig();
            this.ctx = null;
            this.isUnlocked = false;
            this._lastPlayedTimes = {};
            this._setupAutoUnlock();
        }

        /**
         * Initialize AudioContext lazily on user gesture.
         */
        getAudioContext() {
            if (!this.ctx && AudioContextClass) {
                try {
                    this.ctx = new AudioContextClass();
                } catch (e) {
                    console.warn("[StraddleSound] Could not create AudioContext:", e);
                }
            }
            if (this.ctx && this.ctx.state === "suspended") {
                this.ctx.resume().catch(() => {});
            }
            return this.ctx;
        }

        _setupAutoUnlock() {
            const unlock = () => {
                const ctx = this.getAudioContext();
                if (ctx && ctx.state !== "closed") {
                    ctx.resume().then(() => {
                        this.isUnlocked = true;
                    }).catch(() => {});
                }
                // Remove listeners once unlocked
                ["click", "keydown", "touchstart", "pointerdown"].forEach(evt => {
                    window.removeEventListener(evt, unlock, true);
                });
            };
            ["click", "keydown", "touchstart", "pointerdown"].forEach(evt => {
                window.addEventListener(evt, unlock, { capture: true, once: true });
            });
        }

        isEnabled() {
            return this.config.enabled;
        }

        toggleMute() {
            this.config.enabled = !this.config.enabled;
            saveConfig(this.config);
            this.updateUiState();
            if (this.config.enabled) {
                this.play("order"); // Friendly feedback beep on enable
            }
            return this.config.enabled;
        }

        setVolume(val) {
            const num = parseFloat(val);
            if (!isNaN(num)) {
                this.config.volume = Math.min(1.0, Math.max(0.0, num));
                saveConfig(this.config);
            }
        }

        setCategoryEnabled(cat, isEnabled) {
            if (this.config.categories[cat] !== undefined) {
                this.config.categories[cat] = Boolean(isEnabled);
                saveConfig(this.config);
            }
        }

        /**
         * Master synthesizer router
         * @param {string} type 'target' | 'stoploss' | 'error' | 'order' | 'exit' | 'warning'
         * @param {boolean} force Ignore mute check (for preview test buttons)
         */
        play(type, force = false) {
            if (!force && !this.config.enabled) return;
            const cat = type === "order_filled" ? "order" : type;
            if (!force && this.config.categories[cat] === false) return;

            // Debounce rapid bursts of the same sound within 200ms
            const now = Date.now();
            if (!force && this._lastPlayedTimes[type] && (now - this._lastPlayedTimes[type] < 200)) {
                return;
            }
            this._lastPlayedTimes[type] = now;

            const ctx = this.getAudioContext();
            if (!ctx) return;
            if (ctx.state === "suspended") {
                ctx.resume().catch(() => {});
            }

            try {
                switch (type) {
                    case "target":
                        this._synthTargetHit(ctx);
                        break;
                    case "stoploss":
                    case "sl":
                        this._synthStoplossHit(ctx);
                        break;
                    case "error":
                    case "reject":
                    case "rejected":
                        this._synthErrorAlert(ctx);
                        break;
                    case "order":
                    case "order_filled":
                    case "fill":
                    case "entry":
                        this._synthOrderFilled(ctx);
                        break;
                    case "exit":
                    case "squareoff":
                        this._synthExitChime(ctx);
                        break;
                    case "warning":
                    case "caution":
                        this._synthWarningChime(ctx);
                        break;
                    default:
                        this._synthOrderFilled(ctx);
                        break;
                }
            } catch (e) {
                console.debug("[StraddleSound] Error synthesizing sound:", e);
            }
        }

        // =========================================================================
        // WEB AUDIO SYNTHESIZERS
        // =========================================================================

        /**
         * 🎯 TARGET HIT: Inspiring 4-note ascending major arpeggio with golden resonance.
         * Frequencies: C5 (523.25Hz) -> E5 (659.25Hz) -> G5 (783.99Hz) -> C6 (1046.50Hz)
         */
        _synthTargetHit(ctx) {
            const notes = [523.25, 659.25, 783.99, 1046.50];
            const startTime = ctx.currentTime;
            const masterVol = this.config.volume;

            notes.forEach((freq, idx) => {
                const noteStart = startTime + (idx * 0.08);
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();

                osc.type = "sine";
                osc.frequency.setValueAtTime(freq, noteStart);

                // Subtle warm harmonic
                const harmonic = ctx.createOscillator();
                harmonic.type = "triangle";
                harmonic.frequency.setValueAtTime(freq * 2, noteStart);
                const harmGain = ctx.createGain();
                harmGain.gain.setValueAtTime(0.15 * masterVol, noteStart);
                harmonic.connect(harmGain);
                harmGain.connect(ctx.destination);

                gain.gain.setValueAtTime(0.0001, noteStart);
                gain.gain.exponentialRampToValueAtTime(0.6 * masterVol, noteStart + 0.015);
                gain.gain.exponentialRampToValueAtTime(0.0001, noteStart + 0.45);

                osc.connect(gain);
                gain.connect(ctx.destination);

                osc.start(noteStart);
                harmonic.start(noteStart);
                osc.stop(noteStart + 0.48);
                harmonic.stop(noteStart + 0.48);
            });
        }

        /**
         * 🛑 STOPLOSS HIT: Distinct 3-tone descending warning alert.
         * Frequencies: G4 (392Hz) -> Eb4 (311.13Hz) -> C4 (261.63Hz) with filtered sawtooth.
         */
        _synthStoplossHit(ctx) {
            const notes = [392.00, 311.13, 261.63];
            const startTime = ctx.currentTime;
            const masterVol = this.config.volume;

            notes.forEach((freq, idx) => {
                const noteStart = startTime + (idx * 0.11);
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                const filter = ctx.createBiquadFilter();

                osc.type = "sawtooth";
                osc.frequency.setValueAtTime(freq, noteStart);

                filter.type = "lowpass";
                filter.frequency.setValueAtTime(1200, noteStart);
                filter.frequency.exponentialRampToValueAtTime(400, noteStart + 0.28);

                gain.gain.setValueAtTime(0.0001, noteStart);
                gain.gain.exponentialRampToValueAtTime(0.45 * masterVol, noteStart + 0.02);
                gain.gain.exponentialRampToValueAtTime(0.0001, noteStart + 0.30);

                osc.connect(filter);
                filter.connect(gain);
                gain.connect(ctx.destination);

                osc.start(noteStart);
                osc.stop(noteStart + 0.32);
            });
        }

        /**
         * ⚠️ ORDER REJECTED / ERROR: Urgent dual-pulse low-mid frequency buzz.
         * Frequencies: Downward sweep 480Hz -> 220Hz in two quick pulses.
         */
        _synthErrorAlert(ctx) {
            const pulses = [0, 0.14];
            const startTime = ctx.currentTime;
            const masterVol = this.config.volume;

            pulses.forEach((offset) => {
                const pStart = startTime + offset;
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                const filter = ctx.createBiquadFilter();

                osc.type = "sawtooth";
                osc.frequency.setValueAtTime(480, pStart);
                osc.frequency.exponentialRampToValueAtTime(220, pStart + 0.11);

                filter.type = "lowpass";
                filter.frequency.setValueAtTime(900, pStart);

                gain.gain.setValueAtTime(0.0001, pStart);
                gain.gain.exponentialRampToValueAtTime(0.55 * masterVol, pStart + 0.015);
                gain.gain.exponentialRampToValueAtTime(0.0001, pStart + 0.11);

                osc.connect(filter);
                filter.connect(gain);
                gain.connect(ctx.destination);

                osc.start(pStart);
                osc.stop(pStart + 0.12);
            });
        }

        /**
         * ⚡ ORDER PLACED / FILLED: Clean modern high-tech affirmative ping.
         * Frequencies: A5 (880Hz) quick blip into A6 (1760Hz) sine bell.
         */
        _synthOrderFilled(ctx) {
            const startTime = ctx.currentTime;
            const masterVol = this.config.volume;

            // Note 1: 880Hz
            const osc1 = ctx.createOscillator();
            const gain1 = ctx.createGain();
            osc1.type = "sine";
            osc1.frequency.setValueAtTime(880, startTime);
            gain1.gain.setValueAtTime(0.0001, startTime);
            gain1.gain.exponentialRampToValueAtTime(0.35 * masterVol, startTime + 0.01);
            gain1.gain.exponentialRampToValueAtTime(0.0001, startTime + 0.08);
            osc1.connect(gain1);
            gain1.connect(ctx.destination);
            osc1.start(startTime);
            osc1.stop(startTime + 0.09);

            // Note 2: 1760Hz (Bell decay)
            const osc2 = ctx.createOscillator();
            const gain2 = ctx.createGain();
            const t2 = startTime + 0.06;
            osc2.type = "sine";
            osc2.frequency.setValueAtTime(1760, t2);
            gain2.gain.setValueAtTime(0.0001, t2);
            gain2.gain.exponentialRampToValueAtTime(0.5 * masterVol, t2 + 0.01);
            gain2.gain.exponentialRampToValueAtTime(0.0001, t2 + 0.35);
            osc2.connect(gain2);
            gain2.connect(ctx.destination);
            osc2.start(t2);
            osc2.stop(t2 + 0.38);
        }

        /**
         * ⏹️ POSITION EXIT / SQUARE-OFF: Smooth 2-tone closing chime.
         * Frequencies: D5 (587.33Hz) -> A4 (440Hz).
         */
        _synthExitChime(ctx) {
            const startTime = ctx.currentTime;
            const masterVol = this.config.volume;

            [587.33, 440.00].forEach((freq, idx) => {
                const noteStart = startTime + (idx * 0.12);
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.type = "sine";
                osc.frequency.setValueAtTime(freq, noteStart);

                gain.gain.setValueAtTime(0.0001, noteStart);
                gain.gain.exponentialRampToValueAtTime(0.4 * masterVol, noteStart + 0.02);
                gain.gain.exponentialRampToValueAtTime(0.0001, noteStart + 0.32);

                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.start(noteStart);
                osc.stop(noteStart + 0.35);
            });
        }

        /**
         * 🔔 WARNING / CAUTION: Warm 2-tone notification ping.
         * Frequencies: 659.25Hz (E5) -> 880Hz (A5).
         */
        _synthWarningChime(ctx) {
            const startTime = ctx.currentTime;
            const masterVol = this.config.volume;

            [659.25, 880.00].forEach((freq, idx) => {
                const noteStart = startTime + (idx * 0.09);
                const osc = ctx.createOscillator();
                const gain = ctx.createGain();
                osc.type = "triangle";
                osc.frequency.setValueAtTime(freq, noteStart);

                gain.gain.setValueAtTime(0.0001, noteStart);
                gain.gain.exponentialRampToValueAtTime(0.35 * masterVol, noteStart + 0.015);
                gain.gain.exponentialRampToValueAtTime(0.0001, noteStart + 0.28);

                osc.connect(gain);
                gain.connect(ctx.destination);
                osc.start(noteStart);
                osc.stop(noteStart + 0.30);
            });
        }

        // =========================================================================
        // UI SYNC & CONTROLS
        // =========================================================================

        updateUiState() {
            const isEn = this.config.enabled;
            const topbarBtn = document.getElementById("topbarSoundBtn");
            if (topbarBtn) {
                topbarBtn.innerHTML = isEn
                    ? `<span>🔊</span> <span style="font-size:10.5px;">Sound: ON</span>`
                    : `<span>🔇</span> <span style="font-size:10.5px; opacity:0.65;">Sound: OFF</span>`;
                topbarBtn.style.color = isEn ? "#38bdf8" : "#94a3b8";
                topbarBtn.style.borderColor = isEn ? "#0284c7" : "#334155";
                topbarBtn.title = isEn ? "Sound Notifications Active (Click to Mute)" : "Sound Notifications Muted (Click to Unmute)";
            }

            const terminalBtn = document.getElementById("terminalSoundBtn");
            if (terminalBtn) {
                terminalBtn.innerHTML = isEn ? `🔊 Sound` : `🔇 Muted`;
                terminalBtn.style.color = isEn ? "#38bdf8" : "#64748b";
                terminalBtn.style.borderColor = isEn ? "#0284c7" : "#334155";
            }

            // Sync modal controls if open
            const masterToggle = document.getElementById("soundModalMasterToggle");
            if (masterToggle) masterToggle.checked = isEn;

            const volSlider = document.getElementById("soundModalVolumeSlider");
            const volLabel = document.getElementById("soundModalVolumeLabel");
            if (volSlider) volSlider.value = Math.round(this.config.volume * 100);
            if (volLabel) volLabel.textContent = `${Math.round(this.config.volume * 100)}%`;

            for (const [k, v] of Object.entries(this.config.categories)) {
                const cb = document.getElementById(`soundCat_${k}`);
                if (cb) cb.checked = v;
            }
        }
    }

    // Singleton instance
    const instance = new SoundManager();
    window.StraddleSound = instance;

    // Helper exposed globally for one-click toggling
    window.toggleStraddleSound = function (e) {
        if (e && e.stopPropagation) e.stopPropagation();
        const state = instance.toggleMute();
        if (typeof showStraddleToast === "function") {
            showStraddleToast(state ? "🔊 Notification sounds enabled" : "🔇 Notification sounds muted", "info", 1500);
        }
    };

    window.openStraddleSoundModal = function (e) {
        if (e && e.stopPropagation) e.stopPropagation();
        const modal = document.getElementById("straddleSoundModal");
        if (modal) {
            instance.updateUiState();
            modal.style.display = "flex";
        }
    };

    window.closeStraddleSoundModal = function () {
        const modal = document.getElementById("straddleSoundModal");
        if (modal) modal.style.display = "none";
    };

    // Run UI sync when DOM is ready
    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", () => instance.updateUiState());
    } else {
        instance.updateUiState();
    }
})();
