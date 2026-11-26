/**
 * Comprehensive Hover Help Dot (?) System for Straddle Pro Algo Builder.
 * Attaches a uniform (?) badge to every single option, toggle, input, and table column,
 * displaying a detailed explanation and concrete trading example on hover/focus.
 */
(function () {
    "use strict";

    const ALGO_TOOLTIPS = {
        // ==================== SECTION 1: POSITIONS / LEGS ====================
        "cfgSegment": {
            title: "Segment (Options / Futures)",
            desc: "Selects whether this strategy trades Option contracts (CE/PE) or Futures contracts for the chosen underlying.",
            example: "Choose 'Options' for a NIFTY 9:20 Short Straddle, or 'Futures' for a directional NIFTY/CRUDEOIL futures breakout."
        },
        "mult_mode": {
            title: "Multiplier Mode (Manual / Auto)",
            desc: "Manual multiplies each leg's lots by a fixed number. Auto dynamically calculates the maximum safe lots from your available broker margin and Cushion Margin/Lot.",
            example: "If Available Margin is ₹3,00,000 and Margin+Cushion per lot is ₹60,000, Auto mode automatically deploys 5 Lots."
        },
        "cfgMultiplier": {
            title: "Lots Multiplier",
            desc: "Global multiplier applied to the 'Lots' count of every leg in this strategy. Scales position size without editing individual rows.",
            example: "If CE = 2 lots and PE = 2 lots, setting Multiplier = 3 executes 6 lots CE and 6 lots PE."
        },
        "cfgCushionMargin": {
            title: "Cushion Margin / Lot (₹)",
            desc: "Extra safety buffer per lot added on top of broker span+exposure margin when Auto Multiplier is active, protecting against intraday MTM/VIX margin spikes.",
            example: "If broker margin is ₹45,000/lot and Cushion = ₹15,000, Auto mode reserves ₹60,000 per lot."
        },
        "cfgBulkEdit": {
            title: "Bulk Edit",
            desc: "One-click batch action to update Target, Stoploss, Broker/Self placement, Lots, Expiry, Product, or Dynamic Hedge across all legs simultaneously.",
            example: "Select 'Set Both Target & SL to With Broker' to switch all 4 legs to broker-side OCO orders at once."
        },
        "th_side": {
            title: "Side (BUY / SELL)",
            desc: "Direction of the leg. SELL collects option premium (benefits from theta decay/sideways market); BUY pays premium (used for directional trades or margin hedges).",
            example: "Short Straddle uses SELL CE + SELL PE. Iron Condor adds OTM BUY CE + BUY PE."
        },
        "th_prod": {
            title: "Product Type (NRML / MIS)",
            desc: "NRML allows overnight positional carry-forward until expiry. MIS is Intraday-only and auto-squares off before market close.",
            example: "Use MIS for 09:20–15:20 intraday straddles; use NRML for multi-day positional strangles."
        },
        "th_strike": {
            title: "Strike Selection",
            desc: "Determines how the strike price is picked at entry: ATM, ITM/OTM offsets (e.g., OTM 200), Closest Premium (e.g., ~₹100), or Straddle Width.",
            example: "With NIFTY Spot at 25,840, ATM selects 25850; OTM +200 selects 26050 CE / 25650 PE."
        },
        "th_type": {
            title: "Option Type (CE / PE)",
            desc: "CE (Call Option) profits when underlying rises (for BUY) or stays below strike (for SELL). PE (Put Option) profits when underlying falls (for BUY) or stays above strike (for SELL).",
            example: "Selling 25850 CE + 25850 PE creates a delta-neutral ATM Straddle."
        },
        "th_lots": {
            title: "Lots (Per Leg)",
            desc: "Base number of lots for this specific leg before the global Lots Multiplier is applied.",
            example: "For NIFTY (65 qty/lot), Lots = 2 with Multiplier = 1 trades 130 quantity."
        },
        "th_hedge": {
            title: "Dynamic Associated Hedge",
            desc: "Automatically buys a deep OTM option before placing this SELL leg to slash exchange margin by ~60–70% and cap tail-risk.",
            example: "Selling 25850 CE with a +500 OTM Hedge automatically buys 26350 CE first, reducing margin from ~₹1.4L to ~₹45K."
        },
        "th_target": {
            title: "Leg Target",
            desc: "Profit target for this individual leg, configurable in Percentage (%), Points (Pts), or None, and executed With Broker (Limit) or With Self.",
            example: "SELL CE at ₹180 with Target 50% exits when CE drops to ₹90."
        },
        "th_tgt_reexec": {
            title: "Target Re-Execution (Re-Entry / Re-Cost)",
            desc: "Controls how many times and how this leg re-enters after hitting its Target: RE-ENTRY (new ATM strike), RE-COST (same strike at original entry price), or RE-EXECUTE.",
            example: "TGT Re-Exec = RE-ENTRY (6) re-enters a fresh ATM leg up to 6 times whenever Target is hit."
        },
        "th_sl": {
            title: "Leg Stoploss (SL)",
            desc: "Maximum loss threshold for this individual leg in Percentage (%), Points (Pts), or None, placed either With Broker (SL-L) or With Self.",
            example: "SELL CE at ₹180 with SL 30% exits if CE rises to ₹234."
        },
        "th_tsl": {
            title: "Trailing Stoploss (TSL)",
            desc: "Automatically ratchets the leg's Stoploss favorably as price moves in your direction by every X% or X pts.",
            example: "SELL at ₹200 (SL ₹260) with TSL '10% / 10%': when price drops to ₹180, SL trails down from ₹260 to ₹240."
        },
        "th_sl_reexec": {
            title: "SL Re-Execution (Re-Entry / Re-Cost)",
            desc: "Controls how many times and how this leg re-enters after hitting its Stoploss: RE-ENTRY (new ATM strike), RE-COST (re-enters same strike at entry price), or RE-EXECUTE.",
            example: "SL Re-Exec = RE-COST (1) waits for the stopped-out strike to return to your original entry price and re-enters once."
        },
        "th_action": {
            title: "Leg Exit Action",
            desc: "Defines what happens to the rest of the strategy when this leg hits its SL/Target: 'Square off leg' (only this leg exits) or 'Square off all' (exits entire strategy).",
            example: "Set 'Square off all' on a protective leg if you want the whole straddle to close when one side breaches."
        },
        "th_underlying": {
            title: "Underlying Index / Commodity",
            desc: "The market instrument for this leg (NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY, SENSEX, BANKEX, or MCX commodities).",
            example: "Select NIFTY for NSE weekly index options or SENSEX for BSE Friday/Tuesday options."
        },
        "th_expiry": {
            title: "Contract Expiry",
            desc: "Selects which expiry series to trade: Weekly (current week), Next Weekly (upcoming week), or Monthly.",
            example: "On expiry day afternoon, switch to 'Next Weekly' to avoid high gamma spikes."
        },

        // ==================== SECTION 2: TIMINGS, UNDERLYING & TOGGLES ====================
        "cfgToggleMoveSlToCost": {
            title: "Move SL to Cost (Pair SL)",
            desc: "When one leg hits Stoploss or Target and finishes, the opposing open leg's Stoploss is automatically moved to its entry cost price for risk-free riding.",
            example: "CE & PE sold at ₹150 each. If CE hits SL, PE's SL automatically shifts from ₹200 to ₹150 (breakeven)."
        },
        "cfgToggleWaitAndTrade": {
            title: "Wait & Trade (Momentum Entry)",
            desc: "Delays initial entry until option premium or underlying moves by a configured percentage/points from the start-time reference, avoiding premature entries.",
            example: "Wait for CE premium at 09:20 (₹180) to drop 5% (to ₹171) before selling."
        },
        "cfgToggleReEntry": {
            title: "Master Re-Entry / Re-Execute Toggle",
            desc: "Master switch enabling or disabling all leg-level Re-Entry, Re-Cost, and Re-Execute cycles across the entire strategy.",
            example: "Turn OFF mid-day to stop all further re-entries while keeping existing open legs protected."
        },
        "cfgToggleRangeBreakout": {
            title: "Range Breakout (ORB)",
            desc: "Opening Range Breakout: monitors the high/low range of the opening window before triggering entries only when price breaks out of that range.",
            example: "Tracks 09:15–09:30 high/low and enters only when NIFTY breaks above the 15-min high or below the 15-min low."
        },
        "cfgToggleDynamicHedges": {
            title: "Dynamic Associated Hedges (Margin Saver)",
            desc: "Automatically buys deep OTM protective options before SELL legs and dynamically rolls/exits them with parent legs to save ~60–70% margin.",
            example: "On a 10-lot NIFTY straddle, margin drops from ~₹14L to ~₹4.5L while protecting against gap/spike risk."
        },
        "grp_underlying_src": {
            title: "Underlying Price Source (Spot / Futures / Synthetic)",
            desc: "Chooses the reference price used to determine ATM strike: Spot (Cash Index), Futures (Near-Month Future), or Synthetic Futures (Strike + CE - PE).",
            example: "If NIFTY Spot is 25,820 and Futures is 25,880, 'Spot' picks 25800 ATM while 'Futures' picks 25900 ATM."
        },
        "grp_trade_type": {
            title: "Trade Type (Intraday / Positional)",
            desc: "Intraday auto-squares off all positions at End Time with zero overnight risk. Positional keeps positions open across days until SL, Target, or Expiry.",
            example: "Use Intraday for 09:20–15:25 daily straddles; use Positional for multi-day Iron Condors."
        },
        "grp_active_days": {
            title: "Active Schedule Days",
            desc: "Selects which weekdays (Mon–Fri) the algo is permitted to automatically start and execute.",
            example: "Enable only Tuesday & Thursday if you want this strategy to run exclusively on NIFTY/SENSEX expiry days."
        },
        "cfgStartTime": {
            title: "Start Time (HH:MM:SS)",
            desc: "Exact IST time when the strategy wakes up and evaluates entry conditions (or punches orders immediately).",
            example: "Set 09:20:00 to avoid opening 5-minute volatility and enter right at 9:20 AM."
        },
        "cfgEndTime": {
            title: "End Time (HH:MM:SS)",
            desc: "Exact IST auto square-off time for Intraday strategies. All open main legs and hedges are squared off cleanly.",
            example: "Set 15:25:00 (or 15:29:50) to exit before broker RMS auto-square-off penalties."
        },
        "cfgNoReEntryAfterTime": {
            title: "No Re-Entry After Cutoff",
            desc: "Optional cutoff time after which no new re-entries or re-executions are triggered, while existing open positions continue to be managed until End Time.",
            example: "Set 14:45:00 so a leg stopped out at 14:50 does not open a fresh trade in the final 35 minutes."
        },

        // ==================== SECTION 3: MTM TARGET & STOPLOSS ====================
        "grp_mtm_calc_mode": {
            title: "MTM Calculation Mode (Overall / Per Lot)",
            desc: "Overall applies your MTM Target & SL ₹ values to the entire strategy basket. Per Lot multiplies your ₹ values by total active lots.",
            example: "With Per Lot = ₹1,500 SL and 4 lots traded, total strategy MTM SL automatically scales to -₹6,000."
        },
        "cfgMtmTargetType": {
            title: "MTM Trailing / Target Type",
            desc: "Selects how overall strategy profit is booked or trailed: Lock & Trail (locks profit at threshold then trails), Lock Profit, Trail Profit, Fixed Target (₹ or %), or None.",
            example: "Choose 'Lock and Trail' to secure ₹8,000 once profit hits ₹10,000, and trail higher as profit grows."
        },
        "cfgMtmTargetReach": {
            title: "If Profit Reaches (Activation Threshold)",
            desc: "The MTM profit level (in ₹) that must be reached before Lock Profit or Lock & Trail activates.",
            example: "If set to ₹10,000, profit locking activates only after strategy MTM touches +₹10,000."
        },
        "cfgMtmTargetLock": {
            title: "Lock Profit (Minimum Guaranteed Floor)",
            desc: "Once 'If profit reaches' is hit, this becomes your minimum locked profit floor. If MTM falls back to this level, all positions square off.",
            example: "If profit reaches ₹10,000 and Lock Profit = ₹7,000, a pullback to ₹7,000 exits all legs with +₹7,000 locked."
        },
        "cfgMtmTargetTrailStep": {
            title: "For Every Increase in Profit By (Trail Step)",
            desc: "Each time MTM rises by this increment above the activation threshold, the locked profit floor steps up.",
            example: "Step = ₹2,000, Trail = ₹1,500: when profit rises from ₹10,000 to ₹12,000, locked profit rises from ₹7,000 to ₹8,500."
        },
        "cfgMtmTargetTrailVal": {
            title: "Trail Profit By (Step-Up Amount)",
            desc: "The amount (in ₹) added to your locked profit floor each time MTM advances by one Trail Step.",
            example: "With Step = ₹2,000 and Trail = ₹1,500, you retain 75% of every new profit surge."
        },
        "cfgMtmTargetValue": {
            title: "Fixed Target Value (₹ / %)",
            desc: "Exact MTM profit amount (₹) or percentage (%) at which all open positions immediately square off.",
            example: "Fixed Target = ₹15,000 exits the entire strategy the instant combined MTM reaches +₹15,000."
        },
        "cfgMtmTargetReEntry": {
            title: "MTM Target Re-Entry",
            desc: "Determines whether the whole strategy restarts (RE-ENTRY / RE-EXECUTE) after booking an MTM Target, or stops for the day (None).",
            example: "Set 'None' to bank the day's profit and stop; set 'RE ENTRY' to start a fresh cycle."
        },
        "cfgMtmTargetAction": {
            title: "MTM Target Action",
            desc: "Action taken when MTM Target or Trailing floor is hit: square off all open positions, or square off and permanently terminate the algo.",
            example: "Use 'Square off all positions & terminate algo' if you want no further activity after target is booked."
        },
        "cfgMtmSlType": {
            title: "MTM Fixed Stoploss Type",
            desc: "Sets a hard strategy-wide maximum loss limit in Fixed Amount (₹), Fixed Percentage (%), or None.",
            example: "Fixed Amount = -₹5,000 caps your worst-case daily loss across all legs combined."
        },
        "cfgMtmSlValue": {
            title: "MTM Stoploss Value (₹ / %)",
            desc: "The maximum loss threshold (e.g., -5000 for ₹5,000 loss). When combined MTM drops to or below this value, all positions exit.",
            example: "If CE + PE combined MTM hits -₹5,000, the engine immediately squares off all legs."
        },
        "cfgMtmSlAction": {
            title: "MTM Stoploss Action",
            desc: "Action triggered when MTM Stoploss is breached: square off all positions, or square off and terminate the strategy.",
            example: "Select 'Square off all positions & terminate algo' to prevent any re-entries after a max-loss day."
        },

        // ==================== SECTION 4: PREMIUM MATCHING & RANGE ====================
        "radPremNone": {
            title: "No Premium Matching (Immediate Entry)",
            desc: "Standard strike selection. Executes immediately at Start Time without waiting for CE/PE premiums to match any difference or range.",
            example: "At 09:20:00, immediately sells ATM CE (₹180) and ATM PE (₹165) regardless of the ₹15 difference."
        },
        "radPremMaxDiff": {
            title: "Match Premiums with Maximum Difference",
            desc: "Waits or selects strikes such that |CE LTP - PE LTP| is less than or equal to your threshold (in Points or %).",
            example: "With Max Diff = ₹10, if 25850 CE = ₹185 and 25850 PE = ₹168 (diff ₹17), entry waits until |CE - PE| ≤ ₹10."
        },
        "radPremCloseTo": {
            title: "Premium Close To Target",
            desc: "Selects or waits for CE & PE strikes whose premiums are closest to your target rupee value (±10%).",
            example: "Target = ₹100 automatically picks the OTM CE and PE strikes trading nearest to ₹100."
        },
        "radPremRange": {
            title: "Premium Range (Min to Max)",
            desc: "Ensures both CE and PE leg premiums lie strictly within [Min ₹, Max ₹] before entry is allowed.",
            example: "Range 80 TO 150 enters only when both CE and PE premiums are between ₹80 and ₹150."
        },
        "cfgToggleCombinedPrem": {
            title: "Combined Premium Filter (Sum of Legs)",
            desc: "Adds a condition on (CE LTP + PE LTP) being Lower than (≤) or Higher than (≥) your ₹ threshold before entry.",
            example: "Lower than ₹250 ensures you only enter the straddle when combined CE+PE premium is ≤ ₹250."
        },
        "cfgPremFrequency": {
            title: "Condition Matching Frequency",
            desc: "How frequently live quotes are evaluated against your premium matching rules: Continuous (<10ms LTP), 1 sec, or 5 sec (filters wicks).",
            example: "Use '1 sec' or '5 sec' to avoid triggering on a 50ms illiquid tick spike."
        },

        // ==================== SECTION 5: EXECUTION ORDER PARAMETERS ====================
        "cfgEntryOrderType": {
            title: "Entry Order Type",
            desc: "Limit (L) / SL-L sends limit orders with your configured buffer for slippage control. Market (MKT) converts to ultra-tight Marketable Limit for instant fills on Kotak Neo.",
            example: "LTP = ₹100 with 5% buffer places a SELL Limit at ₹95.00 so it fills immediately up to ₹95."
        },
        "cfgHedgeOrderType": {
            title: "Hedge Order Type (Margin Saver)",
            desc: "Marketable Limit uses a smart ask-crossing buffer so buy hedges fill instantaneously (<25ms) before SELL legs fire, avoiding naked margin rejection.",
            example: "Buy hedge LTP ₹5.50 places Limit @ ₹6.00 to guarantee immediate fill before selling the main leg."
        },
        "cfgEntryBufferType": {
            title: "Entry Buffer Type (Percentage / Points)",
            desc: "Unit used to calculate the limit-price protection buffer for entry orders: Percentage (%) of LTP or fixed Points (₹).",
            example: "Points = 2 on a ₹150 option sets SELL limit at ₹148 and BUY limit at ₹152."
        },
        "cfgEntryBufferVal": {
            title: "Entry Buffer Value",
            desc: "Numeric buffer added to BUY limits or subtracted from SELL limits so orders fill immediately while protecting against freak spikes.",
            example: "With 5% buffer on ₹200 SELL, limit order is sent at ₹190.00."
        },
        "cfgEntryMarketTimeoutSec": {
            title: "Entry Unfilled -> Market Timeout (secs)",
            desc: "If a Limit entry order remains unfilled on the exchange for N seconds, automatically converts/re-places it as a Marketable order. Set 0 to disable.",
            example: "Set 3 secs so an unfilled limit order automatically chases and fills after 3 seconds."
        },
        "cfgEntryDelaySide": {
            title: "Entry Delay Side (Hedge Sequencing)",
            desc: "Controls sequencing between BUY hedge legs and SELL main legs. Always executes BUY hedges first so exchange margin benefit is active before SELL orders punch.",
            example: "'Delay entry of Sell positions' confirms all BUY hedges are FILLED before dispatching SELL legs."
        },
        "cfgEntryDelaySec": {
            title: "Entry Delay By (seconds)",
            desc: "Optional extra wait (in seconds) after BUY hedges fill before punching SELL legs. 0s dispatches immediately upon fill confirmation.",
            example: "Keep 0s for fastest execution; use 1–2s only if your broker RMS takes extra time to credit hedge margin."
        },
        "cfgExitOrderType": {
            title: "Exit Order Type",
            desc: "Order type used when squaring off legs on SL, Target, or End Time: Limit (with exit buffer) or Market (Marketable Limit).",
            example: "Use Market or Limit with 0.5–1% buffer on exits to guarantee fast fill during sharp moves."
        },
        "cfgExitBufferType": {
            title: "Exit Buffer Type (Percentage / Points)",
            desc: "Unit (Percentage or Points) for calculating the limit price buffer on exit orders.",
            example: "Percentage 0.5% on a ₹200 BUY-to-close order places Limit at ₹201.00."
        },
        "cfgExitBufferVal": {
            title: "Exit Buffer Value",
            desc: "Buffer magnitude applied to exit Limit orders to ensure immediate execution without excessive slippage.",
            example: "Value = 0.5% on a ₹100 exit sets limit at ₹100.50 (BUY) or ₹99.50 (SELL)."
        },
        "cfgExitMarketTimeoutSec": {
            title: "Exit Unfilled -> Market Timeout (secs)",
            desc: "If a Limit exit order does not fill within N seconds, automatically converts to Marketable Limit to prevent unclosed risk. 0 = disabled.",
            example: "Set 2 secs so any resting exit limit force-closes after 2 seconds."
        },
        "cfgExitDelaySide": {
            title: "Exit Delay Side",
            desc: "Ensures SELL positions are exited before BUY hedge positions during square-off, preventing temporary naked short margin spikes.",
            example: "'Delay exit of Buy positions' closes short CE/PE first, then closes the OTM BUY hedges."
        },
        "cfgExitDelaySec": {
            title: "Exit Delay By (seconds)",
            desc: "Seconds to wait after SELL legs exit before squaring off BUY hedges (when Exit Delay Side is enabled).",
            example: "Set 0–1s for fast square-off, or 3s if you want extra broker RMS settlement buffer."
        },
        "cfgSlOrderPlacement": {
            title: "SL Order Placement (Individual Leg)",
            desc: "'Place in advance' rests the SL-Limit order directly on Kotak Neo/NSE for 0ms exchange trigger. 'Keep with self' monitors LTP in software and sends exit on breach.",
            example: "Choose 'Place in advance' for instant exchange-side protection against sudden spikes."
        },
        "cfgTargetOrderPlacement": {
            title: "Target Order Placement (Individual Leg)",
            desc: "'Place in advance' rests the Target Limit order on the broker OMS (OCO with SL). 'Keep with self' triggers from the platform when LTP hits target.",
            example: "Choose 'Place in advance' to catch fast target wicks directly on the exchange order book."
        },
        "cfgSlPlacementDelaySec": {
            title: "SL Placement Delay (Individual Leg)",
            desc: "Seconds to wait after entry fill before submitting the advance SL/Target orders to the broker.",
            example: "0 secs places SL immediately upon fill; 2 secs waits 2s after entry."
        },
        "cfgMtmSlDelaySec": {
            title: "SL Placement Delay (MTM SL)",
            desc: "Seconds after entry before MTM Stoploss monitoring becomes active, preventing false triggers from initial bid-ask spread noise.",
            example: "Set 3 secs to ignore first-3-second spread widening right at 09:15."
        },

        // ==================== SECTION 6: SL & TARGET PRICE BASIS ====================
        "cfgRefPriceMode": {
            title: "Price Calculation Basis",
            desc: "Reference price used to compute leg SL & Target levels: Actual Entry Fill, Entry 1-Min Candle Close/Open, Previous Candle Close, Specific Time Close, or Spot Close.",
            example: "Choosing 'Entry Minute Candle Close' matches AlgoTest/Stockmock backtests where 09:20 SL% is calculated from the 09:20 candle close."
        },
        "cfgBenchmarkTime": {
            title: "Benchmark Candle Time (HH:MM:SS)",
            desc: "When 'Specific Benchmark Time Candle Close' is selected, specifies the exact 1-minute candle whose close price locks SL & Target.",
            example: "Set 09:16:00 to anchor all SL/Target percentages to the 09:16 1-minute candle close."
        },
        "grp_benchmark_scope": {
            title: "Re-Entry Benchmark Scope",
            desc: "Controls whether candle-close benchmark pricing applies only to Initial Entry, to All Entries & Re-entries, or inherits the 1st entry's benchmark.",
            example: "'Initial Entry Only' uses candle close for 09:20 entry and actual fill price for faster 11:00 re-entries."
        },
        "cfgBenchmarkInterimMode": {
            title: "Interim Protection (During Candle Formation)",
            desc: "Defines how your position is protected during the ~60 seconds while the benchmark 1-min candle is still forming: Actual Fill, Candle Open, or Wait for Close.",
            example: "'Actual Traded Fill' places an immediate protective SL right after fill, then auto-modifies it once the candle closes."
        },
        "cfgBenchmarkMaxDevPct": {
            title: "Max Allowed Wick Deviation (%)",
            desc: "Safety cap against illiquid candle wicks. If the 1-min candle close deviates from your fill price by more than X%, falls back to actual fill price (0 = Off).",
            example: "Set 15% so a freak spike candle close doesn't push your SL abnormally far away."
        },
        "cfgBenchmarkBrokerAction": {
            title: "Broker Advance Orders Action",
            desc: "Once the benchmark candle closes, chooses whether to Auto-Modify resting broker SL/Target orders in-place (fastest) or Cancel & Re-place.",
            example: "'Auto-Modify' updates the existing Kotak Neo SL-L trigger price in one API call without losing queue priority."
        },

        // ==================== SECTION 7: RE-EXECUTION & MARGIN SAFETY GUARDS ====================
        "cfgReExecuteMethod": {
            title: "Re-Execute Method",
            desc: "Determines when a re-entry order fires after a leg hits SL/Target: Immediate (0ms), Candle Close (:00 of next min), Next Minute Open (:01), or 60s Cooldown.",
            example: "Select 'Candle Close (Stockmock)' to wait for the current 1-minute candle to finish before re-entering."
        },
        "cfgReExecuteOrderDelaySec": {
            title: "Re-Execute Order Delay (secs)",
            desc: "Additional cooldown delay (in seconds) after a leg exits before dispatching its re-entry order.",
            example: "Set 5s to let volatile post-SL price action settle before re-entering."
        },
        "cfgReExecuteTriggerFilter": {
            title: "Trigger Re-Execution On",
            desc: "Filters whether automated re-entry runs on Both SL & Target exits, Stoploss hits only, or Target hits only.",
            example: "Choose 'Stoploss Hits Only' if you want to re-enter stopped legs but keep Target wins banked."
        },
        "cfgMarginExitGraceSec": {
            title: "Margin Error Grace Countdown",
            desc: "Countdown window (in seconds) shown when a broker margin shortfall occurs, giving you time to add funds or manually choose before auto-square-off.",
            example: "15 Seconds Grace lets you cancel auto-exit or square off immediately from the alert banner."
        },
        "cfgReEntryPlacement": {
            title: "Re-Cost / Re-Entry Order Placement",
            desc: "Specifies whether newly re-entered legs place their SL/Target orders in advance with Kotak Neo OMS or track them in platform software.",
            example: "'Place in Advance with Broker' keeps re-entered legs protected by exchange-side SL orders."
        },
        "grp_hedge_preshift": {
            title: "Hedge Pre-Shift & Margin Window",
            desc: "Automatically rolls/shifts the associated OTM hedge 2.5s before a re-entry fires so full margin benefit is unlocked with zero RMS rejection.",
            example: "When spot moves 400 pts and a new strike re-enters, the hedge pre-shifts first so margin stays locked."
        },
        "cfgAutoReleaseMarginReEntry": {
            title: "Auto-Release Margin for Re-Execution",
            desc: "Temporarily cancels opposing legs' resting advance Target/SL orders for a split second during re-entry to free blocked broker margin, then instantly reinstates them.",
            example: "Prevents 'Insufficient Margin' rejections when Kotak Neo blocks extra margin for resting exit orders."
        },
        "cfgAutoHandlingSl": {
            title: "Auto-Handling of Unexpected SL Cancellations",
            desc: "Continuously monitors resting broker SL orders. If cancelled externally or by broker OMS, the platform immediately takes over live SL monitoring.",
            example: "If a resting SL-L order is cancelled in the broker app, the algo watches LTP and market-exits on SL breach."
        },
        "cfgAutoExitMargin": {
            title: "Auto Exit on Margin Error",
            desc: "If any leg fails due to RMS margin shortfall during entry or re-entry, triggers the grace countdown and automatically squares off all open positions to avoid naked exposure.",
            example: "Protects your account from holding an unhedged short leg if the hedge or opposing leg is rejected for margin."
        },

        // ==================== SECTION 8: AUTO SIZING & DAY-LOCK SYMMETRY ====================
        "cfgToggleDayLockSymmetry": {
            title: "Day-Lock Symmetry (Lock Initial Auto Lots)",
            desc: "Computes Auto lots once at initial entry and locks that exact lot count for all subsequent CE/PE re-entries today, preventing asymmetric leg sizes after a loss.",
            example: "If 09:16 entry calculates 142 lots, all CE and PE re-entries stay at 142 lots even if intraday margin fluctuates."
        },
        "cfgToggleAutoTrimSymmetric": {
            title: "Auto-Trim Opposing Legs if Margin Drops",
            desc: "If a re-entry occurs after a loss and available margin can only afford fewer lots than locked_day_lots, automatically trims excess lots from the opposing running leg first so both stay 1:1.",
            example: "CE is running 142 lots and PE can only afford 138 lots: trims 4 lots from CE and re-enters PE at 138 lots (138 vs 138)."
        },
        "cfgToggleProfitCushion": {
            title: "Profit-Released Cushion",
            desc: "Starts the day with your full protective Cushion Margin/Lot, then automatically drops it to Reduced Cushion/Lot once your trigger condition is met — freeing cash for extra lots.",
            example: "Base ₹10,000/lot cushion drops to ₹3,000/lot after +₹1,00,000 MTM, releasing ₹9.9L of cash for ~7 extra lots."
        },
        "cfgReducedCushionMargin": {
            title: "Reduced Cushion Margin (₹/lot)",
            desc: "The lower cushion per lot applied once the Profit-Released Cushion trigger fires.",
            example: "Set 3000 so each lot only reserves ₹3,000 buffer instead of ₹10,000 once you are safely in profit."
        },
        "cfgCushionReleaseTrigger": {
            title: "Cushion Release Trigger Condition",
            desc: "Condition that unlocks the reduced cushion: MTM Reaches +₹X, Move SL to Cost triggers, Either MTM or SL-to-Cost, or After Specific IST Time.",
            example: "Choose 'Either MTM or SL-to-Cost' so cushion releases as soon as risk is locked or profit target buffer is built."
        },
        "cfgCushionReleaseMtmThreshold": {
            title: "MTM Profit Threshold (₹)",
            desc: "Strategy MTM profit (in ₹) required to trigger Profit-Released Cushion when using MTM-based triggers.",
            example: "Set 100000 so the cushion drops only after the strategy is +₹1,00,000 in profit."
        },
        "cfgCushionReleaseTime": {
            title: "Cushion Release Time (IST)",
            desc: "IST time (HH:MM:SS) after which cushion drops to the reduced value when 'After Specific Time' trigger is selected.",
            example: "Set 12:30:00 to use full ₹10,000 cushion in the volatile morning and ₹3,000 in the afternoon."
        },
        "cfgToggleCushionReleaseLatch": {
            title: "Latch Reduced Cushion for Rest of Day",
            desc: "Keeps the reduced cushion active for the remainder of the trading day once triggered, rather than reverting if MTM dips slightly.",
            example: "Enabled: once +₹1,00,000 triggers release at 11:00 AM, reduced cushion stays active until market close."
        },
        "cfgTogglePreflightMarginSync": {
            title: "T-10s Pre-Flight Margin Sync",
            desc: "Automatically refreshes live Kotak Neo basket margin and available capital N seconds before Start Time so Auto Multiplier uses exact same-day VIX/expiry margins.",
            example: "At 09:15:50 (10s before 09:16:00), syncs exact live margin (e.g. ₹1,32,450/lot) and updates Auto lots before firing."
        },
        "cfgPreflightLeadSec": {
            title: "Pre-Flight Lead Time (seconds)",
            desc: "How many seconds before Start Time (3–60s) the Pre-Flight Margin Sync runs.",
            example: "10 secs runs the live margin calibration at 09:15:50 for a 09:16:00 strategy."
        },
        "cfgTogglePreflightLiveQuotes": {
            title: "Use Live Option Quotes for Margin Sync",
            desc: "Uses live ATM/OTM option premiums during the Pre-Flight Margin Sync for maximum margin accuracy.",
            example: "Enabled ensures expiry-day premium drops are reflected in the pre-flight margin calculation."
        }
    };

    let portalEl = null;
    let hideTimer = null;

    function ensurePortal() {
        if (portalEl) return portalEl;
        portalEl = document.createElement("div");
        portalEl.id = "algoHelpTooltipPortal";
        portalEl.setAttribute("role", "tooltip");
        document.body.appendChild(portalEl);
        return portalEl;
    }

    function escapeHtml(str) {
        return String(str || "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;");
    }

    function showTooltip(dotEl, key) {
        const info = ALGO_TOOLTIPS[key];
        if (!info) return;
        if (hideTimer) {
            clearTimeout(hideTimer);
            hideTimer = null;
        }
        const portal = ensurePortal();
        portal.innerHTML =
            `<div class="aht-title">${escapeHtml(info.title)}</div>` +
            `<div class="aht-desc">${escapeHtml(info.desc)}</div>` +
            `<div class="aht-example"><strong>Example:</strong> ${escapeHtml(info.example)}</div>`;

        portal.style.left = "-9999px";
        portal.style.top = "-9999px";
        portal.classList.add("visible");

        const rect = dotEl.getBoundingClientRect();
        const tipRect = portal.getBoundingClientRect();
        const vw = window.innerWidth;
        const vh = window.innerHeight;
        const pad = 10;

        let left = rect.left + rect.width / 2 - tipRect.width / 2;
        if (left < pad) left = pad;
        if (left + tipRect.width > vw - pad) left = vw - tipRect.width - pad;

        let top = rect.top - tipRect.height - 8;
        if (top < pad) {
            top = rect.bottom + 8;
        }
        if (top + tipRect.height > vh - pad) {
            top = Math.max(pad, vh - tipRect.height - pad);
        }

        portal.style.left = `${Math.round(left)}px`;
        portal.style.top = `${Math.round(top)}px`;
    }

    function hideTooltip() {
        if (!portalEl) return;
        hideTimer = setTimeout(() => {
            if (portalEl) portalEl.classList.remove("visible");
        }, 60);
    }

    function createDot(key) {
        const dot = document.createElement("span");
        dot.className = "algo-help-dot";
        dot.textContent = "?";
        dot.tabIndex = 0;
        dot.setAttribute("data-help-key", key);
        dot.setAttribute("aria-label", ALGO_TOOLTIPS[key]?.title || "Help");

        dot.addEventListener("mouseenter", () => showTooltip(dot, key));
        dot.addEventListener("mouseleave", hideTooltip);
        dot.addEventListener("focus", () => showTooltip(dot, key));
        dot.addEventListener("blur", hideTooltip);
        dot.addEventListener("click", (e) => {
            e.preventDefault();
            e.stopPropagation();
            showTooltip(dot, key);
        });
        return dot;
    }

    function attachDotToLabel(el, key) {
        if (!el || !ALGO_TOOLTIPS[key]) return;
        if (el.querySelector(`.algo-help-dot[data-help-key="${key}"]`)) return;
        el.appendChild(createDot(key));
    }

    function attachByInputId(inputId, key) {
        const inp = document.getElementById(inputId);
        if (!inp) return;
        const grp = inp.closest(".straddle-form-group");
        if (grp) {
            const lbl = grp.querySelector("label");
            if (lbl) {
                attachDotToLabel(lbl, key);
                return;
            }
        }
        // Fallback: direct parent or previous sibling label
        const box = inp.closest("div[id^='box_mtm_']");
        if (box) {
            const lbl = box.querySelector("label");
            if (lbl) attachDotToLabel(lbl, key);
        }
    }

    function attachByToggleId(inputId, key) {
        const inp = document.getElementById(inputId);
        if (!inp) return;
        const chip = inp.closest(".straddle-toggle-chip");
        if (chip) {
            attachDotToLabel(chip, key);
            return;
        }
        const lbl = inp.closest("label.straddle-checkbox-label");
        if (lbl) {
            const titleRow = lbl.querySelector("div > div:first-child");
            if (titleRow) {
                attachDotToLabel(titleRow, key);
            } else {
                attachDotToLabel(lbl, key);
            }
        }
    }

    function initAlgoTooltips() {
        // 1. Standard form-group inputs/selects
        const idMappings = [
            ["cfgSegment", "cfgSegment"],
            ["cfgMultiplier", "cfgMultiplier"],
            ["cfgCushionMargin", "cfgCushionMargin"],
            ["cfgBulkEdit", "cfgBulkEdit"],
            ["cfgStartTime", "cfgStartTime"],
            ["cfgEndTime", "cfgEndTime"],
            ["cfgNoReEntryAfterTime", "cfgNoReEntryAfterTime"],
            ["cfgMtmTargetType", "cfgMtmTargetType"],
            ["cfgMtmTargetReach", "cfgMtmTargetReach"],
            ["cfgMtmTargetLock", "cfgMtmTargetLock"],
            ["cfgMtmTargetTrailStep", "cfgMtmTargetTrailStep"],
            ["cfgMtmTargetTrailVal", "cfgMtmTargetTrailVal"],
            ["cfgMtmTargetValue", "cfgMtmTargetValue"],
            ["cfgMtmTargetReEntry", "cfgMtmTargetReEntry"],
            ["cfgMtmTargetAction", "cfgMtmTargetAction"],
            ["cfgMtmSlType", "cfgMtmSlType"],
            ["cfgMtmSlValue", "cfgMtmSlValue"],
            ["cfgMtmSlAction", "cfgMtmSlAction"],
            ["cfgPremFrequency", "cfgPremFrequency"],
            ["cfgEntryOrderType", "cfgEntryOrderType"],
            ["cfgHedgeOrderType", "cfgHedgeOrderType"],
            ["cfgEntryBufferType", "cfgEntryBufferType"],
            ["cfgEntryBufferVal", "cfgEntryBufferVal"],
            ["cfgEntryMarketTimeoutSec", "cfgEntryMarketTimeoutSec"],
            ["cfgEntryDelaySide", "cfgEntryDelaySide"],
            ["cfgEntryDelaySec", "cfgEntryDelaySec"],
            ["cfgExitOrderType", "cfgExitOrderType"],
            ["cfgExitBufferType", "cfgExitBufferType"],
            ["cfgExitBufferVal", "cfgExitBufferVal"],
            ["cfgExitMarketTimeoutSec", "cfgExitMarketTimeoutSec"],
            ["cfgExitDelaySide", "cfgExitDelaySide"],
            ["cfgExitDelaySec", "cfgExitDelaySec"],
            ["cfgSlOrderPlacement", "cfgSlOrderPlacement"],
            ["cfgTargetOrderPlacement", "cfgTargetOrderPlacement"],
            ["cfgSlPlacementDelaySec", "cfgSlPlacementDelaySec"],
            ["cfgMtmSlDelaySec", "cfgMtmSlDelaySec"],
            ["cfgRefPriceMode", "cfgRefPriceMode"],
            ["cfgBenchmarkTime", "cfgBenchmarkTime"],
            ["cfgBenchmarkScope", "grp_benchmark_scope"],
            ["cfgBenchmarkInterimMode", "cfgBenchmarkInterimMode"],
            ["cfgBenchmarkMaxDevPct", "cfgBenchmarkMaxDevPct"],
            ["cfgBenchmarkBrokerAction", "cfgBenchmarkBrokerAction"],
            ["cfgReExecuteMethod", "cfgReExecuteMethod"],
            ["cfgReExecuteOrderDelaySec", "cfgReExecuteOrderDelaySec"],
            ["cfgReExecuteTriggerFilter", "cfgReExecuteTriggerFilter"],
            ["cfgMarginExitGraceSec", "cfgMarginExitGraceSec"],
            ["cfgReEntryPlacement", "cfgReEntryPlacement"],
            ["cfgReducedCushionMargin", "cfgReducedCushionMargin"],
            ["cfgCushionReleaseTrigger", "cfgCushionReleaseTrigger"],
            ["cfgCushionReleaseMtmThreshold", "cfgCushionReleaseMtmThreshold"],
            ["cfgCushionReleaseTime", "cfgCushionReleaseTime"],
            ["cfgPreflightLeadSec", "cfgPreflightLeadSec"]
        ];
        idMappings.forEach(([id, key]) => attachByInputId(id, key));

        // 2. Button-group labels
        const btnGroupMap = [
            ["btnMultModeManual", "mult_mode"],
            ["btnUndSpot", "grp_underlying_src"],
            ["btnTradeIntraday", "grp_trade_type"]
        ];
        btnGroupMap.forEach(([btnId, key]) => {
            const btn = document.getElementById(btnId);
            const grp = btn?.closest(".straddle-form-group");
            const lbl = grp?.querySelector("label");
            if (lbl) attachDotToLabel(lbl, key);
        });

        // Active Days label
        const daysPill = document.querySelector(".straddle-days-pill-group");
        const daysLbl = daysPill?.closest(".straddle-form-group")?.querySelector("label");
        if (daysLbl) attachDotToLabel(daysLbl, "grp_active_days");

        // MTM Calculation Mode label
        const mtmOverall = document.getElementById("btnMtmModeOverall");
        const mtmBar = mtmOverall?.parentElement?.parentElement;
        const mtmLbl = mtmBar?.querySelector("span");
        if (mtmLbl) attachDotToLabel(mtmLbl, "grp_mtm_calc_mode");

        // Hedge Pre-Shift & Margin Window
        document.querySelectorAll("#sec_builder_reentry .straddle-form-group label").forEach(lbl => {
            if (lbl.textContent.includes("Hedge Pre-Shift")) {
                attachDotToLabel(lbl, "grp_hedge_preshift");
            }
        });

        // 3. Toggle chips & checkboxes
        const toggleIds = [
            ["cfgToggleMoveSlToCost", "cfgToggleMoveSlToCost"],
            ["cfgToggleWaitAndTrade", "cfgToggleWaitAndTrade"],
            ["cfgToggleReEntry", "cfgToggleReEntry"],
            ["cfgToggleRangeBreakout", "cfgToggleRangeBreakout"],
            ["cfgToggleDynamicHedges", "cfgToggleDynamicHedges"],
            ["cfgToggleCombinedPrem", "cfgToggleCombinedPrem"],
            ["cfgAutoReleaseMarginReEntry", "cfgAutoReleaseMarginReEntry"],
            ["cfgAutoHandlingSl", "cfgAutoHandlingSl"],
            ["cfgAutoExitMargin", "cfgAutoExitMargin"],
            ["cfgToggleDayLockSymmetry", "cfgToggleDayLockSymmetry"],
            ["cfgToggleAutoTrimSymmetric", "cfgToggleAutoTrimSymmetric"],
            ["cfgToggleProfitCushion", "cfgToggleProfitCushion"],
            ["cfgToggleCushionReleaseLatch", "cfgToggleCushionReleaseLatch"],
            ["cfgTogglePreflightMarginSync", "cfgTogglePreflightMarginSync"],
            ["cfgTogglePreflightLiveQuotes", "cfgTogglePreflightLiveQuotes"]
        ];
        toggleIds.forEach(([id, key]) => attachByToggleId(id, key));

        // 4. Premium Matching Radio Cards
        [
            ["radPremNone", "radPremNone"],
            ["radPremMaxDiff", "radPremMaxDiff"],
            ["radPremCloseTo", "radPremCloseTo"],
            ["radPremRange", "radPremRange"]
        ].forEach(([id, key]) => {
            const rad = document.getElementById(id);
            const lbl = rad?.closest("label");
            if (lbl) attachDotToLabel(lbl, key);
        });

        // 5. Legs Builder Table Column Headers
        const thMap = [
            ["Side", "th_side"],
            ["Prod", "th_prod"],
            ["Strike Selection", "th_strike"],
            ["Type", "th_type"],
            ["Lots", "th_lots"],
            ["Hedge", "th_hedge"],
            ["Target", "th_target"],
            ["TGT Re-Exec", "th_tgt_reexec"],
            ["Stoploss", "th_sl"],
            ["Trailing SL", "th_tsl"],
            ["SL Re-Exec", "th_sl_reexec"],
            ["Action", "th_action"],
            ["Underlying", "th_underlying"],
            ["Expiry", "th_expiry"]
        ];
        document.querySelectorAll("#sec_builder_legs table thead th").forEach(th => {
            const txt = th.textContent.trim();
            const match = thMap.find(([prefix]) => txt.startsWith(prefix));
            if (match) attachDotToLabel(th, match[1]);
        });

        // Hide tooltip on scroll so it never floats detached
        window.addEventListener("scroll", hideTooltip, true);
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initAlgoTooltips);
    } else {
        initAlgoTooltips();
    }
})();
