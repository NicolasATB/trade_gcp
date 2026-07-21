# Strategy 3 — cross-asset TSMOM: analysis & validation

> The full analysis arc for Strategy 3 (`strategy_id=3`), one section per plan ticket.
> Spanish mirror (context, outside the repo):
> `Trade GCP/ml_strategy_docs/strategy_3_analysis.md` — keep both in sync in the same
> change. Live data conventions (`source_priority`, bronze-is-raw) are in
> [`../sql/DDL.sql`](../sql/DDL.sql) and the project skill.

| Section | Ticket | Status |
|---|---|---|
| [Research thesis](#research-thesis) | arc | ✅ written |
| [Data sources per class](#data-sources-per-class) | T-18 | ✅ done |
| [Instrument universe](#instrument-universe) | T-19 | ✅ done |
| [TSMOM signal](#tsmom-signal) | T-22 | ✅ done |
| [Portfolio construction](#portfolio-construction) | T-23 | ✅ done |
| [Modeling lifecycle & tooling](#modeling-lifecycle--tooling) | T-24/T-25 | ✅ T-24 / ✅ T-25 |
| [Validation methodology](#validation-methodology) | Epic 8 | ✍️ drafted |
| [Baselines](#baselines) | T-26 | ✅ done |
| [Verdict](#verdict) | T-27/28 | ⏳ pending |

---

## Research thesis

Why cross-asset TSMOM, after several rounds of peer-reviewed literature mapped the
terrain:

- **Single-asset directional crypto TA.** Does not beat data-snooping net of costs.
  Closed in earlier phases (this is Strategy 1's alpha thesis, not the live pipeline's
  engineering value).
- **Per-instrument TSMOM — seriously questioned.** Huang, Li, Wang & Zhou (2020, JFE,
  *"Time series momentum: Is it there?"*): asset-by-asset regressions give little
  evidence (8/55 significant at 10% in-sample; 45/55 with negative out-of-sample R²);
  the pooled MOP t-stat (4.34) does not clear bootstrap thresholds; and decisively,
  TSMOM performs **virtually identically** to a TSH strategy that only buys assets with
  a positive historical mean — i.e. no predictability (alpha differential p≈0.26).
- **Volatility scaling as a confound.** Kim, Tse & Wald (2016, JFM): vol scaling does
  most of the work (alpha 1.08% at 40% scaling vs 0.39% unscaled; an equally-scaled
  buy-and-hold yields 0.73%).
- **XTSMOM — the only clean survivor.** Pitkäjärvi, Suominen & Vaittinen (2020, JFE):
  alpha 0.25%/month (t≈3.5) over Fama-French-Carhart, **without** vol scaling, and the
  per-instrument TSMOM alpha **disappears** once you control for XTSMOM. But:
  bonds↔equities only, sample to 2016, no commodities/FX/crypto.
- **Stock-bond correlation flip.** Molenaar et al. (2024, FAJ): correlation turned
  positive in 2021–2023 (inflation + real rates). XTSMOM's central channel
  (bond→equity) weakens by construction in a positive-correlation regime. This **lowers
  the prior**, it does not close it (correlation regimes are time-varying).
- **Crypto sleeve.** No peer-reviewed evidence as a return source within
  managed-futures frames; its low correlation evaporates under stress. Enters only as a
  **documented diversification experiment**, never as an alpha leg.

**The framing that governs the project.** No OOS replication of XTSMOM post-2016 exists
— that does **not** mean the path is closed, it means it is **untested**. Absence of
evidence ≠ evidence of failure. Nobody ran that test because nobody built the
apparatus; **this project can *be* that replication.** The deliverable is the
end-to-end multi-asset pipeline + the methodology, on a universe the literature
supports with far more rigor than the discarded crypto TA — win or lose, the result is
documented as is. The prior is low (everything directional in this program died under
scrutiny: McLean-Pontiff 2016, Harvey-Liu-Zhu 2016). Low prior ≠ not worth testing.
For the project's real purpose, the test *is* the product.

### Why TSMOM v1 first, XTSMOM as a separate epic

If XTSMOM is the only clean survivor, why is v1 diagonal TSMOM? Four reasons, in
order of weight:

1. **TSMOM is the control XTSMOM needs to be interpretable.** Pitkäjärvi et al.'s
   central claim is *relative*: per-instrument TSMOM alpha disappears once you control
   for XTSMOM. Replicating that post-2016 requires our own measured diagonal TSMOM on
   the same universe and period first — TSMOM v1 is not a detour, it is the
   experiment's denominator.
2. **De-risk the apparatus on the simplest possible strategy.** Diagonal TSMOM has one
   real parameter (formation horizon) and trivial mechanics, so it exercises everything
   else — multi-asset ingest, weekly resample, vol scaling, costs, purged walk-forward,
   DSR/PBO — with minimal strategy-side complexity. This already paid off: the engine's
   look-ahead bug (w(t) paired with r(t) instead of r(t+1)) was caught under the simple
   strategy, before it could contaminate a far more expensive XTSMOM run.
3. **Trial budget and multiplicity.** XTSMOM opens a much larger parameter space (which
   class predicts which, at what lag and horizon). Mixing both into one campaign would
   burn the pre-committed 10–15-trial budget and confound attribution between own-asset
   momentum, the cross-asset channel, and vol scaling.
4. **Holdout economics.** The blind holdout is a one-way door; spending it on XTSMOM
   with an unproven apparatus would bet the project's scarcest asset on the first run.
   The sequence spends v1's holdout on the cheap strategy and leaves XTSMOM its own
   validation route, designed on a debugged apparatus (the train/holdout boundary was
   fixed with that future epic in mind — see `backtest/splitter.py`).

A minor fifth factor: the correlation flip lowers XTSMOM's prior in the current regime,
so buying the cheap information first (does TSMOM ≠ TSH on this universe?) before the
expensive test is simply good experiment ordering.

---

## Data sources per class

**Decision (v1): ETFs for every non-crypto class — Yahoo Finance as the primary
source, Tiingo as a competing fallback. Crypto reuses the existing spot ingest
(Binance/Bitstamp). Futures are deferred.**

Futures are more cost-accurate but carry roll/contango and the point-in-time
active-contract hygiene a v1 pipeline doesn't need. ETFs are simple and continuous; the
conformed close is a clean **split-adjusted** price level, and the weekly return layer
(`vw_asset_returns_weekly`) reinvests split-adjusted cash dividends to form a
**total-return** series (T-26b) — enough to exercise the multi-asset pipeline.

| Asset class | Instrument (v1) | Source (primary / fallback) | Avoids roll? | Trade-off / rationale |
|---|---|---|---|---|
| Equity indices | ETF (e.g. SPY / EFA / EEM) | Yahoo / Tiingo | Yes | Liquid proxy; split-adjusted close + reinvested dividends = total-return (T-26b); futures add roll + active-contract tracking for no v1 gain. |
| Gov. bonds | Treasury ETF (e.g. SHY / IEF / TLT) | Yahoo / Tiingo | Yes | Continuous split-adjusted level without managing the roll calendar / cheapest-to-deliver. |
| Commodities | Physical (GLD) / broad ETF (e.g. DBC) | Yahoo / Tiingo | Partially | "No-roll" holds only for physically-backed ETFs; broad commodity ETFs are futures wrappers — roll/contango is **embedded in NAV**, not eliminated (a cost v1 does not isolate). |
| FX | Currency ETF (e.g. UUP / FXE / FXY) | Yahoo / Tiingo | N/A | Thinner, embeds money-market carry + expense ratio vs spot/forwards; acceptable as a v1 directional proxy. |
| Crypto (sleeve) | Spot BTC (Binance / Bitstamp) | existing ingest | Yes | Native spot, no roll; reuses the live pipeline, no new source. |

- **Honest caveat on "avoids roll".** Real only for equity/bond/physical-gold ETFs. A
  broad commodity ETF (DBC) wraps futures, so roll/contango is **embedded in the NAV**,
  not eliminated — v1 does not isolate that cost.
- **`source_priority` = failover, not `NULL`.** For each non-crypto instrument the two
  sources (Yahoo primary, Tiingo fallback) **compete by `priority`** in the silver
  consolidation, so a source that starts failing / leaving gaps fails over to the other.
  `priority NULL` is reserved for single-source context series only.
- **Both sources must share one adjustment basis, or the failover is unsound.** Validation
  (2026-06-25) found Yahoo's `quote.close` is **split-adjusted, not dividend-adjusted**, while
  Tiingo's `close` is **fully raw** — so they diverge by the split factor wherever a split
  exists (EFA's 3:1 of 2005-06-09 made them differ ×3 pre-split; the other seven ETFs,
  split-free in-window, agreed to <0.01%). The chosen v1 basis is **split-only**: continuous
  *and* point-in-time-stable (split factors are exact and rarely revised, unlike a total-return
  `adjClose` that providers rewrite on every new dividend). Tiingo is reconciled to it by
  reconstructing split-only from its own per-bar `splitFactor`
  (`adj_close = raw_close / Π(split_factor where ex_date > D)`), an **auditable silver step**
  applied before the priority de-dup in `conform` — proven on EFA (divergence collapsed from
  avg 10.1% / max 69% to avg 0.004%). T-21 stores Tiingo's `split_factor` / `div_cash` in bronze
  (raw provider fields) to drive it.
- **T-26b closes the total-return gap.** `vw_asset_returns_weekly` now adds each week's cash
  dividends back to the price return: dividends are sourced from Tiingo's `div_cash` (Yahoo's
  chart quote carries none, so they come from Tiingo even on weeks whose price came from Yahoo)
  and split-adjusted with the **same** factor `conform` applies to the close, so dividend and
  price share one basis. The exported `simple_return` / `excess_return` are total-return;
  `price_simple_return` / `price_log_return` keep the dividend-free level for audit and for the
  price-vs-total bias comparison. This matters because the baselines lean on high-dividend
  assets (TSH favours bonds + SPY; 60/40 holds 40% bonds), so a price-return series understated
  them and inflated TSMOM's edge over TSH/60-40.
- **Per-instrument registration landed in T-20** (the bronze ingest ticket), alongside
  the bronze tables: Yahoo (`source_id` 16, priority 2) and Tiingo (17, priority 1) are
  seeded in `sql/DDL.sql`, competing in the silver consolidation — no orphan
  `source_priority` rows before the tables existed and the universe was frozen (T-19).
- **Tiingo fallback is active (token API).** Tiingo is seeded `is_active = TRUE`: a
  token-authenticated API (free `TIINGO_API_KEY`) reachable from the dev host, the VM and
  CI, so the failover is real. It **replaced stooq** (2026-06-25), whose keyless CSV a
  JavaScript proof-of-work bot challenge made unreachable from cloud IPs (the dev host
  *and* the VM — a provider-level anti-bot wall, not an IP block). Lesson: a keyless
  scraper is fragile from cloud IPs; a token API is the robust fallback. The Yahoo
  back-fill already populated all eight ETFs (1993→2026); the Tiingo back-fill runs once
  the token is set.
- **API keys.** Yahoo needs none. Tiingo needs a **free** token in `TIINGO_API_KEY` (env
  var, never committed) — the one keyed dependency, accepted because a token API stays
  reachable from cloud IPs where keyless scrapers get bot-walled.

---

## Instrument universe

> Status: **frozen (T-19).** Nine instruments across five classes, frozen here
> **before** any signal is computed; sources per class are fixed in the section above.
> Coverage was confirmed once by a throwaway probe (keyless Yahoo chart API); the
> reproducible gate lands as an integration test with T-20.

### Frozen universe (9 instruments)

| Class | Instruments | Why two |
|---|---|---|
| Equity | SPY, EFA | US + international breadth (the XTSMOM bond→equity channel needs equity breadth) |
| Bonds | IEF, TLT | Mid↔long duration spread — without it there is no honest cross-asset test |
| Commodities | GLD, DBC | Physical gold (no roll) vs broad basket (roll embedded in NAV, see T-18) |
| FX | UUP, FXY | USD-broad + non-euro yen (independent signals; FXE≈−UUP dropped as redundant) |
| Crypto | BTC | Documented diversification sleeve; reuses the existing spot ingest |

**Universe rationale.** One instrument per class cannot run a cross-asset test (a single
bond) and is fragile to the coverage gate; three per class buys no proportional
diversification, only more gap/ingest surface. Nine is where the test is fair and
right-sizing holds. FX deliberately avoids FXE: the euro is ~58% of the dollar index, so
FXE ≈ −UUP — two near-inverse positions with almost no independent signal; the yen (FXY)
gives a genuinely distinct second FX leg.

### Coverage gate — two tiers

The gate is **not** a single absolute date (that would break against BTC). Each tier's
start criterion is expressed relative to the blind holdout frontier (post-2020 regime,
suggested first Monday of 2021).

**Tier A — core ETFs (SPY, EFA, IEF, TLT, GLD, DBC, UUP, FXY):**
- Start ≤ 2007-12-31 (binding: UUP 2007-03-01, FXY 2007-02-13, DBC 2006-02-06; the rest
  far earlier). ≥ ~13 yrs of continuous history before the holdout (2007→2021) — absorbs
  the 4–5-week embargo without eating the validation sample.
- Continuity ≥ 99% of NYSE trading days, no long gaps.

**Tier B — crypto sleeve (BTC):**
- Start 2011-08-18 (Bitstamp). No Tier-A parity required: documented diversification leg,
  not an alpha leg.
- BTC is the youngest instrument → it sets the **common portfolio start** for the
  T-23/T-25 backtest.
- ≥ ~9 yrs before the holdout (2011→2021); continuity over a 365-day calendar (BTC trades
  daily), trivially passing.

Ragged start is allowed for the per-instrument signal; the common start (BTC 2011-08) is
documented for the portfolio.

### Coverage — probe result

One-time probe (Yahoo) over the full history. Coverage % is rows
÷ NYSE weekday count; ~96% reflects the ~9 holidays/yr netted out, not gaps (max gap ≤ 5
business days = holiday weeks). Every Tier-A instrument passes; nothing was dropped.

| Class | Sym | Source | First | Last | Rows | Span (yr) | Cov % | Max gap (bd) | Rows ≥2021 | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|
| Equity | SPY | yahoo | 1993-01-29 | 2026-06-24 | 8407 | 33.4 | 96.5 | 5 | 1374 | PASS |
| Equity | EFA | yahoo | 2001-08-27 | 2026-06-24 | 6242 | 24.8 | 96.4 | 5 | 1374 | PASS |
| Bonds | IEF | yahoo | 2002-07-30 | 2026-06-24 | 6014 | 23.9 | 96.4 | 3 | 1374 | PASS |
| Bonds | TLT | yahoo | 2002-07-30 | 2026-06-24 | 6014 | 23.9 | 96.4 | 3 | 1374 | PASS |
| Commodities | GLD | yahoo | 2004-11-18 | 2026-06-24 | 5432 | 21.6 | 96.4 | 3 | 1374 | PASS |
| Commodities | DBC | yahoo | 2006-02-06 | 2026-06-24 | 5127 | 20.4 | 96.4 | 3 | 1374 | PASS |
| FX | UUP | yahoo | 2007-03-01 | 2026-06-24 | 4860 | 19.3 | 96.4 | 3 | 1374 | PASS |
| FX | FXY | yahoo | 2007-02-13 | 2026-06-24 | 4871 | 19.4 | 96.4 | 3 | 1374 | PASS |
| Crypto | BTC | bitstamp/binance | 2011-08-18 | live | — | ~15 | n/a (365d) | — | — | PASS (sleeve, Tier B) |

No candidate was dropped: all eight ETFs clear Tier A, BTC clears Tier B. If a provider
had truncated a series below its real inception, the **data** — not the inception date —
would decide; the probe confirmed none did.

### Holdout (large, blind)

- Frontier fixed blind, by date, aligned to the post-2020 correlation-flip regime
  (suggested: first Monday of 2021). The most adversarial environment for the cross-asset
  channel → the most honest test.
- Size: **~⅓ of history** (deliberately demanding), **≥ 250 weekly observations**.
- Evaluated **exactly once** (T-37); not reused for the XTSMOM epic (separate validation
  route).

**Power caveat (belongs to the verdict, not the gate).** The gate secures *sample
coverage* to evaluate, not statistical *power*. With a 12-month formation horizon, 250
weekly observations are strongly autocorrelated and the effective N is in the tens —
exactly the pooled-t-stat trap of Huang et al. This is acknowledged at T-38; "250 obs" is
never inflated as if it were ample power.

### Coverage gate — integration-test contract (T-20)

The reproducible gate is **not** a committed script (that would duplicate the ingest and
overlap T-20). It landed as an integration test (`tests/test_integration_multiasset.py`,
`pytest.mark.integration`, read-only, skips without ADC and per-symbol before the
back-fill, runs in CI) that validates the **ingest output** in `yahoo_etf_daily_raw`
rather than re-downloading: each instrument reaches the Tier-A frontier (≤ 2007-12-31),
clears a ≥ 0.95 coverage ratio over NYSE trading days, and shows no calendar gap > 7 days
(a real hole, vs holiday weeks) — plus natural-key uniqueness on both source tables.
Calendar note for the contract: the 8 ETFs close Friday (NYSE), BTC closes Sunday (365d);
the Monday→Sunday weekly resample reconciles them, with the crypto sleeve closing 2 days
after the ETFs — immaterial at weekly frequency but stated explicitly for point-in-time
hygiene.

**Exit gate (honest):** coverage proved sufficient for a walk-forward with a post-2020
holdout, so the universe is frozen at nine. Had it been insufficient, the universe would
be documented and narrowed and that limitation reported.

---

## TSMOM signal

> Status: **done (T-22).** Pure, GCP-free signal logic in
> [`../dataflow/strategy/tsmom_signal.py`](../dataflow/strategy/tsmom_signal.py),
> unit-tested in `tests/test_tsmom_signal.py`. Gold materialisation and the
> versioned parameter row are done (T-24 — `dataflow/stages/tsmom_signal_stage.py`
> writes `fact_signals` for strategy_id=3). The backtest engine (T-25) reuses the
> same functions.

**Signal = sign of the cumulative excess return over a formation horizon.** For
instrument *i* at rebalance week *t*, sum the trailing `formation_horizon` weekly
**excess log returns** (additive, so the cumulative is a plain sum) and take its
sign: `+1` long, `−1` short, `0` flat. The horizon `L ∈ {1, 3, 6, 12}` months is a
**counted trial**, calibrated within the trial budget (Epic 8), never hardcoded.

**Volatility scaling is a separate, counted factor — never hidden in the signal.**
The position is `signal × (vol_target / realized_vol)`, where `vol_target` is an
explicit, *required* parameter (no magic default) and `realized_vol` is the
ex-ante annualised volatility from silver. Keeping the **unscaled signal** and the
**scaled position** as distinct outputs is what lets Epic 8 report performance
*with and without* vol scaling (methodology principle 2 below). An optional
leverage cap bounds the factor so a collapsing volatility cannot demand unbounded
size.

**Input contract (point-in-time, from T-21).** The feature layer is
`prod_trade_silver.vw_asset_returns_weekly`: the signal reads `excess_log_return`
and `realized_vol_26w` keyed by `week_start`. No look-ahead is introduced here —
the formation sum uses only the trailing window and the volatility is the estimate
known at *t*; the cross-asset/PIT hygiene that needs market or risk-free data lives
in the view (DFF point-in-time, Monday→Sunday weeks).

**NULL contract (mirrors the RSI warm-up).** The first `formation_horizon − 1`
weeks have no full formation window → no signal. The volatility's own 26-week
warm-up arrives as `NULL`, leaving a directional signal **unsized** (`NULL`
position) until a usable estimate exists; a flat (`0`) signal needs no sizing.

**Parameters (`TsmomParams`).** `formation_horizon`, `vol_target` and
`vol_lookback` are required; `periods_per_year` defaults to 52 (weekly cadence);
`max_leverage` is an optional cap; `vol_scaling: bool = True` (added in T-25)
bypasses the scaling factor when `False`, producing ±1/0 positions for the Kim et
al. (2016) confound isolation — always `True` in production. Both `formation_horizon`
and `vol_target` are **trials counted** against the budget — persisting them as a
versioned strategy row is T-24.

---

## Portfolio construction

> Status: **done (T-23).** Pure, GCP-free cross-sectional logic in
> [`../dataflow/strategy/portfolio.py`](../dataflow/strategy/portfolio.py),
> unit-tested in `tests/test_portfolio.py`. Gold materialisation and the
> versioned parameter row are done (T-24 — `dataflow/stages/portfolio_weights_stage.py`
> writes `fact_portfolio_weights` for strategy_id=3). The backtest engine (T-25)
> reuses these functions.

**From per-instrument positions to one weight book per week.** T-22 emits, per
instrument and rebalance week, a sign and a vol-scaled position. T-23 combines that
**cross-section** into portfolio weights under a named scheme, caps the crypto
sleeve, and produces both a **with-crypto** and a **without-crypto** book (two
separate portfolios, the diversification experiment vs the core).

**Three weighting schemes (plus a distinct baseline).**

- **`equal_weight`** — `wᵢ = sᵢ / N_active`: equal capital per active leg.
- **`inverse_vol`** (alias **`equal_vol`**) — `wᵢ ∝ sᵢ / σᵢ`: lower-vol legs get more
  weight so each contributes equal volatility under zero correlation.
- **`risk_parity`** — equal risk contribution. **v1 is diagonal** (zero
  cross-correlation assumed), which collapses to inverse-vol in closed form; the
  full-covariance ERC solver (rolling covariance + shrinkage, capturing cross-asset
  correlation) is **deferred to a later ticket** and plugs into a reserved `cov`
  hook whose shape is already fixed.

**Honest v1 coincidence.** Under the v1 conventions (per-asset vol scaling from
T-22 + diagonal covariance + gross-1 normalisation), **`equal_vol`, `inverse_vol`
and diagonal `risk_parity` yield identical normalised weights**. They keep distinct
labels (clean trial counting for Epic 8 and a clean ERC swap later) but the
coincidence is documented, not hidden — they diverge only once full-covariance ERC
or a different normalisation lands. `equal_weight` is the genuinely *distinct*
alternative the validation compares against, so the three named schemes do not
inflate the trial budget with identical trials. The identity is pinned by a
**value-based** test (heterogeneous per-instrument vols), so a future ERC change is
caught rather than passing silently.

**Normalisation = gross leverage 1.** Weights are normalised to `Σ|wᵢ| = 1`
(dividing by gross `Σ|w|`, never net `|Σw|`, so a market-neutral book is valid:
gross 1, net 0). The absolute vol-target level stays in T-22's per-instrument
scaling, so performance is still reportable **with and without vol scaling**.

**Crypto cap (a counted trial) and its deliberate coupling.** After
normalisation, each crypto leg above the cap is clipped to `±cap` and the freed
gross redistributed **pro rata** across the non-crypto legs. With a single capped
sleeve this is **one-shot** (no iteration); adding a second cap (e.g. per class)
would break that and need a water-filling solve — flagged so it is not silently
relied on. Unlike T-22's per-instrument vol scaling, which *isolates* each leg, the
cap is a **portfolio-layer constraint that intentionally couples** instruments:
each non-crypto leg's exposure depends on how much crypto was clipped that week.
This is stated as a deliberate v1 choice, not an oversight. **Edge — only the
crypto sleeve trades that week:** the book is left **gross-partial** (`gross =
cap`), honouring the **cap** (the real risk constraint) over the gross-1 invariant
— the sleeve is a documented diversification leg, never held at 100% just because
it is the only thing trading.

**Vol estimator shared with T-22 (confirmed).** The weighting reads the same
`realized_vol_26w` column T-22 scales positions with, so the per-instrument scaling
and the portfolio weighting use *one* vol estimator (26-week realised vol from the
T-21 view), not two silently-different ones.

---

## Modeling lifecycle & tooling

> Status: **T-24 done / T-25 done.** T-24 wired the pure T-22/T-23 functions into two
> Beam stages and created the versioned parameter table; T-25 built the offline backtest
> engine, cost model, walk-forward splitter, and overfit-detection metrics. T-26
> (experiment grid runner, `experiment_runs` writer) is next.
>
> **T-22 minor reopen (T-25 scope):** `vol_scaling: bool = True` added to `TsmomParams`.
> When `False`, the per-instrument position equals the unscaled signal sign (±1/0) — used
> with `scheme=equal_weight` for the Kim et al. (2016) no-vol-scaling baseline. Production
> always uses `vol_scaling=True`; the flag has no effect on T-24 BigQuery output.
>
> **T-24 deliverables:**
> - `dataflow/stages/tsmom_signal_stage.py` — Stage A: silver `vw_asset_returns_weekly`
>   → `gold.fact_signals` (strategy_id=3). Separate staging table (`fact_signals_tsmom_staging`)
>   to avoid concurrent-run collision with the RSI job. MERGE is upsert-only (no WMBS
>   branch); `strategy_id` in the ON clause prevents cross-strategy collisions.
> - `dataflow/stages/portfolio_weights_stage.py` — Stage B: `fact_signals` (strategy_id=3)
>   → `gold.fact_portfolio_weights`. Two weight books per week (include_crypto True/False).
>   Called only after Stage A's MERGE returns (sequential pipelines, not one graph).
> - `dataflow/strategy3_pipeline.py` — standalone entry point (`--stage all/tsmom_signal/
>   portfolio_weights`). Not wired into the daily DAG; run manually until Epic 8 validates.
> - `prod_trade_strategy.strategy_tsmom_multiasset` — versioned params table, seeded with
>   param_version=1 (formation_horizon=52, vol_target=0.10, vol_lookback=26, scheme=inverse_vol,
>   crypto_cap=0.20). **No `strategy_id` column — single-strategy by design** (one table per
>   strategy, isolation by structure). Promotion pattern: atomic flip `WHERE TRUE`:
>   `UPDATE strategy_tsmom_multiasset SET is_active=(param_version=@v) WHERE TRUE;`
> - `prod_trade_gold.fact_portfolio_weights` — new gold table; natural key
>   `(week_start, strategy_id, symbol, include_crypto)`.
> - `experiment_runs` DDL — **deferred writer** to T-26; DDL added in T-25 (schema mirrors
>   `WalkForwardStats`, coherent with the engine that produces it). `research/run_experiments.py`
>   and `promote.py` arrive with T-26.
>
> **T-25 deliverables (offline backtest engine — never imported by the daily pipeline):**
> - `backtest/costs.py` — `InstrumentClass` enum, `SYMBOL_CLASS` (explicit symbol→class dict;
>   "BTCUSD" not "BTC" — matching `vw_asset_returns_weekly`), `ROUND_TRIP_BPS`
>   (EQUITY=8, BOND=8, COMMODITY=12, FX=10, CRYPTO=80 bps), `ROLL_COST_BPS_PA` (all 0 by
>   default; DBC roll is in its NAV — adding it here would double-count), `transaction_cost_return`
>   and `roll_cost_return` (sensitivity hook only). Cost basis for 80 bps BTCUSD documented
>   (retail spread + exchange fee + slippage).
> - `backtest/splitter.py` — `HOLDOUT_START = date(2021, 1, 4)` (theory-driven, sealed as a
>   constant; Molenaar et al. 2024 stock-bond correlation flip), `WalkForwardConfig`
>   (n_splits, min_train_weeks=104, purge_weeks=3, embargo_weeks=2, mode=expanding/rolling),
>   `walk_forward_splits` (purge + embargo, raises `HoldoutViolationError` on holdout dates
>   in input), `open_holdout` (validates ≥ 250 holdout obs; single-use by convention).
> - `backtest/metrics.py` — `annualized_sharpe`, `annualized_sortino`, `max_drawdown`,
>   `calmar_ratio`, `profit_factor` (period-level stats on the **simple** return series),
>   `deflated_sharpe_ratio` (Bailey & López de Prado 2014; takes `sharpe_trials: list[float]`
>   so n and Var(SR) are derived internally — the Var term is what the old n-only signature
>   missed), `pbo_cscv` (CSCV, 20 blocks, PBO ∈ [0, 1]), `hlz_haircut` (Harvey-Liu-Zhu 2016),
>   `WalkForwardStats`. No scipy dependency (pure-Python CDF/PPF via rational approximation).
> - `backtest/engine.py` — `run_backtest`: reads `excess_return` (simple column from T-21)
>   for cross-asset aggregation (log returns are NOT additive across assets); feeds
>   `excess_log_return` to `compute_tsmom_rows` (additive per-asset); reuses
>   `build_portfolio` from T-23; applies `cost_multiplier` grid {1.0, 1.5, 2.0} via
>   `transaction_cost_return`. **Timing contract w(t) × r(t+1):** the signal for week t
>   closes on the Sunday that ends week t (both the formation window and the
>   `realized_vol_26w` window include week t), the book trades on the Monday opening
>   week t+1 and earns that week's return — `BacktestResult.dates` holds **return
>   weeks**. `BacktestResult` with gross/net returns, turnover, the weight book held
>   each week, and `equity_curve()` helper.
> - `prod_trade_strategy.experiment_runs` — trial ledger DDL added to `sql/DDL.sql`; writer
>   is `research/run_experiments.py` (T-26). Schema: `experiment_run_id`, params JSON,
>   `cost_multiplier`, fold stats (Sharpe/Sortino/MaxDD/Calmar), DSR, PBO, HLZ t-stat,
>   `n_trials_at_time`, `holdout_spent`, `holdout_sharpe_net`, `promoted`.
> - 119 unit tests across five files (`test_backtest_costs.py`, `test_backtest_engine.py`,
>   `test_backtest_splitter.py`, `test_backtest_metrics.py`, `test_baselines.py`). Total
>   suite: 703 tests, 91% coverage (gate ≥ 85%). Key tests: hand-verified cross-asset
>   arithmetic (proves engine reads simple column, not log), DSR variance effect (proves
>   `Var(sharpe_trials)` matters), universe coverage gate (asserts `SYMBOL_CLASS.keys() ==
>   EXPECTED_SYMBOLS` where `EXPECTED_SYMBOLS` came from `SELECT DISTINCT symbol FROM
>   vw_asset_returns_weekly`), HAC Newey-West correction for autocorrelated δ_t (proves
>   `t_stat_HAC < t_stat_naïve` on AR(1) ρ=0.9 — the Huang 2020 MOP-error guard).

**The "model" here is a tiny counted-parameter set, not a trained network.** Strategy 3
calibrates ~2–3 parameters (`formation_horizon`, `vol_target`, weighting scheme) over a
budget of 10–15 trials (validation principle 7). That right-sizes the tooling: heavy MLOps
(model registry, serving, Bayesian HPO, a feature store) would be over-engineering and works
*against* the project's right-sizing narrative.

**BigQuery is already the model registry.** The production artifact is a *versioned parameter
row* in `prod_trade_strategy` (`strategy_tsmom_multiasset`, T-24), read by the daily job —
the same "train offline, serve frozen params from BigQuery" pattern the RSI strategy uses. No
MLflow Model Registry / serving: a single param row needs no serialisation, and a second
registry would be redundant.

| Lifecycle stage | Tool | Rationale / caveat |
|---|---|---|
| Parameter search | Explicit grid (nested loop over `TsmomParams`) | 10–15 trials → enumerate, don't sample. **No Optuna**: Bayesian search hides the trial count the DSR needs (the ML skill is vendored without it for the same reason). |
| Experiment tracking | **A BigQuery `experiment_runs` table** (one row per trial) | Principle 7 requires counting every trial and computing DSR over all of them — the table *is* that trial ledger, and it reuses the warehouse that is already the registry (no MLflow server, no `mlruns/` store, one dependency fewer). Log params and metrics (Sharpe, DSR, PBO, t-stat, per-window DD); heavy artifacts (equity-curve PNGs) go to GCS keyed by `experiment_run_id`. |
| Backtest engine | Own pure-Python code (`backtest/`, T-25) | Reuses `dataflow/strategy/tsmom_signal.py` + `portfolio.py` (kept GCP-free for this). |
| Performance metrics | `portfolio-analytics` skill | Sharpe/Sortino/Calmar/profit factor/max DD/rolling — not re-implemented by hand. |
| Temporal validation | `walk-forward-validation` skill | Purge+embargo, CSCV→PBO, DSR, overfit detection (López de Prado) = validation principles 4/7. |
| Promotion to prod | Script → BigQuery param row | Pick the winning run **only after the single holdout** (principle 6), then write the versioned param row, storing its **`experiment_run_id`** (FK into `experiment_runs`) for lineage. One-directional. |
| Live evaluation | `vw_btc_monitor_daily` + Looker Studio (existing) | Live strategy-P&L tracking is a later concern, not v1. |

**Target repo layout (offline vs online boundary).**

```
trade_gcp/
  dataflow/strategy/   # pure signal + portfolio (T-22/23) — reused by prod AND backtest
  backtest/            # T-25 offline engine: WF splits, costs, DSR/PBO (pure logic + thin runners)
  research/            # experiment driver — NEVER imported by the daily pipeline
    run_experiments.py #   grid over the TsmomParams budget → writes the experiment_runs table
    promote.py         #   read the chosen run → write the versioned param row
  sql/DDL.sql          # + experiment_runs ledger + versioned params table for strategy_id=3
  orchestration/       # daily prod — only READS the param row (unchanged)
```

**Offline/online boundary (the "separate training from inference" rule).** Everything under
`research/` and `backtest/` is offline; the daily Airflow/Dataflow job never imports the
backtest engine — it only reads the frozen param row from BigQuery. This is the same boundary
that keeps the RSI genetic algorithm out of the daily job.

**Deliberately not adopted (right-sizing).** **MLflow in any form — registry, serving *and*
Tracking:** BigQuery is already both the registry and (via `experiment_runs`) the trial ledger,
so an MLflow server / `mlruns/` store would add a dependency and a second source of truth for no
gain at 10–15 trials. Optuna / Ray Tune (that trial budget doesn't justify a sampler, and
enumeration is the point); a feature store (silver/gold views are already the versioned,
point-in-time feature layer). Only if a real ML signal *classifier* is ever revisited (not the
current path) would MLflow Tracking, the `signal-classification` + `machine-learning-engineering`
skills and a real model registry genuinely earn their place.

---

## Validation methodology

> Status: **drafted** from the plan's validation principles; hardens as the evaluation
> tickets (Epic 8) land.

These principles govern every evaluation ticket. Any ticket that violates them is
rejected in review.

1. **Triple benchmark, not just buy-and-hold.** Does the signal add anything over
   alternatives that need no predictability? Compare against **(a) TSH** (sign of the
   historical mean, no temporal signal — the most demanding benchmark, per Huang et
   al.: if TSMOM ties TSH, the signal adds nothing), **(b) buy-and-hold at the same vol
   scaling** (Kim et al. — isolates the signal from the scaling), **(c) passive 60/40 +
   per-class buy-and-hold**.
2. **Vol scaling reported with and without, always.** The scaling level is a **counted
   trial**, never a hidden default.
3. **Costs in every profitability metric.** Per-class base (fee + slippage + bid-ask);
   sensitivity to a conservative multiple. Model roll cost for futures.
4. **Strict temporal separation.** Never shuffle. Walk-forward with **purge + embargo**
   (López de Prado): purge observations whose horizon overlaps the test; embargo **4–5
   weeks**.
5. **Single untouchable holdout, fixed blind.** Set by date at setup, **before** any
   signal is computed; evaluated **once** with frozen model and params. Frontier: the
   last **~⅓ of history, aligned to the post-2020/2021 correlation regime** (the most
   adversarial test for the cross-asset channel).
6. **Holdout is a one-way door.** Once spent on TSMOM v1, **not reused** for XTSMOM.
7. **Count experiments, correct for multiple testing.** Budget **10–15 parameter
   variants**. **Deflated Sharpe Ratio** over all trials; significance at Sharpe ~1.2–1.5
   annualized and, per Harvey-Liu-Zhu, **t > 3**. **PBO < 0.3** via CSCV (20 blocks). A
   backtest Sharpe > 2 is suspicious by default.
8. **Point-in-time hygiene.** Only data knowable at each signal's date: futures rolls
   without look-ahead, macro series with their `realtime_start` (vintage), forward-fill
   forward only.
9. **A negative result is documented all the same.** "Evaluated and discarded," with the
   figures — CV value equivalent to a model in production.
10. **DSR and PBO do not anticipate structural breaks** (Harris caveat). The 2022
    correlation flip is exactly that risk; documented as a known limit, not hidden.

---

## Baselines

> Status: **done (T-26).**

Implemented in `backtest/baselines.py`; run via `research/run_experiments.py`.
Each baseline neutralises **exactly one** dimension relative to TSMOM so the
gate is interpretable.

| Baseline | Shares with TSMOM | Neutralises |
|---|---|---|
| TSH | vol scaling, scheme, crypto cap | Signal formation (historical mean vs 52w window) |
| Vol-BH | vol scaling, scheme, crypto cap | Signal direction (always +1 vs momentum sign) |
| 60/40 Passive | nothing | All tactical components |

### (a) TSH — Time-Series Historical (Huang et al. 2020 JFE)

Signal = sign(mean `excess_log_return` over the entire fold training window). Same vol
scaling, portfolio scheme, and crypto cap as the TSMOM configuration being compared; only
signal formation differs. This is the strictest benchmark: Huang et al. (2020 JFE) show
that TSMOM performs virtually identically to TSH (alpha differential p ≈ 0.26). If TSMOM
does not beat TSH, the 52-week formation window adds nothing over the historical mean direction.

### (b) Vol-Scaled Buy-and-Hold (Kim et al. 2016 JFM)

Signal = +1 always. Same vol scaling and portfolio construction as TSMOM. Neutralises
signal direction only. If vol-BH ≈ TSMOM, vol scaling (not momentum) drives returns.

### (c) 60/40 Passive

Fixed weights: 30% SPY + 30% EFA + 20% IEF + 20% TLT. No signal, no vol scaling.
Annual drift rebalance (`rebalance_weeks=52`). Weight drift computed from `simple_return`
(total price return, as in a real portfolio); returns reported as `excess_return` for
comparability.

**Comparability:** Sharpe and Sortino are scale-invariant and directly comparable to
TSMOM. Max drawdown is **not** comparable — 60/40 runs at its natural volatility (~10–12%);
TSMOM targets 10% via vol scaling.

### Walk-forward notes

- **Timing (w(t) × r(t+1), same as the engine):** TSH and vol-BH size each return week
  with the *prior* week's `realized_vol_26w` — the vol window in the T-21 view includes
  the current week, so sizing week t's return with vol(t) would peek at that week. The
  60/40 passive needs no shift: its weights are unconditional constants, and drift is
  applied only after a week's return is recorded.
- **Fold alignment:** all four strategies are trimmed to the intersection of active dates
  per fold before computing metrics. Ensures `δ_t = net_TSMOM(t) − net_TSH(t)` is a
  term-by-term subtraction on identical periods.
- **Crypto sleeve in early folds:** BTC's vol warm-up (26w from 2011-08) may exclude it
  from the first 1–2 fold cross-sections. Declared; not hidden. Fold-level metrics reflect
  a portfolio without crypto in early windows.

### Gate criterion (pre-committed, set before running)

Pool all test-week net returns across folds for TSMOM and TSH. Compute
`δ_t = net_TSMOM(t) − net_TSH(t)` (~360 pooled weekly observations).

A naïve t-stat (`mean(δ) / (std(δ)/√n)`) is not valid here: the 52-week formation signal
barely changes week-to-week, so consecutive `δ_t` values are nearly identical. Treating
360 correlated observations as 360 independent ones inflates the t-stat — the same MOP
error Huang (2020) demonstrated with the pooled t-stat of 4.34.

**HAC-corrected t-stat (Newey-West, Bartlett kernel, L=52):**

    V_NW = γ(0) + 2·Σ_{l=1}^{52} (1 − l/53)·γ(l)
    t_stat_HAC = mean(δ) / √(V_NW / n)

Gate passes if `t_stat_HAC > 1.64` (one-sided α = 0.10, pre-committed).
If not: document that the momentum signal adds nothing beyond the historical mean direction
and consider closing Epic 8 **before** spending the single holdout.

**DSR and HLZ:** both deferred to T-27 — both are multiple-testing corrections that
require the full trial grid. With one TSMOM trial in T-26, there is no multiplicity to
correct. `dsr=NULL`, `hlz_tstat=NULL` in all T-26 `experiment_runs` rows.

### Results

> Run 2026-07-20 (**total-return**, `run_label = 'total-return / fix dividends'`) —
> engine under the `w(t) × r(t+1)` timing contract, 5 expanding folds (min_train
> 104w, purge 3w, embargo 2w), 1,343 pooled test weeks. 12 rows written to
> `experiment_runs` (`dsr` / `pbo` / `hlz_tstat` NULL — deferred to T-27;
> `holdout_spent = FALSE` on every row). The earlier price-return run
> (2026-07-17, `run_label = 'price-return baseline (T-26)'`) is retained in
> `experiment_runs` for the before/after comparison.

| Strategy | Cost mult | CV Sharpe (net) | CV Sortino | Ann. ret (net) | Gate t-stat (HAC) |
|---|---|---|---|---|---|
| TSMOM seed v1 | 1.0 | 0.615 | 0.952 | 5.91% | **0.384 — FAIL** |
| TSH | 1.0 | 0.582 | 0.832 | 4.86% | (ref) |
| Vol-BH | 1.0 | 0.780 | 1.129 | 5.81% | — |
| 60/40 passive | 1.0 | 0.622 | 0.897 | 6.81% | — |

Cost sensitivity (net CV Sharpe at ×1.0 / ×1.5 / ×2.0): TSMOM 0.615 / 0.573 / 0.532;
TSH 0.582 / 0.573 / 0.563; vol-BH 0.780 / 0.769 / 0.757; 60/40 0.622 / 0.621 / 0.620.

*Ann. ret (net)* is the geometric annualized excess return, net of costs — the exact per-fold
mean (`cv_ann_return_net`, T-26b). **The raw returns are compressed (4.9–6.8%) and do not
separate the strategies; the differentiator is efficiency, not level.** Read with the risk
basis: TSMOM/TSH/vol-BH are vol-scaled to ~10%, while 60/40 runs at its natural (higher) vol, so
its 6.81% carries the worst drawdown (−24%) and a middling Sharpe. **vol-BH earns essentially the
same return as TSMOM (5.81% vs 5.91%) at a far better Sharpe (0.780 vs 0.615)** — the vol scaling,
not the momentum signal, does the work; TSMOM takes more risk for no return premium.

**Reading (gate only — the verdict belongs to T-27/T-28):**

- **Gate FAILS:** `mean_δ = 0.00021`/week, `t_HAC = 0.384 < 1.64` (Newey-West,
  L=52). On total-return the 52-week formation adds **no** measurable signal over the
  historical-mean direction — the Huang et al. (2020) equivalence to TSH is **not
  rejected** at the pre-committed one-sided α = 0.10. Under the pre-committed decision
  rule, TSMOM v1 does not clear the baseline.
- **What changed vs the price-return run.** The prior run (2026-07-17) passed the gate
  (`t_HAC = 2.309`) only because the price-return series understated the
  dividend-paying baselines: TSH's CV Sharpe rose from **−0.144 to +0.582** once
  dividends were added (TSH is long bonds + SPY), and the TSMOM-vs-TSH gap collapsed
  (`mean_δ` 0.00171 → 0.00021). **The apparent edge was largely a dividend-accounting
  artifact** — exactly the bias the T-26b total-return fix was built to remove, caught
  before the holdout was spent.
- **Corroborating flag:** vol-BH's CV Sharpe (0.780) is the highest of the four and
  exceeds TSMOM's (0.615) — consistent with Kim et al. (2016): in a net-long sample
  vol scaling, not the momentum sign, does the work.

---

## Verdict

> Status: **pending (T-27/T-28).** This is the **conclusion of Strategy 3's
> development** — filled exactly once, after the single holdout is evaluated, and never
> re-run for a better number.

To be filled:

- **Verdict table** — TSMOM v1 vs TSH vs vol-scaled B&H vs 60/40, per window and
  aggregated, **net of costs**, with and without vol scaling.
- **Significance** — Deflated Sharpe Ratio over all trials, **PBO < 0.3** (CSCV, 20
  blocks), Harvey-Liu-Zhu **t > 3**, and N of trials alongside the Sharpe.
- **The holdout** (post-2020) evaluated **exactly once**; the figure — positive or
  negative — with its cost assumptions.
- **Honest caveats** — low inherited prior; DSR/PBO do not anticipate structural breaks
  (the correlation flip); the holdout is not reused for XTSMOM; the crypto sleeve is a
  documented diversification experiment, not an alpha leg.

> ⚠️ Technical example, not financial advice.
