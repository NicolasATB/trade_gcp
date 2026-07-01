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
| [Modeling lifecycle & tooling](#modeling-lifecycle--tooling) | T-24/T-25 | ✅ T-24 / ⏳ T-25 |
| [Validation methodology](#validation-methodology) | Epic 8 | ✍️ drafted |
| [Baselines](#baselines) | T-26 | ⏳ pending |
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

---

## Data sources per class

**Decision (v1): ETFs for every non-crypto class — Yahoo Finance as the primary
source, Tiingo as a competing fallback. Crypto reuses the existing spot ingest
(Binance/Bitstamp). Futures are deferred.**

Futures are more cost-accurate but carry roll/contango and the point-in-time
active-contract hygiene a v1 pipeline doesn't need. ETFs are simple and continuous; the
conformed close is **split-adjusted but not dividend-adjusted** (a price-return level, not
total-return) — enough to exercise the multi-asset pipeline.

| Asset class | Instrument (v1) | Source (primary / fallback) | Avoids roll? | Trade-off / rationale |
|---|---|---|---|---|
| Equity indices | ETF (e.g. SPY / EFA / EEM) | Yahoo / Tiingo | Yes | Liquid proxy; close is split-adjusted price-return (not dividend-adjusted); futures add roll + active-contract tracking for no v1 gain. |
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
  (raw provider fields) to drive it. Neither source is dividend-adjusted, so the series is
  **price-return, not total-return** — a documented v1 limitation.
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
`max_leverage` is an optional cap. Both `formation_horizon` and `vol_target` are
**trials counted** against the budget — persisting them as a versioned strategy
row is T-24.

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

> Status: **T-24 done / T-25 pending.** T-24 wired the pure T-22/T-23 functions into
> two Beam stages and created the versioned parameter table; T-25 (backtest engine +
> experiment ledger + promotion script) remains. The daily pipeline is unaffected.
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
> - `experiment_runs`, `research/run_experiments.py`, `promote.py` — **deferred to T-25+**.
>   No backtest writer exists yet, so the experiment ledger has no writer. T-24 only
>   seeds the params placeholder; the promotion script arrives with the backtest engine.

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

> Status: **pending (T-26).** The "before" picture TSMOM is judged against (methodology
> principle 1).

To be filled in T-26 — run the backtest engine over the three benchmarks (**(a)** TSH,
**(b)** vol-scaled buy-and-hold, **(c)** 60/40 + per-class buy-and-hold), per
walk-forward window and aggregated, net of costs, with cost sensitivity.

**Exit gate (honest):** if TSMOM ties TSH in validation (the Huang scenario), document
that the signal adds nothing over the historical mean and consider closing the epic with
a negative result **before** spending the single holdout.

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
