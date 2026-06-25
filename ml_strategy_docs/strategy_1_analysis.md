# Strategy 1 — RSI directional: analysis

> Analysis record for Strategy 1 (`strategy_id=1`), the live pipeline. Spanish mirror
> (context, outside the repo): `Trade GCP/ml_strategy_docs/strategy_1_analysis.md` —
> keep both in sync in the same change. Pipeline *mechanics* live in the README
> (*Daily data flow*) and [`../sql/DDL.sql`](../sql/DDL.sql); this doc is the
> *decision / analysis* angle.

| Section | Status |
|---|---|
| [Thesis and status](#thesis-and-status) | ✅ in production |
| [Signal definition](#signal-definition) | ✅ live |
| [Calibration](#calibration) | offline (automation out of scope) |
| [Verdict](#verdict) | ✅ not pursued as alpha |

---

## Thesis and status

Strategy 1 is the live BTC RSI pipeline (`strategy_id=1`), kept in production as the
**engineering backbone** of the project. It is **not** pursued as an *alpha* source:
directional technical analysis on a single crypto does not survive data-snooping once
costs are included — with enough rules and thresholds tested, in-sample edges are
multiple-testing artifacts that decay out of sample (McLean & Pontiff 2016) and rarely
clear the **t > 3** bar that corrects for the search (Harvey, Liu & Zhu 2016). Its value
is the end-to-end data engineering, not an expectation of returns.

## Signal definition

- **Indicator:** RSI with **Wilder** smoothing (recursive state), on daily (`1d`) and
  weekly (`1w`) candles. Warm-up: the first `rsi_period` rows publish `rsi = NULL`.
- **Week convention:** Monday→Sunday (`WEEK(MONDAY)`).
- **Signal:** the active params in `strategy_rsi_daily_week` (seeded `14 / 40 / 70 / 30 /
  70`) drive a weekly trend walk-forward combined with daily RSI thresholds → BUY / SELL
  / NEUTRAL in `fact_signals`. Schema in [`../sql/DDL.sql`](../sql/DDL.sql); mechanics in
  the README's *Daily data flow*.

## Calibration

The four RSI parameters are calibrated **offline** by a genetic algorithm (a separate
notebook), kept out of the daily path. Automating it as a DAG is **out of scope**: since
Strategy 1 isn't pursued as alpha, sharpening its parameters would only fit an edge the
evidence says isn't there. Any automation would require **walk-forward validation**
(purge + embargo) to avoid overfitting and look-ahead bias — the standard Strategy 3 is
held to.

## Verdict

Kept live as the engineering showcase; **not an alpha claim**. The directional
single-crypto thesis is closed; the project's alpha effort moves to cross-asset TSMOM
(see [Strategy 3 — research thesis](strategy_3_analysis.md#research-thesis)).

> ⚠️ Technical example, not financial advice.
