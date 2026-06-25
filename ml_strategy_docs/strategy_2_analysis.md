# Strategy 2 — BTC momentum + on-chain meta-label: analysis

> Analysis record for Strategy 2 — **evaluated in the research arc and discarded; never
> built** (no `strategy_id` assigned). Spanish mirror (context, outside the repo):
> `Trade GCP/ml_strategy_docs/strategy_2_analysis.md` — keep both in sync in the same
> change.

| Section | Status |
|---|---|
| [Thesis](#thesis) | evaluated in the research arc |
| [Why discarded](#why-discarded) | ❌ not prioritized, never built |

---

## Thesis

A single-asset strategy: BTC time-series **momentum** as the primary signal, with
**on-chain context** (MVRV, network factors, etc.) as a **meta-label** to filter or size
the momentum signal — meta-labeling (à la López de Prado), but mono-asset.

## Why discarded

It hits the same wall as Strategy 1: **single-instrument time-series momentum** shows
little per-asset predictability (Huang et al. 2020, JFE — most instruments have a
negative out-of-sample R², and per-asset TSMOM is statistically indistinguishable from
just holding assets with a positive historical mean). An on-chain meta-label does not
rescue a primary signal that is weak per-instrument; it would mostly relabel noise. With
no peer-reviewed evidence of crypto momentum as a return source, the prior was too low to
prioritize building it.

The conclusion reframes the program: the weakness is the **single-asset** framing — which
is exactly what **Strategy 3** drops, identifying momentum *across* assets (cross-asset
TSMOM / XTSMOM), where the literature is far stronger. See
[Strategy 3 — research thesis](strategy_3_analysis.md#research-thesis).
