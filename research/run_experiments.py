"""Walk-forward experiment runner for T-26 baselines comparison.

Runs TSMOM seed v1 + 3 frozen baselines (TSH, vol-BH, 60/40) across 3 cost
multiplier levels (1.0, 1.5, 2.0), computes the HAC gate stat (Newey-West,
L=52) comparing TSMOM to TSH, and writes 12 rows to experiment_runs.

Usage:
    python -m research.run_experiments --project trade-390514 [--dry-run]
    python -m research.run_experiments --project trade-390514 --n-splits 5

open_holdout is NOT imported here.  T-26 does not open the holdout.
Fold dates are asserted to be < HOLDOUT_START before any walk-forward logic.

Gate criterion (pre-committed, α = 0.10 one-sided):
    t_stat_HAC > 1.64  →  TSMOM adds signal over the historical mean direction.
    t_stat_HAC ≤ 1.64  →  signal does not exceed TSH; consider closing Epic 8.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import uuid
from datetime import date, datetime, timezone
from typing import Any

from backtest.baselines import (
    PASSIVE_WEIGHTS,
    run_passive_baseline,
    run_tsh_baseline,
    run_vol_bh_baseline,
)
from backtest.engine import BacktestResult, run_backtest
from backtest.metrics import (
    annualized_return,
    annualized_sharpe,
    annualized_sortino,
    calmar_ratio,
    max_drawdown,
)
from backtest.splitter import (
    HOLDOUT_START,
    WalkForwardConfig,
    walk_forward_splits,
)
from dataflow.strategy.portfolio import PortfolioParams, Scheme
from dataflow.strategy.tsmom_signal import TsmomParams

# ── Seed parameters (v1) ─────────────────────────────────────────────────────

TSMOM_V1 = TsmomParams(
    formation_horizon=52,
    vol_target=0.10,
    vol_lookback=26,
    periods_per_year=52,
)
PORTFOLIO_V1 = PortfolioParams(scheme=Scheme.INVERSE_VOL, crypto_cap=0.20)

COST_MULTIPLIERS: list[float] = [1.0, 1.5, 2.0]

GATE_THRESHOLD: float = 1.64  # one-sided α=0.10, pre-committed
GATE_MAX_LAG: int = 52        # Newey-West Bartlett lag L (= formation horizon)
GATE_COST_MULT: float = 1.0   # gate is evaluated at the base cost only

BQ_TABLE: str = "prod_trade_strategy.experiment_runs"

# ── Public testable helpers ───────────────────────────────────────────────────


def compute_gate_stat(
    tsmom_returns: list[float],
    tsh_returns: list[float],
    *,
    max_lag: int = 52,
) -> tuple[float, float]:
    """HAC-corrected gate stat: mean(δ) and Newey-West t-stat.

    Computes δ_t = tsmom_net(t) − tsh_net(t) and applies Newey-West (Bartlett
    kernel) variance correction for autocorrelation in δ.  A naïve std/√n would
    inflate the t-stat identically to the MOP error Huang (2020) demonstrated
    with pooled t-stat = 4.34.

    Gate passes if t_stat_HAC > 1.64 (one-sided α = 0.10, pre-committed).

    Args:
        tsmom_returns: Pooled net returns for TSMOM across all test folds.
        tsh_returns: Pooled net returns for TSH across the same dates/order.
        max_lag: Bartlett kernel lag cap L (default 52 = formation horizon).

    Returns:
        ``(mean_delta, t_stat_hac)``

    Raises:
        ValueError: If the series have different lengths.
    """
    n = len(tsmom_returns)
    if n != len(tsh_returns):
        raise ValueError(
            f"Length mismatch: tsmom={n}, tsh={len(tsh_returns)}"
        )
    if n == 0:
        return 0.0, 0.0

    delta = [t - s for t, s in zip(tsmom_returns, tsh_returns)]
    mean_d = sum(delta) / n
    demeaned = [d - mean_d for d in delta]

    def _gamma(lag: int) -> float:
        return sum(demeaned[t] * demeaned[t - lag] for t in range(lag, n)) / n

    v_nw = _gamma(0)
    for ell in range(1, max_lag + 1):
        v_nw += 2.0 * (1.0 - ell / (max_lag + 1)) * _gamma(ell)

    if v_nw <= 0.0:
        return mean_d, 0.0

    return mean_d, mean_d / math.sqrt(v_nw / n)


def _to_date(value: Any) -> date:
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _filter_result_to_dates(
    result: BacktestResult,
    date_set: set[date],
) -> BacktestResult:
    """Return a new BacktestResult containing only dates in date_set."""
    out = BacktestResult()
    for d, gr, nr, tv, wts in zip(
        result.dates, result.gross_returns, result.net_returns,
        result.turnover, result.weights,
    ):
        if d in date_set:
            out.dates.append(d)
            out.gross_returns.append(gr)
            out.net_returns.append(nr)
            out.turnover.append(tv)
            out.weights.append(wts)
    return out


def _align_to_common_dates(
    results: list[BacktestResult],
) -> list[BacktestResult]:
    """Trim all BacktestResults to their common date intersection.

    Ensures δ_t = TSMOM(t) − TSH(t) is a term-by-term subtraction on
    identical periods.  Different warm-up lengths or vol availability can cause
    misaligned date ranges between strategies.
    """
    if not results:
        return results
    common = set(results[0].dates)
    for r in results[1:]:
        common &= set(r.dates)
    return [_filter_result_to_dates(r, common) for r in results]


def _build_row(
    tsmom_params_json: str,
    portfolio_params_json: str,
    cost_mult: float,
    fold_net_returns: list[list[float]],
    n_cv_folds: int,
    run_label: str | None = None,
    gate: dict | None = None,
) -> dict:
    """Build one experiment_runs row from aggregated fold results.

    ``gate`` is the pre-committed TSMOM-vs-TSH decision stat and its audit
    fields; it is a per-run scalar, so it is attached to exactly one row (the
    TSMOM row at the base cost) and left NULL on every other row.
    """
    sharpes = [annualized_sharpe(r) for r in fold_net_returns]
    sortinos = [annualized_sortino(r) for r in fold_net_returns]
    max_dds = [max_drawdown(r) for r in fold_net_returns]
    calmars = [calmar_ratio(r) for r in fold_net_returns]
    ann_returns = [annualized_return(r) for r in fold_net_returns]

    def _mean(xs: list[float]) -> float:
        return statistics.mean(xs) if xs else 0.0

    return {
        "experiment_run_id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "run_label": run_label,
        "tsmom_params_json": tsmom_params_json,
        "portfolio_params_json": portfolio_params_json,
        "cost_multiplier": cost_mult,
        "n_cv_folds": n_cv_folds,
        "cv_sharpe_net": _mean(sharpes),
        "cv_sortino_net": _mean(sortinos),
        "cv_max_dd": _mean(max_dds),
        "cv_calmar": _mean(calmars),
        "cv_ann_return_net": _mean(ann_returns),
        # dsr / hlz_tstat: both NULL in T-26 — deferred to T-27.
        # Both are multiple-testing corrections requiring the full trial grid.
        # With 1 TSMOM trial in T-26, Var(SR)=0 → DSR degenerate; and HLZ
        # has no multiplicity to correct.  Populate in T-27 with n_independent.
        "dsr": None,
        "pbo": None,
        "hlz_tstat": None,
        "n_trials_at_time": 1,
        "holdout_spent": False,
        "holdout_sharpe_net": None,
        "promoted": False,
        # Gate stat (per run, on the TSMOM base-cost row only; NULL elsewhere).
        # δ_t = tsmom_net − tsh_net, paired weekly; Newey-West HAC t-stat.
        "gate_mean_delta": (gate or {}).get("mean_delta"),
        "gate_t_hac": (gate or {}).get("t_hac"),
        "gate_pass": (gate or {}).get("pass"),
        "gate_n_obs": (gate or {}).get("n_obs"),
        "gate_max_lag": (gate or {}).get("max_lag"),
        "gate_threshold": (gate or {}).get("threshold"),
        "gate_cost_multiplier": (gate or {}).get("cost_multiplier"),
    }


# ── Walk-forward orchestration ────────────────────────────────────────────────


def run_walk_forward(
    rows_by_symbol: dict[str, list[dict]],
    cfg: WalkForwardConfig,
    *,
    with_crypto: bool,
    run_label: str | None = None,
) -> dict[str, Any]:
    """Run walk-forward for all trials.

    Args:
        rows_by_symbol: Pre-holdout rows keyed by symbol.
        cfg: Walk-forward split configuration.
        with_crypto: Pass ``True`` to include BTCUSD in TSMOM/TSH/vol-BH.

    Returns:
        Dict with keys ``rows`` (list of experiment_runs dicts),
        ``gate_mean_delta`` (float), and ``gate_t_stat_hac`` (float).

    Raises:
        AssertionError: If any date in rows_by_symbol >= HOLDOUT_START.
    """
    all_dates = sorted({
        _to_date(row["week_start"])
        for rows in rows_by_symbol.values()
        for row in rows
    })
    if not all_dates:
        raise ValueError("rows_by_symbol contains no rows")

    # Holdout guard: assertion (HoldoutViolationError from splitter is guard #2).
    assert max(all_dates) < HOLDOUT_START, (
        f"rows_by_symbol contains holdout dates (max={max(all_dates)}). "
        f"Load only week_start < '{HOLDOUT_START}'."
    )

    # Pre-run TSMOM once on all data; slice per fold later.
    # TSMOM is rules-based (trailing window only), so running on all pre-holdout
    # data and slicing to test_dates is equivalent to running per-fold.
    tsmom_full: dict[float, BacktestResult] = {
        cm: run_backtest(
            rows_by_symbol, TSMOM_V1, PORTFOLIO_V1,
            with_crypto=with_crypto, cost_multiplier=cm,
        )
        for cm in COST_MULTIPLIERS
    }

    # Per-fold results: dict[cost_mult][strategy] → list[BacktestResult]
    FoldBucket = dict[float, dict[str, list[BacktestResult]]]
    fold_bucket: FoldBucket = {
        cm: {"tsmom": [], "tsh": [], "vol_bh": [], "passive": []}
        for cm in COST_MULTIPLIERS
    }
    n_completed_folds = 0

    for train_dates, test_dates in walk_forward_splits(all_dates, cfg):
        test_set = set(test_dates)
        max_test = max(test_dates)

        # Restrict rows to data available at end of this fold's test window.
        fold_rows = {
            sym: [r for r in rows if _to_date(r["week_start"]) <= max_test]
            for sym, rows in rows_by_symbol.items()
        }

        for cm in COST_MULTIPLIERS:
            r_tsmom = _filter_result_to_dates(tsmom_full[cm], test_set)
            r_tsh = run_tsh_baseline(
                fold_rows, train_dates, test_dates, PORTFOLIO_V1,
                with_crypto=with_crypto, cost_multiplier=cm,
            )
            r_vbh = run_vol_bh_baseline(
                fold_rows, test_dates, PORTFOLIO_V1,
                with_crypto=with_crypto, cost_multiplier=cm,
            )
            r_pas = run_passive_baseline(fold_rows, test_dates, cost_multiplier=cm)

            aligned = _align_to_common_dates([r_tsmom, r_tsh, r_vbh, r_pas])
            r_tsmom, r_tsh, r_vbh, r_pas = aligned

            fold_bucket[cm]["tsmom"].append(r_tsmom)
            fold_bucket[cm]["tsh"].append(r_tsh)
            fold_bucket[cm]["vol_bh"].append(r_vbh)
            fold_bucket[cm]["passive"].append(r_pas)

        n_completed_folds += 1

    portfolio_json = json.dumps({
        "scheme": PORTFOLIO_V1.scheme.value,
        "crypto_cap": PORTFOLIO_V1.crypto_cap,
    })

    # Gate stat at the base cost (GATE_COST_MULT): pool all test-week net returns
    # and run the paired TSMOM−TSH Newey-West test.  Computed here (before the
    # rows) so its scalars + audit fields can be persisted on the TSMOM base-cost
    # row.  n_obs is the pooled test-week count (audit trail for the t-stat).
    tsmom_pooled = [r for fold in fold_bucket[GATE_COST_MULT]["tsmom"] for r in fold.net_returns]
    tsh_pooled = [r for fold in fold_bucket[GATE_COST_MULT]["tsh"] for r in fold.net_returns]
    mean_d, t_stat_hac = compute_gate_stat(tsmom_pooled, tsh_pooled, max_lag=GATE_MAX_LAG)
    gate = {
        "mean_delta": mean_d,
        "t_hac": t_stat_hac,
        "pass": t_stat_hac > GATE_THRESHOLD,
        "n_obs": len(tsmom_pooled),
        "max_lag": GATE_MAX_LAG,
        "threshold": GATE_THRESHOLD,
        "cost_multiplier": GATE_COST_MULT,
    }

    result_rows: list[dict] = []
    for cm in COST_MULTIPLIERS:
        tsmom_net = [r.net_returns for r in fold_bucket[cm]["tsmom"]]
        tsh_net = [r.net_returns for r in fold_bucket[cm]["tsh"]]
        vbh_net = [r.net_returns for r in fold_bucket[cm]["vol_bh"]]
        pas_net = [r.net_returns for r in fold_bucket[cm]["passive"]]

        result_rows.append(_build_row(
            json.dumps({
                "strategy": "tsmom",
                "formation_horizon": TSMOM_V1.formation_horizon,
                "vol_target": TSMOM_V1.vol_target,
                "vol_lookback": TSMOM_V1.vol_lookback,
                "periods_per_year": TSMOM_V1.periods_per_year,
            }),
            portfolio_json, cm, tsmom_net, n_completed_folds, run_label=run_label,
            # Attach the per-run gate to exactly the TSMOM base-cost row.
            gate=gate if cm == GATE_COST_MULT else None,
        ))
        result_rows.append(_build_row(
            json.dumps({"strategy": "tsh", "vol_target": TSMOM_V1.vol_target}),
            portfolio_json, cm, tsh_net, n_completed_folds, run_label=run_label,
        ))
        result_rows.append(_build_row(
            json.dumps({"strategy": "vol_bh", "vol_target": TSMOM_V1.vol_target}),
            portfolio_json, cm, vbh_net, n_completed_folds, run_label=run_label,
        ))
        result_rows.append(_build_row(
            json.dumps({"strategy": "60_40"}),
            json.dumps({"weights": PASSIVE_WEIGHTS}),
            cm, pas_net, n_completed_folds, run_label=run_label,
        ))

    return {
        "rows": result_rows,
        "gate_mean_delta": mean_d,
        "gate_t_stat_hac": t_stat_hac,
        "gate_n_obs": len(tsmom_pooled),
        "fold_bucket": fold_bucket,
        "n_folds": n_completed_folds,
    }


def _print_results(wf: dict[str, Any]) -> None:  # pragma: no cover
    """Print walk-forward summary table to stdout."""
    rows = wf["rows"]
    print("\nExperiment Results")
    print("-" * 88)
    print(f"{'Strategy':<12} {'CostMult':>8} {'CV Sharpe':>10} {'CV Sortino':>11} {'CV MaxDD':>9} {'CV Calmar':>10} {'AnnRet%':>9}")
    print("-" * 88)
    for row in rows:
        strat = json.loads(row["tsmom_params_json"]).get("strategy", "tsmom")
        print(
            f"{strat:<12} {row['cost_multiplier']:>8.1f} "
            f"{row['cv_sharpe_net']:>10.3f} {row['cv_sortino_net']:>11.3f} "
            f"{row['cv_max_dd']:>9.3f} {row['cv_calmar']:>10.3f} "
            f"{row['cv_ann_return_net'] * 100:>9.2f}"
        )
    print("-" * 88)
    mean_d = wf["gate_mean_delta"]
    t_hac = wf["gate_t_stat_hac"]
    gate_pass = "PASS" if t_hac > GATE_THRESHOLD else "FAIL"
    # ASCII-only output: Windows consoles default to cp1252, which cannot
    # encode characters like the Greek delta.
    print(
        f"\nGate (TSMOM vs TSH, cost x1.0): "
        f"mean_delta={mean_d:.5f}  t_HAC={t_hac:.3f}  "
        f"threshold={GATE_THRESHOLD}  [{gate_pass}]"
    )
    print(f"Pooled obs: {sum(len(r.net_returns) for r in wf['fold_bucket'][1.0]['tsmom'])}")
    print(f"Folds completed: {wf['n_folds']}\n")


# ── BQ I/O (requires ADC; excluded from unit test coverage) ──────────────────


def _load_rows_from_bq(project: str) -> dict[str, list[dict]]:  # pragma: no cover
    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = bigquery.Client(project=project)
    query = f"""
        SELECT
            week_start, symbol,
            excess_log_return, excess_return, simple_return, realized_vol_26w
        FROM `{project}.prod_trade_silver.vw_asset_returns_weekly`
        WHERE week_start < '{HOLDOUT_START}'
          -- Each symbol's first week has NULL returns (LAG has no prior row);
          -- the engine contract forbids non-finite values inside a formation
          -- window (gaps are handled upstream), so exclude them at the loader.
          AND excess_log_return IS NOT NULL
        ORDER BY symbol, week_start
    """
    rows_by_symbol: dict[str, list[dict]] = {}
    for row in client.query(query).result():
        sym = row["symbol"]
        rows_by_symbol.setdefault(sym, []).append({
            "week_start": row["week_start"],
            "excess_log_return": row["excess_log_return"],
            "excess_return": row["excess_return"],
            "simple_return": row["simple_return"],
            "realized_vol_26w": row["realized_vol_26w"],
        })
    return rows_by_symbol


def _insert_rows_to_bq(project: str, rows: list[dict]) -> None:  # pragma: no cover
    from google.cloud import bigquery  # type: ignore[import-untyped]

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{BQ_TABLE}"
    # Batch load job, not insert_rows_json: streaming inserts 404 on freshly
    # created tables (metadata propagation) and leave rows in the streaming
    # buffer; a load job is atomic and buffer-free (project convention:
    # batch loads over streaming inserts).
    job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    print(f"Inserted {len(rows)} rows into {table_ref}")


# ── CLI entry point ───────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:  # pragma: no cover
    parser = argparse.ArgumentParser(
        description="T-26 walk-forward experiment runner (baselines)."
    )
    parser.add_argument("--project", required=True, help="GCP project ID.")
    parser.add_argument("--dry-run", action="store_true", help="Skip BQ write.")
    parser.add_argument("--n-splits", type=int, default=5, help="CV fold count.")
    parser.add_argument(
        "--no-crypto", action="store_true",
        help="Exclude BTCUSD from TSMOM/TSH/vol-BH portfolios.",
    )
    parser.add_argument(
        "--label", default=None,
        help="Optional human-readable tag stored in experiment_runs.run_label "
             "(e.g. 'total-return / fix dividends') to distinguish this run.",
    )
    return parser.parse_args()


def main() -> None:  # pragma: no cover
    args = _parse_args()
    cfg = WalkForwardConfig(
        n_splits=args.n_splits,
        min_train_weeks=104,
        purge_weeks=3,
        embargo_weeks=2,
        mode="expanding",
    )
    rows_by_symbol = _load_rows_from_bq(args.project)
    wf = run_walk_forward(
        rows_by_symbol, cfg, with_crypto=not args.no_crypto, run_label=args.label,
    )
    _print_results(wf)

    if not args.dry_run:
        _insert_rows_to_bq(args.project, wf["rows"])
    else:
        print("Dry-run: skipping BQ insert.")


if __name__ == "__main__":  # pragma: no cover
    main()
