"""Offline multi-asset TSMOM backtest engine (T-25).

Public re-exports — import from here, not from sub-modules, to keep the
engine's public surface stable as the internals evolve.
"""

from backtest.baselines import (
    PASSIVE_WEIGHTS,
    run_passive_baseline,
    run_tsh_baseline,
    run_vol_bh_baseline,
)
from backtest.engine import BacktestResult, run_backtest
from backtest.metrics import WalkForwardStats
from backtest.splitter import (
    HOLDOUT_START,
    HoldoutViolationError,
    WalkForwardConfig,
    open_holdout,
    walk_forward_splits,
)

__all__ = [
    # Engine
    "run_backtest",
    "BacktestResult",
    # Baselines
    "PASSIVE_WEIGHTS",
    "run_tsh_baseline",
    "run_vol_bh_baseline",
    "run_passive_baseline",
    # Splitter
    "HOLDOUT_START",
    "HoldoutViolationError",
    "WalkForwardConfig",
    "walk_forward_splits",
    "open_holdout",
    # Metrics
    "WalkForwardStats",
]
