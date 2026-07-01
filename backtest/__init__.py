"""Offline multi-asset TSMOM backtest engine (T-25).

Public re-exports — import from here, not from sub-modules, to keep the
engine's public surface stable as the internals evolve.
"""

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
    # Splitter
    "HOLDOUT_START",
    "HoldoutViolationError",
    "WalkForwardConfig",
    "walk_forward_splits",
    "open_holdout",
    # Metrics
    "WalkForwardStats",
]
