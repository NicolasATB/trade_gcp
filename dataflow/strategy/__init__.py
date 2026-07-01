"""Pure, asset-agnostic strategy-signal logic (no BigQuery/Beam IO).

The modules here compute trading signals from already-conformed silver series so
the same code can be reused by the gold materialisation stage (Strategy 3,
``strategy_id=3``) and by the backtest engine. Keeping the maths free of GCP
dependencies is what makes it unit-testable end to end.
"""

from dataflow.strategy.portfolio import (
    PortfolioParams,
    Scheme,
    apply_crypto_cap,
    build_portfolio,
    equal_weight_weights,
    inverse_vol_weights,
    normalize_gross,
    risk_parity_weights,
)
from dataflow.strategy.tsmom_signal import (
    TsmomParams,
    compute_tsmom_rows,
    formation_excess_return,
    tsmom_position,
    tsmom_sign,
    vol_scale,
)

__all__ = [
    "PortfolioParams",
    "Scheme",
    "TsmomParams",
    "apply_crypto_cap",
    "build_portfolio",
    "compute_tsmom_rows",
    "equal_weight_weights",
    "formation_excess_return",
    "inverse_vol_weights",
    "normalize_gross",
    "risk_parity_weights",
    "tsmom_position",
    "tsmom_sign",
    "vol_scale",
]
