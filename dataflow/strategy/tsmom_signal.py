"""Per-instrument Time-Series-Momentum (TSMOM) signal — pure logic (T-22).

Strategy 3 (``strategy_id=3``) takes, for each instrument and each rebalance
date, a directional position equal to the **sign of the cumulative excess return
over a formation horizon** ``L``, then scales that position to a target
volatility. The two steps are deliberately separable:

  * the **signal** (sign, ``-1 / 0 / +1``) carries no leverage, and
  * the **position** multiplies the signal by a **volatility-scaling factor**
    (``vol_target / realized_vol``).

so the evaluation tickets can report performance *with and without* vol scaling
(methodology principle: "vol scaling reported with and without, always — the
scaling level is a counted trial, never a hidden default").

Design contract
---------------
* **Inputs come from silver (T-21), already point-in-time.** The feature layer is
  ``prod_trade_silver.vw_asset_returns_weekly``; this module consumes its
  ``excess_log_return`` (additive, so the cumulative formation return is a plain
  sum) and ``realized_vol_26w`` (ex-ante annualised volatility, ``NULL`` during
  its 26-week warm-up) keyed by ``week_start``. The column names are arguments of
  :func:`compute_tsmom_rows` (defaults match the view) so the core stays generic.
  This module never reads the future: the formation sum uses the trailing window
  ending at the rebalance week, and the volatility is the ex-ante estimate known
  at that week. Look-ahead hygiene that needs market data lives upstream; here we
  only forbid peeking past the row.
* **Both ``formation_horizon`` and ``vol_target`` are required parameters** with
  no magic default — that is what "vol scaling as a *counted* parameter, not
  hardcoded" means at the code level. Persisting them as a versioned strategy row
  is a separate ticket (T-24); here they travel in :class:`TsmomParams`.
* **Warm-up is a NULL contract** (mirrors the RSI stage): the first
  ``formation_horizon - 1`` rows lack a full formation window, so they yield no
  signal (``None``) and no position.

Cadence is weekly (Monday→Sunday), so ``periods_per_year`` defaults to 52; the
excess returns are expected to be **log returns** (additive, so the cumulative
formation return is a plain sum).

No BigQuery/Beam imports on purpose: this is the unit-tested core that the gold
stage (T-24) and the backtest engine (T-25) both call.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TsmomParams:
    """Counted parameters of the TSMOM signal (no hidden defaults for the trials).

    Attributes:
        formation_horizon: Number of periods ``L`` summed into the formation
            excess return (e.g. 52 weeks ≈ 12 months). A *counted trial*.
        vol_target: Annualised target volatility the position is scaled to
            (e.g. 0.10 for 10%). A *counted trial* — required, never defaulted.
        vol_lookback: Number of periods used by T-21 to estimate the ex-ante
            ``realized_vol``. Recorded here so the trial is fully described even
            though the estimate itself is materialised upstream.
        periods_per_year: Annualisation factor of the cadence (52 for weekly).
        max_leverage: Optional cap on the absolute vol-scaling factor, so a
            collapsing ``realized_vol`` cannot demand unbounded leverage. ``None``
            leaves the factor uncapped.
        vol_scaling: When ``False`` the position equals the unscaled signal
            (``float(sign)``), bypassing :func:`vol_scale` entirely. Use
            ``False`` + ``scheme=equal_weight`` together to isolate the Kim et al.
            (2016) confound: vol scaling does most of the performance work, so
            reporting with and without is a validation requirement (principle 2).
            This is a backtest/validation flag; production always scales (True).
    """

    formation_horizon: int
    vol_target: float
    vol_lookback: int
    periods_per_year: int = 52
    max_leverage: float | None = None
    vol_scaling: bool = True

    def __post_init__(self) -> None:
        if self.formation_horizon < 1:
            raise ValueError("formation_horizon must be >= 1")
        if not (self.vol_target > 0):
            raise ValueError("vol_target must be > 0")
        if self.vol_lookback < 1:
            raise ValueError("vol_lookback must be >= 1")
        if self.periods_per_year < 1:
            raise ValueError("periods_per_year must be >= 1")
        if self.max_leverage is not None and not (self.max_leverage > 0):
            raise ValueError("max_leverage must be > 0 when set")


def _is_number(x: Any) -> bool:
    """True for a finite, non-NaN real number (rejects ``None``/NaN/inf)."""
    return isinstance(x, (int, float)) and math.isfinite(x)


def formation_excess_return(
    excess_returns: Sequence[float], horizon: int
) -> float | None:
    """Cumulative excess return over the trailing ``horizon`` periods.

    The window is the last ``horizon`` returns of ``excess_returns`` (inclusive of
    the final element, the rebalance period). Returns ``None`` when fewer than
    ``horizon`` returns are available — the warm-up case — so the caller emits no
    signal rather than acting on a partial window.

    Args:
        excess_returns: Trailing excess **log** returns, oldest first, ending at
            the rebalance period. Additive, so the cumulative return is the sum.
        horizon: Number of periods ``L`` to sum.

    Returns:
        The summed excess return, or ``None`` during warm-up.

    Raises:
        ValueError: If ``horizon`` is not positive, or any value in the window is
            not a finite number (a real gap must be handled upstream, not summed).
    """
    if horizon < 1:
        raise ValueError("horizon must be >= 1")
    if len(excess_returns) < horizon:
        return None
    window = excess_returns[-horizon:]
    if not all(_is_number(r) for r in window):
        raise ValueError("excess_returns window contains a non-finite value")
    return float(math.fsum(window))


def tsmom_sign(formation_ret: float | None, eps: float = 0.0) -> int | None:
    """Direction of the position from the formation return.

    Args:
        formation_ret: Cumulative formation excess return, or ``None`` (warm-up).
        eps: Dead-band half-width. ``|formation_ret| <= eps`` maps to ``0`` (flat).
            Defaults to ``0.0`` (only an exactly-zero return is flat); a wider band
            would itself be a counted trial and is left to the caller.

    Returns:
        ``+1`` (long), ``-1`` (short), ``0`` (flat / excess return ≈ 0), or
        ``None`` when ``formation_ret`` is ``None``.
    """
    if formation_ret is None:
        return None
    if formation_ret > eps:
        return 1
    if formation_ret < -eps:
        return -1
    return 0


def vol_scale(realized_vol: float | None, params: TsmomParams) -> float | None:
    """Volatility-scaling factor ``vol_target / realized_vol`` (optionally capped).

    Args:
        realized_vol: Ex-ante annualised volatility for the instrument at the
            rebalance date (from T-21). ``None``/NaN/≤0 yields ``None`` — a
            position cannot be sized without a usable volatility estimate.
        params: Strategy parameters carrying ``vol_target`` and ``max_leverage``.

    Returns:
        The (capped) positive scaling factor, or ``None`` when ``realized_vol`` is
        unusable.
    """
    if not _is_number(realized_vol) or realized_vol <= 0:
        return None
    scale = params.vol_target / realized_vol
    if params.max_leverage is not None and scale > params.max_leverage:
        scale = params.max_leverage
    return scale


def tsmom_position(sign: int | None, scale: float | None) -> float | None:
    """Combine the (unscaled) signal and the vol-scaling factor into a position.

    Args:
        sign: Output of :func:`tsmom_sign` (``-1/0/+1`` or ``None``).
        scale: Output of :func:`vol_scale` (positive factor or ``None``).

    Returns:
        ``None`` during warm-up (``sign is None``); ``0.0`` when flat
        (``sign == 0``), regardless of ``scale``; ``None`` when a directional
        signal cannot be sized (``scale is None``); otherwise ``sign * scale``.
    """
    if sign is None:
        return None
    if sign == 0:
        return 0.0
    if scale is None:
        return None
    return sign * scale


def compute_tsmom_rows(
    rows: Sequence[dict],
    params: TsmomParams,
    *,
    time_key: str = "week_start",
    excess_key: str = "excess_log_return",
    vol_key: str = "realized_vol_26w",
    eps: float = 0.0,
) -> list[dict]:
    """Walk a per-instrument silver series and emit one signal row per input row.

    Defaults read ``prod_trade_silver.vw_asset_returns_weekly`` (T-21):
    ``week_start``, ``excess_log_return`` (additive excess log return) and
    ``realized_vol_26w`` (ex-ante annualised volatility). Rows must be sorted
    ascending by time and belong to a **single instrument**; the formation window
    for row ``i`` is the trailing ``formation_horizon`` excess returns ending at
    ``i`` (so the first ``formation_horizon - 1`` rows are warm-up). The
    volatility's own 26-week warm-up arrives as ``None`` and yields an unsized
    (``None``) position even once the signal is live.

    The emitted ``signal`` (unscaled) and ``position`` (scaled) are kept as
    separate fields so downstream can report with and without vol scaling.

    Args:
        rows: The instrument's silver rows, oldest first.
        params: Strategy parameters (formation horizon, vol target, cap).
        time_key: Column holding each row's period start.
        excess_key: Column holding the period's excess **log** return.
        vol_key: Column holding the ex-ante annualised volatility.
        eps: Dead-band passed through to :func:`tsmom_sign`.

    Returns:
        One dict per input row with ``time_period_start``, ``formation_return``,
        ``signal``, ``vol_scale`` and ``position`` (warm-up rows carry ``None``
        for ``formation_return``/``signal``/``position``).
    """
    excess: list[float] = [r[excess_key] for r in rows]
    out: list[dict] = []
    for i, row in enumerate(rows):
        formation = formation_excess_return(
            excess[: i + 1], params.formation_horizon
        )
        sign = tsmom_sign(formation, eps=eps)
        scale = vol_scale(row.get(vol_key), params)
        if params.vol_scaling:
            position = tsmom_position(sign, scale)
        else:
            # vol_scaling=False: position = unscaled sign (±1/0/None).
            # Use with scheme=equal_weight to isolate the Kim et al. confound.
            position = None if sign is None else float(sign)
        out.append(
            {
                "time_period_start": row[time_key],
                "formation_return": formation,
                "signal": sign,
                "vol_scale": scale if params.vol_scaling else None,
                "position": position,
            }
        )
    return out
