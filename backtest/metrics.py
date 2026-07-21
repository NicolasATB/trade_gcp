"""Performance and overfit-detection metrics for the T-25 backtest engine.

All period-level stats operate on the portfolio's **simple** return series
(not log returns) — the same series produced by ``run_backtest``.  Mixing
return scales (e.g. computing Sharpe from log returns of the combined portfolio)
introduces a bias that grows with cross-asset return dispersion.

Deflated Sharpe Ratio (DSR)
---------------------------
Bailey & López de Prado (2014).  The expected maximum Sharpe under H₀ is scaled
by ``√Var(sharpe_trials)`` — the dispersion of observed Sharpes across trials —
*not* just the count.  Passing a list of all trial Sharpes instead of only the
count encodes both n and the cross-trial variance:

    E[max SR | H₀] = (z₁(1−γ) + γz₂) × √Var(SR_trials)

where γ = Euler-Mascheroni constant, z₁ = Φ⁻¹(1−1/N), z₂ = Φ⁻¹(1−1/(Ne)).

This makes DSR monotone decreasing in ``Var(sharpe_trials)`` for fixed N
(higher dispersion → higher expected max → lower DSR), which the old
``num_trials``-only formula could not express.

PBO (Probability of Backtest Overfitting)
------------------------------------------
López de Prado (2018) CSCV.  ``pbo_cscv`` takes a returns matrix
(trials × observations) and the number of blocks; it generates all
C(n_blocks, n_blocks//2) train/test combinations and measures the fraction
where the in-sample-best strategy underperforms the OOS median.  For
``n_blocks=20`` this is C(20,10)=184,756 paths; acceptable for a one-time
validation step.

HLZ (Harvey, Liu, Zhu 2016)
----------------------------
``hlz_haircut`` computes the t-stat and haircut Sharpe adjusting for multiple
testing.  The default ``n_independent=1`` is deliberately conservative in the
wrong direction for a 10–15 trial budget (it understates the correction).
**Always pass the estimated effective independent trial count in Epic 8 usage**,
not the default.
"""

from __future__ import annotations

import math
import statistics
from dataclasses import dataclass
from itertools import combinations

# ── Period-level stats ───────────────────────────────────────────────────────

def annualized_sharpe(returns: list[float], periods_per_year: int = 52) -> float:
    """Annualized Sharpe ratio of a simple-return series (risk-free = 0).

    Args:
        returns: Period simple returns (the portfolio's excess-return series
            already incorporates the risk-free deduction from the silver view).
        periods_per_year: Annualization factor (52 for weekly).

    Returns:
        Annualized Sharpe, or 0.0 if the series has fewer than 2 observations
        or zero variance.
    """
    if len(returns) < 2:
        return 0.0
    mu = statistics.mean(returns)
    sigma = statistics.stdev(returns)
    if sigma == 0.0:
        return 0.0
    return (mu / sigma) * math.sqrt(periods_per_year)


def annualized_sortino(
    returns: list[float],
    periods_per_year: int = 52,
    target: float = 0.0,
) -> float:
    """Annualized Sortino ratio (penalizes only downside deviation).

    Args:
        returns: Period simple returns.
        periods_per_year: Annualization factor.
        target: Minimum acceptable return per period (default 0.0).

    Returns:
        Annualized Sortino, or ``float("inf")`` when there are no downside
        periods and the mean exceeds the target, or 0.0 for degenerate inputs.
    """
    if len(returns) < 2:
        return 0.0
    mu = statistics.mean(returns)
    downside = [r for r in returns if r < target]
    if not downside:
        return float("inf") if mu > target else 0.0
    downside_std = statistics.stdev(downside) if len(downside) > 1 else abs(downside[0])
    if downside_std == 0.0:
        return 0.0
    return ((mu - target) / downside_std) * math.sqrt(periods_per_year)


def max_drawdown(returns: list[float]) -> float:
    """Maximum peak-to-trough drawdown of an equity curve built from ``returns``.

    Args:
        returns: Period simple returns, oldest first.

    Returns:
        Maximum drawdown as a non-positive fraction (e.g. ``-0.25`` for −25 %).
        Returns ``0.0`` for an empty or monotone-rising series.
    """
    if not returns:
        return 0.0
    equity = 1.0
    peak = 1.0
    worst_dd = 0.0
    for r in returns:
        equity *= (1.0 + r)
        if equity > peak:
            peak = equity
        dd = (equity - peak) / peak
        if dd < worst_dd:
            worst_dd = dd
    return worst_dd


def annualized_return(returns: list[float], periods_per_year: int = 52) -> float:
    """Geometric annualized return of an equity curve built from ``returns``.

    Args:
        returns: Period simple returns (the portfolio's excess-return series),
            oldest first.
        periods_per_year: Annualization factor (52 for weekly).

    Returns:
        Annualized return as a fraction (e.g. ``0.10`` for +10 %/yr).  Returns
        ``0.0`` for fewer than two observations.
    """
    n = len(returns)
    if n < 2:
        return 0.0
    total = math.prod(1.0 + r for r in returns)
    return total ** (periods_per_year / n) - 1.0


def calmar_ratio(returns: list[float], periods_per_year: int = 52) -> float:
    """Annualized return divided by the absolute maximum drawdown.

    Args:
        returns: Period simple returns.
        periods_per_year: Annualization factor.

    Returns:
        Calmar ratio, or ``float("inf")`` when there is no drawdown, or 0.0 for
        degenerate inputs.
    """
    if len(returns) < 2:
        return 0.0
    ann_return = annualized_return(returns, periods_per_year)
    mdd = abs(max_drawdown(returns))
    if mdd == 0.0:
        return float("inf") if ann_return > 0 else 0.0
    return ann_return / mdd


def profit_factor(returns: list[float]) -> float:
    """Gross profit divided by gross loss (absolute value).

    Args:
        returns: Period simple returns.

    Returns:
        Profit factor, or ``float("inf")`` when there are no losing periods.
    """
    gains = sum(r for r in returns if r > 0)
    losses = sum(abs(r) for r in returns if r < 0)
    if losses == 0.0:
        return float("inf") if gains > 0 else 0.0
    return gains / losses


# ── Overfit detection ────────────────────────────────────────────────────────

def deflated_sharpe_ratio(
    sharpe_obs: float,
    sharpe_trials: list[float],
    t: int,
    skewness: float = 0.0,
    kurtosis: float = 3.0,
) -> float:
    """Deflated Sharpe Ratio — probability that the observed SR is genuinely > 0.

    Adjusts for multiple testing, non-normality, and backtest length following
    Bailey & López de Prado (2014).

    The expected maximum Sharpe under H₀ is scaled by ``√Var(sharpe_trials)``
    so that the formula captures both the *count* (via z-scores) and the
    *cross-trial dispersion* (via the variance) of the observed Sharpes:

        E[max SR | H₀] = (z₁(1−γ) + γz₂) × √Var(SR_trials)

    With fixed N and higher Var(SR_trials) → higher E[max SR] → lower DSR.
    This is the effect the ``num_trials``-only signature could not express.

    Args:
        sharpe_obs: Best observed annualized Sharpe over all trials.
        sharpe_trials: Annualized Sharpe ratio for **every** trial (incl. the
            best).  ``n_trials = len(sharpe_trials)``;
            ``var_sr = Var(sharpe_trials)`` are derived internally.
        t: Number of weekly observations in the backtest.
        skewness: Skewness of the best strategy's return series.
        kurtosis: Kurtosis (not excess; normal = 3.0) of the best strategy's
            return series.

    Returns:
        DSR ∈ [0, 1] — probability that the observed Sharpe genuinely exceeds
        zero after adjusting for multiple testing.  Values < 0.95 indicate
        likely overfitting.

    Raises:
        ValueError: If ``sharpe_trials`` is empty or ``t < 2``.
    """
    if not sharpe_trials:
        raise ValueError("sharpe_trials must be non-empty")
    if t < 2:
        raise ValueError("t must be >= 2")

    n_trials = len(sharpe_trials)

    # Standard error of the Sharpe estimator (captures non-normality).
    variance_factor = max(
        1e-12,
        1.0 - skewness * sharpe_obs + (kurtosis - 1.0) / 4.0 * sharpe_obs ** 2,
    )
    sr_std = math.sqrt(variance_factor / (t - 1))

    # E[max SR | H₀] using Euler-Mascheroni approximation scaled by
    # √Var(SR_trials).  Var captures the actual dispersion of the search space;
    # a wider spread raises the expected maximum under the null.
    if n_trials == 1:
        expected_max_sr = 0.0
    else:
        var_sr = statistics.variance(sharpe_trials)
        std_sr = math.sqrt(max(0.0, var_sr))
        euler_gamma = 0.5772156649
        z1 = _norm_ppf(1.0 - 1.0 / n_trials)
        z2 = _norm_ppf(1.0 - 1.0 / (n_trials * math.e))
        e_max_std = z1 * (1.0 - euler_gamma) + euler_gamma * z2
        expected_max_sr = e_max_std * std_sr

    if sr_std <= 0.0:
        return 1.0 if sharpe_obs > expected_max_sr else 0.0

    return _norm_cdf((sharpe_obs - expected_max_sr) / sr_std)


def pbo_cscv(
    returns_matrix: list[list[float]],
    n_blocks: int = 20,
) -> float:
    """Probability of Backtest Overfitting via CSCV (López de Prado 2018).

    Splits the T observations into ``n_blocks`` contiguous groups, generates
    all C(n_blocks, n_blocks//2) train/test combinations, and measures the
    fraction of paths where the in-sample-best strategy underperforms the OOS
    median.

    Args:
        returns_matrix: ``n_trials × T`` matrix — one return series per trial,
            all the same length.  At least 2 trials are required.
        n_blocks: Number of contiguous blocks (default 20, giving C(20,10) =
            184,756 CSCV paths).  Reduce for speed; do not reduce below 4.

    Returns:
        PBO ∈ [0, 1].  Epic 8 gate: PBO < 0.3.

    Raises:
        ValueError: For degenerate inputs (< 2 trials, < 3 blocks, groups too
            small).
    """
    n_trials = len(returns_matrix)
    if n_trials < 2:
        raise ValueError("pbo_cscv requires at least 2 trials")
    t = len(returns_matrix[0])
    if any(len(r) != t for r in returns_matrix):
        raise ValueError("All trial return series must have the same length")
    if n_blocks < 4:
        raise ValueError("n_blocks must be >= 4")

    group_size = t // n_blocks
    if group_size < 2:
        raise ValueError(
            f"Each block has only {group_size} observations.  "
            f"Reduce n_blocks or provide more data."
        )

    # Build block boundaries.
    blocks: list[tuple[int, int]] = []
    for i in range(n_blocks):
        start = i * group_size
        end = (i + 1) * group_size if i < n_blocks - 1 else t
        blocks.append((start, end))

    n_test = n_blocks // 2
    n_overfit = 0
    n_paths = 0

    for test_combo in combinations(range(n_blocks), n_test):
        test_set = set(test_combo)
        train_idx: list[int] = []
        test_idx: list[int] = []
        for g, (start, end) in enumerate(blocks):
            bucket = test_idx if g in test_set else train_idx
            bucket.extend(range(start, end))

        if not train_idx or not test_idx:
            continue

        # In-sample: mean return per trial over train indices.
        is_perf = [
            sum(returns_matrix[s][i] for i in train_idx) / len(train_idx)
            for s in range(n_trials)
        ]
        best_is = is_perf.index(max(is_perf))

        # Out-of-sample: mean return per trial over test indices.
        oos_perf = [
            sum(returns_matrix[s][i] for i in test_idx) / len(test_idx)
            for s in range(n_trials)
        ]
        best_oos = oos_perf[best_is]
        # Count how many strategies beat the IS-winner OOS.
        rank_above = sum(1 for p in oos_perf if p > best_oos)
        relative_rank = (rank_above + 1) / n_trials  # 1-based normalised

        if relative_rank > 0.5:
            n_overfit += 1
        n_paths += 1

    return n_overfit / n_paths if n_paths > 0 else 1.0


@dataclass(frozen=True)
class HLZResult:
    """Harvey-Liu-Zhu (2016) multiple-testing correction result."""

    t_stat: float
    haircut_sharpe: float
    significant: bool  # t_stat > 3.0 (HLZ threshold)


def hlz_haircut(
    sharpe: float,
    t: int,
    n_independent: int = 1,
) -> HLZResult:
    """Harvey-Liu-Zhu haircut Sharpe and t-stat.

    Adjusts the Sharpe ratio for multiple independent tests using the HLZ
    threshold: ``t_stat > 3.0`` is required for significance.

    .. warning::
        The default ``n_independent=1`` treats the test as a single independent
        trial — **optimistic** (understates the correction) for a 10–15 trial
        budget.  In Epic 8 usage, pass the estimated effective independent trial
        count (or the raw trial count as a conservative upper bound on the
        correction).

    Args:
        sharpe: Observed annualized Sharpe ratio.
        t: Number of weekly observations.
        n_independent: Number of effectively independent tests.  Default 1
            (single test); pass the actual independent trial count for a proper
            correction.

    Returns:
        :class:`HLZResult` with t-stat, haircut Sharpe, and significance flag.
    """
    if t < 2:
        return HLZResult(t_stat=0.0, haircut_sharpe=0.0, significant=False)

    # Per-period (non-annualized) Sharpe.
    sr_period = sharpe / math.sqrt(52)  # weekly cadence
    t_stat_raw = sr_period * math.sqrt(t)

    # HLZ haircut: apply a correction for n_independent tests.
    # With n_independent=1, t_stat = t_stat_raw (no haircut).
    # With n_independent > 1, the threshold rises — we model this by computing
    # the expected max t-stat under H₀ and subtracting it from the raw t-stat.
    if n_independent <= 1:
        t_stat = t_stat_raw
    else:
        # Approximate E[max |t| | H₀] over n_independent ~ N(0,1) tests.
        euler_gamma = 0.5772156649
        z1 = _norm_ppf(1.0 - 0.5 / n_independent)
        z2 = _norm_ppf(1.0 - 0.5 / (n_independent * math.e))
        e_max = z1 * (1.0 - euler_gamma) + euler_gamma * z2
        t_stat = t_stat_raw - e_max

    haircut_sr = (t_stat / math.sqrt(t)) * math.sqrt(52)
    return HLZResult(
        t_stat=t_stat,
        haircut_sharpe=haircut_sr,
        significant=t_stat > 3.0,
    )


# ── Walk-forward aggregate ───────────────────────────────────────────────────

@dataclass(frozen=True)
class WalkForwardStats:
    """Aggregate statistics from a walk-forward validation run.

    Attributes:
        fold_sharpes: Per-fold annualized net Sharpe ratios.
        fold_sortinos: Per-fold annualized net Sortino ratios.
        fold_max_dds: Per-fold maximum drawdowns (≤ 0).
        mean_sharpe: Mean of ``fold_sharpes``.
        dsr: Deflated Sharpe Ratio across all folds treated as trials.
        pbo: PBO if multiple trial series are available, else ``float("nan")``.
        hlz: HLZ result for the mean fold Sharpe.
    """

    fold_sharpes: list[float]
    fold_sortinos: list[float]
    fold_max_dds: list[float]
    mean_sharpe: float
    dsr: float
    pbo: float
    hlz: HLZResult


# ── Internal helpers (pure Python, no scipy) ────────────────────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via ``math.erfc`` (no scipy dependency)."""
    return 0.5 * math.erfc(-x / math.sqrt(2.0))


def _norm_ppf(p: float) -> float:
    """Inverse standard normal CDF (percent-point function).

    Uses the Beasley-Springer-Moro algorithm for p ∈ (0, 1).  Accurate to
    about 6 significant figures — sufficient for overfit-detection metrics.
    """
    if p <= 0.0 or p >= 1.0:
        raise ValueError(f"p must be in (0, 1), got {p}")
    # Rational approximation (Abramowitz & Stegun 26.2.17 / Peter Acklam).
    a = [-3.969683028665376e1, 2.209460984245205e2,
         -2.759285104469687e2, 1.383577518672690e2,
         -3.066479806614716e1, 2.506628277459239]
    b = [-5.447609879822406e1, 1.615858368580409e2,
         -1.556989798598866e2, 6.680131188771972e1, -1.328068155288572e1]
    c = [-7.784894002430293e-3, -3.223964580411365e-1,
         -2.400758277161838, -2.549732539343734,
          4.374664141464968, 2.938163982698783]
    d = [7.784695709041462e-3, 3.224671290700398e-1,
         2.445134137142996, 3.754408661907416]

    p_low = 0.02425
    p_high = 1.0 - p_low

    if p < p_low:
        q = math.sqrt(-2.0 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
               ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1.0)
    elif p <= p_high:
        q = p - 0.5
        r = q * q
        return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / \
               (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1.0)
    else:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / \
                ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1.0)
