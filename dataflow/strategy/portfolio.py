"""Cross-asset portfolio construction for Strategy 3 (TSMOM) — pure logic (T-23).

T-22 (:mod:`dataflow.strategy.tsmom_signal`) produces, *per instrument* and per
rebalance week, a directional sign (``-1 / 0 / +1``) and a vol-scaled position.
T-23 is the **cross-sectional** step the backtest (T-25) needs: combine those
per-instrument positions, for one rebalance week, into **portfolio weights** under
a named weighting scheme, cap the crypto sleeve, and support a **with-crypto** and
a **without-crypto** book.

No BigQuery/Beam imports on purpose — this is the unit-tested core that the gold
stage (T-24) and the backtest engine (T-25) both call; materialisation and the
versioned parameter row are T-24.

Weighting schemes
-----------------
* ``EQUAL_WEIGHT`` — ``wᵢ = sᵢ / N_active``: equal capital on each active leg.
* ``INVERSE_VOL`` (alias ``EQUAL_VOL``) — ``wᵢ ∝ sᵢ / σᵢ``: lower-vol legs get more
  weight so each contributes equal volatility *under zero correlation*.
* ``RISK_PARITY`` — equal risk contribution. **v1 is diagonal** (zero
  cross-correlation assumed), which collapses to :data:`INVERSE_VOL` in closed
  form; the full-covariance ERC solver is a separate later ticket and plugs into
  the reserved ``cov`` hook of :func:`risk_parity_weights`.

**v1 coincidence (documented, not hidden).** Under the project's v1 conventions
(per-asset vol scaling from T-22 + diagonal covariance + gross-1 normalisation),
``EQUAL_VOL``, ``INVERSE_VOL`` and diagonal ``RISK_PARITY`` yield *identical*
normalised weights. They are kept as distinct labels (clean trial counting and a
clean ERC swap later) but the coincidence is real until full-covariance ERC or a
different normalisation lands. :data:`EQUAL_WEIGHT` is the genuinely *distinct*
alternative the validation compares against.

Normalisation & the crypto cap
------------------------------
Weights are normalised to **gross leverage 1** (``Σ|wᵢ| = 1``); the absolute
vol-target level stays in T-22's per-instrument scaling, so performance can still
be reported with and without vol scaling. The crypto sleeve is then capped (a
*counted trial*) and the freed gross redistributed pro rata across the non-crypto
legs.

The cap is a **portfolio-layer** constraint and, unlike T-22's per-instrument vol
scaling, it deliberately **couples** instruments: redistributing the clipped
crypto gross scales the non-crypto legs up, so each non-crypto leg's exposure
depends on how much crypto was clipped that week. This is intentional for v1 and
pinned by a test so the verdict can audit it.

Vol estimator shared with T-22
------------------------------
The weighting reads the same ``realized_vol_26w`` column that T-22
(:func:`~dataflow.strategy.tsmom_signal.compute_tsmom_rows`) scales positions with,
so the per-instrument scaling and the portfolio weighting use *one* vol estimator
(26-week realised vol from the T-21 view ``vw_asset_returns_weekly``), not two
silently-different ones. ``vol_key`` is a parameter defaulting to that column.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any


class Scheme(str, Enum):
    """Portfolio weighting scheme (``str`` mixin so the value round-trips to BQ/JSON).

    ``EQUAL_VOL`` is an explicit alias of ``INVERSE_VOL`` — in v1 they denote the
    same weighting (see the module docstring); both resolve to the same member.
    """

    EQUAL_WEIGHT = "equal_weight"
    INVERSE_VOL = "inverse_vol"
    EQUAL_VOL = "inverse_vol"  # alias — same v1 weighting as INVERSE_VOL
    RISK_PARITY = "risk_parity"


@dataclass(frozen=True)
class PortfolioParams:
    """Counted parameters of the portfolio construction.

    Attributes:
        scheme: Weighting scheme (:class:`Scheme`).
        crypto_cap: Maximum **gross** weight of the crypto sleeve (e.g. ``0.30``),
            or ``None`` to leave it uncapped. A *counted trial*; validated to lie in
            ``(0, 1]``.
    """

    scheme: Scheme
    crypto_cap: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.scheme, Scheme):
            raise ValueError(f"scheme must be a Scheme, got {self.scheme!r}")
        if self.crypto_cap is not None and not (0.0 < self.crypto_cap <= 1.0):
            raise ValueError("crypto_cap must be in (0, 1] when set")


def _is_number(x: Any) -> bool:
    """True for a finite, non-NaN real number (rejects ``None``/NaN/inf)."""
    return isinstance(x, (int, float)) and math.isfinite(x)


def _gross(weights: Mapping[str, float]) -> float:
    """Gross leverage ``Σ|w|`` of a weight book."""
    return math.fsum(abs(w) for w in weights.values())


def normalize_gross(weights: Mapping[str, float]) -> dict[str, float]:
    """Scale ``weights`` so the gross leverage ``Σ|w|`` equals 1.

    An empty book or an all-zero book (a flat week) returns an empty dict — there
    is nothing to scale and no divide-by-zero. Mixed long/short signs are fine: the
    normaliser divides by ``Σ|w|`` (gross), never ``|Σw|`` (net), so a
    market-neutral book keeps gross 1 with net 0.

    Args:
        weights: Raw ``{symbol: weight}`` (signed).

    Returns:
        ``{symbol: weight}`` with ``Σ|w| = 1``, or ``{}`` when the input carries no
        gross exposure.
    """
    gross = _gross(weights)
    if gross <= 0.0:
        return {}
    return {sym: w / gross for sym, w in weights.items()}


def equal_weight_weights(signs: Mapping[str, int]) -> dict[str, float]:
    """Equal-capital weights: ``wᵢ = sᵢ / N_active`` over active legs.

    Args:
        signs: ``{symbol: sign}`` with ``sign ∈ {-1, 0, +1}``. Flat (``0``) legs
            carry no capital and are dropped.

    Returns:
        Gross-1 ``{symbol: weight}`` (empty when no leg is active).
    """
    active = {sym: s for sym, s in signs.items() if s}
    n = len(active)
    if n == 0:
        return {}
    return {sym: s / n for sym, s in active.items()}


def inverse_vol_weights(
    signs: Mapping[str, int], vols: Mapping[str, float]
) -> dict[str, float]:
    """Inverse-volatility (a.k.a. equal-vol) weights: ``wᵢ ∝ sᵢ / σᵢ``.

    A leg is included only if it is active (``sign ∈ {-1, +1}``) **and** has a
    usable volatility (finite and ``> 0``); legs with an unusable ``σ`` cannot be
    sized and are dropped, mirroring the T-22 NULL contract.

    Args:
        signs: ``{symbol: sign}`` with ``sign ∈ {-1, 0, +1}``.
        vols: ``{symbol: realized_vol}`` (ex-ante annualised vol from T-21).

    Returns:
        Gross-1 ``{symbol: weight}`` (empty when no leg is sizable).
    """
    raw: dict[str, float] = {}
    for sym, s in signs.items():
        if not s:
            continue
        sigma = vols.get(sym)
        if not _is_number(sigma) or sigma <= 0:
            continue
        raw[sym] = s / sigma
    return normalize_gross(raw)


def risk_parity_weights(
    signs: Mapping[str, int],
    vols: Mapping[str, float],
    cov: Any | None = None,
) -> dict[str, float]:
    """Risk-parity (equal risk contribution) weights.

    **v1 is diagonal** (``cov is None``): zero cross-correlation is assumed, which
    collapses ERC to inverse-vol in closed form, so this delegates to
    :func:`inverse_vol_weights`.

    The ``cov`` parameter is the reserved hook for the later full-covariance ERC
    ticket; its contract is fixed **now** so the hook is real, not decorative. When
    provided it is the cross-asset covariance either as a
    ``dict[tuple[str, str], float]`` keyed by symbol pair (symmetric, diagonal =
    variances) or as an ``numpy.ndarray`` aligned to ``sorted(signs)``. The ERC
    solver fills against this shape without re-touching the signature.

    Args:
        signs: ``{symbol: sign}`` with ``sign ∈ {-1, 0, +1}``.
        vols: ``{symbol: realized_vol}``.
        cov: Cross-asset covariance for full ERC (deferred); must be ``None`` in v1.

    Returns:
        Gross-1 ``{symbol: weight}``.

    Raises:
        NotImplementedError: If ``cov`` is provided (full ERC is a later ticket).
    """
    if cov is not None:
        raise NotImplementedError(
            "full-covariance ERC is deferred to a later ticket; v1 risk parity is "
            "diagonal (cov=None) and equals inverse-vol"
        )
    return inverse_vol_weights(signs, vols)


def apply_crypto_cap(
    weights: Mapping[str, float],
    crypto_symbols: set[str],
    cap: float | None,
) -> dict[str, float]:
    """Cap the crypto sleeve's gross weight and redistribute the excess.

    Runs **after** :func:`normalize_gross` (it caps an already gross-1 book). For
    each crypto leg whose ``|w| > cap`` the weight is clipped to ``±cap`` (sign
    kept) and the freed gross is redistributed **pro rata** across the non-crypto
    legs, preserving gross 1.

    With a **single** capped sleeve and a single cap this is **one-shot**: pro-rata
    redistribution cannot push a non-crypto leg over a cap it does not have, so it
    converges in one pass and needs no re-normalisation loop. *This one-shot
    property breaks the day a second cap is added* (e.g. a per-class cap), which
    would need an iterative water-filling solve — flagged so it is not silently
    relied on.

    Edge — **no active non-crypto leg**: the clipped crypto sleeve is left at
    ``cap`` and the book is **gross-partial** (``gross = cap`` that week). The
    **cap** (the real risk constraint) is honoured at the cost of the gross-1
    invariant, not the reverse: the crypto sleeve is a documented diversification
    leg, never held at 100% just because it is the only leg trading that week.

    Args:
        weights: Gross-1 ``{symbol: weight}``.
        crypto_symbols: Symbols belonging to the crypto sleeve (e.g. ``{"BTC"}``).
        cap: Maximum gross weight per crypto leg, or ``None`` to leave untouched.

    Returns:
        ``{symbol: weight}`` with every crypto ``|w| ≤ cap`` (gross 1 when a
        non-crypto leg is active, else gross ``= cap``).
    """
    if cap is None or not weights:
        return dict(weights)

    out = dict(weights)
    freed = 0.0
    for sym in crypto_symbols:
        w = out.get(sym)
        if w is None:
            continue
        if abs(w) > cap:
            clipped = math.copysign(cap, w)
            freed += abs(w) - cap
            out[sym] = clipped

    if freed <= 0.0:
        return out

    noncrypto = {sym: w for sym, w in out.items() if sym not in crypto_symbols}
    noncrypto_gross = math.fsum(abs(w) for w in noncrypto.values())
    if noncrypto_gross <= 0.0:
        # No leg to absorb the freed gross → respect the cap, drop gross-1.
        return out

    for sym, w in noncrypto.items():
        out[sym] = w + math.copysign(freed * abs(w) / noncrypto_gross, w)
    return out


def build_portfolio(
    rows: Sequence[Mapping[str, Any]],
    params: PortfolioParams,
    *,
    include_crypto: bool,
    sign_key: str = "signal",
    vol_key: str = "realized_vol_26w",
    symbol_key: str = "symbol",
    crypto_key: str = "is_crypto",
) -> dict[str, float]:
    """Build one rebalance week's portfolio weights from a cross-section.

    Each element of ``rows`` is one instrument's record for the week, carrying its
    symbol, T-22 ``signal`` (sign), ``realized_vol_26w`` and an ``is_crypto`` flag.
    Warm-up rows (``sign`` is ``None``) and rows that the chosen scheme cannot size
    (unusable vol for the vol-based schemes) are dropped. When ``include_crypto`` is
    ``False`` the crypto legs are dropped **before** weighting, yielding a pure
    no-crypto book; the crypto cap is applied only to the with-crypto book.

    Args:
        rows: The week's per-instrument cross-section.
        params: Scheme + crypto cap.
        include_crypto: ``False`` builds the no-crypto book (crypto legs removed
            before weighting and the cap is a no-op).
        sign_key: Column holding each row's signal (``-1/0/+1`` or ``None``).
        vol_key: Column holding the ex-ante realised vol (shared with T-22).
        symbol_key: Column holding the instrument symbol.
        crypto_key: Column holding the boolean crypto-sleeve flag.

    Returns:
        ``{symbol: weight}`` for the week (empty on a flat / all-warm-up week).
    """
    signs: dict[str, int] = {}
    vols: dict[str, float] = {}
    crypto_symbols: set[str] = set()
    for row in rows:
        is_crypto = bool(row.get(crypto_key))
        if is_crypto and not include_crypto:
            continue
        sym = row[symbol_key]
        sign = row.get(sign_key)
        if sign is None:  # warm-up → not held this week
            continue
        signs[sym] = int(sign)
        vols[sym] = row.get(vol_key)
        if is_crypto:
            crypto_symbols.add(sym)

    if params.scheme is Scheme.EQUAL_WEIGHT:
        weights = equal_weight_weights(signs)
    elif params.scheme is Scheme.RISK_PARITY:
        weights = risk_parity_weights(signs, vols)
    else:  # INVERSE_VOL / EQUAL_VOL
        weights = inverse_vol_weights(signs, vols)

    if include_crypto and crypto_symbols:
        weights = apply_crypto_cap(weights, crypto_symbols, params.crypto_cap)
    return weights
