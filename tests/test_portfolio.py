"""Unit tests for the pure portfolio-construction logic in
``dataflow/strategy/portfolio.py``.

Locks the T-23 contract: three named weighting schemes that *coincide* in v1
(``inverse_vol`` ≡ ``equal_vol`` ≡ diagonal ``risk_parity``) with ``equal_weight``
as the genuinely distinct alternative; gross-1 normalisation that divides by
``Σ|w|`` (not ``|Σw|``); a crypto cap that redistributes pro rata in one pass and
falls back to a gross-partial book when no non-crypto leg can absorb the excess;
and the with/without-crypto split. No BigQuery/Beam IO. Hypothesis is not a project
dependency, so the property checks are seeded randomised loops instead.
"""

from __future__ import annotations

import random

import pytest

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


def _gross(w: dict[str, float]) -> float:
    return sum(abs(v) for v in w.values())


def _net(w: dict[str, float]) -> float:
    return sum(w.values())


# ---------------------------------------------------------------------------
# Scheme — the EQUAL_VOL alias resolves to INVERSE_VOL (same v1 weighting)
# ---------------------------------------------------------------------------

class TestScheme:
    def test_equal_vol_is_inverse_vol_alias(self):
        assert Scheme.EQUAL_VOL is Scheme.INVERSE_VOL

    def test_values_round_trip_as_str(self):
        assert Scheme.EQUAL_WEIGHT == "equal_weight"
        assert Scheme.RISK_PARITY == "risk_parity"


# ---------------------------------------------------------------------------
# PortfolioParams — validation (counted cap, no nonsense)
# ---------------------------------------------------------------------------

class TestPortfolioParams:
    def test_valid_uncapped(self):
        p = PortfolioParams(scheme=Scheme.INVERSE_VOL)
        assert p.crypto_cap is None

    def test_valid_capped(self):
        p = PortfolioParams(scheme=Scheme.EQUAL_WEIGHT, crypto_cap=0.3)
        assert p.crypto_cap == pytest.approx(0.3)

    @pytest.mark.parametrize("cap", [0.0, -0.1, 1.5])
    def test_invalid_cap_raises(self, cap):
        with pytest.raises(ValueError):
            PortfolioParams(scheme=Scheme.INVERSE_VOL, crypto_cap=cap)

    def test_invalid_scheme_raises(self):
        with pytest.raises(ValueError):
            PortfolioParams(scheme="inverse_vol")  # raw str, not a Scheme


# ---------------------------------------------------------------------------
# normalize_gross — Σ|w| = 1, empty/flat, mixed-sign (gross not net)
# ---------------------------------------------------------------------------

class TestNormalizeGross:
    def test_scales_to_unit_gross(self):
        out = normalize_gross({"a": 2.0, "b": 2.0})
        assert out == {"a": pytest.approx(0.5), "b": pytest.approx(0.5)}

    def test_empty_is_empty(self):
        assert normalize_gross({}) == {}

    def test_all_zero_is_empty(self):
        # a flat week carries no gross → nothing to scale, no divide-by-zero
        assert normalize_gross({"a": 0.0, "b": 0.0}) == {}

    def test_mixed_sign_market_neutral(self):
        # equal |w|, opposite signs → gross 1, net 0; confirms the divisor is
        # Σ|w| (gross), never |Σw| (net). |Σw|=0 would blow up here.
        out = normalize_gross({"a": 3.0, "b": -3.0})
        assert _gross(out) == pytest.approx(1.0)
        assert _net(out) == pytest.approx(0.0)
        assert out == {"a": pytest.approx(0.5), "b": pytest.approx(-0.5)}


# ---------------------------------------------------------------------------
# equal_weight_weights — equal capital over active legs
# ---------------------------------------------------------------------------

class TestEqualWeight:
    def test_equal_capital(self):
        out = equal_weight_weights({"a": 1, "b": -1, "c": 1})
        assert out == {
            "a": pytest.approx(1 / 3),
            "b": pytest.approx(-1 / 3),
            "c": pytest.approx(1 / 3),
        }
        assert _gross(out) == pytest.approx(1.0)

    def test_flat_legs_dropped(self):
        out = equal_weight_weights({"a": 1, "b": 0, "c": -1})
        assert set(out) == {"a", "c"}
        assert out["a"] == pytest.approx(0.5)

    def test_no_active_is_empty(self):
        assert equal_weight_weights({"a": 0, "b": 0}) == {}


# ---------------------------------------------------------------------------
# inverse_vol_weights — wᵢ ∝ sᵢ/σᵢ, drop flat / unusable vol
# ---------------------------------------------------------------------------

class TestInverseVol:
    def test_low_vol_gets_more_weight(self):
        # raw 1/0.1=10, 1/0.4=2.5 → gross 12.5 → 0.8 / 0.2
        out = inverse_vol_weights({"a": 1, "b": 1}, {"a": 0.1, "b": 0.4})
        assert out == {"a": pytest.approx(0.8), "b": pytest.approx(0.2)}

    def test_sign_is_carried(self):
        out = inverse_vol_weights({"a": -1, "b": 1}, {"a": 0.1, "b": 0.1})
        assert out == {"a": pytest.approx(-0.5), "b": pytest.approx(0.5)}

    def test_flat_leg_dropped(self):
        out = inverse_vol_weights({"a": 1, "b": 0}, {"a": 0.2, "b": 0.2})
        assert set(out) == {"a"}

    @pytest.mark.parametrize("bad_vol", [None, 0.0, -0.1, float("nan"), float("inf")])
    def test_unusable_vol_dropped(self, bad_vol):
        out = inverse_vol_weights({"a": 1, "b": 1}, {"a": 0.2, "b": bad_vol})
        assert set(out) == {"a"}

    def test_all_unsizable_is_empty(self):
        assert inverse_vol_weights({"a": 1}, {"a": None}) == {}


# ---------------------------------------------------------------------------
# risk_parity_weights — diagonal (v1) == inverse-vol; cov hook deferred
# ---------------------------------------------------------------------------

class TestRiskParity:
    def test_diagonal_equals_inverse_vol(self):
        signs, vols = {"a": 1, "b": 1}, {"a": 0.1, "b": 0.4}
        assert risk_parity_weights(signs, vols) == inverse_vol_weights(signs, vols)

    def test_cov_hook_is_not_implemented(self):
        with pytest.raises(NotImplementedError):
            risk_parity_weights({"a": 1}, {"a": 0.1}, cov={("a", "a"): 0.01})


# ---------------------------------------------------------------------------
# Identity lock — VALUE-based, heterogeneous vols (not a shared-path assertion)
# ---------------------------------------------------------------------------

class TestSchemeIdentity:
    def _rows(self):
        # heterogeneous vols so the v1 coincidence is real and non-trivial
        return [
            {"symbol": "a", "signal": 1, "realized_vol_26w": 0.10, "is_crypto": False},
            {"symbol": "b", "signal": -1, "realized_vol_26w": 0.25, "is_crypto": False},
            {"symbol": "c", "signal": 1, "realized_vol_26w": 0.40, "is_crypto": False},
        ]

    def test_inverse_equal_riskparity_coincide(self):
        rows = self._rows()
        iv = build_portfolio(
            rows, PortfolioParams(Scheme.INVERSE_VOL), include_crypto=True
        )
        ev = build_portfolio(
            rows, PortfolioParams(Scheme.EQUAL_VOL), include_crypto=True
        )
        rp = build_portfolio(
            rows, PortfolioParams(Scheme.RISK_PARITY), include_crypto=True
        )
        assert iv == ev == rp
        # …and they are non-trivial (distinct weights from the heterogeneous vols)
        assert len(set(round(v, 9) for v in iv.values())) == 3

    def test_equal_weight_is_distinct(self):
        rows = self._rows()
        iv = build_portfolio(
            rows, PortfolioParams(Scheme.INVERSE_VOL), include_crypto=True
        )
        ew = build_portfolio(
            rows, PortfolioParams(Scheme.EQUAL_WEIGHT), include_crypto=True
        )
        assert ew != iv
        assert all(abs(v) == pytest.approx(1 / 3) for v in ew.values())


# ---------------------------------------------------------------------------
# apply_crypto_cap — clip + pro-rata redistribute, one-pass, gross-partial edge
# ---------------------------------------------------------------------------

class TestApplyCryptoCap:
    def test_none_cap_is_passthrough(self):
        w = {"BTC": 0.9, "a": 0.1}
        assert apply_crypto_cap(w, {"BTC"}, None) == w

    def test_under_cap_untouched(self):
        w = normalize_gross({"BTC": 0.2, "a": 0.5, "b": 0.3})
        assert apply_crypto_cap(w, {"BTC"}, 0.3) == w

    def test_clip_and_redistribute_keeps_gross_one(self):
        # BTC 0.6 over cap 0.3 → freed 0.3 spread pro rata over a(0.3)/b(0.1)
        w = {"BTC": 0.6, "a": 0.3, "b": 0.1}
        out = apply_crypto_cap(w, {"BTC"}, 0.3)
        assert out["BTC"] == pytest.approx(0.3)
        assert _gross(out) == pytest.approx(1.0)
        # a gets 3/4 of the freed 0.3, b gets 1/4
        assert out["a"] == pytest.approx(0.3 + 0.3 * 0.75)
        assert out["b"] == pytest.approx(0.1 + 0.3 * 0.25)

    def test_redistribution_preserves_signs(self):
        w = {"BTC": 0.6, "a": -0.3, "b": 0.1}
        out = apply_crypto_cap(w, {"BTC"}, 0.3)
        assert out["a"] < 0 and out["b"] > 0
        assert _gross(out) == pytest.approx(1.0)

    def test_one_pass_convergence_single_sleeve(self):
        # a single capped sleeve converges in one pass: BTC lands exactly at cap
        # and no non-crypto leg acquires a cap to violate → no second iteration.
        w = {"BTC": 0.8, "a": 0.15, "b": 0.05}
        out = apply_crypto_cap(w, {"BTC"}, 0.25)
        assert out["BTC"] == pytest.approx(0.25)
        assert _gross(out) == pytest.approx(1.0)

    def test_no_noncrypto_leg_is_gross_partial(self):
        # only BTC trades → respect the cap, drop gross-1 (gross == cap that week)
        w = {"BTC": 1.0}
        out = apply_crypto_cap(w, {"BTC"}, 0.3)
        assert out == {"BTC": pytest.approx(0.3)}
        assert _gross(out) == pytest.approx(0.3)

    def test_missing_crypto_symbol_ignored(self):
        # a crypto symbol absent from the book is a no-op (w is None branch)
        w = {"a": 0.5, "b": 0.5}
        assert apply_crypto_cap(w, {"BTC"}, 0.3) == w

    def test_empty_book_passthrough(self):
        assert apply_crypto_cap({}, {"BTC"}, 0.3) == {}


# ---------------------------------------------------------------------------
# build_portfolio — assembly, dispatch, with/without crypto, gross-by-branch
# ---------------------------------------------------------------------------

class TestBuildPortfolio:
    def _rows(self):
        return [
            {"symbol": "SPY", "signal": 1, "realized_vol_26w": 0.10, "is_crypto": False},
            {"symbol": "TLT", "signal": -1, "realized_vol_26w": 0.20, "is_crypto": False},
            {"symbol": "BTC", "signal": 1, "realized_vol_26w": 0.05, "is_crypto": True},
        ]

    def test_warmup_and_flat_rows_dropped(self):
        rows = [
            {"symbol": "a", "signal": None, "realized_vol_26w": 0.1, "is_crypto": False},
            {"symbol": "b", "signal": 0, "realized_vol_26w": 0.1, "is_crypto": False},
            {"symbol": "c", "signal": 1, "realized_vol_26w": 0.1, "is_crypto": False},
        ]
        out = build_portfolio(rows, PortfolioParams(Scheme.INVERSE_VOL), include_crypto=True)
        assert set(out) == {"c"}

    def test_all_warmup_week_is_flat(self):
        rows = [
            {"symbol": "a", "signal": None, "realized_vol_26w": 0.1, "is_crypto": False},
        ]
        out = build_portfolio(rows, PortfolioParams(Scheme.INVERSE_VOL), include_crypto=True)
        assert out == {}
        assert _gross(out) == 0.0  # gross == 0 branch

    def test_without_crypto_drops_btc_and_renormalises(self):
        rows = self._rows()
        out = build_portfolio(
            rows, PortfolioParams(Scheme.INVERSE_VOL, crypto_cap=0.3),
            include_crypto=False,
        )
        assert "BTC" not in out
        assert _gross(out) == pytest.approx(1.0)  # renormalised over SPY/TLT only

    def test_with_crypto_applies_cap(self):
        rows = self._rows()
        out = build_portfolio(
            rows, PortfolioParams(Scheme.INVERSE_VOL, crypto_cap=0.3),
            include_crypto=True,
        )
        assert abs(out["BTC"]) <= 0.3 + 1e-9
        assert _gross(out) == pytest.approx(1.0)  # gross == 1 (non-crypto active)

    def test_crypto_pure_capped_is_gross_cap(self):
        # only BTC active → gross == cap_eff branch
        rows = [
            {"symbol": "SPY", "signal": 0, "realized_vol_26w": 0.10, "is_crypto": False},
            {"symbol": "BTC", "signal": 1, "realized_vol_26w": 0.05, "is_crypto": True},
        ]
        out = build_portfolio(
            rows, PortfolioParams(Scheme.INVERSE_VOL, crypto_cap=0.3),
            include_crypto=True,
        )
        assert out == {"BTC": pytest.approx(0.3)}
        assert _gross(out) == pytest.approx(0.3)

    def test_equal_weight_dispatch(self):
        rows = self._rows()
        out = build_portfolio(
            rows, PortfolioParams(Scheme.EQUAL_WEIGHT), include_crypto=False
        )
        assert all(abs(v) == pytest.approx(0.5) for v in out.values())

    def test_risk_parity_dispatch_matches_inverse_vol(self):
        rows = self._rows()
        rp = build_portfolio(rows, PortfolioParams(Scheme.RISK_PARITY), include_crypto=True)
        iv = build_portfolio(rows, PortfolioParams(Scheme.INVERSE_VOL), include_crypto=True)
        assert rp == iv


# ---------------------------------------------------------------------------
# Property checks (seeded randomised; Hypothesis is not a project dependency)
# ---------------------------------------------------------------------------

class TestProperties:
    SCHEMES = [Scheme.EQUAL_WEIGHT, Scheme.INVERSE_VOL, Scheme.RISK_PARITY]

    def _random_rows(self, rng):
        symbols = ["SPY", "EFA", "IEF", "TLT", "GLD", "DBC", "UUP", "FXY", "BTC"]
        rows = []
        for sym in symbols:
            sign = rng.choice([-1, 0, 1, None])
            rows.append({
                "symbol": sym,
                "signal": sign,
                "realized_vol_26w": rng.uniform(0.02, 0.8),
                "is_crypto": sym == "BTC",
            })
        # guarantee ≥1 active non-crypto leg so gross-1 is the governing branch
        rows[rng.randrange(8)]["signal"] = rng.choice([-1, 1])
        return rows

    def test_gross_one_and_crypto_capped(self):
        rng = random.Random(20230623)
        cap = 0.3
        for _ in range(2000):
            rows = self._random_rows(rng)
            scheme = rng.choice(self.SCHEMES)
            out = build_portfolio(
                rows, PortfolioParams(scheme, crypto_cap=cap), include_crypto=True
            )
            # ≥1 active non-crypto leg ⇒ the gross-1 branch (the genuinely
            # invariant property); a per-leg |w|≤1 ceiling is NOT asserted —
            # cap redistribution can lift a low-vol leg above 1 by construction.
            assert _gross(out) == pytest.approx(1.0)
            if "BTC" in out:
                assert abs(out["BTC"]) <= cap + 1e-9

    def test_no_crypto_book_never_holds_btc(self):
        rng = random.Random(99)
        for _ in range(500):
            rows = self._random_rows(rng)
            out = build_portfolio(
                rows, PortfolioParams(Scheme.INVERSE_VOL, crypto_cap=0.3),
                include_crypto=False,
            )
            assert "BTC" not in out
