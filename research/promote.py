"""Promote a validated experiment run to the live strategy table (T-27 stub).

Usage (after T-27 is implemented):
    python -m research.promote --run-id <uuid> --project <gcp-project>
"""

from __future__ import annotations

import argparse


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Promote an experiment run (T-27).")
    parser.add_argument("--run-id", required=True, help="experiment_run_id UUID to promote.")
    parser.add_argument("--project", required=True, help="GCP project ID.")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL; do not execute.")
    return parser.parse_args()


def main() -> None:  # pragma: no cover
    _parse_args()
    # TODO: T-27 — read experiment_runs row, validate holdout_spent=FALSE,
    # write versioned params to prod_trade_strategy.strategy_tsmom_multiasset.
    raise NotImplementedError(
        "Promotion runs after holdout evaluation in T-27. "
        "Do not call this until the holdout has been opened and spent."
    )


if __name__ == "__main__":  # pragma: no cover
    main()
