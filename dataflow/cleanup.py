"""Cleanup utility for the silver and gold medallion tables.

Deletes rows from the pipeline's output tables. Every filter is required and
ANDed together; to match an entire dimension you must say so explicitly with
the literal ``all`` (case-insensitive) — never by omission, so a mass delete is
always a deliberate, readable choice:

  * symbol       — a symbol, or ``all`` for every symbol.
  * temporality  — a temporality, or ``all`` for every temporality.
  * start_date   — a date, or ``all`` for no lower bound (all history).
  * end_date     — a date, or ``all`` for no upper bound (everything from
                   ``start_date`` onward).

Passing ``all`` for the four filters empties the selected tables. Handy before
re-running a back-fill, or to force an RSI bootstrap: emptying ``rsi_features``
makes the RSI stage recompute the recursion from scratch.

Target tables (all carry ``symbol`` and ``temporality``; the date column
differs by layer):
  * silver ``ohlcv_validated``  — date column ``time_period_start``
  * silver ``rsi_features``     — date column ``time_period_start``
  * gold   ``fact_signals``     — date column ``signal_start``

Authentication uses Google Application Default Credentials (same as the
ingest job). Date filters compare on ``DATE(<col>)``, which prunes the date
partitions of every target table.

Standalone usage (every filter is required; use ``all`` to match a dimension):
    python -m dataflow.cleanup --symbol BTCUSD --temporality 1d \\
        --start 2024-01-01 --end 2024-01-31
    python -m dataflow.cleanup --symbol BTCUSD --temporality 1d \\
        --start all --end all --dry-run             # count only, delete nothing
    python -m dataflow.cleanup --symbol all --temporality all \\
        --start all --end all --layer silver        # empty the silver tables
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime

from google.cloud import bigquery

logger = logging.getLogger(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")

# Explicit token a caller must pass to match an entire dimension (vs. omission).
_ALL = "all"

# (table, date_column) per layer. Every table also has symbol + temporality.
_SILVER_TARGETS = [
    ("prod_trade_silver.ohlcv_validated", "time_period_start"),
    ("prod_trade_silver.rsi_features",    "time_period_start"),
]
_GOLD_TARGETS = [
    ("prod_trade_gold.fact_signals", "signal_start"),
]


def _targets_for_layer(layer: str) -> list[tuple[str, str]]:
    if layer == "silver":
        return list(_SILVER_TARGETS)
    if layer == "gold":
        return list(_GOLD_TARGETS)
    if layer == "all":
        return _SILVER_TARGETS + _GOLD_TARGETS
    raise ValueError(f"Unknown layer {layer!r}; expected 'silver', 'gold' or 'all'.")


def _resolve(value, field: str):
    """Normalise a required filter: the literal ``all`` → ``None`` (no filter).

    A missing value (``None``) is rejected so that matching a whole dimension is
    always the explicit, typed choice ``all`` rather than a silent omission.
    """
    if value is None:
        raise ValueError(
            f"{field} is required: pass a value or '{_ALL}' to match every {field}."
        )
    if isinstance(value, str) and value.lower() == _ALL:
        return None
    return value


def _filter_clause(
    date_col: str,
    symbol: str | None,
    temporality: str | None,
    start_date: date | None,
    end_date: date | None,
) -> tuple[str, list[bigquery.ScalarQueryParameter]]:
    """Build the WHERE clause and its query parameters for one table.

    Returns ``("TRUE", [])`` when no filter is given so the DELETE removes every
    row (BigQuery requires an explicit predicate).
    """
    conditions: list[str] = []
    params: list[bigquery.ScalarQueryParameter] = []

    if symbol is not None:
        conditions.append("symbol = @symbol")
        params.append(bigquery.ScalarQueryParameter("symbol", "STRING", symbol))
    if temporality is not None:
        conditions.append("temporality = @temporality")
        params.append(bigquery.ScalarQueryParameter("temporality", "STRING", temporality))
    if start_date is not None:
        conditions.append(f"DATE({date_col}) >= @start_date")
        params.append(bigquery.ScalarQueryParameter("start_date", "DATE", start_date))
    if end_date is not None:
        conditions.append(f"DATE({date_col}) <= @end_date")
        params.append(bigquery.ScalarQueryParameter("end_date", "DATE", end_date))

    where = " AND ".join(conditions) if conditions else "TRUE"
    return where, params


def clean_tables(
    symbol: str,
    temporality: str,
    start_date: str | date,
    end_date: str | date,
    layer: str = "all",
    client: bigquery.Client | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """Delete rows from the silver/gold tables matching the given filters.

    The four filters are required: pass a concrete value, or the literal
    ``'all'`` to match the whole dimension (every symbol / temporality / no
    lower or upper date bound). ``start_date`` / ``end_date`` accept a
    ``datetime.date`` or ``'all'``. ``layer`` selects which tables to touch:
    ``'silver'``, ``'gold'`` or ``'all'`` (default).

    When ``dry_run`` is True, no rows are deleted: each target is counted and
    the would-be-deleted row count is returned instead.

    Returns ``{table_fqn: affected_rows}`` (deleted rows, or matched rows when
    ``dry_run``).
    """
    targets = _targets_for_layer(layer)  # fail fast on a bad layer, before any I/O

    symbol = _resolve(symbol, "symbol")
    temporality = _resolve(temporality, "temporality")
    start_date = _resolve(start_date, "start_date")
    end_date = _resolve(end_date, "end_date")

    if start_date is not None and end_date is not None and end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date "
            f"({start_date.isoformat()})."
        )

    if client is None:
        client = bigquery.Client(project=PROJECT_ID)

    if symbol is None and temporality is None and start_date is None and end_date is None:
        logger.warning(
            "Filters are all 'all': this will %s EVERY row from the %s table(s).",
            "count" if dry_run else "DELETE", layer,
        )

    results: dict[str, int] = {}
    for table, date_col in targets:
        fqn = f"{PROJECT_ID}.{table}"
        where, params = _filter_clause(date_col, symbol, temporality, start_date, end_date)
        job_config = bigquery.QueryJobConfig(query_parameters=params)

        if dry_run:
            query = f"SELECT COUNT(*) AS n FROM `{fqn}` WHERE {where}"
            n = list(client.query(query, job_config=job_config).result())[0]["n"]
            logger.info("[dry-run] %s rows would be deleted from %s", n, fqn)
            results[fqn] = int(n)
        else:
            query = f"DELETE FROM `{fqn}` WHERE {where}"
            job = client.query(query, job_config=job_config)
            job.result()
            affected = job.num_dml_affected_rows or 0
            logger.info("Deleted %s rows from %s", affected, fqn)
            results[fqn] = int(affected)

    return results


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Delete rows from the silver/gold medallion tables. "
                    f"Every filter is required; use '{_ALL}' to match a whole dimension."
    )

    def _date_or_all(s: str):
        if s.lower() == _ALL:
            return _ALL
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument("--symbol", required=True,
                        help=f"Symbol to delete, or '{_ALL}' for every symbol.")
    parser.add_argument("--temporality", required=True,
                        help=f"Temporality to delete (e.g. 1d / 1w), or '{_ALL}' for every temporality.")
    parser.add_argument("--start", type=_date_or_all, required=True,
                        help=f"First date YYYY-MM-DD, or '{_ALL}' for no lower bound.")
    parser.add_argument("--end", type=_date_or_all, required=True,
                        help=f"Last date YYYY-MM-DD inclusive, or '{_ALL}' for everything from --start onward.")
    parser.add_argument("--layer", choices=["all", "silver", "gold"], default="all",
                        help="Which layer(s) to clean (default: all).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Count matching rows without deleting anything.")
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover - CLI entry point (logging + I/O glue)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    results = clean_tables(
        symbol=args.symbol,
        temporality=args.temporality,
        start_date=args.start,
        end_date=args.end,
        layer=args.layer,
        dry_run=args.dry_run,
    )
    # Per-table detail is already logged inside clean_tables; just total here.
    verb = "Would delete" if args.dry_run else "Deleted"
    logger.info("%s %d row(s) across %d table(s).", verb, sum(results.values()), len(results))


if __name__ == "__main__":
    main()
