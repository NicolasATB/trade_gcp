"""BTC investor-attention ingestion: Google Trends -> BigQuery bronze (RAW).

Loads the **weekly** Google Trends search-interest index for "Bitcoin" into
``prod_trade_bronze.google_trends_btc_weekly_raw``, storing the value **exactly as
Google delivers it for each request window** — no stitching, no re-scaling. Bronze
is raw (medallion rule): a weekly point appears once per request ``window_start``,
each on its own per-request 0-100 scale. The continuous, stitched, re-normalised
series is built **downstream** by the silver view ``vw_google_trends_btc_weekly``.

Why per window: Google only returns a continuous *weekly* series for request windows
under ~5 years (daily below ~9 months, monthly above ~5y) and re-normalises its
0-100 scale per request, so windows cannot be concatenated as-is. The back-fill
therefore issues overlapping <5-year windows and stores each window's raw values;
the overlap is what the silver view uses to re-scale them onto one another.

Two entry points share the same idempotent upsert (MERGE on the natural key
``(search_term, window_start, trend_date)``):

  * **Back-fill** the full history once (``--backfill``) as overlapping windows.
  * **Refresh** the latest window (``--start/--end`` or the default) so the newest
    completed week lands; re-running updates that window in place.

The still-forming current week (Google's ``isPartial`` flag) is dropped. Like every
recurring ingest here it **never fabricates a value**: a missing datum is *alerted*
(``_alert_missing``) and the row is skipped.

Google Trends is unofficial and rate-limited, so the live fetch backs off on
``TooManyRequestsError``. ``pytrends`` is imported lazily inside the fetch so the
pure parsing/upsert logic (and its tests) need no network dependency. BigQuery auth
is Application Default Credentials.

Run standalone:
  python -m orchestration.ingest.google_trends_btc_ingest --backfill                # full weekly history (raw, per window)
  python -m orchestration.ingest.google_trends_btc_ingest --start 2026-01-01 --end 2026-06-16
  python -m orchestration.ingest.google_trends_btc_ingest                            # refresh latest window
"""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date, datetime, timedelta, timezone

from google.cloud import bigquery

logger = logging.getLogger(__name__)

# --- Configuration (overridable via environment variables) -------------------
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "trade-390514")
BQ_DATASET = os.environ.get("BQ_BRONZE_DATASET", "prod_trade_bronze")
BQ_TABLE = os.environ.get("BQ_TRENDS_TABLE", "google_trends_btc_weekly_raw")
SOURCE_ID = int(os.environ.get("TRENDS_SOURCE_ID", "15"))
SEARCH_TERM = os.environ.get("TRENDS_TERM", "Bitcoin")
GEO = os.environ.get("TRENDS_GEO", "")           # "" = worldwide
HL = os.environ.get("TRENDS_HL", "en-US")
TZ = int(os.environ.get("TRENDS_TZ", "0"))
HISTORY_START = os.environ.get("TRENDS_HISTORY_START", "2011-01-01")
# Refresh: how many days back to re-request; widened to MIN_WEEKLY_DAYS so the
# request still returns a WEEKLY series.
DAILY_LOOKBACK_DAYS = int(os.environ.get("TRENDS_LOOKBACK_DAYS", "30"))
# pytrends rate-limit backoff.
FETCH_ATTEMPTS = int(os.environ.get("TRENDS_FETCH_ATTEMPTS", "5"))

# Granularity control. Google Trends returns a WEEKLY series only for request
# windows between ~9 months and ~5 years. The back-fill requests overlapping <5-year
# windows and stores each window's RAW values; the silver view stitches them using
# the overlap. (No stitching happens here — bronze is raw.)
WINDOW_DAYS = int(os.environ.get("TRENDS_WINDOW_DAYS", "1460"))     # ~4y (< 5y -> weekly)
OVERLAP_DAYS = int(os.environ.get("TRENDS_OVERLAP_DAYS", "365"))    # ~1y overlap (silver re-scales on it)
# A request shorter than this returns DAILY data; the refresh path widens its window
# to at least this many days so the series stays weekly.
MIN_WEEKLY_DAYS = int(os.environ.get("TRENDS_MIN_WEEKLY_DAYS", "300"))
# Politeness pause between successive windowed requests (rate-limit friendliness).
INTER_REQUEST_SLEEP_S = float(os.environ.get("TRENDS_INTER_REQUEST_SLEEP_S", "1.0"))
# Weekly granularity -> few partitions, but chunk for consistency with the others.
MAX_MERGE_PARTITIONS = int(os.environ.get("TRENDS_MAX_MERGE_PARTITIONS", "3500"))


def _table_fqn() -> str:
    return f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"


# --- Parsing / row building (pure, unit-tested) ------------------------------

def _to_date(value) -> date:
    """Coerce a Trends point date (``date`` or ``YYYY-MM-DD`` string) to a ``date``."""
    if isinstance(value, date):
        return value
    return datetime.strptime(value[:10], "%Y-%m-%d").date()


def _parse_value(raw) -> int | None:
    """Parse a raw interest cell; empty/``NaN``/None -> ``None``. Google gives 0-100 ints."""
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "" or text.lower() == "nan":
        return None
    return int(round(float(text)))


def _window_bounds(
    start: date, end: date, window_days: int = WINDOW_DAYS, overlap_days: int = OVERLAP_DAYS
) -> list[tuple[date, date]]:
    """Split ``[start, end]`` into overlapping <5-year windows (pure).

    Consecutive windows overlap by ``overlap_days`` (the step is
    ``window_days - overlap_days``) so the silver view can re-scale each onto the
    previous one. The last window always ends at ``end``.
    """
    if end <= start:
        return [(start, end)]
    step = max(1, window_days - overlap_days)
    bounds: list[tuple[date, date]] = []
    s = start
    while s < end:
        e = min(s + timedelta(days=window_days), end)
        bounds.append((s, e))
        if e >= end:
            break
        s = s + timedelta(days=step)
    return bounds


def parse_interest_over_time(raw_rows: list[dict]) -> list[dict]:
    """Parse normalised Trends rows into raw records, dropping partial weeks.

    ``raw_rows`` is the framework-agnostic shape the live fetch produces from the
    pytrends DataFrame: ``{"date": <str|date>, "interest": <num|str|None>,
    "is_partial": <bool>}``. Rows flagged ``is_partial`` (the still-forming current
    week) are dropped so only fully-closed weeks are stored. Each output record is
    ``{"d": <str|date>, "interest": <int|None>}``.
    """
    records: list[dict] = []
    for row in raw_rows:
        if row.get("is_partial"):
            continue
        records.append({"d": row["date"], "interest": _parse_value(row.get("interest"))})
    return records


def _build_row(
    record: dict, window_start: date, window_end: date, fetched_at: datetime | None = None
) -> dict:
    """Map one source record to a bronze row (pure mapping; no fabrication).

    The value is stored as delivered for ``window_start`` (no stitching/re-scaling).
    """
    return {
        "trend_date": _to_date(record["d"]),
        "search_term": SEARCH_TERM,
        "window_start": window_start,
        "window_end": window_end,
        "interest_raw": record.get("interest"),
        "source_id": SOURCE_ID,
        "datetime_update": fetched_at or datetime.now(timezone.utc),
    }


def _alert_missing(rows: list[dict], context: str) -> list[date]:
    """Alert (log WARNING) for every row with a missing value; return the dates.

    The seam the Telegram alert (T-10) plugs into. The ingest relies on it instead
    of silently writing NULLs or fabricating values.
    """
    missing = [r["trend_date"] for r in rows if r["interest_raw"] is None]
    for d in missing:
        logger.warning(
            "Google Trends %s: missing interest for %s - source delivered no value",
            context, d.isoformat(),
        )
    return missing


def _prepare_rows(
    records: list[dict], window_start: date, window_end: date, fetched_at: datetime, context: str
) -> list[dict]:
    """Build rows for one request window, alert on missing values, and drop them."""
    rows = [_build_row(r, window_start, window_end, fetched_at=fetched_at) for r in records]
    _alert_missing(rows, context)
    return [r for r in rows if r["interest_raw"] is not None]


def _chunked(seq: list, size: int):
    """Yield successive ``size``-length chunks of ``seq``."""
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


# --- BigQuery upsert ----------------------------------------------------------

# Idempotent MERGE on the natural key (search_term, window_start, trend_date). Source
# rows are deduped within the batch (latest datetime_update wins) so MERGE never sees
# a key twice.
_MERGE_SQL = """
MERGE `{table}` AS T
USING (
  SELECT * EXCEPT(rn) FROM (
    SELECT s.*, ROW_NUMBER() OVER (
      PARTITION BY s.search_term, s.window_start, s.trend_date ORDER BY s.datetime_update DESC
    ) AS rn
    FROM UNNEST(@rows) AS s
  ) WHERE rn = 1
) AS S
ON T.search_term = S.search_term AND T.window_start = S.window_start AND T.trend_date = S.trend_date
WHEN MATCHED THEN UPDATE SET
  window_end      = S.window_end,
  interest_raw    = S.interest_raw,
  source_id       = S.source_id,
  datetime_update = S.datetime_update
WHEN NOT MATCHED THEN INSERT (
  trend_date, search_term, window_start, window_end, interest_raw, source_id, datetime_update
) VALUES (
  S.trend_date, S.search_term, S.window_start, S.window_end, S.interest_raw, S.source_id, S.datetime_update
)
"""


def _struct_param(row: dict) -> bigquery.StructQueryParameter:
    return bigquery.StructQueryParameter(
        None,
        bigquery.ScalarQueryParameter("trend_date", "DATE", row["trend_date"]),
        bigquery.ScalarQueryParameter("search_term", "STRING", row["search_term"]),
        bigquery.ScalarQueryParameter("window_start", "DATE", row["window_start"]),
        bigquery.ScalarQueryParameter("window_end", "DATE", row["window_end"]),
        bigquery.ScalarQueryParameter("interest_raw", "INT64", row["interest_raw"]),
        bigquery.ScalarQueryParameter("source_id", "INT64", row["source_id"]),
        bigquery.ScalarQueryParameter("datetime_update", "TIMESTAMP", row["datetime_update"]),
    )


def _upsert_chunk(client: bigquery.Client, rows: list[dict]) -> None:
    struct_params = [_struct_param(r) for r in rows]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("rows", "STRUCT", struct_params)]
    )
    client.query(_MERGE_SQL.format(table=_table_fqn()), job_config=job_config).result()


def _upsert_rows(client: bigquery.Client, rows: list[dict], chunk_size: int | None = None) -> None:
    """Idempotent MERGE of raw weekly Trends rows, chunked under the partition limit.

    Partitions are ``trend_date`` (~52/year); a chunk of ``chunk_size`` rows touches
    at most that many partitions, well under BigQuery's 4000-per-DML cap. Chunking is
    transparent to idempotency: replaying any range MERGEs the same keys.
    """
    if not rows:
        return
    size = chunk_size or MAX_MERGE_PARTITIONS
    for chunk in _chunked(rows, size):
        _upsert_chunk(client, chunk)


# --- Live fetch (pytrends; rate-limited, imported lazily) --------------------

def _fetch_with_backoff(thunk, attempts: int = FETCH_ATTEMPTS):  # pragma: no cover - live network
    """Call ``thunk`` retrying on Google Trends rate-limit errors with backoff."""
    import random
    import time

    from pytrends.exceptions import ResponseError, TooManyRequestsError

    delay = 2.0
    for i in range(attempts):
        try:
            return thunk()
        except (TooManyRequestsError, ResponseError) as exc:
            if i == attempts - 1:
                raise
            sleep_s = delay * (2 ** i) + random.uniform(0, 1)
            logger.warning(
                "Google Trends rate-limited (%s); backing off %.1fs (attempt %d/%d)",
                exc, sleep_s, i + 1, attempts,
            )
            time.sleep(sleep_s)


def fetch_interest_over_time(
    term: str, start_date: date, end_date: date, geo: str = GEO
) -> list[dict]:  # pragma: no cover - live network
    """Fetch the weekly interest-over-time series for ``term`` over one window.

    Returns the framework-agnostic rows ``parse_interest_over_time`` expects. The
    window should be < ~5 years so Google returns weekly (not monthly) data.
    """
    from pytrends.request import TrendReq

    timeframe = f"{start_date.isoformat()} {end_date.isoformat()}"
    pytrends = TrendReq(hl=HL, tz=TZ)

    def _run():
        pytrends.build_payload([term], timeframe=timeframe, geo=geo)
        return pytrends.interest_over_time()

    df = _fetch_with_backoff(_run)
    if df is None or df.empty:
        return []
    rows: list[dict] = []
    for idx, row in df.iterrows():
        rows.append(
            {
                "date": idx.date() if hasattr(idx, "date") else idx,
                "interest": row.get(term),
                "is_partial": bool(row.get("isPartial", False)),
            }
        )
    return rows


def _latest_window_start(client: bigquery.Client, term: str) -> date | None:  # pragma: no cover - live BigQuery
    """Return the most recent ``window_start`` already stored for ``term`` (or None)."""
    sql = (
        f"SELECT MAX(window_start) AS ws FROM `{_table_fqn()}` WHERE search_term = @term"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("term", "STRING", term)]
    )
    for r in client.query(sql, job_config=job_config).result():
        return r["ws"]
    return None


# --- Public entry points ------------------------------------------------------

def backfill_history(
    client: bigquery.Client | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    term: str = SEARCH_TERM,
) -> int:
    """Back-fill the full weekly history into bronze as overlapping raw windows.

    Each window is requested separately and stored under its own ``window_start``
    (no stitching here — the silver view stitches). Returns the total rows written.
    """
    import time

    start = start_date or datetime.strptime(HISTORY_START, "%Y-%m-%d").date()
    end = end_date or datetime.now(timezone.utc).date()
    fetched_at = datetime.now(timezone.utc)
    if client is None:  # pragma: no cover - live client
        client = bigquery.Client(project=PROJECT_ID)
    total = 0
    for w_start, w_end in _window_bounds(start, end):
        raw = fetch_interest_over_time(term, w_start, w_end)  # pragma: no cover - live network
        rows = _prepare_rows(parse_interest_over_time(raw), w_start, w_end, fetched_at, "back-fill")
        if rows:
            logger.info(
                "Google Trends back-fill: window %s..%s -> %d week(s)",
                w_start.isoformat(), w_end.isoformat(), len(rows),
            )
            _upsert_rows(client, rows)
            total += len(rows)
        if INTER_REQUEST_SLEEP_S > 0:  # pragma: no cover - live pacing
            time.sleep(INTER_REQUEST_SLEEP_S)
    logger.info("Google Trends back-fill complete: %d raw row(s) across windows", total)
    return total


def ingest_range(
    start_date: date,
    end_date: date,
    client: bigquery.Client | None = None,
    term: str = SEARCH_TERM,
) -> list[dict]:  # pragma: no cover - live network
    """Fetch one window and store its raw completed weeks under ``window_start``.

    The request window is widened to at least ``MIN_WEEKLY_DAYS`` so Google still
    returns a WEEKLY series; the widened start becomes the stored ``window_start``.
    """
    if end_date < start_date:
        raise ValueError(
            f"end_date ({end_date.isoformat()}) is before start_date ({start_date.isoformat()})."
        )
    req_start = min(start_date, end_date - timedelta(days=MIN_WEEKLY_DAYS))
    fetched_at = datetime.now(timezone.utc)
    raw = fetch_interest_over_time(term, req_start, end_date)
    rows = _prepare_rows(parse_interest_over_time(raw), req_start, end_date, fetched_at, "range")
    if not rows:
        logger.warning("Google Trends range: no writable weeks for %s..%s", req_start, end_date)
        return []
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    _upsert_rows(client, rows)
    return rows


def ingest_latest(client: bigquery.Client | None = None, term: str = SEARCH_TERM) -> list[dict]:  # pragma: no cover - live network
    """Refresh the latest stored window up to today (reuses its ``window_start``).

    Re-fetching the same window keeps the window set stable (no proliferation) while
    picking up the newest completed week; idempotent MERGE updates it in place.
    """
    if client is None:
        client = bigquery.Client(project=PROJECT_ID)
    today = datetime.now(timezone.utc).date()
    ws = _latest_window_start(client, term) or (today - timedelta(days=WINDOW_DAYS))
    fetched_at = datetime.now(timezone.utc)
    raw = fetch_interest_over_time(term, ws, today)
    rows = _prepare_rows(parse_interest_over_time(raw), ws, today, fetched_at, "refresh")
    if not rows:
        logger.warning("Google Trends refresh: no writable weeks for window %s..%s", ws, today)
        return []
    _upsert_rows(client, rows)
    return rows


def _parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Ingest weekly BTC Google Trends investor attention into BigQuery bronze (raw)."
    )
    def iso_date(s):
        return datetime.strptime(s, "%Y-%m-%d").date()

    parser.add_argument(
        "--backfill", action="store_true",
        help="Back-fill the full weekly history as overlapping raw windows (ignores --start/--end).",
    )
    parser.add_argument(
        "--start", type=iso_date, default=None,
        help="First date to request from Google Trends (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end", type=iso_date, default=None,
        help="Last date (YYYY-MM-DD), inclusive. Defaults to --start.",
    )
    return parser.parse_args(argv)


def main(argv=None) -> None:  # pragma: no cover - CLI wiring
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = _parse_args(argv)
    if args.backfill:
        n = backfill_history()
        logger.info("Back-filled %d raw row(s)", n)
    elif args.start is not None:
        end = args.end if args.end is not None else args.start
        rows = ingest_range(args.start, end)
        logger.info("Wrote %d week(s)", len(rows))
    else:
        rows = ingest_latest()
        logger.info("Wrote %d week(s)", len(rows))


if __name__ == "__main__":  # pragma: no cover
    main()
