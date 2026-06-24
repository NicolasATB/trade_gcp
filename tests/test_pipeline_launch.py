"""Tests for orchestration.pipeline_launch (T-12 DAG helpers).

Only the Airflow-free helper module is imported, so these run in CI where
apache-airflow is not installed.
"""

from __future__ import annotations

import inspect

from orchestration import pipeline_launch


def _value_after(cmd: list[str], flag: str) -> str:
    return cmd[cmd.index(flag) + 1]


def test_build_dataflow_command_explicit_args():
    cmd = pipeline_launch.build_dataflow_command(
        "2026-06-15",
        project="proj",
        region="reg",
        temp_location="gs://b/temp",
        staging_location="gs://b/staging",
        service_account="sa@x.iam.gserviceaccount.com",
    )
    # Launches via the isolated Beam venv, not the Airflow interpreter.
    assert cmd[:3] == ["/opt/beam-venv/bin/python", "-m", "dataflow.pipeline"]
    assert _value_after(cmd, "--runner") == "DataflowRunner"
    # Packages the dataflow module for the GCP workers (relative path).
    assert _value_after(cmd, "--setup_file") == "./setup.py"
    assert _value_after(cmd, "--project") == "proj"
    assert _value_after(cmd, "--region") == "reg"
    assert _value_after(cmd, "--temp_location") == "gs://b/temp"
    assert _value_after(cmd, "--staging_location") == "gs://b/staging"
    assert _value_after(cmd, "--service_account_email") == "sa@x.iam.gserviceaccount.com"
    # Single positional arg → end defaults to start (one deterministic day).
    assert _value_after(cmd, "--start_date") == "2026-06-15"
    assert _value_after(cmd, "--end_date") == "2026-06-15"


def test_build_dataflow_command_window():
    # Distinct start/end → a trailing window is processed (self-heals late days).
    cmd = pipeline_launch.build_dataflow_command("2026-06-12", "2026-06-15")
    assert _value_after(cmd, "--start_date") == "2026-06-12"
    assert _value_after(cmd, "--end_date") == "2026-06-15"


def test_build_dataflow_command_defaults_from_env(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT", "envproj")
    monkeypatch.setenv("GCP_REGION", "envreg")
    monkeypatch.setenv("DATAFLOW_TEMP_LOCATION", "gs://env/temp")
    monkeypatch.setenv("DATAFLOW_STAGING_LOCATION", "gs://env/staging")
    monkeypatch.setenv("DATAFLOW_SERVICE_ACCOUNT", "env-sa@x.iam.gserviceaccount.com")
    cmd = pipeline_launch.build_dataflow_command("2026-06-15")
    assert _value_after(cmd, "--project") == "envproj"
    assert _value_after(cmd, "--region") == "envreg"
    assert _value_after(cmd, "--temp_location") == "gs://env/temp"
    assert _value_after(cmd, "--staging_location") == "gs://env/staging"
    assert _value_after(cmd, "--service_account_email") == "env-sa@x.iam.gserviceaccount.com"


def test_build_dataflow_command_defaults_to_module_constants(monkeypatch):
    for var in (
        "GCP_PROJECT",
        "GCP_REGION",
        "DATAFLOW_TEMP_LOCATION",
        "DATAFLOW_STAGING_LOCATION",
        "DATAFLOW_SERVICE_ACCOUNT",
    ):
        monkeypatch.delenv(var, raising=False)
    cmd = pipeline_launch.build_dataflow_command("2026-06-15")
    assert _value_after(cmd, "--project") == "trade-390514"
    assert _value_after(cmd, "--region") == "us-central1"
    assert _value_after(cmd, "--temp_location") == "gs://trade-dataflow-trade-390514/temp"
    assert (
        _value_after(cmd, "--staging_location")
        == "gs://trade-dataflow-trade-390514/staging"
    )


def test_ingest_steps_cover_expected_series():
    task_ids = [tid for tid, _ in pipeline_launch.INGEST_STEPS]
    assert task_ids == [
        "ingest_binance_btc",
        "ingest_mvrv",
        "ingest_dxy",
        "ingest_10y",
        "ingest_2y",
        "ingest_fedfunds",
        "ingest_vix",
        "ingest_m2",
        "ingest_supply",
        "ingest_active_addresses",
        "ingest_tx_count",
        "ingest_trends",
    ]
    # Historical-only Bitstamp is not part of the daily run.
    assert not any("bitstamp" in tid for tid in task_ids)
    assert all(callable(fn) for _, fn in pipeline_launch.INGEST_STEPS)


def test_ingest_callables_accept_ds():
    # Every wrapper accepts an optional ds kwarg (binance uses it; others ignore).
    for _, fn in pipeline_launch.INGEST_STEPS:
        assert "ds" in inspect.signature(fn).parameters


def test_run_binance_btc_ingests_trailing_window(monkeypatch):
    captured = {}

    def fake_ingest(start_date=None, end_date=None, client=None):
        captured["start_date"] = start_date
        captured["end_date"] = end_date

    # The wrapper imports the module lazily; patch it at the source so the
    # in-function ``from orchestration.ingest import ...`` picks up the fake.
    from orchestration.ingest import binance_btc_ingest

    monkeypatch.setattr(binance_btc_ingest, "ingest_daily_candles", fake_ingest)
    pipeline_launch.run_binance_btc(ds="2026-06-15")
    from datetime import date, timedelta

    # End = logical date; start = end - lookback (self-healing window, idempotent).
    assert captured["end_date"] == date(2026, 6, 15)
    assert captured["start_date"] == date(2026, 6, 15) - timedelta(
        days=pipeline_launch.CANDLE_LOOKBACK_DAYS
    )


def test_run_active_addresses_delegates_to_ingest_latest(monkeypatch):
    calls = []
    from orchestration.ingest import coinmetrics_btc_active_addresses_ingest

    monkeypatch.setattr(
        coinmetrics_btc_active_addresses_ingest,
        "ingest_latest",
        lambda: calls.append(True),
    )
    pipeline_launch.run_active_addresses(ds="2026-06-15")
    assert calls == [True]


def test_run_tx_count_delegates_to_ingest_latest(monkeypatch):
    calls = []
    from orchestration.ingest import coinmetrics_btc_tx_count_ingest

    monkeypatch.setattr(
        coinmetrics_btc_tx_count_ingest,
        "ingest_latest",
        lambda: calls.append(True),
    )
    pipeline_launch.run_tx_count(ds="2026-06-15")
    assert calls == [True]


def test_run_trends_delegates_to_ingest_latest(monkeypatch):
    calls = []
    from orchestration.ingest import google_trends_btc_ingest

    monkeypatch.setattr(
        google_trends_btc_ingest,
        "ingest_latest",
        lambda: calls.append(True),
    )
    pipeline_launch.run_trends(ds="2026-06-15")
    assert calls == [True]
