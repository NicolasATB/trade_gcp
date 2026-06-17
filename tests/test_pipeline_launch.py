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
    # Both bounds equal the logical date → one deterministic, idempotent day.
    assert _value_after(cmd, "--start_date") == "2026-06-15"
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
    ]
    # Historical-only Bitstamp is not part of the daily run.
    assert not any("bitstamp" in tid for tid in task_ids)
    assert all(callable(fn) for _, fn in pipeline_launch.INGEST_STEPS)


def test_ingest_callables_accept_ds():
    # Every wrapper accepts an optional ds kwarg (binance uses it; others ignore).
    for _, fn in pipeline_launch.INGEST_STEPS:
        assert "ds" in inspect.signature(fn).parameters


def test_run_binance_btc_passes_logical_date(monkeypatch):
    captured = {}

    def fake_ingest(start_date=None, end_date=None, client=None):
        captured["start_date"] = start_date
        captured["end_date"] = end_date

    monkeypatch.setattr(
        pipeline_launch.binance_btc_ingest, "ingest_daily_candles", fake_ingest
    )
    pipeline_launch.run_binance_btc(ds="2026-06-15")
    from datetime import date

    assert captured["start_date"] == date(2026, 6, 15)
    assert captured["end_date"] == date(2026, 6, 15)
