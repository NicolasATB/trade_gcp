"""daily_btc_signal — daily orchestration DAG (T-12).

Flow:  ingest (12 series, in parallel)  →  launch Dataflow pipeline  →  signal alert.

Runs once a day at 12:00 UTC on the e2-micro Airflow VM (T-11, LocalExecutor +
Postgres). The compose caps concurrency (``MAX_ACTIVE_TASKS_PER_DAG=2``), so the
ingest fan-out runs two at a time — fine on 1 GB of RAM. The pipeline launch
shells out to the isolated ``/opt/beam-venv`` interpreter (apache-beam's pins
clash with the Airflow environment) and submits a DataflowRunner job.

The logical date ``{{ ds }}`` (= yesterday's fully closed candle when the DAG
fires at 12:00 UTC) is the single day processed end to end.

The alert tasks (signal alert + failure callback) are stubs until the Telegram
client lands in T-10; they log for now and will call ``orchestration.alerts`` then.

Pure logic (the launch command, the ingest step list) lives in the Airflow-free
``orchestration.pipeline_launch`` module so it is unit-tested in CI.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from orchestration.pipeline_launch import INGEST_STEPS, build_dataflow_command

logger = logging.getLogger(__name__)


def _alert_on_failure(context):
    """Failure-alert stub — wire Telegram in T-10."""
    ti = context.get("task_instance")
    dag = context.get("dag")
    logger.error(
        "TASK FAILED dag=%s task=%s ds=%s (failure-alert stub; wire Telegram in T-10)",
        dag.dag_id if dag else "?",
        ti.task_id if ti else "?",
        context.get("ds"),
    )


def _signal_alert(**context):
    """Signal-alert stub.

    In T-10 this reads the day's row from ``prod_trade_gold.fact_signals`` and
    sends it to Telegram only when the signal changed.
    """
    logger.info(
        "signal-alert stub for ds=%s — wire orchestration.alerts (T-10)",
        context.get("ds"),
    )


# The only ingest that feeds the signal pipeline (conform → rsi → signals); it
# gates launch_dataflow. The rest are context series that feed the training views,
# so they run in parallel but must NOT block the signal.
_SIGNAL_INGEST_TASK_ID = "ingest_binance_btc"


def _make_ingest_callable(fn):
    """Strict callable for the critical (candle) ingest — failures fail the task."""

    def _run(**context):
        fn(ds=context.get("ds"))

    return _run


def _make_best_effort_callable(fn):
    """Tolerant callable for context ingests.

    These feed only the training views, and some sources block the VM's cloud IP
    (e.g. Google Trends 429s from datacenter ranges). A fetch failure is logged
    but does NOT fail the task, so a flaky context series never reddens the run
    nor — combined with the dependency wiring below — blocks the daily signal.
    """

    def _run(**context):
        try:
            fn(ds=context.get("ds"))
        except Exception:  # noqa: BLE001 - best-effort context feed
            logger.warning(
                "context ingest failed (non-blocking) for ds=%s",
                context.get("ds"),
                exc_info=True,
            )

    return _run


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # Generous on purpose: on the e2-micro a single ingest can crawl under swap.
    # With MAX_ACTIVE_TASKS_PER_DAG=1 tasks run serially, so this caps a genuinely
    # stuck task without tripping on mere slowness.
    "execution_timeout": timedelta(minutes=45),
    "on_failure_callback": _alert_on_failure,
}

with DAG(
    dag_id="daily_btc_signal",
    description="Daily BTC RSI signal: ingest → Dataflow → alert.",
    schedule="0 12 * * *",
    start_date=pendulum.datetime(2026, 6, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["trade", "btc", "rsi"],
) as dag:
    signal_ingest = None
    context_ingests = []
    for task_id, fn in INGEST_STEPS:
        if task_id == _SIGNAL_INGEST_TASK_ID:
            signal_ingest = PythonOperator(
                task_id=task_id, python_callable=_make_ingest_callable(fn)
            )
        else:
            context_ingests.append(
                PythonOperator(
                    task_id=task_id, python_callable=_make_best_effort_callable(fn)
                )
            )

    # Launch the medallion pipeline on Dataflow with the isolated Beam venv.
    # cd into the repo root so the relative --setup_file ./setup.py resolves.
    launch_dataflow = BashOperator(
        task_id="launch_dataflow",
        bash_command=(
            "cd /opt/airflow/repo && "
            + " ".join(build_dataflow_command("{{ ds }}"))
        ),
        execution_timeout=timedelta(minutes=90),  # blocks until the Dataflow job finishes
        retries=1,  # MERGE is idempotent, so a single re-launch is safe.
    )

    signal_alert = PythonOperator(
        task_id="signal_alert",
        python_callable=_signal_alert,
    )

    # Critical path: the candle gates Dataflow, which gates the alert. Context
    # ingests run in parallel (best-effort) and do not gate the signal.
    signal_ingest >> launch_dataflow >> signal_alert
