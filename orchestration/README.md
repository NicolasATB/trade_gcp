# Orchestration — Airflow on an e2-micro VM (T-11)

Everything that runs **inside Airflow on the VM**: the daily DAG (`dags/`, T-12),
the ingest tasks (`ingest/`) and the alert task (`alerts/`, T-10). Dataflow is
**launched** from here but executes on managed GCP workers.

## Layout

| Path                 | What it is                                                      |
|----------------------|----------------------------------------------------------------|
| `docker-compose.yaml`| Minimal Airflow stack: LocalExecutor + Postgres (no Celery).   |
| `Dockerfile`         | Airflow 2.10.5 image + ingest deps + an isolated Beam venv.    |
| `requirements.txt`   | Extra deps baked into the Airflow environment.                 |
| `.env.example`       | Secrets/config template — copy to `.env` on the VM.            |
| `scripts/provision_vm.sh` | gcloud bootstrap: create the VM, swap and Docker.         |
| `ingest/`            | Ingestion modules (imported as `orchestration.ingest.*`).      |
| `dags/`              | The daily DAG `daily_btc_signal` (T-12).                        |
| `pipeline_launch.py` | Airflow-free helpers for the DAG (launch command + ingest steps). |
| `alerts/`            | Telegram alert client (T-10, pending).                         |

## Design notes

- **e2-micro + swap.** The VM has only ~1 GB of RAM, so the stack is trimmed to
  LocalExecutor + Postgres (no Redis/Celery/worker/Flower/triggerer) and the
  bootstrap adds **2 GB of swap**. Parallelism is kept low. A once-a-day DAG fits;
  this is a deliberate cost trade-off (cents/month). Watch for OOM if the workload
  grows — bump to `e2-small` (2 GB) if needed.
- **No namespace clash.** The repo is mounted read-only at `/opt/airflow/repo` and
  added to `PYTHONPATH`, so the DAG imports `orchestration.ingest.*` directly
  (the package was renamed away from `airflow.*` precisely to avoid shadowing the
  installed Airflow library).
- **Beam is isolated.** `apache-beam[gcp]` pins libraries that conflict with
  Airflow's, so it lives in `/opt/beam-venv`. The DAG launches the pipeline with
  `/opt/beam-venv/bin/python -m dataflow.pipeline` (PYTHONPATH=/opt/airflow/repo).
- **Secrets never in git.** The service-account key (`keys/sa.json`) and `.env`
  are git-ignored and live only on the VM. Migrating them to Secret Manager is
  tracked as T-13.

## Daily DAG — `daily_btc_signal` (T-12)

One DAG runs the whole pipeline once a day at **12:00 UTC** (`catchup=False`,
`max_active_runs=1`). It processes `{{ ds }}` — yesterday's fully closed candle —
in three phases:

1. **Ingest** — one `PythonOperator` per series (Binance BTC, MVRV, DXY, 10Y, 2Y,
   Fed funds, VIX, M2, supply), reusing `orchestration.ingest.*`. They fan out in
   parallel; the compose caps it to two at a time (`MAX_ACTIVE_TASKS_PER_DAG=2`),
   which suits 1 GB of RAM. Bitstamp is excluded — it is pre-2017 history, not a
   daily source.
2. **Launch Dataflow** — a `BashOperator` runs
   `cd /opt/airflow/repo && /opt/beam-venv/bin/python -m dataflow.pipeline
   --runner DataflowRunner --setup_file ./setup.py … --start_date {{ ds }}
   --end_date {{ ds }}` in the isolated Beam venv. The MERGE-based stages make the
   run idempotent (single re-launch is safe).
3. **Signal alert** — a `PythonOperator` (stub today; wired to Telegram in T-10).

**Retries & failure alert:** `default_args` set `retries=2` with exponential
backoff (5→30 min) and a per-task `execution_timeout`; an `on_failure_callback`
fires the failure alert (stub today, Telegram in T-10).

The launch command and the ingest step list live in the Airflow-free
`orchestration/pipeline_launch.py` so they are unit-tested in CI.

The bucket, project, region and service account are read from `.env`
(`DATAFLOW_TEMP_LOCATION`, `DATAFLOW_STAGING_LOCATION`, `DATAFLOW_SERVICE_ACCOUNT`,
`GCP_PROJECT`, `GCP_REGION`), with module defaults matching the Dataflow runs.

Verify on the VM after deploy:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors   # must be empty
docker compose exec airflow-scheduler airflow tasks test daily_btc_signal ingest_binance_btc 2026-06-15
docker compose exec airflow-scheduler airflow tasks test daily_btc_signal launch_dataflow   2026-06-15
docker compose exec airflow-scheduler airflow dags trigger daily_btc_signal
```

## Deploy

```bash
# 1) Provision the VM (Docker + 2 GB swap via startup script):
./scripts/provision_vm.sh

# 2) SSH in through IAP (the VM has no public IP):
gcloud compute ssh trade-airflow --zone us-central1-a --tunnel-through-iap

# On the VM, inside trade_gcp/orchestration:
cp .env.example .env && nano .env          # fill in real values
mkdir -p keys                              # SA key goes here (scp from your box)

docker compose build
docker compose up airflow-init             # one-shot: db migrate + admin user
docker compose up -d                       # scheduler + webserver
```

Upload the service-account key from your machine:

```bash
gcloud compute scp <PATH_TO_SA_KEY>.json \
    trade-airflow:~/trade_gcp/orchestration/keys/sa.json \
    --zone us-central1-a --tunnel-through-iap
```

Reach the UI (never expose port 8080 publicly) via an IAP port-forward, then open
<http://localhost:8080>:

```bash
gcloud compute ssh trade-airflow --zone us-central1-a \
    --tunnel-through-iap -- -L 8080:localhost:8080
```

## Local smoke test (no GCP)

```bash
cd orchestration
cp .env.example .env
docker compose config        # validate the compose file
docker compose build
docker compose up airflow-init && docker compose up -d
# Confirm the ingest packages import inside the image:
docker compose exec airflow-scheduler \
    python -c "import ccxt, google.cloud.bigquery; import orchestration.ingest"
```
