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
| `scripts/provision_vm.sh` | gcloud bootstrap: firewall lockdown, the VM, swap, Docker. |
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
- **No GCP key file — attached service account.** The VM is created with the
  `trade-pipeline@…` service account attached (`--scopes cloud-platform`), so the
  google client libraries (BigQuery, Beam) authenticate through the metadata
  server (Application Default Credentials), reachable from inside the containers.
  There is **no `keys/sa.json`** to upload, mount, manage or rotate, and
  `GOOGLE_APPLICATION_CREDENTIALS` is intentionally left unset (a missing path
  would break ADC). This is the secret-handling approach for T-13.
- **Egress via an external IP, inbound locked down.** The VM has an ephemeral
  external IP for outbound internet (Docker Hub, the ingest source APIs). For a
  single always-on VM that is ~10× cheaper than Cloud NAT (~$3.6 vs ~$32/mo). To
  keep it safe despite the public IP, `provision_vm.sh` locks the `default` VPC
  down: SSH is allowed only from IAP (35.235.240.0/20) and the world-open
  `default-allow-ssh`/`-rdp` rules are removed — inbound stays closed (only
  replies to outbound return, via the stateful firewall) and SSH goes through IAP.
- **Secrets never in git.** `.env` (Fernet key, admin password, `FRED_API_KEY`)
  is git-ignored and lives only on the VM.

## Daily DAG — `daily_btc_signal` (T-12)

One DAG runs the whole pipeline once a day at **12:00 UTC** (`catchup=False`,
`max_active_runs=1`). It processes `{{ ds }}` — yesterday's fully closed candle —
in three phases:

1. **Ingest** — one `PythonOperator` per series (Binance BTC, MVRV, DXY, 10Y, 2Y,
   Fed funds, VIX, M2, supply, active addresses, tx count, Google Trends investor
   attention), reusing `orchestration.ingest.*`. They fan out in parallel; the
   compose caps it to two at a time (`MAX_ACTIVE_TASKS_PER_DAG=2`), which suits 1 GB
   of RAM. Google Trends is weekly — its step refreshes the latest window in place
   (idempotent), so a daily firing just picks up the newest closed week. Bitstamp is
   excluded — it is pre-2017 history, not a daily source.
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
# 1) Firewall lockdown + the VM (Docker + 2 GB swap via startup script):
./scripts/provision_vm.sh

# 2) SSH in through IAP (inbound locked down; SSH only via IAP):
gcloud compute ssh trade-airflow --zone us-central1-a --tunnel-through-iap

# On the VM, inside trade_gcp/orchestration:
cp .env.example .env && nano .env          # AIRFLOW_UID, admin password,
                                           # Fernet key, FRED_API_KEY
                                           # (no GCP key file — attached SA / ADC)

docker compose build
docker compose up airflow-init             # one-shot: db migrate + admin user
docker compose up -d                       # scheduler + webserver
```

> GCP auth needs no key file: the VM runs as its attached service account
> (`trade-pipeline@…`) and the libraries pick it up via the metadata server (ADC).
> If the startup script timed out installing Docker (it has a ~5-min deadline and
> the convenience script can be slow on an e2-micro), finish it by hand on the VM:
> `curl -fsSL https://get.docker.com | sh` then `sudo systemctl enable --now docker`.

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
