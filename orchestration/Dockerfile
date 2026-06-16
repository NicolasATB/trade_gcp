# Airflow image for the e2-micro orchestration VM (T-11).
#
# Build context is the REPO ROOT (see docker-compose.yaml: context: ..) so this
# Dockerfile can read both orchestration/requirements.txt and
# dataflow/requirements.txt.
#
# Pinned to the 2.x line on Python 3.12 (the project runtime). Airflow 2.x is
# lighter on RAM and ships the canonical docker-compose layout we trim down to a
# LocalExecutor stack; Airflow 3 restructures the components (api-server) and
# uses more memory, a poor fit for a 1 GB VM.
FROM apache/airflow:2.10.5-python3.12

# 1) Deps the ingest PythonOperators need (they import orchestration.ingest.*),
#    plus the Google provider so the DAG (T-12) has the Dataflow/Beam operators.
#    Installed into Airflow's own environment.
COPY orchestration/requirements.txt /tmp/airflow-extra-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-extra-requirements.txt

# 2) Isolated venv to LAUNCH the Dataflow pipeline (`python -m dataflow.pipeline`)
#    without polluting Airflow's dependency set: apache-beam pins libraries
#    (protobuf, grpcio, dill, ...) that conflict with Airflow's constraints, so
#    Beam lives in its own interpreter. Reuses dataflow/requirements.txt as the
#    single source of truth for the pipeline's deps. The DAG (T-12) invokes
#    /opt/beam-venv/bin/python with PYTHONPATH=/opt/airflow/repo.
#    Built as root because /opt is root-owned (the image runs as the non-root
#    `airflow` user); the venv ends up world-readable/executable, so the airflow
#    user can run it at runtime. We drop back to USER airflow afterwards.
COPY dataflow/requirements.txt /tmp/beam-requirements.txt
USER root
RUN python -m venv /opt/beam-venv \
    && /opt/beam-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/beam-venv/bin/pip install --no-cache-dir -r /tmp/beam-requirements.txt
USER airflow
