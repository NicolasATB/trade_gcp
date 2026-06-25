"""Render the project architecture diagram with official GCP icons.

Diagram-as-code (mingrammer ``diagrams``) so the architecture image stays
versioned and reproducible instead of a one-off manual export. Regenerate with:

    python assets/architecture.py        # writes assets/architecture.png

Requires Graphviz (the ``dot`` binary) on PATH and the ``diagrams`` package
(``pip install diagrams``). On Windows, Graphviz installs to
``C:\\Program Files\\Graphviz\\bin``; this script adds it to PATH automatically.

Layering convention (KEEP THIS — top→bottom layers, the standard cloud
architecture style). The diagram is ``direction="TB"`` and every node lives in
exactly one horizontal layer cluster, ordered top to bottom:

    1. Users & external endpoints  — clients, external APIs, DNS/CDN, the
       user-facing sinks (here: market-data sources, Telegram, Looker Studio).
    2. Application / logic         — services, APIs, serverless, containers
       (here: the Airflow VM operators + Dataflow/Beam).
    3. Data & platform            — databases, queues, caches, object storage
       (here: BigQuery + Cloud Storage).
    4. Base infrastructure & IaC  — VPC/subnets, firewalls, IAM, and the
       provisioning tooling (here: Terraform, GitHub, CI) that underpins it all.

When adding a node, place it in the layer it belongs to; never put IaC on top.
Downward edges (constraint=True) lock the layer order; cross-layer data-flow
edges use constraint=False so they don't disturb the ranking.
"""

from __future__ import annotations

import os
from pathlib import Path

# Make the Graphviz `dot` binary discoverable even when the installer did not
# refresh the current shell's PATH (common on Windows after `winget install`).
for _candidate in (r"C:\Program Files\Graphviz\bin", r"C:\Program Files (x86)\Graphviz\bin"):
    if os.path.isdir(_candidate) and _candidate not in os.environ.get("PATH", ""):
        os.environ["PATH"] = _candidate + os.pathsep + os.environ.get("PATH", "")

from diagrams import Cluster, Diagram, Edge  # noqa: E402
from diagrams.gcp.analytics import Bigquery, Dataflow, Looker  # noqa: E402
from diagrams.gcp.compute import ComputeEngine  # noqa: E402
from diagrams.gcp.storage import GCS  # noqa: E402
from diagrams.onprem.ci import GithubActions  # noqa: E402
from diagrams.onprem.iac import Terraform  # noqa: E402
from diagrams.onprem.network import Internet  # noqa: E402
from diagrams.onprem.vcs import Github  # noqa: E402
from diagrams.onprem.workflow import Airflow  # noqa: E402
from diagrams.programming.language import Python  # noqa: E402
from diagrams.saas.chat import Telegram  # noqa: E402

# Brand-ish styling for a clean, Google-docs-like look.
GRAPH_ATTR = {
    "fontname": "Helvetica-Bold",
    "fontsize": "40",
    "labelloc": "t",
    "bgcolor": "white",
    "pad": "1.2",
    "nodesep": "1.1",
    "ranksep": "1.7",
    "splines": "spline",
    "compound": "true",
}
# GitHub renders the PNG scaled to the README column width, so legibility comes
# from a COMPACT canvas (tight nodesep/ranksep → less down-scaling) plus large
# fonts and SHORT, wrapped labels that don't collide with neighbours.
CLUSTER_ATTR = {"fontname": "Helvetica-Bold", "fontsize": "24"}
NODE_ATTR = {"fontname": "Helvetica", "fontsize": "22"}
EDGE_ATTR = {"fontname": "Helvetica", "fontsize": "18", "color": "#5f6368"}

# Edge styles: solid blue = data flow, dashed grey = provisioning (IaC).
DATA = Edge(color="#4285F4")
PROVISION = Edge(color="#9aa0a6", style="dashed")

_OUT = str(Path(__file__).with_name("architecture"))  # diagrams appends .png

with Diagram(
    "Quantitative Trading-Signal Pipeline on GCP",
    filename=_OUT,
    outformat="png",
    show=False,
    direction="TB",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    edge_attr=EDGE_ATTR,
):
    # ---- LAYER 1 (top): users & external endpoints -----------------------
    with Cluster("Users & external endpoints", graph_attr=CLUSTER_ATTR):
        sources = Internet("Market-data APIs\n(Binance · FRED ·\nCoin Metrics · Yahoo)")
        telegram = Telegram("Telegram\nalert on change")
        looker = Looker("Looker Studio\nQA dashboard")

    # ---- LAYER 2: application / logic (compute) --------------------------
    with Cluster("Application / logic", graph_attr=CLUSTER_ATTR):
        with Cluster("e2-micro VM — Compute Engine (Airflow)", graph_attr=CLUSTER_ATTR):
            ingest = Python("Ingest\nPythonOperator")
            scheduler = Airflow("Airflow\nscheduler")
            # Alert is also a PythonOperator orchestrated by Airflow on the VM.
            alert = Python("Alert\nPythonOperator")
        df = Dataflow("Dataflow\n(Apache Beam)")

    # ---- LAYER 3: data & platform ----------------------------------------
    with Cluster("Data & platform", graph_attr=CLUSTER_ATTR):
        bq = Bigquery("BigQuery — medallion\nbronze → silver → gold")
        gcs = GCS("Cloud Storage\ntemp_location")

    # ---- LAYER 4 (bottom): base infrastructure & IaC ---------------------
    with Cluster("Base infrastructure & IaC", graph_attr=CLUSTER_ATTR):
        iac = Terraform("Terraform\n(BigQuery + VM)")
        repo = Github("GitHub")
        ci = GithubActions("CI\nruff + pytest")
        repo >> Edge(color="#5f6368", constraint="false") >> ci   # push triggers CI

    # --- Constraining edges (downward) lock the four layers top→bottom ---
    sources >> Edge(color="#4285F4", label="download") >> ingest
    ingest >> Edge(color="#4285F4", label="raw candles +\ncontext series") >> bq
    bq >> Edge(style="invis") >> iac   # layout only: anchor the IaC layer at the bottom

    # --- Cross-layer data flow (constraint=false so it doesn't move ranks) ---
    scheduler >> Edge(color="#4285F4", label="launch", constraint="false") >> df
    df >> Edge(color="#4285F4", label="read OHLCV + params /\nwrite RSI + signal", constraint="false") >> bq
    bq >> Edge(color="#4285F4", constraint="false") >> df
    bq >> Edge(color="#4285F4", label="last signal", constraint="false") >> alert
    alert >> Edge(color="#34A853", label="send only if changed", constraint="false") >> telegram
    bq >> Edge(color="#A142F4", label="gold training +\nmonitor views", constraint="false") >> looker

    # GCS temp_location is the staging intermediary for BigQuery I/O: reads
    # export to GCS, writes stage temp files there for the FILE_LOADS load job.
    df >> Edge(color="#9aa0a6", style="dashed", label="temp files", constraint="false") >> gcs
    gcs >> Edge(color="#9aa0a6", style="dashed", dir="both",
                label="FILE_LOADS /\nexport staging", constraint="false") >> bq

    # --- Provisioning (dashed, upward from the IaC layer) ---
    iac >> Edge(color="#9aa0a6", style="dashed", constraint="false") >> bq
    iac >> Edge(color="#9aa0a6", style="dashed", constraint="false") >> scheduler
