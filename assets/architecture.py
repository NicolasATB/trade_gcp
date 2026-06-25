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
    "nodesep": "1.5",
    "ranksep": "2.7",
    "splines": "spline",
    "compound": "true",
}
# GitHub renders the PNG scaled to the README column width, so legibility comes
# from a COMPACT canvas (tight nodesep/ranksep → less down-scaling) plus large
# fonts and SHORT, wrapped labels that don't collide with neighbours.
CLUSTER_ATTR = {"fontname": "Helvetica-Bold", "fontsize": "24"}
NODE_ATTR = {"fontname": "Helvetica", "fontsize": "22"}
EDGE_ATTR = {"fontname": "Helvetica-Bold", "fontsize": "28", "color": "#5f6368", "penwidth": "2.6"}

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
    # ---- LAYER 1 (top): input and output split into two boxes ------------
    with Cluster("Data sources (input)", graph_attr=CLUSTER_ATTR) as src_box:
        sources = Internet("Market-data APIs\n(Binance · FRED ·\nCoin Metrics · Yahoo)")
    with Cluster("User-facing outputs", graph_attr=CLUSTER_ATTR) as out_box:
        telegram = Telegram("Telegram\nalert on change")
        looker = Looker("Looker Studio\nQA dashboard")

    # ---- LAYER 2: application / logic (compute) --------------------------
    with Cluster("Application / logic", graph_attr=CLUSTER_ATTR):
        with Cluster("e2-micro VM — Compute Engine (Airflow)", graph_attr=CLUSTER_ATTR) as vm_box:
            ingest = Python("Ingest\nPythonOperator")
            # Alert is also a PythonOperator orchestrated by Airflow on the VM.
            alert = Python("Alert\nPythonOperator")
            # Scheduler is right-most so its "launch" edge to Dataflow is a short
            # horizontal hop into the adjacent box.
            scheduler = Airflow("Airflow\nscheduler")
        # Dataflow runs on managed GCP workers; Airflow only launches it. Its own
        # box makes the orchestration boundary explicit.
        with Cluster("Managed Dataflow (Apache Beam)", graph_attr=CLUSTER_ATTR) as df_box:
            df = Dataflow("Dataflow\njob")

    # ---- LAYER 3: data & platform ----------------------------------------
    with Cluster("Data & platform", graph_attr=CLUSTER_ATTR) as data_box:
        bq = Bigquery("BigQuery — medallion\nbronze → silver → gold")
        gcs = GCS("Cloud Storage\ntemp_location")

    # ---- LAYER 4 (bottom): base infrastructure & IaC ---------------------
    with Cluster("Base infrastructure & IaC", graph_attr=CLUSTER_ATTR) as iac_box:
        iac = Terraform("Terraform\n(BigQuery + VM)")
        repo = Github("GitHub")
        ci = GithubActions("CI\nruff + pytest")
        iac >> Edge(style="invis", constraint="false") >> repo   # order: Terraform leftmost
        repo >> Edge(color="#5f6368", constraint="false") >> ci  # push triggers CI

    # Edges are clipped to the cluster borders (lhead/ltail + compound=true) so
    # they connect box→box instead of icon→icon, which reads much cleaner.
    # --- Constraining edges (downward) lock the four layers top→bottom ---
    sources >> Edge(color="#4285F4", label="download",
                    ltail=src_box.name, lhead=vm_box.name) >> ingest
    ingest >> Edge(color="#4285F4", label="raw candles +\ncontext series",
                   ltail=vm_box.name, lhead=data_box.name) >> bq
    bq >> Edge(style="invis") >> iac   # layout only: anchor the IaC layer at the bottom

    # --- Cross-layer data flow (constraint=false so it doesn't move ranks) ---
    # launch/orchestrate: horizontal arrow from the scheduler to Dataflow (same
    # layer). Node→node with east→west ports keeps it flat (cluster clipping
    # pushed it up over the top, so no lhead/ltail here).
    scheduler >> Edge(color="#4285F4", label="launch", constraint="false",
                      tailport="e", headport="w") >> df
    df >> Edge(color="#4285F4", label="read OHLCV + params /\nwrite RSI + signal",
               dir="both", constraint="false", ltail=df_box.name, lhead=data_box.name) >> bq
    bq >> Edge(color="#4285F4", label="last signal", constraint="false",
               ltail=data_box.name, lhead=vm_box.name, tailport="n", headport="s") >> alert
    alert >> Edge(color="#34A853", label="send only if changed", constraint="false",
                  ltail=vm_box.name, lhead=out_box.name) >> telegram
    # gold views: hug the RIGHT margin (leave bq east, enter outputs box east).
    bq >> Edge(color="#A142F4", label="gold training +\nmonitor views", constraint="false",
               ltail=data_box.name, lhead=out_box.name, tailport="e", headport="e") >> looker

    # GCS temp_location is the staging intermediary for BigQuery I/O: reads
    # export to GCS, writes stage temp files there for the FILE_LOADS load job.
    df >> Edge(color="#9aa0a6", style="dashed", label="temp files", constraint="false",
               ltail=df_box.name, lhead=data_box.name) >> gcs
    gcs >> Edge(color="#9aa0a6", style="dashed", dir="both",
                label="FILE_LOADS /\nexport staging", constraint="false") >> bq

    # --- Provisioning (dashed) — hug the LEFT margin, enter the boxes from west.
    # Terraform provisions the VM + BigQuery (target the boxes, not single nodes).
    iac >> Edge(color="#9aa0a6", style="dashed", constraint="false",
                ltail=iac_box.name, lhead=data_box.name, tailport="w", headport="w") >> bq
    iac >> Edge(color="#9aa0a6", style="dashed", constraint="false",
                ltail=iac_box.name, lhead=vm_box.name, tailport="w", headport="w") >> ingest
