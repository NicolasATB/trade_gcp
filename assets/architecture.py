"""Render the project architecture diagram with official GCP icons.

Diagram-as-code (mingrammer ``diagrams``) so the architecture image stays
versioned and reproducible instead of a one-off manual export. Regenerate with:

    python assets/architecture.py        # writes assets/architecture.png

Requires Graphviz (the ``dot`` binary) on PATH and the ``diagrams`` package
(``pip install diagrams``). On Windows, Graphviz installs to
``C:\\Program Files\\Graphviz\\bin``; this script adds it to PATH automatically.
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
    "nodesep": "0.6",
    "ranksep": "1.1",
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
    direction="LR",
    graph_attr=GRAPH_ATTR,
    node_attr=NODE_ATTR,
    edge_attr=EDGE_ATTR,
):
    # Source / IaC / CI — declared first and chained left→right so it renders as
    # a horizontal band on top, provisioning the rest (dashed edges below).
    with Cluster("Source · IaC · CI", graph_attr=CLUSTER_ATTR):
        repo = Github("GitHub")
        ci = GithubActions("CI\nruff + pytest")
        iac = Terraform("Terraform\n(IaC)")
        repo >> Edge(color="#5f6368") >> ci          # push triggers CI
        ci >> Edge(style="invis") >> iac             # layout only: keep the row horizontal

    sources = Internet("Market-data APIs\n(Binance · Bitstamp · FRED ·\nCoin Metrics · Yahoo/Tiingo)")

    with Cluster("e2-micro VM — Compute Engine (Airflow)", graph_attr=CLUSTER_ATTR):
        scheduler = Airflow("Airflow\nscheduler")
        ingest = Python("Ingest\nPythonOperator")
        # Alert is also a PythonOperator orchestrated by Airflow on the VM.
        alert = Python("Alert\nPythonOperator")
        scheduler >> Edge(color="#5f6368", constraint="false") >> ingest
        scheduler >> Edge(color="#5f6368", constraint="false") >> alert

    with Cluster("Managed GCP services", graph_attr=CLUSTER_ATTR):
        bq = Bigquery("BigQuery\nmedallion\nbronze → silver → gold")
        df = Dataflow("Dataflow\n(Apache Beam)")
        gcs = GCS("Cloud Storage\ntemp_location")

    telegram = Telegram("Telegram\nalert on change")
    looker = Looker("Looker Studio\nQA dashboard")  # consumes the gold views

    # --- Data flow: constraining edges define the left→right ranks ---
    #   sources | VM (scheduler+ingest+alert) | bq+df+gcs | telegram+looker
    sources >> DATA >> ingest
    ingest >> Edge(color="#4285F4", label="raw candles +\ncontext series") >> bq
    scheduler >> Edge(color="#4285F4", label="launch") >> df
    bq >> Edge(color="#A142F4", label="gold training +\nmonitor views") >> looker

    # Alert stays inside the VM cluster, so its signal edges don't constrain rank
    # (otherwise the cluster box would span ranks and overlap the GCP one).
    bq >> Edge(color="#4285F4", label="last signal", constraint="false") >> alert
    alert >> Edge(color="#34A853", label="send only if changed", constraint="false") >> telegram
    bq >> Edge(style="invis") >> telegram   # layout only: anchor telegram to the right

    # --- Same-rank / return / provisioning edges: constraint="false" ---
    df >> Edge(color="#4285F4", label="read OHLCV + params /\nwrite RSI + signal", constraint="false") >> bq
    bq >> Edge(color="#4285F4", constraint="false") >> df
    # GCS temp_location is the staging intermediary for BigQuery I/O: reads
    # export to GCS, writes stage temp files there for the FILE_LOADS load job.
    df >> Edge(color="#9aa0a6", style="dashed", label="temp files", constraint="false") >> gcs
    gcs >> Edge(color="#9aa0a6", style="dashed", dir="both",
                label="FILE_LOADS /\nexport staging", constraint="false") >> bq
    iac >> Edge(color="#9aa0a6", style="dashed", constraint="false") >> bq
    iac >> Edge(color="#9aa0a6", style="dashed", constraint="false") >> scheduler
