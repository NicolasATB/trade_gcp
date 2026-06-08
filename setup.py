"""Packaging for the Dataflow pipeline.

DataflowRunner serialises the Beam DoFns and ships them to GCP workers, which are
clean machines that do NOT have this repo on their path. Without packaging, the
workers fail to unpickle the stages with ``ModuleNotFoundError: No module named
'dataflow'``.

Passing ``--setup_file ./setup.py`` makes Beam build an sdist of the ``dataflow``
package and install it on every worker, so ``dataflow.stages.*`` is importable
there. DirectRunner does not need this (it runs locally with the repo on path).

Usage (DataflowRunner):
    python -m dataflow.pipeline --runner DataflowRunner --setup_file ./setup.py ...
"""

import setuptools

setuptools.setup(
    name="trade-gcp-dataflow",
    version="0.1.0",
    description="BTC RSI medallion pipeline — Apache Beam stages (bronze→silver→gold).",
    # Only ship the Beam pipeline package; ingest/alerts run inside Airflow, not on
    # Dataflow workers, so they are intentionally excluded.
    packages=setuptools.find_packages(include=["dataflow", "dataflow.*"]),
    install_requires=[
        "apache-beam[gcp]==2.60.0",
        "google-cloud-bigquery==3.25.0",
    ],
    python_requires=">=3.9",
)
