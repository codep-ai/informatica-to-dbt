"""
informatica_converter — Informatica PowerCenter repository exports (powrmart XML) → assessment, then dbt on Iceberg + Airflow.

Sibling of `agents.controlm_converter`, built on the same contracts (result dicts, coverage report, lift targets, rerun safety,
validator, native-tools agent). What is new here is the source side: the XML parser, the canonical mapping model, the port graph,
the transformation-type registry and the mapping classifier.

MVP scope (agreed 2026-09-27, "the easy 80%"): parse any export; classify every mapping as convertible / convertible-with-TODOs /
skipped by the transformation types and features it uses; compile the eight common transformations (Source Qualifier, Filter,
Expression, Aggregator, Joiner, Sorter, Router, connected static Lookup) to dbt models; translate the most-used expression
functions; render workflows to Airflow. Everything else is flagged, never guessed.

    from informatica_to_dbt import parse_export, assess_export
    folder = parse_export(Path("exports/SALES_DW.xml"))          # canonical model (dataclasses)
    results = assess_export(Path("exports/"))                    # one result dict per mapping, report-ready

No PowerCenter licence is needed: the parser follows the public repository DTD (powrmart.dtd, grammar 8.x); external DTD
entities are never fetched.
"""
__version__ = "0.3.0"   # 0.1 assessment · 0.2 compiler + dbt project · 0.3 Tier 2 agent, real-export fixes, seeds, provenance

from .model import Folder, Mapping, Instance, Transformation, Port, Connector, Session, Workflow, WorkflowLink, TaskInstance
from .parser import parse_export, parse_export_all, parse_export_dir
from .registry import TRANSFORMATION_REGISTRY, resolve_transformation, TransformationSpec
from .classifier import classify_mapping, classify_folder
from .assess import assess_export, render_assessment_html, render_assessment_md

__all__ = [
    "Folder", "Mapping", "Instance", "Transformation", "Port", "Connector", "Session", "Workflow", "WorkflowLink", "TaskInstance",
    "parse_export", "parse_export_all", "parse_export_dir", "TRANSFORMATION_REGISTRY", "resolve_transformation", "TransformationSpec",
    "classify_mapping", "classify_folder", "assess_export", "render_assessment_html", "render_assessment_md",
]
