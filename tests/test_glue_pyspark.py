"""Optional Glue PySpark output: every compiled model renders to a job whose Python compiles and whose SQL parses as Spark SQL;
with pyspark installed, each SQL's logical plan is built against empty typed tables in a local session (the Glue bench)."""
import json
import os
import py_compile
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from informatica_to_dbt import parse_export_dir  # noqa: E402
from informatica_to_dbt.compiler import compile_mapping  # noqa: E402
from informatica_to_dbt.glue_pyspark import render_job, write_glue_jobs  # noqa: E402
from informatica_to_dbt.project import _yml_type  # noqa: E402

FIXDIR = ROOT / "sample_exports"
_SPARK_T = {"numeric": "decimal(38,6)", "bigint": "bigint", "integer": "int", "float": "double", "string": "string", "timestamp": "timestamp", "binary": "binary"}


def _models():
    out = []
    for f in parse_export_dir(FIXDIR):
        for m in f.mappings:
            for o in compile_mapping(m, f):
                if o.decision != "skip" and o.sql and not any("SQL override used verbatim" in t for t in o.todos):   # Oracle SQL kept verbatim: engineer's TODO, not benchable
                    out.append((f, o))
    return out


def test_macros_render_to_spark():
    sql = "{{ config(materialized='table') }} select {{ dbt.dateadd('day', 7, d) }} a, {{ dbt.datediff(a, b, 'day') }} b, {{ dbt.safe_cast(x, dbt.type_int()) }} c, " \
          "{{ dbt.hash(dbt.concat([\"a\", \"'|'\", \"b\"])) }} h, {{ var('LOAD_FROM', '1900-01-01') }} v from {{ source('src', 't') }} join {{ ref('m') }} on 1=1"
    js, args = render_job("m2", sql, target_db="tgt", source_dbs={"src": "raw"})
    s = json.loads(js)["full"].lower()
    assert "interval" in s and "day" in s and "datediff(b, a)" in s and "try_cast(x as int)" in s and "sha2(" in s and "dbt." not in s
    assert "raw.t" in s and "tgt.m" in s and "${load_from}" in s and args == ["LOAD_FROM"]


def test_every_model_renders_compiles_and_parses(tmp_path):
    import sqlglot
    n = 0
    for f, o in _models():
        js, _ = render_job(o.name, o.sql, target_db="tgt", materialization=o.materialization, unique_key=o.unique_key)
        for k, s in json.loads(js).items():
            import re as _re
            sqlglot.parse_one(_re.sub(r"\$\{\w+\}", "1900-01-01", s), read="spark"); n += 1
    assert n >= 4
    folders = parse_export_dir(FIXDIR)
    res = write_glue_jobs([o for _, o in _models()], tmp_path, target_db="tgt", rendered_at="2026-10-09T00:00:00Z", source_hash="0" * 64, folder="SALES_DW")
    assert res["jobs"] >= 4 and not res["failed"], res
    for p in tmp_path.glob("glue_jobs/*.py"):
        py_compile.compile(str(p), doraise=True)
        assert p.read_text().startswith("# datapai:source_hash=")
    m = json.loads((tmp_path / "glue_jobs" / "jobs.json").read_text())
    assert m["jobs"][0]["GlueVersion"] == "4.0" and m["jobs"][0]["DefaultArguments"]["--datalake-formats"] == "iceberg"


def test_spark_plans_against_empty_tables():
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("infa_glue_bench").config("spark.ui.enabled", "false").getOrCreate()
    folders = parse_export_dir(FIXDIR)
    for f in folders:
        for s in f.sources:
            cols = ", ".join(f"`{p.name.lower()}` {_SPARK_T.get(_yml_type(p.datatype), 'string')}" for p in s.fields) or "_d int"
            db = (s.dbd_name or "src").lower()
            spark.sql(f"create database if not exists {db}"); spark.sql(f"create or replace temp view {db}_{s.name.lower()} ({cols}) as select * from (select 1) where 1=0") if False else None
            spark.sql(f"create table if not exists {db}.{s.name.lower()} ({cols}) using parquet")
    spark.sql("create database if not exists tgt"); spark.sql("create database if not exists lookup")
    planned = 0
    for f, o in _models():
        if any(sd["name"] for sd in getattr(o, "seeds", [])):
            for sd in o.seeds:
                cols = ", ".join(f"`{c.lower()}` string" for c, _ in sd["columns"]) or "_d int"
                spark.sql(f"create table if not exists tgt.{sd['name']} ({cols}) using parquet")
        js, args = render_job(o.name, o.sql, target_db="tgt", materialization=o.materialization, unique_key=o.unique_key)
        for k, s in json.loads(js).items():
            for a in args: s = s.replace("${" + a + "}", "1900-01-01")
            if k == "incremental": continue             # needs the target to exist; full branch creates it
            spark.sql(s).explain(); planned += 1
    assert planned >= 4
    spark.stop()


def test_databricks_jobs_output(tmp_path):
    from informatica_to_dbt.databricks_jobs import write_databricks_jobs
    res = write_databricks_jobs([o for _, o in _models()], tmp_path, catalog="azure_datapai_databricks_dev", schema="infa", rendered_at="2026-10-09T00:00:00Z", source_hash="0" * 64, folder="SALES_DW")
    assert res["jobs"] >= 4 and not res["failed"], res
    nb = next(tmp_path.glob("databricks_jobs/*.py")).read_text()
    assert nb.startswith("# Databricks notebook source\n# datapai:source_hash=") and "saveAsTable" in nb
    from informatica_to_dbt.provenance import parse_header, verify
    assert parse_header(nb)["template_id"] == "informatica.databricks_notebook"
    assert all(r.status == "clean" for r in verify(tmp_path, patterns=("databricks_jobs/*.py",)))
    py_compile.compile(str(next(tmp_path.glob("databricks_jobs/*.py"))), doraise=True)
    spec = json.loads((tmp_path / "databricks_jobs" / "jobs.json").read_text())["jobs"][0]
    assert spec["tasks"][0]["notebook_task"]["notebook_path"].endswith(spec["tasks"][0]["task_key"]) and spec["environments"]
