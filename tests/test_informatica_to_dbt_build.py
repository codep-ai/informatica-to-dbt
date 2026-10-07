"""End-to-end: the generated dbt project must BUILD with a real dbt adapter. DuckDB is the test bench only (no credentials,
runs on a laptop); the output is adapter-neutral dbt. Sources are empty tables typed from the export's SOURCE definitions.
Skipped when dbt-duckdb is not installed."""
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
duckdb = pytest.importorskip("duckdb")
pytest.importorskip("dbt.adapters.duckdb")

from informatica_to_dbt import parse_export  # noqa: E402,F401
from informatica_to_dbt.project import _yml_type, write_project  # noqa: E402

FIXDIR = ROOT / "sample_exports"
_DUCK = {"numeric": "DECIMAL(38,6)", "bigint": "BIGINT", "integer": "INTEGER", "float": "DOUBLE", "string": "VARCHAR", "timestamp": "TIMESTAMP", "binary": "BLOB"}


@pytest.fixture(scope="module")
def project(tmp_path_factory):
    out = tmp_path_factory.mktemp("infa_dbt")
    from informatica_to_dbt import parse_export_dir
    folders = parse_export_dir(FIXDIR)
    summ = write_project(folders, out, project_name="infa_sales_dw", profile="infa_sales_dw", iceberg=False)
    db = out / "bench.duckdb"
    con = duckdb.connect(str(db))
    for s in [x for f in folders for x in f.sources]:
        cols = ", ".join(f"{p.name.lower()} {_DUCK.get(_yml_type(p.datatype), 'VARCHAR')}" for p in s.fields)
        # the dbt source schema (DBD) and, for SQL overrides kept verbatim, the owner schema the override names (e.g. GL.GL_JOURNAL_NZ)
        for schema in {s.dbd_name.lower(), (s.owner or s.dbd_name).lower()}:
            con.execute(f"create schema if not exists {schema}")
            con.execute(f"create table if not exists {schema}.{s.name.lower()} ({cols})")

    con.close()
    (out / "profiles.yml").write_text(f"infa_sales_dw:\n  target: bench\n  outputs:\n    bench:\n      type: duckdb\n      path: {db}\n      threads: 1\n", encoding="utf-8")
    return out, summ


def _dbt(out: Path, *args: str) -> subprocess.CompletedProcess:
    env = dict(os.environ, DBT_PROFILES_DIR=str(out))
    for k in ("DBT_PROFILE", "DBT_TARGET", "DBT_PROJECT_DIR", "DBT_PROJECT_PATH"): env.pop(k, None)
    dbt_bin = Path(sys.executable).parent / "dbt"          # the adapter's venv provides the CLI entry point
    return subprocess.run([str(dbt_bin), *args, "--project-dir", str(out), "--profiles-dir", str(out), "--no-use-colors"],
                          capture_output=True, text=True, env=env, timeout=600)


def test_generated_project_parses(project):
    out, summ = project
    assert summ["models"] >= 3
    r = _dbt(out, "parse")
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-1000:]


def _verbatim_override_models(out: Path):
    """Models whose Source Qualifier SQL override was kept verbatim are Oracle SQL by design (a TODO for the engineer); the bench
    cannot run them and does not pretend to."""
    return sorted(p.stem for p in out.glob("models/*/*.sql") if "SQL override used verbatim" in p.read_text(encoding="utf-8"))


def test_generated_project_builds_on_duckdb(project):
    out, _ = project
    skip = _verbatim_override_models(out)
    assert skip == ["fact_gl_top_accounts"]
    r = _dbt(out, "build", *(["--exclude", *skip] if skip else []))
    assert r.returncode == 0, r.stdout[-4000:] + r.stderr[-1000:]
    results = json.loads((out / "target" / "run_results.json").read_text())["results"]
    statuses = {x["unique_id"]: x["status"] for x in results}
    models = {k: v for k, v in statuses.items() if k.startswith("model.")}
    tests = {k: v for k, v in statuses.items() if k.startswith("test.")}
    assert len(models) == 4 and all(v == "success" for v in models.values()), statuses   # SALES_DW; FINANCE_DW's model carries a verbatim override
    assert tests and all(v == "pass" for v in tests.values()), statuses
