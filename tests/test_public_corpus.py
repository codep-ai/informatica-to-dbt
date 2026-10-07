"""Regression gate on REAL PowerCenter exports (public GitHub corpus, fetched with
`python -m informatica_to_dbt fetch-public-corpus sample_exports_public`; git-ignored, licences unchecked —
a test input, never a deliverable). Skipped when the corpus or dbt-duckdb is absent.

Why: the compiler was first proven on two hand-written exports; the first run over the public corpus (2026-10-07) found five
converter bugs they could not show (Union exported as Custom Transformation, master ports marked in PORTTYPE, a BOM inside the first
flat-file column and the expressions naming it, reserved words as instance names, self-referencing variable ports). This test keeps
the bench number from regressing: every genuine export must parse and compile, and every generated model must BUILD."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CORPUS = ROOT / "sample_exports_public"
pytestmark = pytest.mark.skipif(not CORPUS.exists(), reason="public corpus not fetched")


def test_report_runs_over_every_file_without_crashing():
    from informatica_to_dbt.corpus import report
    res = report(CORPUS)
    crashes = {k: v for k, v in res["summary"]["crashes"].items() if k != "parse:ParseError"}   # one file in the corpus is deliberately malformed
    assert res["summary"]["exports"] >= 100 and not crashes, res["summary"]


def test_every_genuine_export_builds(tmp_path):
    pytest.importorskip("duckdb"); pytest.importorskip("dbt.adapters.duckdb")
    import shutil
    from informatica_to_dbt.corpus import bench, genuine_exports, render_bench_md
    src = tmp_path / "genuine"; src.mkdir()
    files = genuine_exports(CORPUS)
    assert len(files) >= 15, "corpus changed: fewer genuine exports than expected"
    for f in files: shutil.copy(f, src / f"{f.parent.name}__{f.name}")
    res = bench(src, tmp_path / "bench")
    s = res["summary"]
    assert s["models"] >= 20 and s["fail"] == 0, render_bench_md(res)
