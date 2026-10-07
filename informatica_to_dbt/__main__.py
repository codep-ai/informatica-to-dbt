"""
CLI — python -m informatica_to_dbt <command> …

    assess  <export.xml | dir/>  [--html out.html] [--md out.md] [--json out.json]     week-1 product: the assessment
    convert <export.xml | dir/>  --out dbt_project/ [--project-name X] [--profile P]      week-2 product: the dbt project
    inspect <export.xml>         [--mapping NAME]                                       dump the canonical model of one mapping
    registry                                                                            print the transformation-type registry
    report  <dir/>  [--json out.json] [--out projects/]                                 run the whole pipeline over a directory of exports; per-file rows, never stops on one failure
    bench   <dir/>  --out bench/ [--md bench.md] [--json bench.json]                    the acceptance gate: real `dbt build` of every export on DuckDB (needs dbt-duckdb)
    fetch-public-corpus <dir/>                                                          pull public PowerCenter exports from GitHub for regression testing (needs gh)
"""
from __future__ import annotations
import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="informatica_converter", description="Informatica PowerCenter exports → assessment / dbt + Airflow")
    sub = p.add_subparsers(dest="cmd", required=True)
    a = sub.add_parser("assess", help="assess an export file or a directory of exports")
    a.add_argument("input", type=Path); a.add_argument("--html", type=Path); a.add_argument("--md", type=Path); a.add_argument("--json", type=Path)
    i = sub.add_parser("inspect", help="print the canonical model of an export (or one mapping) as JSON")
    i.add_argument("input", type=Path); i.add_argument("--mapping")
    sub.add_parser("registry", help="print the transformation-type registry")
    c = sub.add_parser("convert", help="write a dbt project (one model per PowerCenter target) from an export file or directory")
    c.add_argument("input", type=Path); c.add_argument("--out", type=Path, required=True); c.add_argument("--project-name", default="informatica_conversion")
    c.add_argument("--profile"); c.add_argument("--no-iceberg", action="store_true")
    c.add_argument("--tier", type=int, choices=[1, 2], default=1, help="2 = also run the Claude-first modernizer agent on mappings with TODOs")
    r = sub.add_parser("report", help="run parse → classify → compile → write over a directory of exports and report per file")
    r.add_argument("input", type=Path); r.add_argument("--json", type=Path); r.add_argument("--out", type=Path)
    b = sub.add_parser("bench", help="real dbt build of every export on DuckDB (test bench); the acceptance gate")
    b.add_argument("input", type=Path); b.add_argument("--out", type=Path, required=True); b.add_argument("--md", type=Path); b.add_argument("--json", type=Path)
    b.add_argument("--genuine-only", action="store_true", help="only files that look like Designer exports (CREATION_DATE, REPOSITORY VERSION, INSTANCE)")
    fp = sub.add_parser("fetch-public-corpus", help="download public PowerCenter exports from GitHub into a directory")
    fp.add_argument("dest", type=Path); fp.add_argument("--limit", type=int, default=100)
    return p


def main(argv=None) -> int:
    from . import assess_export, parse_export_all, render_assessment_md
    from .registry import list_registry
    a = _build_parser().parse_args(argv)
    if a.cmd == "registry":
        for r in list_registry():
            print(f"{r['type']:22s} {r['family']:16s} {r['status']:9s} {r['aliases']}")
        return 0
    if a.cmd == "inspect":
        folders = parse_export_all(a.input)
        for f in folders:
            if a.mapping:
                m = f.mapping(a.mapping)
                if m: print(json.dumps(asdict(m), indent=2, default=str)); return 0
            else:
                print(json.dumps(f.summary(), indent=2))
        if a.mapping: print(f"mapping {a.mapping!r} not found", file=sys.stderr); return 1
        return 0
    if a.cmd == "report":
        from .corpus import report
        res = report(a.input, a.out)
        if a.json: a.json.write_text(json.dumps(res, indent=1, default=str))
        print(json.dumps(res["summary"], indent=1))
        for row in res["rows"]:
            if row.get("error") or row.get("errors"): print(f"  {row['file'][:70]} | {row.get('error', '')} | {'; '.join(row.get('errors', [])[:3])}", file=sys.stderr)
        return 0
    if a.cmd == "bench":
        from .corpus import bench, genuine_exports, render_bench_md
        import tempfile, shutil
        src = a.input
        if a.genuine_only and a.input.is_dir():
            tmp = Path(tempfile.mkdtemp(prefix="infa_genuine_")); 
            for f in genuine_exports(a.input): shutil.copy(f, tmp / f"{f.parent.name}__{f.name}")
            src = tmp
        res = bench(src, a.out)
        md = render_bench_md(res)
        if a.md: a.md.write_text(md)
        if a.json: a.json.write_text(json.dumps(res, indent=1, default=str))
        print(md); return 0 if not res["summary"].get("fail") else 1
    if a.cmd == "fetch-public-corpus":
        from .corpus import fetch_public_corpus
        print(json.dumps(fetch_public_corpus(a.dest, a.limit), indent=1)); return 0
    if a.cmd == "convert":
        from .parser import parse_export_dir
        from .project import write_project
        folders = parse_export_dir(a.input) if a.input.is_dir() else parse_export_all(a.input)
        summ = write_project(folders, a.out, project_name=a.project_name, profile=a.profile, iceberg=not a.no_iceberg, tier=a.tier)
        print(json.dumps(summ, indent=2)); return 0
    results = assess_export(a.input, report_html=a.html, report_md=a.md, results_json=a.json)
    print(render_assessment_md(results, inputs=[str(a.input)]))
    for out in (a.html, a.md, a.json):
        if out: print(f"  → wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
