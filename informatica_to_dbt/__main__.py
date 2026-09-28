"""
CLI — python -m informatica_to_dbt <command> …

    assess  <export.xml | dir/>  [--html out.html] [--md out.md] [--json out.json]     week-1 product: the assessment
    convert <export.xml | dir/>  --out dbt_project/ [--project-name X] [--profile P]      week-2 product: the dbt project
    inspect <export.xml>         [--mapping NAME]                                       dump the canonical model of one mapping
    registry                                                                            print the transformation-type registry
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
    if a.cmd == "convert":
        from .parser import parse_export_dir
        from .project import write_project
        folders = parse_export_dir(a.input) if a.input.is_dir() else parse_export_all(a.input)
        summ = write_project(folders, a.out, project_name=a.project_name, profile=a.profile, iceberg=not a.no_iceberg)
        print(json.dumps(summ, indent=2)); return 0
    results = assess_export(a.input, report_html=a.html, report_md=a.md, results_json=a.json)
    print(render_assessment_md(results, inputs=[str(a.input)]))
    for out in (a.html, a.md, a.json):
        if out: print(f"  → wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
