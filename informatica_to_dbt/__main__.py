"""
CLI — python -m informatica_to_dbt <command> …

    assess  <export.xml | dir/>  [--html out.html] [--md out.md] [--json out.json]     week-1 product: the assessment
    convert <export.xml | dir/>  --out dbt_project/ [--project-name X] [--profile P]      week-2 product: the dbt project
    inspect <export.xml>         [--mapping NAME]                                       dump the canonical model of one mapping
    registry                                                                            print the transformation-type registry
    report  <dir/>  [--json out.json] [--out projects/]                                 run the whole pipeline over a directory of exports; per-file rows, never stops on one failure
    bench   <dir/>  --out bench/ [--md bench.md] [--json bench.json]                    the acceptance gate: real `dbt build` of every export on DuckDB (needs dbt-duckdb)
    verify-drift <dbt_project/> [--exports exports/] [--md drift.md]                  which generated files were hand-edited (content_hash) or are stale (export changed)
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
    c.add_argument("--output", choices=["dbt", "dbt+glue-pyspark"], default="dbt", help="dbt (default) or also Glue 4.0 PySpark jobs under glue_jobs/")
    c.add_argument("--glue-target-db", help="Glue database the PySpark jobs write to (default: the project name)")
    c.add_argument("--gate", type=Path, help="accepted discovery-gate answers file (.datapai/gate.json) — required unless --no-gate")
    c.add_argument("--no-gate", action="store_true", help="write the project without gate answers (dev / bench only; logged)")
    g = sub.add_parser("gate", help="discovery gate: print the questions, or accept a human's answers file")
    g.add_argument("what", choices=["questions", "accept", "pack"]); g.add_argument("--scope", default="migration")
    g.add_argument("--regulation", action="append", help="regulation pack(s) to add: APRA_CPS_234 | APRA_CPG_235 | AU_PRIVACY_ACT | AUSTRAC_AML_CTF")
    g.add_argument("--answers", type=Path, help="accept: JSON {question_id: answer}"); g.add_argument("--by", help="accept: the human who answered")
    g.add_argument("--out", type=Path, help="accept: directory to write .datapai/gate.json into (the project dir)")
    r = sub.add_parser("report", help="run parse → classify → compile → write over a directory of exports and report per file")
    r.add_argument("input", type=Path); r.add_argument("--json", type=Path); r.add_argument("--out", type=Path)
    b = sub.add_parser("bench", help="real dbt build of every export on DuckDB (test bench); the acceptance gate")
    b.add_argument("input", type=Path); b.add_argument("--out", type=Path, required=True); b.add_argument("--md", type=Path); b.add_argument("--json", type=Path)
    b.add_argument("--genuine-only", action="store_true", help="only files that look like Designer exports (CREATION_DATE, REPOSITORY VERSION, INSTANCE)")
    vd = sub.add_parser("verify-drift", help="report hand-edited (content_hash) and stale (source_hash vs current export) generated files")
    vd.add_argument("project", type=Path); vd.add_argument("--exports", type=Path, help="export file or directory to compare source hashes against")
    vd.add_argument("--md", type=Path)
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
    if a.cmd == "verify-drift":
        from .provenance import render_drift_md, source_hash, verify
        exports = None
        if a.exports:
            from .parser import parse_export_dir
            folders = parse_export_dir(a.exports) if a.exports.is_dir() else parse_export_all(a.exports)
            exports = {f.name: source_hash([Path(x) for x in f.source_files if Path(x).exists()]) for f in folders}
        rows = verify(a.project, exports)
        md = render_drift_md(rows)
        if a.md: a.md.write_text(md)
        print(md); return 0 if all(r.status in ("clean", "no-header") for r in rows) else 1
    if a.cmd == "fetch-public-corpus":
        from .corpus import fetch_public_corpus
        print(json.dumps(fetch_public_corpus(a.dest, a.limit), indent=1)); return 0
    if a.cmd == "gate":
        from . import discovery_gate as dg
        if a.what == "pack":
            from agents.discovery_gate.regulation import PACKS, render_pack_md
            for code in (a.regulation or sorted(PACKS)): print(render_pack_md(code, dg._load_config()))
            return 0
        if a.what == "questions":
            for q in dg.questions(a.scope, a.regulation):
                print(f"[{q['id']}] {'required' if q.get('required', True) else 'optional'}{'' if q.get('allow_default') else ', no default'}"
                      f"{' choices=' + '|'.join(q['choices']) if q.get('choices') else ''}\n    {q['q']}")
            return 0
        if not (a.answers and a.by and a.out): print("gate accept needs --answers, --by and --out", file=sys.stderr); return 2
        try:
            rec = dg.accept(a.scope, json.loads(a.answers.read_text(encoding="utf-8")), answered_by=a.by, regulations=a.regulation)
        except dg.GateError as e:
            print(str(e), file=sys.stderr); return 3
        print(f"accepted → {dg.write_answers(a.out, rec)}"); return 0
    if a.cmd == "convert":
        from . import discovery_gate as dg
        from .parser import parse_export_dir
        from .project import write_project
        gate_rec = None
        if a.gate:
            try: gate_rec = dg.load_answers(a.gate)
            except Exception as e: print(f"gate: {e}", file=sys.stderr); return 3
        elif dg.require_for_convert() and not a.no_gate:
            print("convert refused: no discovery-gate answers. Run `gate questions`, have a human answer, `gate accept --answers … --by … --out …`, "
                  "then pass --gate <dir>/.datapai/gate.json (or --no-gate for dev/bench; logged).", file=sys.stderr); return 3
        elif a.no_gate:
            sys.stderr.write("convert: --no-gate — project written WITHOUT human gate answers (dev/bench use only)\n")
        folders = parse_export_dir(a.input) if a.input.is_dir() else parse_export_all(a.input)
        summ = write_project(folders, a.out, project_name=a.project_name, profile=a.profile, iceberg=not a.no_iceberg, tier=a.tier,
                             glue_pyspark=(a.output == "dbt+glue-pyspark"), glue_target_db=a.glue_target_db)
        if gate_rec: dg.write_answers(a.out, gate_rec); summ["gate"] = {"answered_by": gate_rec["answered_by"], "answered_at": gate_rec["answered_at"]}
        else: summ["gate"] = {"answered_by": None, "no_gate": True}
        print(json.dumps(summ, indent=2)); return 0
    results = assess_export(a.input, report_html=a.html, report_md=a.md, results_json=a.json)
    print(render_assessment_md(results, inputs=[str(a.input)]))
    for out in (a.html, a.md, a.json):
        if out: print(f"  → wrote {out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
