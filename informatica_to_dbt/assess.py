"""
assess — the week-1 product: a directory of exports in, one result dict per folder out, plus the corpus-completeness findings
phData's corpus guide describes (mappings referenced by workflows but missing; mappings no workflow runs), rendered with the
Control-M coverage report so the two migrations read the same way.

Result contract (per folder) = controlm_converter's, so `report.render_html_report` / `render_coverage_md` work unchanged:
    {folder, dag_id, n_jobs, ok, validation, path, source, metadata{...}, decisions[...], lift_summary{dbt, dlt, native}}
Here: n_jobs = mappings, decisions = one per mapping (classifier), lift dbt = convert+todo, native = skip.
"""
from __future__ import annotations
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .classifier import classify_folder
from .model import Folder
from .parser import parse_export_all, parse_export_dir


def _workflow_findings(f: Folder) -> Dict[str, Any]:
    """Which mappings are scheduled (a session in a workflow runs them), which are referenced but missing, which are orphans."""
    mapping_names = {m.name for m in f.mappings}
    session_by_name = {s.name: s for s in f.sessions}
    for w in f.workflows:
        for s in w.sessions:
            session_by_name.setdefault(s.name, s)
    scheduled: Dict[str, List[str]] = {}
    missing: List[Dict[str, str]] = []
    for w in f.workflows:
        for t in w.session_tasks:
            s = session_by_name.get(t.task_name or t.name)
            if s is None:
                missing.append({"workflow": w.name, "session": t.task_name or t.name, "mapping": "", "why": "session definition not in export"}); continue
            if s.mapping_name in mapping_names:
                scheduled.setdefault(s.mapping_name, []).append(w.name)
            else:
                missing.append({"workflow": w.name, "session": s.name, "mapping": s.mapping_name, "why": "mapping not in export"})
    orphans = sorted(mapping_names - set(scheduled))
    return {"scheduled": scheduled, "missing": missing, "orphans": orphans,
            "n_workflows": len(f.workflows), "n_sessions": len(session_by_name),
            "task_types": dict(Counter(t.task_type for w in f.workflows for t in w.tasks))}


def assess_folder(f: Folder) -> Dict[str, Any]:
    decisions = classify_folder(f)
    n = Counter(d["decision"] for d in decisions)
    wf = _workflow_findings(f)
    types: Counter = Counter()
    for d in decisions:
        types.update(d["transformation_types"])
    features: Counter = Counter(x for d in decisions for x in d["features"])
    return {
        "folder": f.name, "dag_id": f"infa_{f.name.lower()}", "n_jobs": len(f.mappings), "ok": True, "validation": "assessment only",
        "path": "", "source": "powrmart-xml", "repository": f.repository, "repository_version": f.repository_version,
        "source_files": f.source_files,
        "metadata": {"n_sources": len(f.sources), "n_targets": len(f.targets), "n_reusable_transformations": len(f.transformations),
                     "n_mapplets": len(f.mapplets), "n_sessions": wf["n_sessions"], "n_workflows": wf["n_workflows"],
                     "n_not_ok_edges": 0, "n_cross_folder_deps": 0, "n_cyclic_jobs": 0, "n_resources": 0, "subapps": [],
                     "variables_used": sorted({v.name for m in f.mappings for v in m.variables}),
                     "workflow_task_types": wf["task_types"], "scheduled_mappings": len(wf["scheduled"]),
                     "missing_mappings": wf["missing"], "orphan_mappings": wf["orphans"],
                     "transformation_types": dict(types.most_common()), "features": dict(features.most_common())},
        "decisions": decisions,
        "lift_summary": {"dbt": n["convert"] + n["todo"], "dlt": 0, "native": n["skip"], "convert": n["convert"], "todo": n["todo"], "skip": n["skip"]},
    }


def assess_export(payload: Union[str, Path], report_html: Optional[Union[str, Path]] = None,
                  report_md: Optional[Union[str, Path]] = None, results_json: Optional[Union[str, Path]] = None) -> List[Dict[str, Any]]:
    """A file or a directory of exports → results (one per folder). Optionally writes the HTML/Markdown report and a JSON dump."""
    p = Path(payload)
    folders = parse_export_dir(p) if p.is_dir() else parse_export_all(p)
    results = [assess_folder(f) for f in folders]
    if results_json:
        Path(results_json).write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    if report_html:
        Path(report_html).write_text(render_assessment_html(results, inputs=[str(p)]), encoding="utf-8")
    if report_md:
        Path(report_md).write_text(render_assessment_md(results, inputs=[str(p)]), encoding="utf-8")
    return results


def _totals(results: List[Dict[str, Any]]) -> Dict[str, int]:
    t = Counter()
    for r in results:
        t.update({k: r["lift_summary"].get(k, 0) for k in ("convert", "todo", "skip")}); t["mappings"] += r["n_jobs"]
        t["workflows"] += r["metadata"]["n_workflows"]; t["sessions"] += r["metadata"]["n_sessions"]
        t["missing"] += len(r["metadata"]["missing_mappings"]); t["orphans"] += len(r["metadata"]["orphan_mappings"])
    return dict(t)


def render_assessment_md(results: List[Dict[str, Any]], inputs: Optional[List[str]] = None) -> str:
    """Markdown assessment: totals, per-folder table, transformation-type coverage, features, findings, per-mapping decisions."""
    t = _totals(results)
    pct = lambda a, b: f"{(100.0 * a / b):.0f}%" if b else "n/a"
    out = ["# Informatica PowerCenter → dbt · assessment", "",
           f"Inputs: {', '.join(inputs or [])}", "",
           "| | Count | Share |", "|---|---:|---:|",
           f"| Mappings | {t.get('mappings', 0)} | |",
           f"| Convertible as-is (Tier 1) | {t.get('convert', 0)} | {pct(t.get('convert', 0), t.get('mappings', 0))} |",
           f"| Convertible with TODOs | {t.get('todo', 0)} | {pct(t.get('todo', 0), t.get('mappings', 0))} |",
           f"| Needs review (skipped in MVP) | {t.get('skip', 0)} | {pct(t.get('skip', 0), t.get('mappings', 0))} |",
           f"| Workflows / sessions | {t.get('workflows', 0)} / {t.get('sessions', 0)} | |",
           f"| Mappings referenced but missing from export | {t.get('missing', 0)} | |",
           f"| Mappings no workflow runs (orphans) | {t.get('orphans', 0)} | |", ""]
    types: Counter = Counter(); feats: Counter = Counter()
    for r in results:
        types.update(r["metadata"]["transformation_types"]); feats.update(r["metadata"]["features"])
    if types:
        out += ["## Transformation types in the corpus", "", "| Type | Instances | MVP status |", "|---|---:|---|"]
        from .registry import TRANSFORMATION_REGISTRY
        for ty, n in types.most_common():
            out.append(f"| {ty} | {n} | {TRANSFORMATION_REGISTRY.get(ty).status if ty in TRANSFORMATION_REGISTRY else 'other'} |")
        out.append("")
    if feats:
        out += ["## Features that change the conversion", "", "| Feature | Mappings |", "|---|---:|"]
        out += [f"| {k} | {v} |" for k, v in feats.most_common()]; out.append("")
    for r in results:
        m = r["metadata"]
        out += [f"## Folder `{r['folder']}`", "",
                f"{r['n_jobs']} mappings · {m['n_sources']} sources · {m['n_targets']} targets · {m['n_reusable_transformations']} reusable transformations · "
                f"{m['n_mapplets']} mapplets · {m['n_sessions']} sessions · {m['n_workflows']} workflows", ""]
        if m["missing_mappings"]:
            out += ["**Referenced but missing (export incomplete):** " + "; ".join(f"{x['workflow']} → {x['session']} → {x['mapping'] or '?'} ({x['why']})" for x in m["missing_mappings"]), ""]
        if m["orphan_mappings"]:
            out += ["**No workflow runs them (dead code, or workflow XML not exported):** " + ", ".join(m["orphan_mappings"]), ""]
        out += ["| Mapping | Decision | Transformations | Sources → Targets | Reasons |", "|---|---|---|---|---|"]
        for d in r["decisions"]:
            out.append(f"| {d['job']} | {d['decision']} | {', '.join(f'{k}×{v}' for k, v in d['transformation_types'].items())} | "
                       f"{d['n_sources']} → {d['n_targets']} | {'; '.join(d['reasons'])[:300]} |")
        out.append("")
    return "\n".join(out)


def render_assessment_html(results: List[Dict[str, Any]], inputs: Optional[List[str]] = None) -> str:
    """The Control-M HTML coverage report over the same result dicts (same look for both migrations), with the assessment
    Markdown embedded as a preformatted appendix so nothing Informatica-specific is lost."""
    try:
        from informatica_to_dbt._controlm_report_shim import render_html_report
        html = render_html_report(results, inputs=inputs or [], tier=2, run_meta={"customer": "assessment", "source": "Informatica PowerCenter"})
        html = html.replace("Control-M → Airflow", "Informatica PowerCenter → dbt").replace("Control-M Folders", "PowerCenter Folders").replace("Control-M", "PowerCenter")
    except Exception:
        html = "<html><body>"
    import html as _h
    appendix = "<section style='font-family:ui-monospace,monospace;font-size:13px;white-space:pre-wrap;padding:24px'>" + _h.escape(render_assessment_md(results, inputs)) + "</section>"
    return html.replace("</body>", appendix + "</body>") if "</body>" in html else html + appendix + "</body></html>"
