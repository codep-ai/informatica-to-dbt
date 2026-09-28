"""
workflows — PowerCenter WORKFLOW → Airflow DAG, on the Control-M converter's renderer (same header, validator, rerun safety).

    Session task      → BashOperator `dbt build --select <models of the session's mapping>` (the models `convert` wrote)
    Command task      → BashOperator (each VALUEPAIR command, in EXECORDER)
    Decision / Assignment / Email / Event-Wait / Event-Raise / Timer / Control / Worklet
                      → EmptyOperator placeholder + TODO (the condition/text is kept in the task doc)
    WORKFLOWLINK      → dependency; a CONDITION other than "$task.Status = SUCCEEDED" is kept as a TODO comment on the edge;
                        "… = FAILED" → trigger_rule all_failed
    Disabled tasks    → rendered, but commented as disabled (kept so the graph stays complete)
    SCHEDULER         → schedule None (RUN ON DEMAND / CUSTOM) with the original schedule in the DAG doc; a simple daily/weekly
                        recurrence becomes a cron guess flagged TODO
Session pre/post SQL, connections and overrides are listed in the task doc; they are not executed (dbt hooks are the place).
"""
from __future__ import annotations
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from .model import Folder, Session, Workflow

try:  # reuse the Control-M renderer's DAG header + validator when the platform package is present
    from agents.controlm_converter.renderer import render_dag as _ctm_render_dag, _safe_id as _ctm_safe_id
    from agents.controlm_converter.validator import validate_dag_source
except Exception:  # standalone (public repo)
    _ctm_render_dag = None
    def _ctm_safe_id(s: str) -> str:
        s = re.sub(r"[^0-9a-zA-Z_]+", "_", s).strip("_").lower(); return s if s and not s[0].isdigit() else f"t_{s}"
    def validate_dag_source(src: str):
        try: compile(src, "<dag>", "exec"); return True, "compiled"
        except SyntaxError as exc: return False, str(exc)


def _sid(s: str) -> str:
    return _ctm_safe_id(s)


def _model_id(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name).strip("_").lower()
    return s if s and not s[0].isdigit() else f"t_{s}"


def _sessions_of(folder: Folder, wf: Workflow) -> Dict[str, Session]:
    by = {s.name: s for s in folder.sessions}
    for s in wf.sessions: by.setdefault(s.name, s)
    return by


def _models_for_mapping(folder: Folder, mapping_name: str, model_names: Optional[Dict[str, List[str]]]) -> List[str]:
    if model_names and mapping_name in model_names: return model_names[mapping_name]
    m = folder.mapping(mapping_name)
    return [_model_id(t.name) for t in m.target_instances] if m else []


def _schedule(wf: Workflow) -> Tuple[Optional[str], str]:
    st = (wf.scheduler.get("type") or "").upper()
    if not st or "DEMAND" in st: return None, f"PowerCenter scheduler: {st or 'none'} (run on demand)"
    freq = wf.scheduler.get("dailyfrequency.frequency", "") or wf.scheduler.get("recurring.frequency", "")
    return None, f"PowerCenter scheduler: {st} {json.dumps({k: v for k, v in wf.scheduler.items() if k != 'name'})} — TODO set the cron"


def render_workflow(folder: Folder, wf: Workflow, *, dbt_project_dir: str = "/opt/airflow/dbt/informatica_conversion",
                    dbt_profile: str = "", model_names: Optional[Dict[str, List[str]]] = None, dag_id_prefix: str = "infa_") -> Dict[str, Any]:
    """One workflow → {dag_id, source, ok, validation, tasks[], edges[], todos[]} (the Control-M result-dict shape)."""
    sessions = _sessions_of(folder, wf)
    tasks: List[Dict[str, Any]] = []; todos: List[str] = []
    ids: Dict[str, str] = {}
    for t in wf.tasks:
        tid = _sid(t.name); ids[t.name] = tid
        tt = t.task_type.lower()
        doc: List[str] = [f"# {t.name} — PowerCenter {t.task_type} task" + ("" if t.enabled else " (DISABLED in the workflow)")]
        if tt == "session":
            s = sessions.get(t.task_name or t.name)
            if s is None:
                doc.append("# TODO session definition not in export"); snippet = _empty(tid, doc); fqn = "airflow.operators.empty.EmptyOperator"
                todos.append(f"{t.name}: session definition missing from the export")
            else:
                models = _models_for_mapping(folder, s.mapping_name, model_names)
                sel = " ".join(models) if models else _model_id(s.mapping_name)
                for k in ("Pre SQL", "Post SQL"):
                    if s.attributes.get(k, "").strip(): doc.append(f"# TODO session {k}: {s.attributes[k].strip()[:160]} (use a dbt pre/post hook)"); todos.append(f"{t.name}: {k} present")
                if s.attributes.get("Parameter Filename", "").strip(): doc.append(f"# parameter file: {s.attributes['Parameter Filename'].strip()} → dbt vars")
                for inst, c in s.connections.items():
                    if c.get("connection"): doc.append(f"# connection {inst}: {c['connection']} ({c.get('connection_type', '')})")
                if not models:
                    todos.append(f"{t.name}: mapping {s.mapping_name} has no converted model (skipped or missing) — placeholder")
                    doc.append(f"# TODO mapping {s.mapping_name} was not converted (skipped or not in the export)")
                    snippet = _empty(tid, doc); fqn = "airflow.operators.empty.EmptyOperator"
                else:
                    cmd = f"dbt build --select {sel} --project-dir {dbt_project_dir}" + (f" --profile {dbt_profile}" if dbt_profile else "")
                    snippet = "\n".join(doc) + f"\n{tid} = BashOperator(\n    task_id={tid!r},\n    bash_command={json.dumps(cmd)},\n)"
                    fqn = "airflow.operators.bash.BashOperator"
        elif tt == "command":
            tdef = next((d for d in (wf.task_defs + folder.tasks) if d.name == (t.task_name or t.name)), None)
            cmds = [v for _, v in sorted((tdef.value_pairs if tdef else {}).items())] or ["echo TODO command text not in export"]
            snippet = "\n".join(doc) + f"\n{tid} = BashOperator(\n    task_id={tid!r},\n    bash_command={json.dumps(' && '.join(cmds))},\n)"
            fqn = "airflow.operators.bash.BashOperator"
        elif tt == "start":
            snippet = "\n".join(doc) + f"\n{tid} = EmptyOperator(task_id={tid!r})"; fqn = "airflow.operators.empty.EmptyOperator"
        else:
            tdef = next((d for d in (wf.task_defs + folder.tasks) if d.name == (t.task_name or t.name)), None)
            detail = (tdef.attributes if tdef else {}) or t.attributes
            for k, v in list(detail.items())[:4]: doc.append(f"#   {k}: {str(v)[:140]}")
            doc.append(f"# TODO {t.task_type} task has no dbt/Airflow equivalent in the MVP — placeholder")
            todos.append(f"{t.name}: {t.task_type} task rendered as a placeholder")
            snippet = _empty(tid, doc); fqn = "airflow.operators.empty.EmptyOperator"
        tasks.append({"task_id": tid, "snippet": snippet, "operator_fqn": fqn, "job_type": t.task_type.lower(), "cyclic": False, "enabled": t.enabled})
    edges: List[Tuple[str, str, str]] = []
    for l in wf.links:
        if l.from_task not in ids or l.to_task not in ids: continue
        rule = "all_success"; cond = (l.condition or "").strip()
        if cond and "FAILED" in cond.upper(): rule = "all_failed"
        elif cond and not re.fullmatch(r"\$\w+\.Status\s*=\s*SUCCEEDED", cond, re.I):
            todos.append(f"link {l.from_task} → {l.to_task}: condition '{cond}' kept as a TODO (Airflow needs a BranchOperator or a sensor)")
        edges.append((ids[l.from_task], ids[l.to_task], rule))
    schedule, sched_doc = _schedule(wf)
    dag_id = f"{dag_id_prefix}{_sid(wf.name)}"
    desc = f"PowerCenter workflow {wf.name} (folder {folder.name}). {sched_doc}. " + (wf.description or "")
    if _ctm_render_dag is not None:
        src = _ctm_render_dag(dag_id=dag_id, schedule=schedule, tasks=tasks, edges=edges, description=desc[:500],
                              tags=["informatica", "migration", _sid(folder.name)], variables_used=[], cross_folder_deps=[])
        src = src.replace("Control-M", "PowerCenter")
    else:
        src = _render_plain(dag_id, schedule, tasks, edges, desc)
    ok, validation = validate_dag_source(src)
    return {"folder": folder.name, "dag_id": dag_id, "workflow": wf.name, "n_jobs": len(tasks), "ok": ok, "validation": validation,
            "source": src, "tasks": tasks, "edges": edges, "todos": sorted(set(todos))}


def _empty(tid: str, doc: List[str]) -> str:
    return "\n".join(doc) + f"\n{tid} = EmptyOperator(task_id={tid!r})"


def _render_plain(dag_id: str, schedule: Optional[str], tasks: List[Dict[str, Any]], edges: List[Tuple[str, str, str]], desc: str) -> str:
    imports = sorted({t["operator_fqn"] for t in tasks})
    lines = ["# Generated by informatica_converter — PowerCenter workflow → Airflow DAG. Review every TODO before enabling.",
             "from datetime import datetime", "from airflow import DAG"]
    for fqn in imports:
        mod, _, cls = fqn.rpartition("."); lines.append(f"from {mod} import {cls}")
    lines += ["", f"with DAG(dag_id={dag_id!r}, schedule={schedule!r}, start_date=datetime(2026, 1, 1), catchup=False, doc_md={json.dumps(desc)}, tags=['informatica', 'migration']) as dag:", ""]
    for t in tasks: lines += ["    " + ln for ln in t["snippet"].splitlines()] + [""]
    for a, b, rule in edges:
        lines.append(f"    {a} >> {b}" + (f"  # trigger_rule={rule}" if rule != "all_success" else ""))
    return "\n".join(lines) + "\n"


def render_folder_workflows(folder: Folder, **kw) -> List[Dict[str, Any]]:
    return [render_workflow(folder, wf, **kw) for wf in folder.workflows]
