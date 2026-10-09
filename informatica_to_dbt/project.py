"""
project — write the dbt project the compiler produced: one model per PowerCenter target, `_sources.yml` per folder (one dbt
source per DBD / lookup connection, tables typed from the export), `_models.yml` (descriptions, not-null on primary keys,
unique on single-column keys), `dbt_project.yml` with the estate's Iceberg conventions, and a `CONVERSION.md` that lists
every TODO the compiler raised. Adapter-neutral on purpose: the project builds on whichever profile you point it at; dbt's
adapter renders the portable SQL and the cross-database macros.

    write_project(folders, out_dir, project_name="infa_sales_dw", profile="infa_sales_dw") → summary dict
"""
from __future__ import annotations
import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from .provenance import Provenance, run_started_at, source_hash
from .compiler import ModelOut, _id, compile_mapping
from .model import Folder
from .workflows import render_folder_workflows

_TYPE = {"number": "numeric", "decimal": "numeric", "bigint": "bigint", "integer": "integer", "int": "integer", "smallint": "integer",
         "double": "float", "float": "float", "real": "float", "varchar2": "string", "varchar": "string", "nvarchar2": "string",
         "nvarchar": "string", "char": "string", "nchar": "string", "string": "string", "text": "string", "clob": "string",
         "date": "timestamp", "date/time": "timestamp", "datetime": "timestamp", "timestamp": "timestamp", "raw": "binary"}


_SEED_TYPES = {"numeric": "numeric(38,6)", "bigint": "bigint", "integer": "integer", "float": "double precision", "string": "varchar",
               "timestamp": "timestamp", "binary": "varchar"}   # seed column_types are engine SQL types; these parse on every adapter we target


def _yml_type(dt: str) -> str:
    return _TYPE.get(re.sub(r"\(.*\)", "", (dt or "").strip().lower()), "string")


def _yaml_str(s: str) -> str:
    return '"' + s.replace("\\", "\\\\").replace('"', '\\"') + '"'


def convert_folder(folder: Folder, tier: int = 1, export_path: str = "") -> List[ModelOut]:
    """Tier 1: the deterministic compiler. Tier 2: the same, then the Claude-first modernizer agent resolves what it can of the
    TODOs of each mapping that has any (Tier 1 stays the floor; the agent never blocks)."""
    outs: List[ModelOut] = []
    agent = None
    if tier >= 2:
        try:
            from .modernizer_agent import InformaticaModernizerAgent
        except ImportError as exc:      # public mirror: Tier 2 (Claude agent) ships with the DATAP.AI platform only
            raise RuntimeError("Tier 2 needs the DATAP.AI platform package (agents.*) and ANTHROPIC_API_KEY") from exc
        agent = InformaticaModernizerAgent()
    for m in folder.mappings:
        t1 = compile_mapping(m, folder)
        if agent is not None and export_path and any(o.todos and o.decision != "skip" for o in t1):
            r = agent.modernize_mapping(export_path, m.name, folder)
            by = {d["name"]: d for d in r["models"]}
            for o in t1:
                d = by.get(o.name)
                if d and r.get("tier") == 2:
                    o.sql = d["sql"]; o.portable_sql = d["sql"]; o.todos = d["todos"]
                    if d.get("resolved"): o.todos = list(o.todos) + [f"resolved by Tier 2 agent: {x}" for x in d["resolved"]]
            for o in t1: o.agent = r.get("agent")  # type: ignore[attr-defined]
        outs.extend(t1)
    return outs


def write_project(folders: List[Folder], out_dir: Path, project_name: str = "informatica_conversion", profile: Optional[str] = None,
                  materialized_default: str = "table", iceberg: bool = True, dbt_project_dir: Optional[str] = None, tier: int = 1,
                  rendered_at: Optional[str] = None) -> Dict[str, Any]:
    out_dir = Path(out_dir); models_root = out_dir / "models"
    rendered_at = rendered_at or run_started_at()                       # one run → one timestamp on every artefact (ADOP determinism rule)
    summary: Dict[str, Any] = {"project": project_name, "folders": [], "models": 0, "skipped": 0, "todos": 0, "rendered_at": rendered_at}
    all_hashes: List[str] = []

    def emit(path: Path, body: str, folder_name: str, obj: str, template_id: str, shash: str) -> None:
        """Write an artefact with its provenance header (see provenance.py); the header's content_hash is over `body`."""
        path.write_text(Provenance(source_hash=shash, object=f"{folder_name}/{obj}", template_id=template_id, rendered_at=rendered_at)
                        .render(body, "".join(path.suffixes[-1:]) or path.suffix), encoding="utf-8")

    for f in folders:
        fdir = models_root / _id(f.name); fdir.mkdir(parents=True, exist_ok=True)
        shash = source_hash([Path(x) for x in f.source_files if Path(x).exists()]) if f.source_files else "unknown"
        all_hashes.append(shash)
        outs = convert_folder(f, tier=tier, export_path=(f.source_files[0] if f.source_files else ""))
        # several mappings loading the same target → model per (mapping, target), with a note; one mapping → model per target
        by_target = Counter(o.target for o in outs)
        for o in outs:
            if by_target[o.target] > 1:
                o.name = f"{_id(o.mapping)}__{_id(o.target)}"
                o.todos = sorted(set(o.todos) | {f"target {o.target} is loaded by {by_target[o.target]} mappings — union or sequence them (one model per mapping generated)"})
        src_tables: Dict[str, Dict[str, Dict[str, Any]]] = defaultdict(dict)     # source_name → table → {identifier, columns}
        sdefs = {s.name: s for s in f.sources}
        model_yml: List[str] = ["version: 2", "", "models:"]
        conv_md: List[str] = [f"# Conversion notes · folder {f.name}", ""]
        n_todo = 0
        for o in outs:
            for s in o.sources:
                sd = sdefs.get(s["identifier"])
                src_tables[s["source_name"]][s["table"]] = {"identifier": s["identifier"], "dbd": s["dbd"],
                                                            "columns": [(p.name, p.datatype, p.keytype) for p in sd.fields] if sd else list(s.get("columns") or [])}
            if o.decision == "skip":
                summary["skipped"] += 1
                emit(fdir / f"{o.name}.sql.skipped", "-- NOT CONVERTED (needs review)\n" + "\n".join(f"-- {t}" for t in o.todos) + "\n",
                     f.name, f"{o.mapping}/{o.target}", "informatica.model_skipped", shash)
                conv_md += [f"## {o.name} — SKIPPED", ""] + [f"- {t}" for t in o.todos] + [""]
                continue
            tier_note = f"; Tier 2 agent: {getattr(o, 'agent', {}).get('status', 'n/a') if getattr(o, 'agent', None) else 'not run'}" if tier >= 2 else ""
            header = [f"-- {o.name}: converted from PowerCenter target {o.target} (mapping decision: {o.decision}{tier_note})",
                      "-- Generated by informatica_converter; review every TODO below before enabling."]
            if o.todos: header += ["-- TODO:"] + [f"--   - {t}" for t in o.todos]
            emit(fdir / f"{o.name}.sql", "\n".join(header) + "\n" + o.sql, f.name, f"{o.mapping}/{o.target}",
                 "informatica.model_tier2" if getattr(o, "agent", None) else "informatica.model", shash)
            summary["models"] += 1; n_todo += len(o.todos)
            model_yml += [f"  - name: {o.name}", f"    description: {_yaml_str('Converted from PowerCenter target ' + o.target + '. Decision: ' + o.decision + '.')}"]
            if o.columns:
                model_yml.append("    columns:")
                single_pk = [c for c in o.columns if c["keytype"].upper() == "PRIMARY KEY"]
                for c in o.columns:
                    tests = []
                    if c["keytype"].upper() == "PRIMARY KEY": tests.append("not_null")
                    if len(single_pk) == 1 and c is single_pk[0]: tests.append("unique")
                    line = f"      - name: {c['name']}"
                    if c["datatype"]: line += f"\n        data_type: {_yml_type(c['datatype'])}"
                    if tests: line += "\n        tests: [" + ", ".join(tests) + "]"
                    model_yml.append(line)
            if o.todos: conv_md += [f"## {o.name}", ""] + [f"- {t}" for t in o.todos] + [""]
        # sources
        sy: List[str] = ["version: 2", "", "sources:"]
        for sname, tables in sorted(src_tables.items()):
            sy += [f"  - name: {sname}", f"    description: {_yaml_str('PowerCenter source system (DBD) ' + (next(iter(tables.values()))['dbd'] or sname))}",
                   "    schema: \"{{ var('" + sname + "_schema', '" + sname + "') }}\"", "    tables:"]
            for tname, t in sorted(tables.items()):
                sy += [f"      - name: {tname}", f"        identifier: \"{{{{ var('{sname}_{tname}_identifier', '{t['identifier'].lower()}') }}}}\""]
                if t["columns"]:
                    sy.append("        columns:")
                    for (cn, dt, kt) in t["columns"]:
                        # a dbt source column is the PHYSICAL column: plain identifiers lower-cased, anything else exactly as the source defines it
                        phys = cn.lower() if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cn) else cn
                        sy.append(f"          - name: {json.dumps(phys)}" + (f"\n            data_type: {_yml_type(dt)}" if dt else ""))
        emit(fdir / "_sources.yml", "\n".join(sy) + "\n", f.name, "_sources", "informatica.sources", shash)
        emit(fdir / "_models.yml", "\n".join(model_yml) + "\n", f.name, "_models", "informatica.models", shash)
        # flat-file lookups → dbt seeds (Donny 2026-10-07): header + typed properties written; the engineer copies the real file over
        seeds: Dict[str, Dict[str, Any]] = {}
        for o in outs:
            for sd in getattr(o, "seeds", []) or []: seeds.setdefault(sd["name"], sd)
        if seeds:
            seed_dir = out_dir / "seeds" / _id(f.name); seed_dir.mkdir(parents=True, exist_ok=True)
            sdy: List[str] = ["version: 2", "", "seeds:"]
            for name, sd in sorted(seeds.items()):
                cols = [(_id(cn), _yml_type(dt)) for cn, dt in sd["columns"]]
                csv = seed_dir / f"{name}.csv"
                if not csv.exists():                                   # never overwrite a file the engineer has already dropped in
                    csv.write_text(",".join(c for c, _ in cols) + "\n", encoding="utf-8")
                sdy += [f"  - name: {name}", f"    description: {_yaml_str('PowerCenter flat-file lookup ' + sd['instance'] + ' — replace the header-only CSV with the lookup file (session attribute Lookup source file name)')}",
                        "    config:", "      column_types:"] + [f"        {c}: {_SEED_TYPES.get(t, 'varchar')}" for c, t in cols]
                conv_md += [f"## seed {name}", "", f"- flat-file lookup `{sd['instance']}`: copy the lookup file to `seeds/{_id(f.name)}/{name}.csv` (header written; columns {', '.join(c for c, _ in cols)})", ""]
            emit(seed_dir / "_seeds.yml", "\n".join(sdy) + "\n", f.name, "_seeds", "informatica.seeds", shash)
            summary["seeds"] = summary.get("seeds", 0) + len(seeds)
        # workflows → Airflow DAGs (airflow/<dag_id>.py), sessions select the models their mapping produced
        model_names: Dict[str, List[str]] = defaultdict(list)
        for o in outs:
            if o.decision != "skip": model_names[o.mapping].append(o.name)
        dag_dir = out_dir / "airflow"; dags: List[Dict[str, Any]] = []
        for r in render_folder_workflows(f, model_names=dict(model_names), dbt_project_dir=dbt_project_dir or f"/opt/airflow/dbt/{project_name}", dbt_profile=profile or ""):
            dag_dir.mkdir(parents=True, exist_ok=True)
            emit(dag_dir / f"{r['dag_id']}.py", r["source"], f.name, f"workflow/{r['workflow']}", "informatica.airflow_dag", shash)
            dags.append({"dag_id": r["dag_id"], "workflow": r["workflow"], "tasks": r["n_jobs"], "ok": r["ok"], "todos": r["todos"]})
            if r["todos"]: conv_md += [f"## workflow {r['workflow']} → {r['dag_id']}.py", ""] + [f"- {t}" for t in r["todos"]] + [""]
            n_todo += len(r["todos"])
        emit(fdir / "CONVERSION.md", "\n".join(conv_md) + "\n", f.name, "CONVERSION", "informatica.conversion_notes", shash)
        summary["folders"].append({"folder": f.name, "models": len([o for o in outs if o.decision != "skip"]), "skipped": len([o for o in outs if o.decision == "skip"]),
                                   "dags": dags, "todos": n_todo})
        summary["todos"] += n_todo; summary["dags"] = summary.get("dags", 0) + len(dags)
    prof = profile or project_name
    ice = "\n      +table_type: iceberg\n      +format: parquet" if iceberg else ""
    import hashlib as _hl
    project_hash = _hl.sha256("\n".join(sorted(all_hashes)).encode()).hexdigest()
    (out_dir / "dbt_project.yml").write_text(Provenance(source_hash=project_hash, object="project/dbt_project", template_id="informatica.dbt_project",
                                                        rendered_at=rendered_at).render(f"""# Generated by informatica_converter — Informatica PowerCenter → dbt. One model per PowerCenter target; sources per DBD.
# Adapter-neutral: point `profile` at Athena / Snowflake / Databricks / ClickHouse / DuckDB and dbt renders the portable SQL.
name: {project_name}
version: "0.1.0"
config-version: 2
profile: {prof}
model-paths: ["models"]
seed-paths: ["seeds"]
target-path: "target"
clean-targets: ["target", "dbt_packages"]
require-dbt-version: [">=1.7.0", "<2.0.0"]

vars:
  LOAD_FROM: "1900-01-01"          # PowerCenter mapping parameters ($$X) become dbt vars; set them per environment

models:
  {project_name}:
    +materialized: {materialized_default}
    +tags: ["informatica_conversion", "role_consumer"]{ice}
""", ".yml"), encoding="utf-8")
    (out_dir / "README.md").write_text(f"# {project_name}\n\nGenerated from Informatica PowerCenter exports by `informatica_converter`.\n"
                                       f"Models: {summary['models']} · skipped: {summary['skipped']} · Airflow DAGs: {summary.get('dags', 0)} (airflow/) · TODOs: {summary['todos']} (see models/*/CONVERSION.md).\n\n"
                                       "    dbt deps && dbt build\n", encoding="utf-8")
    return summary
