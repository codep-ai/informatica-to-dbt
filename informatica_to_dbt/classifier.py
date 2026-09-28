"""
classifier — one mapping → {decision, reasons, features, transformation types, counts}. Deterministic, no LLM (Tier 1).

Decision:
    convert   every transformation instance is `convert`/`passthru` and no downgrading feature is present
    todo      convertible, but at least one instance/feature needs a TODO block (untranslated function, aggregator pass-through,
              lookup variant, sequence continuity, SQL override …); the model is still generated
    skip      at least one `skip` transformation or a structural blocker (mapplet instance, no target, invalid mapping)

The per-mapping dict is the `decision` entry of the Control-M result contract (`decisions[{job, job_type, lift, decision}]`),
so the existing coverage report renders it unchanged.
"""
from __future__ import annotations
import re
from collections import Counter
from typing import Any, Dict, List

from .model import Folder, Instance, Mapping, Transformation
from .registry import resolve_transformation

# Expression functions the MVP translator knows (expressions.py mirrors this list; keep them in sync).
TRANSLATED_FUNCTIONS = {
    "IIF", "DECODE", "ISNULL", "IS_NULL", "NVL", "TO_CHAR", "TO_DATE", "TO_INTEGER", "TO_DECIMAL", "TO_FLOAT", "TRUNC", "ROUND",
    "LTRIM", "RTRIM", "TRIM", "UPPER", "LOWER", "SUBSTR", "INSTR", "LENGTH", "CONCAT", "REPLACESTR", "REPLACECHR", "LPAD", "RPAD",
    "ABS", "CEIL", "FLOOR", "MOD", "POWER", "SQRT", "SYSDATE", "SYSTIMESTAMP", "ADD_TO_DATE", "DATE_DIFF", "GET_DATE_PART",
    "LAST_DAY", "DATE_COMPARE", "IS_DATE", "IS_NUMBER", "IS_SPACES", "IN", "GREATEST", "LEAST", "MD5", "SUM", "COUNT", "AVG",
    "MIN", "MAX", "FIRST", "LAST", "MEDIAN", "STDDEV", "VARIANCE", "AND", "OR", "NOT", "TRUE", "FALSE", "NULL", "ERROR", "ABORT",
    "SETVARIABLE", "SETMAXVARIABLE", "SETMINVARIABLE", "SETCOUNTVARIABLE", "REG_EXTRACT", "REG_MATCH", "REG_REPLACE", "CHR", "ASCII",
    "INITCAP", "REVERSE", "SOUNDEX", "METAPHONE", "CHOOSE", "INDEXOF", "IS_INTEGER", "MAKE_DATE_TIME", "SET_DATE_PART", "TO_BIGINT",
    "SIGN", "ISNUMERIC", "EXP", "LN", "LOG", "CUME", "COS", "SIN", "TAN", "RAND", "SYSTIMESTAMP", "TO_TIMESTAMP", "DATE_TRUNC",
}
_FUNC_RE = re.compile(r"\b([A-Z_][A-Z0-9_]*)\s*\(", re.I)
_FLAG_FUNCS = {"ERROR", "ABORT", "SETVARIABLE", "SETMAXVARIABLE", "SETMINVARIABLE", "SETCOUNTVARIABLE"}


def _functions_in(expr: str) -> List[str]:
    return [m.upper() for m in _FUNC_RE.findall(expr or "")]


def _features_of(inst: Instance, t: Transformation) -> List[str]:
    """Feature flags that change how (or whether) a transformation compiles. Names match registry.todo_when."""
    f: List[str] = []
    a = {k.lower(): (v or "") for k, v in {**t.attributes, **inst.attributes}.items()}
    ty = t.type.lower()
    if ty.startswith("source qualifier") or "source qualifier" in ty:
        if a.get("sql query", "").strip(): f.append("sql_override")
        if a.get("user defined join", "").strip() and len(re.findall(r"\bAND\b|\bOR\b", a["user defined join"], re.I)) > 2: f.append("user_defined_join_complex")
        if a.get("pre sql", "").strip() or a.get("post sql", "").strip(): f.append("pre_post_sql")
        if a.get("source filter", "").strip(): f.append("source_filter")
        if a.get("select distinct", "").upper() == "YES": f.append("select_distinct")
    if ty.startswith("lookup"):
        if not any(p.is_input for p in t.ports) or a.get("lookup policy on multiple match", "").lower().startswith("report"):
            pass
        if a.get("lookup sql override", "").strip(): f.append("lookup_sql_override")
        if a.get("dynamic lookup cache", "").upper() == "YES": f.append("lookup_dynamic_cache")
        if not a.get("lookup condition", "").strip(): f.append("lookup_no_condition")
        if a.get("lookup policy on multiple match", "").lower().startswith("use all"): f.append("lookup_return_all")
        if any(p.porttype.upper() == "RETURN" for p in t.ports) and not any(p.is_output and not p.porttype.upper() == "RETURN" for p in t.ports):
            f.append("lookup_unconnected")
        if a.get("lookup caching enabled", "").upper() == "NO": f.append("lookup_uncached")
    if ty == "aggregator":
        group = {p.name for p in t.ports if p.expressiontype.upper() == "GROUPBY"}
        for p in t.output_ports:
            if p.name in group: continue
            funcs = set(_functions_in(p.expression))
            if not funcs & {"SUM", "COUNT", "AVG", "MIN", "MAX", "FIRST", "LAST", "MEDIAN", "STDDEV", "VARIANCE", "PERCENTILE"}:
                f.append("aggregator_passthrough"); break
    if ty.startswith("sequence"):
        if t.reusable or a.get("number of cached values", "0").strip() not in ("", "0"): f.append("sequence_shared")
    if ty == "update strategy":
        # the strategy lives in the "Update Strategy Expression" attribute (ports only carry data)
        expr = (a.get("update strategy expression", "") + " " + " ".join(p.expression for p in t.ports if p.expression)).upper()
        for dd in ("DD_INSERT", "DD_UPDATE", "DD_DELETE", "DD_REJECT"):
            if dd in expr or {"DD_INSERT": "0", "DD_UPDATE": "1", "DD_DELETE": "2", "DD_REJECT": "3"}[dd] == expr.strip():
                f.append(dd.lower())
    for p in t.ports:
        if p.expression and ":LKP." in p.expression.upper():
            f.append("lookup_unconnected_call")             # :LKP.<name>(args) — an unconnected lookup used like a function
        if p.expression and re.search(r"\b(LAG|LEAD|MOVINGAVG|MOVINGSUM)\s*\(", p.expression, re.I):
            f.append("window_function")                     # row-relative functions → window functions in SQL (TODO block)
        if p.expression:
            for fn in _functions_in(p.expression):
                if fn in _FLAG_FUNCS: f.append("flow_control_function:" + fn)
                elif fn not in TRANSLATED_FUNCTIONS and fn not in {"DD_INSERT", "DD_UPDATE", "DD_DELETE", "DD_REJECT"}:
                    f.append("untranslated_function:" + fn)
    return sorted(set(f))


def _targets_keyed(m: Mapping, folder: Folder | None) -> bool:
    tdefs = {t.name: t for t in (folder.targets if folder else [])}
    for ti in m.target_instances:
        td = tdefs.get(ti.transformation_name or ti.name)
        if td is None or not any(p.keytype.upper() == "PRIMARY KEY" for p in td.fields): return False
    return bool(m.target_instances)


def classify_mapping(m: Mapping, folder: Folder | None = None) -> Dict[str, Any]:
    types: Counter = Counter()
    instance_rows: List[Dict[str, Any]] = []
    features: List[str] = []
    reasons: List[str] = []
    decision = "convert"
    if not m.is_valid:
        reasons.append("mapping marked ISVALID=NO in the repository"); decision = "skip"
    if not m.target_instances and not m.is_mapplet:
        reasons.append("no target instance"); decision = "skip"
    if not m.source_instances and not m.is_mapplet:
        reasons.append("no source instance"); decision = "skip"
    for inst in m.transformation_instances:
        t = inst.transformation
        ttype = inst.transformation_type or (t.type if t else "")
        spec = resolve_transformation(ttype)
        types[spec.name] += 1
        feats = _features_of(inst, t) if t else ["definition_missing"]
        if spec.family == "update_strategy" and set(feats) & {"dd_update", "dd_delete", "dd_reject"} and not _targets_keyed(m, folder):
            feats.append("update_strategy_no_key")          # the merge needs KEYTYPE="PRIMARY KEY" on the target definition
        if not ttype and t is None:
            spec = resolve_transformation("Mapplet") if inst.type.upper() == "MAPPLET" else spec
        features += feats
        status = spec.status
        downgraded = [x for x in feats if x.split(":")[0] in spec.todo_when or x.startswith(("untranslated_function", "flow_control_function", "window_function", "lookup_unconnected_call"))]
        if status == "convert" and downgraded: status = "todo"
        if status == "skip": reasons.append(f"{inst.name}: {spec.name} is not converted in the MVP ({spec.notes.split('.')[0]})")
        elif status == "todo": reasons.append(f"{inst.name}: {spec.name} needs a TODO ({', '.join(downgraded) or spec.notes.split('.')[0]})")
        if t is None: reasons.append(f"{inst.name}: transformation definition {inst.transformation_name!r} not found in the export"); status = "skip"
        instance_rows.append({"instance": inst.name, "type": spec.name, "family": spec.family, "status": status, "features": feats})
        if status == "skip": decision = "skip"
        elif status == "todo" and decision == "convert": decision = "todo"
    if any(i.type.upper() == "MAPPLET" for i in m.instances):
        reasons.append("instantiates a mapplet (not flattened in the MVP)"); decision = "skip"
    n_targets = len(m.target_instances)
    return {"job": m.name, "job_type": "/".join(sorted(types)) or ("mapplet" if m.is_mapplet else "empty"),
            "lift": "dbt" if decision in ("convert", "todo") else "native", "decision": decision,
            "reasons": reasons, "features": sorted(set(features)), "transformation_types": dict(types),
            "instances": instance_rows, "n_sources": len(m.source_instances), "n_targets": n_targets,
            "n_transformations": len(m.transformation_instances), "n_connectors": len(m.connectors),
            "n_variables": len(m.variables), "is_mapplet": m.is_mapplet}


def classify_folder(f: Folder) -> List[Dict[str, Any]]:
    return [classify_mapping(m, f) for m in f.mappings]
