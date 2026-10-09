"""
glue_pyspark — OPTIONAL second output: one AWS Glue 4.0 PySpark job per compiled model, from the same plan the dbt writer uses
(ADOP plan W6; Donny 2026-10-08: "dbt is the default outcome, Glue PySpark optional"). Nothing is compiled twice: the renderer
takes the dbt model text the compiler produced and resolves its Jinja into Spark SQL —

    {{ source('s','t') }}        → <source_db>.t        (Glue catalog; db from --glue-source-db / vars, default = the dbt source name)
    {{ ref('m') }}               → <target_db>.m
    {{ this }}                   → <target_db>.<model>
    {{ var('X', 'd') }}          → job argument --X (default d)       {{ config(...) }} → dropped (materialization → write mode)
    {{ dbt.<macro>(...) }}       → Spark SQL (dateadd, datediff, date_trunc, safe_cast→try_cast, hash→sha2, concat, length,
                                   position→locate, replace, last_day, current_timestamp, type_* → Spark types)
    {% if is_incremental() %}    → the incremental branch when the target table exists, else the full branch
    {% set x = … %}              → substituted

then `sqlglot` normalises the result to the Spark dialect. The job writes an Iceberg table through the Glue catalog
(`glue_catalog.<db>.<table>`), `createOrReplace` for table models and a MERGE for incremental ones, and carries the same
provenance header as every other artefact. A `jobs.json` manifest lists what to create with `aws glue create-job`.

Bench: `tests/test_informatica_glue_pyspark.py` compiles every job (py_compile), parses every SQL with sqlglot(spark) and — when
pyspark is installed — runs each statement's plan against empty typed tables in a local Spark session. Glue itself is not run.
"""
from __future__ import annotations
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .provenance import Provenance

_TYPES = {"type_string": "string", "type_int": "int", "type_bigint": "bigint", "type_float": "double", "type_numeric": "decimal(38,6)",
          "type_timestamp": "timestamp", "type_boolean": "boolean"}


def _split_args(s: str) -> List[str]:
    out, depth, cur, q = [], 0, "", None
    for ch in s:
        if q:
            cur += ch
            if ch == q: q = None
            continue
        if ch in ("'", '"'): q = ch; cur += ch; continue
        if ch in "([": depth += 1
        if ch in ")]": depth -= 1
        if ch == "," and depth == 0: out.append(cur.strip()); cur = ""; continue
        cur += ch
    if cur.strip(): out.append(cur.strip())
    return out


def _unq(s: str) -> str:
    s = s.strip()
    return s[1:-1] if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"" else s


def _macro(name: str, args: List[str]) -> str:
    a = args
    if name == "dateadd":          # dateadd(datepart, interval, from_date)
        part, n, d = _unq(a[0]).lower(), a[1], a[2]
        return f"({d} + INTERVAL {n} {part})" if part in ("day", "month", "year", "hour", "minute", "second", "week") else f"date_add({d}, {n})"
    if name == "datediff":         # datediff(first, second, datepart)
        f, s, part = a[0], a[1], _unq(a[2]).lower()
        return {"day": f"datediff({s}, {f})", "month": f"months_between({s}, {f})", "year": f"(year({s}) - year({f}))",
                "hour": f"((unix_timestamp({s}) - unix_timestamp({f})) / 3600)", "minute": f"((unix_timestamp({s}) - unix_timestamp({f})) / 60)",
                "second": f"(unix_timestamp({s}) - unix_timestamp({f}))"}.get(part, f"datediff({s}, {f})")
    if name == "date_trunc": return f"date_trunc('{_unq(a[0])}', {a[1]})"
    if name == "safe_cast": return f"try_cast({a[0]} as {_unq(a[1])})"
    if name == "hash": return f"sha2(cast({a[0]} as string), 256)"
    if name == "concat": return "concat(" + ", ".join(a) + ")"
    if name == "length": return f"length({a[0]})"
    if name == "position": return f"locate({a[0]}, {a[1]})"
    if name == "replace": return f"replace({a[0]}, {a[1]}, {a[2]})"
    if name == "last_day": return f"last_day({a[0]})"
    if name == "current_timestamp": return "current_timestamp()"
    if name in _TYPES: return _TYPES[name]
    raise ValueError(f"dbt macro {name} has no Spark rendering")


def _unescape_jinja_literal(lit: str) -> str:
    q = lit[0]; body = lit[1:-1]
    return body.replace("\\" + q, q).replace("\\\\", "\\")


_TOK = re.compile(r"""'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"|~|[^~'"]+""", re.S)


def _jinja_concat(expr: str) -> str:
    """Evaluate Jinja `~` string concatenation inside one expression: '\'' ~ var('X') ~ '\'' → '${X}'; "a" ~ _str ~ "b" → astringb.
    A quoted token adjacent to a `~` is a Jinja string literal (SQL text); a quoted token with no `~` neighbour is SQL and kept."""
    if "~" not in expr:
        return expr
    toks = [t for t in _TOK.findall(expr) if t.strip() != ""]      # whitespace between a literal and `~` is not a token
    out: List[str] = []
    for i, t in enumerate(toks):
        if t == "~": continue
        near = (i > 0 and toks[i - 1] == "~") or (i + 1 < len(toks) and toks[i + 1] == "~")
        if near and t and t[0] in "'\"":
            out.append(_unescape_jinja_literal(t))
        else:
            out.append(t.strip() if near else t)
    return "".join(out)


def _args_to_sql(raw: str) -> List[str]:
    """Macro arguments as the dbt template wrote them → SQL text (Jinja double-quoted literals are SQL fragments)."""
    out = []
    for x in _split_args(raw):
        x = _jinja_concat(x.strip())
        if len(x) >= 2 and x[0] == '"' and x[-1] == '"':
            x = _unescape_jinja_literal(x)
        out.append(x)
    return out


def _innermost_call(text: str):
    """(start, end, name, args_text) of a dbt.<name>(...) call whose arguments contain no further dbt. call; None if none."""
    best = None
    for m in re.finditer(r"dbt\.([a-z_]+)\(", text):
        depth, i = 1, m.end()
        while i < len(text) and depth:
            if text[i] == "(": depth += 1
            elif text[i] == ")": depth -= 1
            i += 1
        args = text[m.end(): i - 1]
        if "dbt." not in args:
            best = (m.start(), i, m.group(1), args); break
    return best


def _resolve_expr(expr: str) -> str:
    """One Jinja expression body: var()/this handled by the caller; here `~` concatenation, _str, nested dbt macros."""
    expr = re.sub(r"\bvar\(\s*'([^']+)'(?:\s*,\s*[^)]*)?\)", lambda m: "${" + m.group(1) + "}", expr)
    expr = re.sub(r"\b_str\b", "string", expr)
    for _ in range(40):
        c = _innermost_call(expr)
        if not c: break
        st, en, name, raw = c
        if name == "concat" and raw.strip().startswith("["):
            repl = "concat(" + ", ".join(_args_to_sql(raw.strip()[1:-1])) + ")"
        else:
            repl = _macro(name, _args_to_sql(raw) if raw.strip() else [])
        expr = expr[:st] + repl + expr[en:]
    return _jinja_concat(expr)


def _resolve_macros(sql: str) -> str:
    """Every {{ dbt.… }} expression → Spark SQL."""
    pat = re.compile(r"\{\{\s*(dbt\.[a-z_]+\(.*?\))\s*\}\}", re.S)
    sql = pat.sub(lambda m: _resolve_expr(m.group(1)), sql)
    if "{{ dbt." in sql: raise ValueError("unresolved dbt macro: " + sql[sql.index("{{ dbt."):][:80])
    return sql


def render_job(model_name: str, dbt_sql: str, *, target_db: str, source_dbs: Optional[Dict[str, str]] = None, materialization: str = "table",
               unique_key: Optional[List[str]] = None, target_exists_default: bool = False) -> Tuple[str, List[str]]:
    """(spark_sql, job_args) — the Spark SQL for the model, and the var() names it needs as job arguments."""
    sql = dbt_sql
    sql = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", sql, flags=re.S)
    sql = re.sub(r"\{%-?\s*set\s+_str\s*=.*?%\}", "", sql, flags=re.S)
    # incremental template: keep ONE branch; the job decides at runtime which text to run (both rendered)
    inc = re.search(r"\{%-?\s*if is_incremental\(\)\s*-?%\}(.*?)\{%-?\s*else\s*-?%\}(.*?)\{%-?\s*endif\s*-?%\}", sql, re.S)
    branches: Dict[str, str] = {}
    if inc:
        head = sql[: inc.start()]
        branches = {"incremental": head + inc.group(1), "full": head + inc.group(2)}
    else:
        branches = {"full": sql}
    args: List[str] = []
    out: Dict[str, str] = {}
    for k, text in branches.items():
        text = re.sub(r"\{%-?\s*if var\('infa_soft_delete_missing', true\)\s*-?%\}(.*?)\{%-?\s*endif\s*-?%\}", r"\1", text, flags=re.S)
        text = re.sub(r"\{\{\s*source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}", lambda m: f"{(source_dbs or {}).get(m.group(1), m.group(1))}.{m.group(2)}", text)
        text = re.sub(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}", lambda m: f"{target_db}.{m.group(1)}", text)
        text = re.sub(r"\{\{\s*this\s*\}\}", f"{target_db}.{model_name}", text)
        text = re.sub(r"\{\{\s*adapter\.quote\(\s*'([^']+)'\s*\)\s*\}\}", lambda m: f"`{m.group(1)}`", text)
        def _var(m):
            name = m.group(1); args.append(name) if name not in args else None
            return "${" + name + "}"
        text = re.sub(r"\{\{\s*var\(\s*'([^']+)'(?:\s*,\s*[^)]*)?\)\s*\}\}", _var, text)
        text = _resolve_macros(text)
        text = re.sub(r"\{\{\s*_str\s*\}\}", "string", text)
        if "{{" in text or "{%" in text:
            raise ValueError("unresolved Jinja in model " + model_name + ": " + text[text.index("{"):][:80])
        try:
            import sqlglot
            text = sqlglot.transpile(text, read="spark", write="spark", pretty=True)[0]
        except Exception:  # noqa: BLE001  keep the text; the bench will tell
            pass
        out[k] = text.strip()
    # a dbt model is one SELECT; the job wraps it
    return json.dumps(out), args


_JOB = '''"""
Glue 4.0 PySpark job for model {model} — generated by informatica_converter (optional Glue output; dbt is the default).
Reads the Glue Data Catalog, writes Iceberg via the glue_catalog. Job arguments: {args}
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

TARGET_DB = "{target_db}"
MODEL = "{model}"
MATERIALIZATION = "{materialization}"
UNIQUE_KEY = {unique_key}
SQL = {sql_json}

spark = (SparkSession.builder.appName(f"infa_{{MODEL}}")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
         .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
         .config("spark.sql.catalog.glue_catalog.warehouse", [a.split("=", 1)[1] for a in sys.argv if a.startswith("--iceberg_warehouse=")][0]
                 if any(a.startswith("--iceberg_warehouse=") for a in sys.argv) else sys.argv[sys.argv.index("--iceberg_warehouse") + 1])
         .getOrCreate())
opts = getResolvedOptions(sys.argv, ["JOB_NAME", "iceberg_warehouse"] + {arg_names})
for k in {arg_names}:                                   # only the declared dbt vars; Glue passes other args (and None values) we must not touch
    v = opts.get(k)
    if v is None: continue
    for key in SQL: SQL[key] = SQL[key].replace("${{" + k + "}}", str(v))

target = f"glue_catalog.{{TARGET_DB}}.{{MODEL}}"
exists = spark.catalog.tableExists(target)
branch = "incremental" if (exists and "incremental" in SQL) else "full"
df = spark.sql(SQL[branch])

if MATERIALIZATION == "incremental" and exists and UNIQUE_KEY:
    df.createOrReplaceTempView("_src")
    on = " AND ".join(f"t.{{k}} = s.{{k}}" for k in UNIQUE_KEY)
    spark.sql(f"MERGE INTO {{target}} t USING _src s ON {{on}} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
else:
    df.writeTo(target).using("iceberg").createOrReplace()
print(f"{{MODEL}}: {{branch}} → {{target}}")
'''


def write_glue_jobs(outs: List[Any], out_dir: Path, *, target_db: str, source_dbs: Optional[Dict[str, str]] = None, rendered_at: str,
                    source_hash: str, folder: str) -> Dict[str, Any]:
    """Write glue_jobs/<model>.py per compiled model and a jobs.json manifest. Returns {jobs, failed:[{model, error}]}."""
    jdir = Path(out_dir) / "glue_jobs"; jdir.mkdir(parents=True, exist_ok=True)
    manifest: List[Dict[str, Any]] = []; failed: List[Dict[str, str]] = []
    for o in outs:
        if getattr(o, "decision", "") == "skip" or not getattr(o, "sql", ""):
            continue
        try:
            sql_json, args = render_job(o.name, o.sql, target_db=target_db, source_dbs=source_dbs, materialization=o.materialization, unique_key=o.unique_key)
        except Exception as exc:  # noqa: BLE001
            failed.append({"model": o.name, "error": f"{type(exc).__name__}: {str(exc)[:160]}"}); continue
        body = _JOB.format(model=o.name, args=", ".join(args) or "none", target_db=target_db, materialization=o.materialization,
                           unique_key=json.dumps(o.unique_key), sql_json=sql_json, arg_names=json.dumps(args))
        (jdir / f"{o.name}.py").write_text(Provenance(source_hash=source_hash, object=f"{folder}/{o.mapping}/{o.target}", template_id="informatica.glue_pyspark_job",
                                                     rendered_at=rendered_at).render(body, ".py"), encoding="utf-8")
        manifest.append({"Name": f"infa_{o.name}", "ScriptLocation": f"glue_jobs/{o.name}.py", "GlueVersion": "4.0", "Command": "glueetl",
                         "DefaultArguments": {"--datalake-formats": "iceberg", "--enable-glue-datacatalog": "true", "--iceberg_warehouse": "s3://<bucket>/<prefix>/",
                                              **{f"--{a}": "" for a in args}},
                         "model": o.name, "materialization": o.materialization})
    (jdir / "jobs.json").write_text(json.dumps({"target_db": target_db, "jobs": manifest, "failed": failed}, indent=1), encoding="utf-8")
    return {"jobs": len(manifest), "failed": failed}
