"""
corpus — run the converter over a DIRECTORY of exports (any customer, any domain) and measure it, never stopping on one failure.

    report   parse → classify → compile → write_project per file; per-file rows + totals (decisions, crashes, types, features)
    bench    the real acceptance gate: write the dbt project per export, create its sources as empty typed tables in DuckDB,
             run `dbt build`, report models built / failed with the engine's error. Needs dbt-duckdb in the running venv.
    fetch    pull public PowerCenter exports from GitHub (every export references powrmart.dtd) for regression testing

Why this exists (2026-10-07): the compiler, dbt build and Tier 2 agent were developed on two hand-written exports; the first run
over 122 public exports found five real-data bugs the synthetic files could not show. The bench number is the only claim we make.
"""
from __future__ import annotations
import collections
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

_EXPORT_GLOB = ("*.xml", "*.XML", "*.Xml")


def export_files(root: Path) -> List[Path]:
    if root.is_file():
        return [root]
    out: List[Path] = []
    for pat in _EXPORT_GLOB:
        out.extend(root.rglob(pat))
    return sorted(set(out))


# ─────────────────────────────────────────────────────────────────────────── report

def report(root: Path, out_dir: Optional[Path] = None) -> Dict[str, Any]:
    from . import parse_export_all
    from .classifier import classify_mapping
    from .compiler import compile_mapping
    from .project import write_project
    rows: List[Dict[str, Any]] = []
    dec: collections.Counter = collections.Counter(); types: collections.Counter = collections.Counter()
    feats: collections.Counter = collections.Counter(); crashes: collections.Counter = collections.Counter()
    for f in export_files(root):
        row: Dict[str, Any] = {"file": str(f.relative_to(root) if root.is_dir() else f.name), "size": f.stat().st_size}
        try:
            folders = parse_export_all(str(f))
        except Exception as e:  # noqa: BLE001
            row.update(stage="parse", error=f"{type(e).__name__}: {str(e)[:160]}"); rows.append(row); crashes["parse:" + type(e).__name__] += 1; continue
        if not folders:
            row.update(stage="parse", error="no FOLDER element (not a repository export)"); rows.append(row); crashes["not_export"] += 1; continue
        row.update(folders=len(folders), mappings=sum(len(x.mappings) for x in folders), workflows=sum(len(x.workflows) for x in folders),
                   sources=sum(len(x.sources) for x in folders), targets=sum(len(x.targets) for x in folders), models=0, decisions={}, errors=[])
        rdec: collections.Counter = collections.Counter()
        for fo in folders:
            for m in fo.mappings:
                for t in m.transformation_types(): types[t] += 1
                try:
                    c = classify_mapping(m, fo); d = c["decision"]
                    for ft in c.get("features", []): feats[str(ft).split(":")[0]] += 1
                except Exception as e:  # noqa: BLE001
                    d = "classify_crash"; crashes["classify:" + type(e).__name__] += 1; row["errors"].append(f"classify {m.name}: {type(e).__name__}: {str(e)[:120]}")
                rdec[d] += 1; dec[d] += 1
                try:
                    row["models"] += len(compile_mapping(m, fo))
                except Exception as e:  # noqa: BLE001
                    crashes["compile:" + type(e).__name__] += 1; row["errors"].append(f"compile {m.name}: {type(e).__name__}: {str(e)[:120]}")
        row["decisions"] = dict(rdec)
        if out_dir is not None:
            try:
                write_project(folders, out_dir / f.stem, project_name="infa_" + re.sub(r"\W+", "_", f.stem.lower())[:24], profile="bench", iceberg=False)
                row["stage"] = "ok"
            except Exception as e:  # noqa: BLE001
                row.update(stage="write_project", error=f"{type(e).__name__}: {str(e)[:160]}"); crashes["write:" + type(e).__name__] += 1
        else:
            row["stage"] = "ok"
        rows.append(row)
    summary = {"files": len(rows), "exports": sum(1 for r in rows if "mappings" in r), "mappings": sum(r.get("mappings", 0) for r in rows),
               "workflows": sum(r.get("workflows", 0) for r in rows), "models_compiled": sum(r.get("models", 0) for r in rows),
               "decisions": dict(dec), "crashes": dict(crashes), "transformation_types": dict(types.most_common()), "features": dict(feats.most_common())}
    return {"summary": summary, "rows": rows}


# ─────────────────────────────────────────────────────────────────────────── bench

_DUCK = {"numeric": "DECIMAL(38,6)", "bigint": "BIGINT", "integer": "INTEGER", "float": "DOUBLE", "string": "VARCHAR", "timestamp": "TIMESTAMP", "binary": "BLOB"}


def bench(root: Path, out_dir: Path, keep: bool = False, timeout: int = 900) -> Dict[str, Any]:
    """Real `dbt build` of every export on DuckDB. Returns {"summary", "rows"}; each row has models {name: status/message}."""
    import duckdb  # noqa: F401  (ImportError here is the right signal: install dbt-duckdb in this venv)
    from . import parse_export_all
    from .project import _yml_type, write_project
    if out_dir.exists() and not keep:
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    dbt_bin = Path(sys.executable).parent / "dbt"
    if not dbt_bin.exists():
        dbt_bin = Path(shutil.which("dbt") or "dbt")
    rows: List[Dict[str, Any]] = []
    tot = collections.Counter()
    for f in export_files(root):
        row: Dict[str, Any] = {"file": str(f.relative_to(root) if root.is_dir() else f.name)}
        try:
            folders = parse_export_all(str(f))
        except Exception as e:  # noqa: BLE001
            row.update(error=f"parse: {type(e).__name__}: {str(e)[:120]}"); rows.append(row); continue
        if not any(fo.mappings for fo in folders):
            continue
        out = out_dir / f.stem; out.mkdir(parents=True, exist_ok=True)
        write_project(folders, out, project_name="bench", profile="bench", iceberg=False)
        if not list(out.glob("models/*/*.sql")):
            row.update(models={}, note="no compiled models (all skipped)"); rows.append(row); continue
        db = out / "bench.duckdb"
        con = duckdb.connect(str(db))
        # every table the generated project DECLARES (folder sources and lookups alike), typed from its own _sources.yml; plus the
        # owner schema for SQL overrides kept verbatim (GL.GL_JOURNAL)
        import yaml
        for sy in out.glob("models/*/_sources.yml"):
            for src in (yaml.safe_load(sy.read_text(encoding="utf-8")) or {}).get("sources", []):
                schema = re.sub(r"\{\{.*?'([^']*)'\s*\)\s*\}\}", r"\1", str(src.get("schema", src["name"]))).strip() or src["name"]
                con.execute(f'create schema if not exists "{schema}"')
                for tb in src.get("tables", []):
                    ident = re.sub(r"\{\{.*?'([^']*)'\s*\)\s*\}\}", r"\1", str(tb.get("identifier", tb["name"]))).strip() or tb["name"]
                    cols = ", ".join(f'"{c["name"]}" {_DUCK.get(str(c.get("data_type", "string")), "VARCHAR")}' for c in tb.get("columns", [])) or "_dummy integer"
                    con.execute(f'create table if not exists "{schema}"."{ident}" ({cols})')
        for s in [x for fo in folders for x in fo.sources]:
            cols = ", ".join(f'"{p.name.lower()}" {_DUCK.get(_yml_type(p.datatype), "VARCHAR")}' for p in s.fields) or "_dummy integer"
            for schema in {(s.dbd_name or "src").lower(), (s.owner or s.dbd_name or "src").lower()}:
                con.execute(f'create schema if not exists "{schema}"')
                con.execute(f'create table if not exists "{schema}"."{s.name.lower()}" ({cols})')
        con.close()
        (out / "profiles.yml").write_text(f"bench:\n  target: b\n  outputs:\n    b:\n      type: duckdb\n      path: {db}\n      threads: 1\n", encoding="utf-8")
        env = {k: v for k, v in os.environ.items() if not k.startswith("DBT_")}
        try:
            r = subprocess.run([str(dbt_bin), "build", "--project-dir", str(out), "--profiles-dir", str(out), "--no-use-colors"],
                               capture_output=True, text=True, env=env, timeout=timeout)
            tail = (r.stdout + r.stderr)[-600:]
        except Exception as e:  # noqa: BLE001
            r = None; tail = f"{type(e).__name__}: {e}"
        rr = out / "target" / "run_results.json"
        res = json.loads(rr.read_text())["results"] if rr.exists() else []
        models = {x["unique_id"].split(".")[-1]: {"status": x["status"], "message": (x.get("message") or "")[:300]} for x in res if x["unique_id"].startswith("model.")}
        tests = [x for x in res if x["unique_id"].startswith("test.")]
        row.update(models=models, tests=len(tests), tests_pass=sum(1 for x in tests if x["status"] == "pass"), rc=(r.returncode if r else None))
        if not res:
            row["error"] = tail
        tot["projects"] += 1; tot["models"] += len(models); tot["tests"] += len(tests); tot["tests_pass"] += row["tests_pass"]
        tot["ok"] += sum(1 for v in models.values() if v["status"] == "success"); tot["fail"] += sum(1 for v in models.values() if v["status"] != "success")
        rows.append(row)
    return {"summary": dict(tot), "rows": rows}


def render_bench_md(result: Dict[str, Any]) -> str:
    s = result["summary"]
    lines = [f"# dbt build bench — {s.get('projects', 0)} projects · {s.get('models', 0)} models · {s.get('ok', 0)} built · {s.get('fail', 0)} failed · tests {s.get('tests_pass', 0)}/{s.get('tests', 0)}", ""]
    lines += ["| export | models | built | failures |", "|---|---|---|---|"]
    for r in result["rows"]:
        if "models" not in r:
            lines.append(f"| {r['file']} | — | — | {r.get('error', '')} |"); continue
        bad = [f"`{k}`: {next((l.strip() for l in v['message'].splitlines() if 'Error' in l and 'Runtime Error in model' not in l), v['status'])[:140]}"
               for k, v in r["models"].items() if v["status"] != "success"]
        ok = sum(1 for v in r["models"].values() if v["status"] == "success")
        lines.append(f"| {r['file']} | {len(r['models'])} | {ok} | {'<br>'.join(bad) or (r.get('error', '') or r.get('note', ''))[:160]} |")
    return "\n".join(lines) + "\n"


# ─────────────────────────────────────────────────────────────────────────── fetch

_QUERIES = ('"powrmart.dtd" extension:xml', '"powrmart.dtd" extension:XML', '"<POWERMART" "<REPOSITORY" extension:xml',
            '"CREATION_DATE" "REPOSITORY_VERSION" extension:xml', '"TRANSFORMATION TYPE=\\"Source Qualifier\\"" extension:xml')


def fetch_public_corpus(dest: Path, limit: int = 100) -> Dict[str, Any]:
    """GitHub code search (needs `gh` authenticated) → <dest>/<owner>__<repo>/<path with / → __>. Returns counts. Files are
    other people's; keep them out of our repos unless their licence is checked — they are a test input, not a deliverable."""
    dest.mkdir(parents=True, exist_ok=True)
    hits: Dict[str, Dict[str, Any]] = {}
    for q in _QUERIES:
        r = subprocess.run(["gh", "search", "code", q, "--limit", str(limit), "--json", "repository,path,url"], capture_output=True, text=True)
        if r.returncode != 0:
            continue
        for x in json.loads(r.stdout or "[]"):
            hits[x["url"]] = x
    ok = fail = 0
    for x in hits.values():
        repo = x["repository"]["nameWithOwner"]; path = x["path"]
        m = re.search(r"/blob/([0-9a-f]+)/", x["url"]); ref = m.group(1) if m else "HEAD"
        f = dest / repo.replace("/", "__") / path.replace("/", "__")
        f.parent.mkdir(parents=True, exist_ok=True)
        if f.exists():
            ok += 1; continue
        r = subprocess.run(["gh", "api", "-H", "Accept: application/vnd.github.raw", f"repos/{repo}/contents/{path}?ref={ref}"], capture_output=True)
        if r.returncode == 0 and r.stdout.lstrip()[:1] == b"<":
            f.write_bytes(r.stdout); ok += 1
        else:
            fail += 1
    repos = collections.Counter(x["repository"]["nameWithOwner"] for x in hits.values())
    return {"files": len(hits), "downloaded": ok, "failed": fail, "repos": dict(repos)}


def genuine_exports(root: Path) -> List[Path]:
    """Heuristic: a Designer/Repository Manager export carries CREATION_DATE on POWERMART and VERSION on REPOSITORY, and at least
    one INSTANCE in a mapping. Hand-written samples (ours and other people's) usually lack one of these."""
    out = []
    for f in export_files(root):
        t = f.read_text(errors="ignore")
        if re.search(r'<POWERMART[^>]*CREATION_DATE\s*=\s*"\d', t) and re.search(r'<REPOSITORY[^>]*VERSION\s*=\s*"\d', t) and "<INSTANCE" in t:
            out.append(f)
    return out
