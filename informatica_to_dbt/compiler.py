"""
compiler — one Mapping → one dbt model per target instance: portable SQL (Snowflake-flavoured), then the target dialect via
sqlglot, plus what a dbt project needs around it (sources, key tests, TODO blocks).

Informatica mappings are ROW STREAMS. Most transformations only add columns to the row that passes through (Expression,
Lookup, Sequence Generator), filter it (Filter) or leave it (Sorter, Update Strategy). Only Joiner, Aggregator, Router and
Union change the set of rows. So the compiler does not emit a CTE per instance; it carries a `Relation` down the stream:

    Relation(base CTE, joins[], where[], cols{port → SQL})

column-adding transformations extend `cols` (expressions inlined), Filter appends to `where`, Lookup appends a LEFT JOIN, and
only the row-changing transformations materialise a new CTE. A consumer fed by several branches of the same stream (Sorter
reading Filter + Lookup + Expression, as PowerCenter draws it) merges those branches back into one relation. This keeps the
generated SQL close to what a data engineer would write by hand.

    ModelOut(name, sql, todos, sources, columns, materialization, unique_key)  ← compile_mapping(mapping, folder, dialect)
"""
from __future__ import annotations
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from .classifier import classify_mapping
from .expressions import finalize_macro_args, to_portable_sql
from .model import Folder, Instance, Mapping, Transformation
from .registry import resolve_transformation


@dataclass
class ModelOut:
    name: str
    target: str
    sql: str
    portable_sql: str
    decision: str
    mapping: str = ""
    todos: List[str] = field(default_factory=list)
    sources: List[Dict[str, str]] = field(default_factory=list)
    columns: List[Dict[str, str]] = field(default_factory=list)
    materialization: str = "table"
    unique_key: List[str] = field(default_factory=list)
    dialect_error: Optional[str] = None


@dataclass
class Relation:
    base: str                                   # CTE name (or a FROM fragment for sources)
    cols: Dict[str, str]                        # port name → SQL expression valid over `base` + joins
    joins: List[str] = field(default_factory=list)
    where: List[str] = field(default_factory=list)
    distinct: bool = False
    origin: str = ""                            # instance that produced the base (for merging branches)
    dd: Optional[str] = None                    # Update Strategy row operation (SQL → 0 insert/1 update/2 delete/3 reject); None = insert-only

    def copy(self) -> "Relation":
        return Relation(self.base, dict(self.cols), list(self.joins), list(self.where), self.distinct, self.origin, self.dd)


def _id(name: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z_]+", "_", name).strip("_").lower()
    return s if s and not s[0].isdigit() else f"t_{s}"


def _q(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


class _Ctx:
    def __init__(self, m: Mapping, f: Optional[Folder]):
        self.m, self.f = m, f
        self.inst: Dict[str, Instance] = {i.name: i for i in m.instances}
        self.inputs: Dict[str, Dict[str, Tuple[str, str]]] = defaultdict(dict)   # to_instance → {to_field: (from_instance, from_field)}
        for c in m.connectors:
            self.inputs[c.to_instance][c.to_field] = (c.from_instance, c.from_field)
        self.rel: Dict[str, Relation] = {}       # instance → relation after it
        self.ctes: List[str] = []
        self.todos: List[str] = []
        self.seq: Dict[str, Tuple[int, int]] = {}
        self.materialized: Set[str] = set()

    def upstreams(self, name: str) -> List[str]:
        out: List[str] = []
        for (fi, _) in self.inputs.get(name, {}).values():
            if fi not in out: out.append(fi)
        return out

    def ttype(self, name: str) -> str:
        i = self.inst.get(name)
        return (i.transformation_type or (i.transformation.type if i and i.transformation else "")) if i else ""

    def is_seq(self, name: str) -> bool:
        return self.ttype(name).lower().startswith("sequence")

    # ── relations ─────────────────────────────────────────────────────────────────────────────
    def input_relation(self, name: str) -> Relation:
        """The relation a single-stream transformation reads: its non-sequence upstream branches merged. A Router upstream
        resolves to the output GROUP the consumer's ports come from."""
        ups = [u for u in self.upstreams(name) if not self.is_seq(u)]
        rels: List[Relation] = []
        for u in ups:
            if self.ttype(u).lower() == "router":
                ff = next(ff for (fi, ff) in self.inputs[name].values() if fi == u)
                grp = next((p.group for p in self.inst[u].transformation.ports if p.name == ff), "")
                if f"{u}__{grp}" in self.rel: rels.append(self.rel[f"{u}__{grp}"])
            elif u in self.rel:
                rels.append(self.rel[u])
        if not rels:
            return Relation(base="(select 1 as _one)", cols={}, origin="")
        merged = rels[0].copy()
        for r in rels[1:]:
            if r.base == merged.base:
                for j in r.joins:
                    if j not in merged.joins: merged.joins.append(j)
                for w in r.where:
                    if w not in merged.where: merged.where.append(w)
                merged.cols.update({k: v for k, v in r.cols.items() if k not in merged.cols})
            else:
                self.todos.append(f"{name}: fed by two different row sets ({merged.origin} and {r.origin}); cross join emitted, verify")
                merged.joins.append(f"cross join {r.base}")
                merged.cols.update({k: v for k, v in r.cols.items() if k not in merged.cols})
        return merged

    def port_map(self, name: str) -> Dict[str, str]:
        """port of `name` → SQL expression, via the connectors into it and the upstream relations' columns."""
        out: Dict[str, str] = {}
        for port, (fi, ff) in self.inputs.get(name, {}).items():
            if self.is_seq(fi):
                start, inc = self.seq.get(fi, (1, 1))
                out[port] = f"((row_number() over (order by 1) - 1) * {inc} + {start})"
                continue
            r = self.rel.get(fi)
            if self.ttype(fi).lower() == "router":                    # router ports live in group relations
                grp = next((p.group for p in self.inst[fi].transformation.ports if p.name == ff), "")
                r = self.rel.get(f"{fi}__{grp}")
            if r is None: continue
            if ff in r.cols: out[port] = r.cols[ff]
        return out

    def materialize(self, rel: Relation, name: str, cols: Optional[List[str]] = None) -> Relation:
        """Write `rel` as a CTE and return the relation over it (columns become plain references)."""
        cid = _id(name)
        use = cols or list(rel.cols)
        sel = ", ".join(f"{rel.cols[c]} as {_id(c)}" for c in use)
        body = f"select {'distinct ' if rel.distinct else ''}{sel}\n    from {rel.base}"
        for j in rel.joins: body += f"\n    {j}"
        if rel.where: body += "\n    where " + "\n      and ".join(f"({w})" for w in rel.where)
        self.ctes.append(f"{cid} as (\n    {body}\n)")
        self.materialized.add(cid)
        return Relation(base=cid, cols={c: f"{cid}.{_id(c)}" for c in use}, origin=name)


def _translate(ctx: _Ctx, expr: str, pm: Dict[str, str], where: str) -> str:
    t = to_portable_sql(expr)
    for u in t.unknown: ctx.todos.append(f"{where}: function {u}() not translated, kept verbatim")
    for fc in t.flow_control: ctx.todos.append(f"{where}: {fc}() is flow control; value passed through, side effect not reproduced")
    for lk in t.lookups: ctx.todos.append(f"{where}: unconnected lookup :LKP.{lk} replaced by NULL — join the lookup table explicitly")
    out = t.sql
    for p in sorted(pm, key=len, reverse=True):
        out = re.sub(rf"(?<![\w.'\"]){re.escape(p)}(?![\w'\"(])", lambda _m, v=pm[p]: v, out)
    return out


# ── families ───────────────────────────────────────────────────────────────────────────────────
def _source_qualifier(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    a = {k.lower(): v for k, v in {**t.attributes, **inst.attributes}.items()}
    srcs = [u for u in ctx.upstreams(inst.name) if ctx.inst[u].type.upper() == "SOURCE"]
    cols: Dict[str, str] = {}
    for p in t.ports:
        src = ctx.inputs.get(inst.name, {}).get(p.name)
        if src: cols[p.name] = f"{_id(src[0])}.{_id(src[1])}"
    if a.get("sql query", "").strip():
        ctx.todos.append(f"{inst.name}: SQL override used verbatim; rewrite as dbt sources")
        cid = _id(inst.name); ctx.ctes.append(f"{cid} as (\n    -- TODO SQL override of Source Qualifier {inst.name}\n    {a['sql query'].strip()}\n)")
        return Relation(base=cid, cols={p.name: f"{cid}.{_id(p.name)}" for p in t.ports}, origin=inst.name)
    if not srcs:
        ctx.todos.append(f"{inst.name}: no source connected"); return Relation(base="(select 1 as _one)", cols={}, origin=inst.name)
    frm = f"{{{{ source({_q(_id(ctx.inst[srcs[0]].dbd_name or 'src'))}, {_q(_id(ctx.inst[srcs[0]].transformation_name or srcs[0]))}) }}}} as {_id(srcs[0])}"
    rel = Relation(base=frm, cols=cols, origin=inst.name)
    join = a.get("user defined join", "").strip()
    for s in srcs[1:]:
        src_ref = f"{{{{ source({_q(_id(ctx.inst[s].dbd_name or 'src'))}, {_q(_id(ctx.inst[s].transformation_name or s))}) }}}} as {_id(s)}"
        rel.joins.append(f"join {src_ref} on 1=1" if join else f"cross join {src_ref}")
    if join: rel.where.append(_qualify(ctx, join, srcs))
    elif len(srcs) > 1: ctx.todos.append(f"{inst.name}: several sources without a User Defined Join — cross join emitted")
    if a.get("source filter", "").strip(): rel.where.append(_qualify(ctx, a["source filter"], srcs))
    if a.get("select distinct", "").upper() == "YES": rel.distinct = True
    return ctx.materialize(rel, inst.name)          # a Source Qualifier is always its own CTE (the model's staging step)


def _qualify(ctx: _Ctx, expr: str, srcs: List[str]) -> str:
    sql = to_portable_sql(expr).sql
    for s in srcs:
        sql = re.sub(rf"(?<![\w.]){re.escape(s)}\.(\w+)", lambda mm, s=s: f"{_id(s)}.{_id(mm.group(1))}", sql, flags=re.I)
    return sql


def _expression(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    var_sql: Dict[str, str] = {}
    for p in t.ports:
        if p.is_variable: var_sql[p.name] = "(" + _translate(ctx, p.expression, {**pm, **var_sql}, f"{inst.name}.{p.name}") + ")"
    out = rel.copy(); out.cols = {}
    for p in t.ports:
        if not p.is_output: continue
        if p.expression and p.porttype.upper() == "OUTPUT": out.cols[p.name] = _translate(ctx, p.expression, {**pm, **var_sql}, f"{inst.name}.{p.name}")
        elif p.name in pm: out.cols[p.name] = pm[p.name]
    return out


def _filter(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    out = rel.copy(); out.cols = {p.name: pm[p.name] for p in t.ports if p.is_output and p.name in pm}
    out.where.append(_translate(ctx, t.attr("Filter Condition") or "TRUE", pm, f"{inst.name}.filter"))
    return out


def _sorter(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    out = rel.copy(); out.cols = {p.name: pm[p.name] for p in t.ports if p.name in pm}
    if t.attr("Distinct", "NO").upper() == "YES": out.distinct = True
    keys = [p.name for p in t.ports if p.attrs.get("Sort Key", "").upper() == "YES"]
    if keys: ctx.todos.append(f"{inst.name}: sort keys {keys} — dbt tables are unordered; order at the consumer if it matters")
    return out


def _update_strategy(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    """DD_INSERT-only → a plain table. Anything else → the target becomes a merge on its primary key (see _merge_model)."""
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    out = rel.copy(); out.cols = {p.name: pm[p.name] for p in t.ports if p.name in pm}
    expr = t.attr("Update Strategy Expression", "DD_INSERT").strip()
    if expr.upper() not in ("DD_INSERT", "0"):
        out.dd = _translate(ctx, expr, pm, f"{inst.name}.strategy")
    return out


def _rank(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    """Top/Bottom N per group → row_number() over (partition by <group-by ports> order by <rank port>) <= N, as RANKINDEX."""
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    out = rel.copy(); out.cols = {p.name: pm[p.name] for p in t.ports if p.name in pm}
    group = [p.name for p in t.ports if p.expressiontype.upper() == "GROUPBY" and p.name in pm]
    rank_port = next((p.name for p in t.ports if ("MASTER" in p.porttype.upper() or p.attrs.get("Rank Port", "").upper() == "YES") and p.name in pm), None)
    idx = next((p.name for p in t.ports if p.name.upper() == "RANKINDEX"), "RANKINDEX")
    if not rank_port:
        ctx.todos.append(f"{inst.name}: rank port not identified — set the ORDER BY of {idx}"); rank_port = None
    n = int(re.sub(r"\D", "", t.attr("Number Of Ranks", "1")) or 1)
    order = f"{pm[rank_port]} {'asc' if t.attr('Top/Bottom', 'Top').lower().startswith('bottom') else 'desc'}" if rank_port else "1"
    part = ("partition by " + ", ".join(pm[g] for g in group) + " ") if group else ""
    out.cols[idx] = f"row_number() over ({part}order by {order})"
    m = ctx.materialize(out, inst.name)                 # a window column cannot be filtered in the same SELECT
    m.where.append(f"{m.cols[idx]} <= {n}")
    return m


_JOIN = {"normal join": "inner join", "master outer join": "left join", "detail outer join": "right join", "full outer join": "full outer join"}


def _joiner(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    master = {p.name for p in t.ports if p.attrs.get("Master", "").upper() == "YES"}
    m_up = d_up = None
    for p in t.ports:
        src = ctx.inputs.get(inst.name, {}).get(p.name)
        if not src: continue
        if p.name in master: m_up = m_up or src[0]
        else: d_up = d_up or src[0]
    d_rel = ctx.materialize(ctx.rel[d_up], f"{inst.name}__detail") if d_up else Relation("(select 1 as _one)", {})
    m_rel = ctx.materialize(ctx.rel[m_up], f"{inst.name}__master") if m_up else Relation("(select 1 as _one)", {})
    pm: Dict[str, str] = {}
    for p in t.ports:
        src = ctx.inputs.get(inst.name, {}).get(p.name)
        if not src: continue
        side = m_rel if p.name in master else d_rel
        if src[1] in side.cols: pm[p.name] = side.cols[src[1]]
    cond = _translate(ctx, t.attr("Join Condition") or "1=1", pm, f"{inst.name}.join")
    jt = _JOIN.get(t.attr("Join Type", "Normal Join").lower(), "inner join")     # detail drives; Master Outer keeps all detail rows
    rel = Relation(base=d_rel.base, cols={p.name: pm[p.name] for p in t.ports if p.name in pm}, joins=[f"{jt} {m_rel.base} on {cond}"], origin=inst.name)
    return ctx.materialize(rel, inst.name)


_AGG = ("SUM(", "COUNT(", "AVG(", "MIN(", "MAX(", "FIRST(", "LAST(", "MEDIAN(", "STDDEV(", "VARIANCE(", "PERCENTILE(")


def _aggregator(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    group = [p.name for p in t.ports if p.expressiontype.upper() == "GROUPBY"]
    cols: Dict[str, str] = {}
    for p in t.ports:
        if not p.is_output: continue
        if p.name in group: cols[p.name] = pm.get(p.name, _id(p.name))
        elif p.expression and any(fn in p.expression.upper() for fn in _AGG): cols[p.name] = _translate(ctx, p.expression, pm, f"{inst.name}.{p.name}")
        elif p.name in pm:
            cols[p.name] = f"max({pm[p.name]})"
            ctx.todos.append(f"{inst.name}.{p.name}: non-grouped pass-through — Informatica returns the LAST row's value; max() emitted")
        elif p.expression: cols[p.name] = _translate(ctx, p.expression, pm, f"{inst.name}.{p.name}")
    cid = _id(inst.name)
    body = f"select {', '.join(f'{v} as {_id(k)}' for k, v in cols.items())}\n    from {rel.base}"
    for j in rel.joins: body += f"\n    {j}"
    if rel.where: body += "\n    where " + "\n      and ".join(f"({w})" for w in rel.where)
    if group: body += "\n    group by " + ", ".join(pm.get(g, _id(g)) for g in group)
    ctx.ctes.append(f"{cid} as (\n    {body}\n)"); ctx.materialized.add(cid)
    return Relation(base=cid, cols={k: f"{cid}.{_id(k)}" for k in cols}, origin=inst.name)


def _router(ctx: _Ctx, inst: Instance, t: Transformation) -> None:
    """One relation per output group, registered as `<inst>__<group>`; the router itself has no relation."""
    base = ctx.materialize(ctx.input_relation(inst.name), f"{inst.name}__in")
    pm_in = {p.name: base.cols[ctx.inputs[inst.name][p.name][1]] for p in t.ports if p.is_input and not p.is_output
             and p.name in ctx.inputs.get(inst.name, {}) and ctx.inputs[inst.name][p.name][1] in base.cols}
    # base.cols are keyed by the upstream's port names; re-key by this router's input port names
    pm = {}
    for p in t.ports:
        if p.is_input and not p.is_output:
            src = ctx.inputs.get(inst.name, {}).get(p.name)
            if src and src[1] in base.cols: pm[p.name] = base.cols[src[1]]
    exprs = {g["name"]: g.get("expression", "") for g in t.groups if g.get("type", "").upper().startswith("OUTPUT")}
    conds = {g: _translate(ctx, e, pm, f"{inst.name}.{g}") for g, e in exprs.items() if e}
    for g in exprs:
        outs = [p for p in t.ports if p.group == g and p.is_output]
        rel = base.copy(); rel.cols = {p.name: pm.get(p.ref_field or p.name, "null") for p in outs}
        rel.where.append(conds.get(g) or ("not (" + " or ".join(f"({c})" for c in conds.values()) + ")" if conds else "TRUE"))
        rel.origin = f"{inst.name}__{g}"
        ctx.rel[f"{inst.name}__{g}"] = rel


def _union(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    outs = [p for p in t.ports if p.is_output]
    groups = [g["name"] for g in t.groups if g.get("type", "").upper() == "INPUT"] or sorted({p.group for p in t.ports if p.is_input and p.group})
    parts: List[str] = []
    for g in groups:
        ins = [p for p in t.ports if p.group == g and p.is_input]
        ups = {ctx.inputs[inst.name][p.name][0] for p in ins if p.name in ctx.inputs.get(inst.name, {})}
        if not ups: continue
        up = next(iter(ups)); r = ctx.materialize(ctx.rel[up], f"{inst.name}__{g}")
        sel = []
        for p, o in zip(ins, outs):
            src = ctx.inputs.get(inst.name, {}).get(p.name)
            sel.append(f"{r.cols.get(src[1], 'null') if src else 'null'} as {_id(o.name)}")
        parts.append(f"select {', '.join(sel)} from {r.base}")
    cid = _id(inst.name); ctx.ctes.append(f"{cid} as (\n    " + "\n    union all\n    ".join(parts) + "\n)"); ctx.materialized.add(cid)
    return Relation(base=cid, cols={p.name: f"{cid}.{_id(p.name)}" for p in outs}, origin=inst.name)


def _lookup(ctx: _Ctx, inst: Instance, t: Transformation) -> Relation:
    a = {k.lower(): v for k, v in {**t.attributes, **inst.attributes}.items()}
    rel = ctx.input_relation(inst.name); pm = ctx.port_map(inst.name)
    lk_ports = [p for p in t.ports if "LOOKUP" in p.porttype.upper() or (p.is_output and not p.is_input)]
    alias = f"{_id(inst.name)}_lk"
    table = a.get("lookup table name", "") or inst.name
    conn = _id(a.get("connection information", "") or "lookup")
    cond_src = a.get("lookup condition", "").strip()
    cond = to_portable_sql(cond_src).sql if cond_src else "1=1"
    for p in lk_ports: cond = re.sub(rf"(?<![\w.]){re.escape(p.name)}(?![\w])", f"{alias}.{_id(p.name)}", cond)
    for p, v in sorted(pm.items(), key=lambda kv: -len(kv[0])): cond = re.sub(rf"(?<![\w.]){re.escape(p)}(?![\w])", lambda _m, v=v: v, cond)
    keys = [_id(p.name) for p in lk_ports if re.search(rf"(?<![\w.]){re.escape(p.name)}(?![\w])", cond_src)]
    lk_cols = ", ".join(_id(p.name) for p in lk_ports)
    src_ref = f"{{{{ source({_q(conn)}, {_q(_id(table))}) }}}}"
    if a.get("lookup sql override", "").strip():
        sub = a["lookup sql override"].strip(); ctx.todos.append(f"{inst.name}: Lookup SQL override used verbatim")
    elif keys:
        sub = (f"select {lk_cols} from (select {lk_cols}, row_number() over (partition by {', '.join(keys)} order by 1) as _rn from {src_ref}) as _d where _rn = 1")
        ctx.todos.append(f"{inst.name}: '{a.get('lookup policy on multiple match', 'Use First Value')}' on multiple match — row_number() order is unspecified; set it")
    else:
        sub = f"select {lk_cols} from {src_ref}"; ctx.todos.append(f"{inst.name}: no lookup condition — cross join")
    out = rel.copy()
    out.joins.append(f"left join ({sub}) as {alias} on {cond}")
    for p in lk_ports: out.cols[p.name] = f"{alias}.{_id(p.name)}"
    return out


# ── the walk ───────────────────────────────────────────────────────────────────────────────────
def _topo(ctx: _Ctx) -> List[str]:
    order: List[str] = []; seen: Set[str] = set()
    def visit(n: str):
        if n in seen: return
        seen.add(n)
        for u in ctx.upstreams(n): visit(u)
        order.append(n)
    for i in ctx.m.instances: visit(i.name)
    return order


def compile_mapping(m: Mapping, folder: Optional[Folder] = None) -> List[ModelOut]:
    decision = classify_mapping(m, folder)
    if decision["decision"] == "skip":
        return [ModelOut(name=_id(tg.name), target=tg.transformation_name or tg.name, mapping=m.name, sql="", portable_sql="", decision="skip",
                         todos=[f"not compiled: {r}" for r in decision["reasons"]]) for tg in m.target_instances]
    ctx = _Ctx(m, folder)
    for i in m.transformation_instances:
        if ctx.is_seq(i.name) and i.transformation:
            ctx.seq[i.name] = (int(i.transformation.attr("Start Value", "1") or 1), int(i.transformation.attr("Increment By", "1") or 1))
    for name in _topo(ctx):
        inst = ctx.inst[name]
        if inst.type.upper() != "TRANSFORMATION" or inst.transformation is None: continue
        fam = resolve_transformation(ctx.ttype(name)).family
        t = inst.transformation
        if fam == "source": ctx.rel[name] = _source_qualifier(ctx, inst, t)
        elif fam == "projection": ctx.rel[name] = _expression(ctx, inst, t)
        elif fam == "filter": ctx.rel[name] = _filter(ctx, inst, t)
        elif fam == "sort": ctx.rel[name] = _sorter(ctx, inst, t)
        elif fam == "update_strategy": ctx.rel[name] = _update_strategy(ctx, inst, t)
        elif fam == "rank": ctx.rel[name] = _rank(ctx, inst, t)
        elif fam == "join": ctx.rel[name] = _joiner(ctx, inst, t)
        elif fam == "aggregate": ctx.rel[name] = _aggregator(ctx, inst, t)
        elif fam == "router": _router(ctx, inst, t)
        elif fam == "union": ctx.rel[name] = _union(ctx, inst, t)
        elif fam == "lookup": ctx.rel[name] = _lookup(ctx, inst, t)
        elif fam == "sequence": continue
        else: ctx.todos.append(f"{name}: {ctx.ttype(name)} not compiled")
    shared_ctes = len(ctx.ctes)
    outs: List[ModelOut] = []
    for tg in m.target_instances:
        tdef = next((x for x in (folder.targets if folder else []) if x.name == (tg.transformation_name or tg.name)), None)
        pm = ctx.port_map(tg.name)
        rel = ctx.input_relation(tg.name)
        fields = [p.name for p in (tdef.fields if tdef else [])] or list(ctx.inputs.get(tg.name, {}).keys())
        sel: List[str] = []; meta: List[Dict[str, str]] = []; keys: List[str] = []
        for fname in fields:
            if fname in pm: sel.append(f"{pm[fname]} as {_id(fname)}")
            else: sel.append(f"null as {_id(fname)}"); ctx.todos.append(f"{tg.name}.{fname}: not connected — null emitted")
            fd = next((p for p in (tdef.fields if tdef else []) if p.name == fname), None)
            meta.append({"name": _id(fname), "datatype": fd.datatype if fd else "", "keytype": fd.keytype if fd else ""})
            if fd and fd.keytype.upper() == "PRIMARY KEY": keys.append(_id(fname))
        if rel.dd is not None and not keys:
            ctx.todos.append(f"{tg.name}: Update Strategy needs the target's primary key — define it in Informatica (KEYTYPE) and re-export; plain table emitted")
        if rel.dd is not None and keys:
            materialization = "incremental"
            sql = _merge_model(ctx.ctes[:shared_ctes], rel, sel, [_id(f) for f in fields], keys)
        else:
            materialization = "table"
            final = f"select {'distinct ' if rel.distinct else ''}{', '.join(sel)}\nfrom {rel.base}"
            for j in rel.joins: final += f"\n{j}"
            if rel.where: final += "\nwhere " + "\n  and ".join(f"({w})" for w in rel.where)
            cfg = f"materialized={_q(materialization)}"
            sql = f"{{{{ config({cfg}) }}}}\n\nwith\n" + ",\n".join(ctx.ctes[:shared_ctes]) + f"\n\n{final}\n" if ctx.ctes else f"{{{{ config({cfg}) }}}}\n\n{final}\n"
        sql = finalize_macro_args(sql)
        if "/* TODO" in sql: ctx.todos.append(f"{tg.name}: inline TODO markers in the SQL (date/regex formats to verify on the target)")
        outs.append(ModelOut(name=_id(tg.name), target=tg.transformation_name or tg.name, mapping=m.name, sql=sql, portable_sql=sql, decision=decision["decision"],
                             todos=sorted(set(ctx.todos)), sources=_sources_used(ctx), columns=meta, materialization=materialization, unique_key=keys))
    return outs


def _merge_model(ctes: List[str], rel: Relation, sel: List[str], cols: List[str], keys: List[str]) -> str:
    """Update Strategy → merge on the target (Donny 2026-09-28): key matches and row_hash differs → update; new key → insert;
    key gone from the source → soft delete (is_deleted = true, var infa_soft_delete_missing, default on); DD_DELETE rows → soft
    delete; DD_REJECT rows → dropped; key and hash match → unchanged (not in the batch)."""
    non_key = [c for c in cols if c not in keys] or keys
    hash_parts = ", \"'|'\", ".join(f'"coalesce(cast({c} as " ~ _str ~ "), \'\')"' for c in non_key)
    on = lambda a, b: " and ".join(f"{a}.{k} = {b}.{k}" for k in keys)
    stream = f"select {', '.join(sel)}, ({rel.dd}) as _dd_op\n    from {rel.base}"
    for j in rel.joins: stream += f"\n    {j}"
    if rel.where: stream += "\n    where " + "\n      and ".join(f"({w})" for w in rel.where)
    col_list = ", ".join(cols)
    s_cols = ", ".join(f"s.{c}" for c in cols); t_cols = ", ".join(f"t.{c}" for c in cols)
    body = ",\n".join(ctes + [
        f"_stream as (\n    {stream}\n)",
        f"_src as (\n    select {col_list},\n        {{{{ dbt.hash(dbt.concat([{hash_parts}])) }}}} as row_hash,\n"
        f"        case when _dd_op = 2 then true else false end as is_deleted\n    from _stream\n    where coalesce(_dd_op, 0) <> 3\n)"])
    return (f"{{{{ config(materialized='incremental', incremental_strategy=var('infa_incremental_strategy', 'merge'), unique_key={keys!r}, "
            f"on_schema_change='append_new_columns') }}}}\n{{% set _str = dbt.type_string() %}}\n\nwith\n{body}\n\n"
            f"{{% if is_incremental() %}}\n"
            f"select {s_cols}, s.row_hash, s.is_deleted, {{{{ dbt.current_timestamp() }}}} as dbt_updated_at\nfrom _src s\n"
            f"left join {{{{ this }}}} t on {on('t', 's')}\nwhere t.{keys[0]} is null or t.row_hash <> s.row_hash or t.is_deleted <> s.is_deleted\n"
            f"{{% if var('infa_soft_delete_missing', true) %}}\nunion all\n"
            f"select {t_cols}, t.row_hash, true as is_deleted, {{{{ dbt.current_timestamp() }}}} as dbt_updated_at\nfrom {{{{ this }}}} t\n"
            f"left join _src s on {on('s', 't')}\nwhere s.{keys[0]} is null and not t.is_deleted\n{{% endif %}}\n"
            f"{{% else %}}\nselect {col_list}, row_hash, is_deleted, {{{{ dbt.current_timestamp() }}}} as dbt_updated_at\nfrom _src\n{{% endif %}}\n")


def _sources_used(ctx: _Ctx) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []; seen = set()
    def add(sn, tb, dbd, ident):
        if (sn, tb) not in seen: seen.add((sn, tb)); out.append({"source_name": sn, "table": tb, "dbd": dbd, "identifier": ident})
    for i in ctx.m.source_instances: add(_id(i.dbd_name or "src"), _id(i.transformation_name or i.name), i.dbd_name, i.transformation_name or i.name)
    for i in ctx.m.transformation_instances:
        if ctx.ttype(i.name).lower().startswith("lookup") and i.transformation:
            a = {k.lower(): v for k, v in i.transformation.attributes.items()}
            add(_id(a.get("connection information", "") or "lookup"), _id(a.get("lookup table name", "") or i.name), a.get("connection information", ""), a.get("lookup table name", "") or i.name)
    return out
