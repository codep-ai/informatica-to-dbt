"""
expressions — Informatica PowerCenter expression language → portable SQL, then per-engine via sqlglot.

Two stages, on purpose:
  1. `to_portable_sql(expr)`: a small tokenizer + recursive-descent function-call parser rewrites Informatica constructs into a
     Snowflake/ANSI-flavoured SQL string (IIF → CASE WHEN, DECODE → searched CASE, ISNULL → IS NULL, NVL → COALESCE, `||` kept,
     `!=` → `<>`, TO_DATE/TO_CHAR formats kept, date arithmetic via DATEADD/DATEDIFF, and so on). Anything it does not know is
     kept verbatim and reported in `unknown`, so the compiler can wrap the model in a TODO block instead of guessing.
  2. `to_dialect(sql, dialect)`: `sqlglot.transpile(read="snowflake", write=dialect)` — Athena/Trino, Databricks/Spark,
     ClickHouse, Snowflake all come from the same portable string. If sqlglot cannot parse, the portable SQL is returned as is
     and the caller is told.

Port references stay as bare identifiers; the compiler qualifies them with the CTE alias. `$$PARAM` mapping parameters become
dbt `{{ var('PARAM') }}`; `$$VAR` mapping variables likewise (their SETVARIABLE semantics are flagged by the classifier).
"""
from __future__ import annotations
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ── tokenizer ────────────────────────────────────────────────────────────────────────────────────
_TOKEN = re.compile(r"""
    (?P<ws>\s+)
  | (?P<jinja>\{\{.*?\}\})
  | (?P<ph>«[^»]*»)
  | (?P<str>'(?:[^']|'')*')
  | (?P<num>\d+(?:\.\d+)?(?:[eE][+-]?\d+)?)
  | (?P<param>\$\$[A-Za-z_][A-Za-z0-9_]*)
  | (?P<lkp>:LKP\.[A-Za-z_][A-Za-z0-9_]*)
  | (?P<ident>[A-Za-z_][A-Za-z0-9_$.]*)
  | (?P<op>\|\||<=|>=|!=|<>|[-+*/%=<>(),])
""", re.X)


@dataclass
class Tok:
    kind: str
    text: str


def tokenize(expr: str) -> List[Tok]:
    out: List[Tok] = []
    pos = 0
    expr = expr.replace("\r", " ")
    while pos < len(expr):
        m = _TOKEN.match(expr, pos)
        if not m:
            out.append(Tok("other", expr[pos])); pos += 1; continue
        pos = m.end()
        kind = m.lastgroup or "other"
        if kind == "ws":
            continue
        out.append(Tok(kind, m.group(kind)))
    return out


# ── function table: Informatica name → renderer ────────────────────────────────────────────────
# Renderers return dbt-portable SQL. Where engines differ, they emit dbt cross-database macros. An argument that a macro takes
# as a *string* is wrapped in «…»; the compiler turns that into a quoted Jinja string after port names are resolved (or into a
# nested macro call when the argument is itself a macro). Missing entries are kept verbatim and reported.
_UNIT = {"DD": "day", "D": "day", "DAY": "day", "MM": "month", "MON": "month", "MONTH": "month", "YY": "year", "YYYY": "year", "Y": "year",
         "HH": "hour", "HH12": "hour", "HH24": "hour", "MI": "minute", "SS": "second", "MS": "millisecond", "US": "microsecond",
         "DOW": "dayofweek", "DOY": "dayofyear", "WW": "week", "W": "week", "Q": "quarter"}


def _unit(a: List[str], i: int, default: str = "day") -> str:
    return _UNIT.get(a[i].strip("'").upper(), a[i].strip("'").lower()) if len(a) > i else default


def _iif(a: List[str]) -> str:
    if len(a) == 2:
        return f"CASE WHEN {a[0]} THEN {a[1]} ELSE NULL END"
    els = a[2]
    if els.startswith("CASE WHEN ") and els.endswith(" END"):
        return f"CASE WHEN {a[0]} THEN {a[1]} {els[5:-4]} END"
    return f"CASE WHEN {a[0]} THEN {a[1]} ELSE {els} END"


def _decode(a: List[str]) -> str:
    head = "" if a and a[0].upper() == "TRUE" else a[0]
    pairs = a[1:]
    whens: List[str] = []
    i = 0
    while i + 1 < len(pairs):
        whens.append(f"WHEN {pairs[i] if not head else f'{head} = {pairs[i]}'} THEN {pairs[i + 1]}"); i += 2
    return "CASE " + " ".join(whens) + (f" ELSE {pairs[-1]}" if len(pairs) % 2 == 1 else "") + " END"


def _to_date(a: List[str]) -> str:
    # a format-specific parse has no cross-database macro; the format is kept and the compiler adds a TODO to verify it on the target
    # a format-specific parse has no cross-database macro: cast (works for ISO strings) and keep the original format as a TODO
    return ("{{ dbt.safe_cast(«" + a[0] + "», dbt.type_timestamp()) }} /* TODO parse format " + a[1].replace("*/", "") + " on target */") if len(a) > 1 else "{{ dbt.safe_cast(«" + a[0] + "», dbt.type_timestamp()) }}"


def _to_char(a: List[str]) -> str:
    return ("CAST(" + a[0] + " AS {{ dbt.type_string() }}) /* TODO format " + a[1].replace("*/", "") + " on target */") if len(a) > 1 else "CAST(" + a[0] + " AS {{ dbt.type_string() }})"


def _simple(name: str):
    return lambda a: f"{name}({', '.join(a)})"


FUNCTIONS: Dict[str, Any] = {
    "IIF": _iif, "DECODE": _decode, "ISNULL": lambda a: f"({a[0]} IS NULL)", "IS_NULL": lambda a: f"({a[0]} IS NULL)",
    "NVL": _simple("COALESCE"), "TO_DATE": _to_date, "TO_CHAR": _to_char,
    "TO_INTEGER": lambda a: "{{ dbt.safe_cast(«" + a[0] + "», dbt.type_int()) }}",
    "TO_BIGINT": lambda a: "{{ dbt.safe_cast(«" + a[0] + "», dbt.type_bigint()) }}",
    "TO_DECIMAL": lambda a: "{{ dbt.safe_cast(«" + a[0] + "», dbt.type_numeric()) }}",
    "TO_FLOAT": lambda a: "{{ dbt.safe_cast(«" + a[0] + "», dbt.type_float()) }}",
    "TRUNC": lambda a: ("{{ dbt.date_trunc(" + _q1(_unit(a, 1)) + ", «" + a[0] + "») }}") if len(a) == 2 and a[1].startswith("'") else f"TRUNC({', '.join(a)})",
    "ROUND": _simple("ROUND"), "LTRIM": _simple("LTRIM"), "RTRIM": _simple("RTRIM"), "TRIM": _simple("TRIM"),
    "UPPER": _simple("UPPER"), "LOWER": _simple("LOWER"), "INITCAP": _simple("INITCAP"), "SUBSTR": _simple("SUBSTR"),
    "INSTR": lambda a: "{{ dbt.position(«" + a[1] + "», «" + a[0] + "») }}" if len(a) == 2 else f"INSTR({', '.join(a)}) /* TODO start/occurrence args */",
    "LENGTH": lambda a: "{{ dbt.length(«" + a[0] + "») }}", "CONCAT": lambda a: "{{ dbt.concat([" + ", ".join("«" + x + "»" for x in a) + "]) }}",
    "LPAD": _simple("LPAD"), "RPAD": _simple("RPAD"),
    "REPLACESTR": lambda a: "{{ dbt.replace(«" + a[1] + "», «" + a[2] + "», «" + (a[3] if len(a) > 3 else "''") + "») }}",
    "REPLACECHR": lambda a: f"TRANSLATE({a[1]}, {a[2]}, {a[3] if len(a) > 3 else chr(39) + chr(39)})",
    "REVERSE": _simple("REVERSE"), "CHR": _simple("CHR"), "ASCII": _simple("ASCII"),
    "ABS": _simple("ABS"), "CEIL": _simple("CEIL"), "FLOOR": _simple("FLOOR"), "MOD": _simple("MOD"), "POWER": _simple("POWER"),
    "SQRT": _simple("SQRT"), "SIGN": _simple("SIGN"), "EXP": _simple("EXP"), "LN": _simple("LN"), "LOG": _simple("LOG"),
    "SYSDATE": lambda a: "{{ dbt.current_timestamp() }}", "SYSTIMESTAMP": lambda a: "{{ dbt.current_timestamp() }}",
    "ADD_TO_DATE": lambda a: "{{ dbt.dateadd(" + _q1(_unit(a, 1)) + ", " + (a[2] if len(a) > 2 else "1") + ", «" + a[0] + "») }}",
    "DATE_DIFF": lambda a: "{{ dbt.datediff(«" + a[1] + "», «" + a[0] + "», " + _q1(_unit(a, 2)) + ") }}",
    "GET_DATE_PART": lambda a: f"EXTRACT({_unit(a, 1).upper()} FROM {a[0]})",
    "LAST_DAY": lambda a: "{{ dbt.last_day(«" + a[0] + "», 'month') }}",
    "DATE_COMPARE": lambda a: f"CASE WHEN {a[0]} < {a[1]} THEN -1 WHEN {a[0]} > {a[1]} THEN 1 ELSE 0 END",
    "IS_DATE": lambda a: "({{ dbt.safe_cast(«" + a[0] + "», dbt.type_timestamp()) }} IS NOT NULL)",
    "IS_NUMBER": lambda a: "({{ dbt.safe_cast(«" + a[0] + "», dbt.type_numeric()) }} IS NOT NULL)",
    "ISNUMERIC": lambda a: "({{ dbt.safe_cast(«" + a[0] + "», dbt.type_numeric()) }} IS NOT NULL)",
    "IS_SPACES": lambda a: f"(TRIM({a[0]}) = '')", "IN": lambda a: f"{a[0]} IN ({', '.join(x for x in a[1:] if x not in ('0', '1'))})" if len(a) > 2 else f"{a[0]} IN ({', '.join(a[1:])})",
    "GREATEST": _simple("GREATEST"), "LEAST": _simple("LEAST"), "MD5": lambda a: "{{ dbt.hash(«" + a[0] + "») }}",
    "SUM": _simple("SUM"), "COUNT": _simple("COUNT"), "AVG": _simple("AVG"), "MIN": _simple("MIN"), "MAX": _simple("MAX"),
    "MEDIAN": _simple("MEDIAN"), "STDDEV": _simple("STDDEV"), "VARIANCE": _simple("VARIANCE"),
    "FIRST": lambda a: f"MIN({a[0]}) /* TODO FIRST() is order-dependent */", "LAST": lambda a: f"MAX({a[0]}) /* TODO LAST() is order-dependent */",
    "REG_EXTRACT": lambda a: f"REGEXP_SUBSTR({', '.join(a)}) /* TODO regex dialect */", "REG_MATCH": lambda a: f"REGEXP_LIKE({a[0]}, {a[1]}) /* TODO regex dialect */",
    "REG_REPLACE": lambda a: f"REGEXP_REPLACE({', '.join(a)}) /* TODO regex dialect */",
    "CHOOSE": lambda a: "CASE " + " ".join(f"WHEN {a[0]} = {i} THEN {v}" for i, v in enumerate(a[1:], 1)) + " END",
    "INDEXOF": lambda a: "CASE " + " ".join(f"WHEN {a[0]} = {v} THEN {i}" for i, v in enumerate(a[1:], 1)) + " ELSE 0 END",
    "IS_INTEGER": lambda a: "({{ dbt.safe_cast(«" + a[0] + "», dbt.type_int()) }} IS NOT NULL)",
    "SET_DATE_PART": lambda a: f"{a[0]} /* TODO SET_DATE_PART */", "MAKE_DATE_TIME": lambda a: f"MAKE_TIMESTAMP({', '.join(a)}) /* TODO verify on target */",
    "TO_TIMESTAMP": _simple("TO_TIMESTAMP"), "DATE_TRUNC": _simple("DATE_TRUNC"), "SOUNDEX": _simple("SOUNDEX"),
}


def _q1(s: str) -> str:
    return "'" + s + "'"


# functions that are flow control, not values: the classifier flags them; here they become their pass-through argument
FLOW_CONTROL = {"SETVARIABLE": lambda a: a[1] if len(a) > 1 else "NULL", "SETMAXVARIABLE": lambda a: a[1] if len(a) > 1 else "NULL",
                "SETMINVARIABLE": lambda a: a[1] if len(a) > 1 else "NULL", "SETCOUNTVARIABLE": lambda a: "NULL",
                "ERROR": lambda a: "NULL", "ABORT": lambda a: "NULL"}
CONSTANTS = {"TRUE": "TRUE", "FALSE": "FALSE", "NULL": "NULL", "SYSDATE": "{{ dbt.current_timestamp() }}", "SYSTIMESTAMP": "{{ dbt.current_timestamp() }}", "DD_INSERT": "0", "DD_UPDATE": "1", "DD_DELETE": "2", "DD_REJECT": "3"}
OPS = {"!=": "<>", "||": "||", "=": "=", "<>": "<>", "<": "<", ">": ">", "<=": "<=", ">=": ">=", "+": "+", "-": "-", "*": "*", "/": "/", "%": "%"}
WORD_OPS = {"AND", "OR", "NOT", "IS", "IN", "LIKE", "BETWEEN", "CASE", "WHEN", "THEN", "ELSE", "END", "DISTINCT"}


@dataclass
class Translation:
    sql: str
    unknown: List[str] = field(default_factory=list)       # function names kept verbatim
    flow_control: List[str] = field(default_factory=list)  # SETVARIABLE / ERROR / ABORT seen
    params: List[str] = field(default_factory=list)        # $$PARAM references → dbt vars
    lookups: List[str] = field(default_factory=list)       # :LKP.name unconnected lookup calls


class _Parser:
    def __init__(self, toks: List[Tok], t: Translation):
        self.toks, self.i, self.t = toks, 0, t

    def peek(self, k: int = 0) -> Optional[Tok]:
        j = self.i + k
        return self.toks[j] if j < len(self.toks) else None

    def take(self) -> Tok:
        tok = self.toks[self.i]; self.i += 1; return tok

    def parse_until(self, stop: Tuple[str, ...]) -> str:
        """Render tokens as SQL until a top-level stop token (',' or ')'). Function calls are parsed recursively."""
        parts: List[str] = []
        while self.peek() is not None:
            tok = self.peek()
            if tok.kind == "op" and tok.text in stop:
                break
            self.take()
            if tok.kind == "ident" and self.peek() is not None and self.peek().kind == "op" and self.peek().text == "(":
                if tok.text.upper() == "IN" and parts and parts[-1] not in OPS.values() and parts[-1] not in WORD_OPS:
                    parts.append("IN"); continue                       # SQL operator form: <operand> IN (...)
                parts.append(self.call(tok.text)); continue
            if tok.kind == "lkp":
                name = tok.text[5:]; self.t.lookups.append(name)
                if self.peek() is not None and self.peek().text == "(":
                    self.take(); args = self.args()
                    parts.append(f"/* TODO unconnected lookup {name}({', '.join(args)}) */ NULL"); continue
            if tok.kind == "param":
                self.t.params.append(tok.text[2:]); parts.append("{{ var('" + tok.text[2:] + "') }}"); continue
            if tok.kind == "op":
                if tok.text == "(":
                    inner = self.parse_until((")",))
                    if self.peek() is not None and self.peek().text == ")": self.take()
                    parts.append(f"({inner})"); continue
                parts.append(OPS.get(tok.text, tok.text)); continue
            if tok.kind == "ident":
                up = tok.text.upper()
                if up in CONSTANTS: parts.append(CONSTANTS[up]); continue
                if up in WORD_OPS: parts.append(up); continue
                parts.append(tok.text); continue
            if tok.kind == "str" and "$$" in tok.text:
                def _v(mm):
                    self.t.params.append(mm.group(1)); return "{{ var('" + mm.group(1) + "') }}"
                parts.append(re.sub(r"\$\$([A-Za-z_][A-Za-z0-9_]*)", _v, tok.text)); continue
            parts.append(tok.text)
        return _join(parts)

    def args(self) -> List[str]:
        out: List[str] = []
        if self.peek() is not None and self.peek().text == ")":
            self.take(); return out
        while True:
            out.append(self.parse_until((",", ")")))
            nxt = self.peek()
            if nxt is None: break
            self.take()
            if nxt.text == ")": break
        return out

    def call(self, name: str) -> str:
        self.take()                                   # '('
        a = self.args()
        up = name.upper()
        if up in FUNCTIONS:
            try: return FUNCTIONS[up](a)
            except IndexError: self.t.unknown.append(up); return f"{name}({', '.join(a)})"
        if up in FLOW_CONTROL:
            self.t.flow_control.append(up); return FLOW_CONTROL[up](a)
        self.t.unknown.append(up)
        return f"{name}({', '.join(a)})"


def _join(parts: List[str]) -> str:
    s = " ".join(parts)
    s = re.sub(r"\(\s+", "(", s); s = re.sub(r"\s+\)", ")", s); s = re.sub(r"\s+,", ",", s)
    s = re.sub(r"(^|[(,]\s*)-\s+(?=[\w(])", r"\1-", s)          # unary minus: "( - 7" → "(-7", ", - 7" → ", -7"
    return re.sub(r"\s{2,}", " ", s).strip()


def to_portable_sql(expr: str) -> Translation:
    t = Translation(sql="")
    if not expr or not expr.strip():
        return t
    p = _Parser(tokenize(expr), t)
    t.sql = p.parse_until(())
    t.unknown = sorted(set(t.unknown)); t.flow_control = sorted(set(t.flow_control)); t.params = sorted(set(t.params)); t.lookups = sorted(set(t.lookups))
    return t


def to_dialect(sql: str, dialect: str) -> Tuple[str, Optional[str]]:
    """Portable (Snowflake-flavoured) SQL expression → target dialect. Returns (sql, error). dialect: athena|snowflake|databricks|clickhouse|…"""
    if not sql:
        return sql, None
    try:
        import sqlglot
        write = {"athena": "trino", "databricks": "databricks", "snowflake": "snowflake", "clickhouse": "clickhouse", "spark": "spark",
                 "trino": "trino", "duckdb": "duckdb", "bigquery": "bigquery", "redshift": "redshift", "postgres": "postgres"}.get(dialect.lower(), dialect.lower())
        if write == "snowflake":
            return sql, None
        out = sqlglot.transpile(sql, read="snowflake", write=write, pretty=False)
        return (out[0] if out else sql), None
    except Exception as exc:  # noqa: BLE001
        return sql, str(exc)[:200]


def finalize_macro_args(sql: str) -> str:
    """«expr» → a Jinja string argument ('expr' with quotes escaped), or the bare inner expression when it is itself a macro
    call (so macros nest as dbt.datediff(dbt.dateadd(...), ...)). Innermost first."""
    def one(mm):
        inner = mm.group(1).strip()
        if inner.startswith("{{") and inner.endswith("}}") and inner.count("{{") == 1:
            return inner[2:-2].strip()
        # a string argument that itself contains {{ var('X') }} must stay a Jinja expression, or the var would not render
        parts = re.split(r"\{\{\s*var\('([A-Za-z_][A-Za-z0-9_]*)'\)\s*\}\}", inner)
        lit = lambda x: "'" + x.replace("\\", "\\\\").replace("'", "\\'") + "'"
        if len(parts) == 1:
            return lit(inner)
        out = []
        for i, part in enumerate(parts):
            if i % 2 == 0:
                if part: out.append(lit(part))
            else:
                out.append(f"var('{part}')")
        return " ~ ".join(out)
    prev = None
    while prev != sql:
        prev = sql; sql = re.sub(r"«([^«»]*)»", one, sql)
    return sql
