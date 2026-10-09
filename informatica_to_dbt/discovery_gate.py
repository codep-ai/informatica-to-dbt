"""
discovery_gate — the human-in-the-loop gate before any project is written for a customer (ADOP plan W2).

The rule ADOP states in prose, as code: the agent may profile and PRESENT, it never INFERS the answers that decide a migration
or a pipeline (dedup key, null policy, PII, schedule + timezone, parity engine, cut-over policy, TODO owner, promotion path).
A human answers; the answers are validated against the question list, written with who / when / a hash, and the writers
refuse to run without them (`convert` CLI: `--gate answers.json`, or `--no-gate` which is explicit and logged).

Questions per scope come from `sys_common_config` (config_type='gate', key 'questions.<scope>') with the same list as the
code default, so a deployment can change the questions without a release. Cedar control AG-002 (`deploy` needs
`gate_answered`) reads `status(project_dir)`.
"""
from __future__ import annotations
import hashlib
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

log = logging.getLogger(__name__)
GATE_DIR = ".datapai"
GATE_FILE = "gate.json"


class GateError(ValueError):
    """The answers do not satisfy the gate. The message names every failing question."""


_DEFAULT_QUESTIONS: Dict[str, List[Dict[str, Any]]] = {
    "migration": [
        {"id": "target_engine", "q": "Target engine and dbt adapter for the converted estate", "required": True, "allow_default": False,
         "choices": ["athena", "redshift", "snowflake", "databricks", "clickhouse"]},
        {"id": "parity_policy", "q": "Parity acceptance per target table: row counts + checksums on full data, or sampled? Tolerances?", "required": True, "allow_default": False},
        {"id": "cutover_policy", "q": "Cut-over policy: parallel run length, who signs off, rollback window", "required": True, "allow_default": False},
        {"id": "todo_owner", "q": "Named owner (person) for every remaining TODO after conversion", "required": True, "allow_default": False},
        {"id": "pii_columns", "q": "Which columns are personal data (confirm or amend the detected list) and the handling rule", "required": True, "allow_default": False},
        {"id": "schedule_timezone", "q": "Schedule for the generated DAGs and the IANA timezone (never inferred)", "required": True, "allow_default": False},
        {"id": "soft_delete_missing", "q": "Update Strategy targets: soft-delete keys missing from the source?", "required": True, "allow_default": True, "choices": ["true", "false"]},
        {"id": "incremental_strategy", "q": "Incremental strategy for merge targets", "required": False, "allow_default": True, "choices": ["merge", "delete+insert", "append"]},
        {"id": "environment_promotion", "q": "How generated artefacts are promoted (git + CI/CD); agents never deploy to production", "required": True, "allow_default": False},
    ],
    "pipeline": [
        {"id": "zone", "q": "Which zone(s)", "required": True, "allow_default": False, "choices": ["bronze", "silver", "gold", "all"]},
        {"id": "primary_key", "q": "Primary key / dedup key per table (never inferred from column names)", "required": True, "allow_default": False},
        {"id": "null_policy", "q": "Null handling per critical column", "required": True, "allow_default": False},
        {"id": "pii_columns", "q": "Personal-data columns and handling", "required": True, "allow_default": False},
        {"id": "quality_thresholds", "q": "Quality thresholds per dimension and blocking rules", "required": True, "allow_default": True},
        {"id": "schedule_timezone", "q": "Schedule cron and IANA timezone", "required": True, "allow_default": False},
        {"id": "transformations", "q": "Derived columns, calculations, custom business logic (ask even when none seem needed)", "required": True, "allow_default": False},
        {"id": "consumers", "q": "Who consumes the output and at what grain", "required": True, "allow_default": True},
    ],
}
_DEFAULT_ANSWER_TOKENS = {"default", "use defaults", "defaults"}

_CACHE: Dict[str, str] = {}; _CACHE_TS = 0.0


def _load_config() -> Dict[str, str]:
    """config_type='gate' rows from sys_common_config; {} when the DB is unreachable (code defaults apply). Cached 5 min."""
    global _CACHE, _CACHE_TS
    if _CACHE and time.time() - _CACHE_TS < 300:
        return _CACHE
    try:
        import psycopg2
        try:
            from core.db import framework_conn_kwargs
            conn = psycopg2.connect(**framework_conn_kwargs(connect_timeout=3))
        except Exception:  # noqa: BLE001
            conn = psycopg2.connect(host=os.environ.get("FRAMEWORK_DB_HOST", "127.0.0.1"), port=int(os.environ.get("FRAMEWORK_DB_PORT", "5433")),
                                    dbname=os.environ.get("FRAMEWORK_DB_NAME", "datapai_auth_db"), user=os.environ.get("FRAMEWORK_DB_USER", "postgres"),
                                    password=os.environ.get("FRAMEWORK_DB_PASSWORD", ""), connect_timeout=3)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT config_key, config_value FROM datapai.sys_common_config WHERE config_type = 'gate' "
                        "AND (effective_to IS NULL OR effective_to > now())")
            _CACHE = {k: v for k, v in cur.fetchall()}
        conn.close()
    except Exception as exc:  # noqa: BLE001
        log.info("discovery_gate: DB config unavailable (%s); using code defaults", exc)
        _CACHE = {}
    _CACHE_TS = time.time()
    return _CACHE


def questions(scope: str = "migration") -> List[Dict[str, Any]]:
    raw = _load_config().get(f"questions.{scope}")
    if raw:
        try:
            return json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            log.warning("discovery_gate: bad JSON in sys_common_config gate/questions.%s (%s); code default used", scope, exc)
    if scope not in _DEFAULT_QUESTIONS:
        raise GateError(f"unknown gate scope {scope!r} (known: {sorted(_DEFAULT_QUESTIONS)})")
    return _DEFAULT_QUESTIONS[scope]


def require_for_convert() -> bool:
    v = (_load_config().get("require_for_convert") or os.environ.get("DATAPAI_GATE_REQUIRE_FOR_CONVERT") or "true").strip().lower()
    return v in ("1", "true", "yes")


def accept(scope: str, answers: Dict[str, Any], *, answered_by: str, notes: str = "") -> Dict[str, Any]:
    """Validate human answers against the scope's questions. Raises GateError naming every failing question. Returns the record."""
    if not (answered_by or "").strip():
        raise GateError("answered_by: a named human must own the answers")
    qs = questions(scope); problems: List[str] = []; clean: Dict[str, Any] = {}
    for q in qs:
        v = answers.get(q["id"])
        sval = "" if v is None else str(v).strip()
        if not sval:
            if q.get("required", True): problems.append(f"{q['id']}: required, not answered — {q['q']}")
            continue
        if sval.lower() in _DEFAULT_ANSWER_TOKENS:
            if not q.get("allow_default", False): problems.append(f"{q['id']}: 'default' is not an acceptable answer — a human must decide")
            else: clean[q["id"]] = "default"
            continue
        if q.get("choices") and sval.lower() not in [c.lower() for c in q["choices"]]:
            problems.append(f"{q['id']}: {sval!r} is not one of {q['choices']}")
            continue
        clean[q["id"]] = sval
    unknown = sorted(set(answers) - {q["id"] for q in qs})
    if unknown:
        problems.append(f"unknown question ids: {unknown}")
    if problems:
        raise GateError("gate not satisfied:\n  - " + "\n  - ".join(problems))
    rec = {"scope": scope, "answers": clean, "answered_by": answered_by.strip(), "answered_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
           "notes": notes, "questions_hash": hashlib.sha256(json.dumps(qs, sort_keys=True).encode()).hexdigest()}
    rec["hash"] = _hash(rec)
    return rec


def _hash(rec: Dict[str, Any]) -> str:
    body = {k: v for k, v in rec.items() if k != "hash"}
    return hashlib.sha256(json.dumps(body, sort_keys=True).encode()).hexdigest()


def write_answers(project_dir: Path, rec: Dict[str, Any]) -> Path:
    d = Path(project_dir) / GATE_DIR; d.mkdir(parents=True, exist_ok=True)
    p = d / GATE_FILE
    p.write_text(json.dumps(rec, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return p


def load_answers(path: Path) -> Dict[str, Any]:
    rec = json.loads(Path(path).read_text(encoding="utf-8"))
    if rec.get("hash") != _hash(rec):
        raise GateError(f"{path}: answers file has been modified after acceptance (hash mismatch)")
    return rec


def status(project_dir: Path) -> Dict[str, Any]:
    """What Cedar AG-002 and the writers consult: {gate_answered, scope, answered_by, answered_at, path}."""
    p = Path(project_dir) / GATE_DIR / GATE_FILE
    if not p.exists():
        return {"gate_answered": False, "path": str(p), "reason": "no gate answers file"}
    try:
        rec = load_answers(p)
    except Exception as exc:  # noqa: BLE001
        return {"gate_answered": False, "path": str(p), "reason": str(exc)[:200]}
    return {"gate_answered": True, "path": str(p), "scope": rec["scope"], "answered_by": rec["answered_by"], "answered_at": rec["answered_at"]}
