"""
provenance — every generated artefact carries a header that says where it came from, and `verify` tells you whether it
has been hand-edited since or is stale against its source. The ADOP pattern (spec → template → artefact with a provenance
header and a drift validator), applied to converters whose "spec" is the source system's export.

Header (first lines of the file; `--` for SQL, `#` for YAML / Python / shell):

    -- datapai:source_hash=<sha256 of the export file(s)>      what the model was compiled from
    -- datapai:object=<folder>/<mapping>/<target>              the PowerCenter object
    -- datapai:template_id=<renderer family>                    e.g. informatica.model, informatica.sources, informatica.airflow_dag
    -- datapai:converter_version=<package version>
    -- datapai:rendered_at=<run start, UTC ISO>                 run START, not now(): one run → one timestamp
    -- datapai:content_hash=<sha256 of everything below the header>

`content_hash` is what makes drift detection cheap: no re-render is needed to know a file was edited. A hand edit is
legitimate (an engineer resolved a TODO) but must be *declared*; `verify` is the review list. A changed `source_hash`
against the current export means the artefact is stale and must be regenerated.
"""
from __future__ import annotations
import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

PREFIX = "datapai:"
NOTEBOOK_MARKER = "# Databricks notebook source"
_COMMENT = {".sql": "--", ".skipped": "--", ".yml": "#", ".yaml": "#", ".py": "#", ".sh": "#", ".md": "<!--"}
_FIELDS = ("source_hash", "object", "template_id", "converter_version", "rendered_at", "content_hash")


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    return sha256_bytes(text.encode("utf-8"))


def source_hash(paths: Iterable[Path]) -> str:
    """One hash over the export file(s) a folder was parsed from, in sorted path order (stable across machines)."""
    h = hashlib.sha256()
    for p in sorted(Path(x) for x in paths):
        h.update(p.name.encode("utf-8")); h.update(b"\0"); h.update(p.read_bytes()); h.update(b"\0")
    return h.hexdigest()


def converter_version() -> str:
    try:
        from . import __version__  # type: ignore
        return str(__version__)
    except Exception:  # noqa: BLE001
        return "0.0.0"


def run_started_at() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


@dataclass
class Provenance:
    source_hash: str
    object: str
    template_id: str
    rendered_at: str
    converter_version: str = field(default_factory=converter_version)

    def render(self, body: str, suffix: str) -> str:
        """Header + body. The header's content_hash is over `body` exactly as written after the header."""
        c = _COMMENT.get(suffix, "#")
        close = " -->" if c == "<!--" else ""
        vals = {"source_hash": self.source_hash, "object": self.object, "template_id": self.template_id,
                "converter_version": self.converter_version, "rendered_at": self.rendered_at, "content_hash": sha256_text(body)}
        head = "\n".join(f"{c} {PREFIX}{k}={vals[k]}{close}" for k in _FIELDS)
        return head + "\n" + body


def parse_header(text: str) -> Optional[Dict[str, str]]:
    """The provenance fields if the file starts with a datapai header, else None. Returns also 'body' (text after the header)."""
    lines = text.split("\n")
    out: Dict[str, str] = {}
    n = 0
    if lines and lines[0].strip() == NOTEBOOK_MARKER:        # Databricks notebooks must start with their marker; the header follows it
        lines = lines[1:]; n = 0
    for ln in lines[: len(_FIELDS) + 2]:
        m = re.match(r"^\s*(?:--|#|<!--)\s*" + re.escape(PREFIX) + r"([a-z_]+)=(.*?)(?:\s*-->)?\s*$", ln)
        if not m:
            break
        out[m.group(1)] = m.group(2).strip(); n += 1
    if not out or any(k not in out for k in _FIELDS):
        return None
    out["body"] = "\n".join(lines[n:])
    return out


@dataclass
class DriftRow:
    path: str
    status: str            # clean | edited | stale | stale+edited | no-header
    object: str = ""
    template_id: str = ""
    rendered_at: str = ""
    detail: str = ""


def verify(project_dir: Path, exports: Optional[Dict[str, str]] = None, patterns: Iterable[str] = ("**/*.sql", "**/*.sql.skipped", "**/*.yml", "**/*.py")) -> List[DriftRow]:
    """Walk the generated project. `exports` maps object prefix (folder name) → current source_hash; when given, artefacts whose
    header hash differs are `stale`. Files without a header are reported (seeds CSV and README are expected to have none)."""
    rows: List[DriftRow] = []
    project_dir = Path(project_dir)
    seen = set()
    for pat in patterns:
        for f in sorted(project_dir.glob(pat)):
            if f in seen or "target" in f.parts or "dbt_packages" in f.parts:
                continue
            seen.add(f)
            text = f.read_text(encoding="utf-8", errors="replace")
            h = parse_header(text)
            rel = str(f.relative_to(project_dir))
            if h is None:
                rows.append(DriftRow(rel, "no-header")); continue
            edited = sha256_text(h["body"]) != h["content_hash"]
            folder = h["object"].split("/", 1)[0]
            stale = bool(exports) and folder in exports and exports[folder] != h["source_hash"]
            status = {(False, False): "clean", (True, False): "edited", (False, True): "stale", (True, True): "stale+edited"}[(edited, stale)]
            rows.append(DriftRow(rel, status, h["object"], h["template_id"], h["rendered_at"],
                                 "content differs from header content_hash" if edited else ("export changed since render" if stale else "")))
    return rows


def render_drift_md(rows: List[DriftRow]) -> str:
    from collections import Counter
    c = Counter(r.status for r in rows)
    lines = [f"# drift — {len(rows)} files · " + " · ".join(f"{k} {v}" for k, v in sorted(c.items())), "", "| file | status | object | template | rendered_at |", "|---|---|---|---|---|"]
    for r in rows:
        if r.status != "clean":
            lines.append(f"| {r.path} | **{r.status}** | {r.object} | {r.template_id} | {r.rendered_at} |")
    if all(r.status in ("clean", "no-header") for r in rows):
        lines.append("| — | all generated files clean | | | |")
    return "\n".join(lines) + "\n"
