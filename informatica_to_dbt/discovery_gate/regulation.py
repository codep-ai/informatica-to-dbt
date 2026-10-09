"""
Regulation packs for the discovery gate (ADOP plan W5): the ADOP "regulation prompt" idea, for the regulators our beachhead
answers to. A pack adds required questions and handling rules to a gate scope; the answers travel with the project and feed the
compliance assessment's evidence. Packs come from sys_common_config (`gate`/`pack.<CODE>`) with identical code defaults.

Each pack is a plain statement of what the regulation requires of the DATA that a migration or pipeline touches — not legal
advice, and not a claim that the platform makes a customer compliant. Citations are to the public instruments.
"""
from __future__ import annotations
import json
from typing import Any, Dict, List

PACKS: Dict[str, Dict[str, Any]] = {
    "APRA_CPS_234": {
        "title": "APRA CPS 234 Information Security (effective 1 July 2019; CPG 234 guidance)",
        "applies_to": "APRA-regulated entities: ADIs, insurers, superannuation RSE licensees, and their service providers' handling of information assets",
        "requires": [
            "Information assets classified by criticality and sensitivity (para 20): every converted target table and generated artefact is an information asset",
            "Controls commensurate with classification, tested systematically (paras 21, 27): access control on PII-classified columns; test evidence retained",
            "Third-party/service-provider information security assessed (para 22): the platform runs in the entity's own account; no data leaves it",
            "Incident notification to APRA within 72 hours of a material incident (para 35) and control weaknesses within 10 business days (para 36)",
        ],
        "questions": [
            {"id": "cps234_asset_classification", "q": "Classification scheme (criticality × sensitivity) to tag every target table with, and who owns the register", "required": True, "allow_default": False},
            {"id": "cps234_access_control", "q": "Access control rule per sensitivity class (who may query PII-classified columns; break-glass process)", "required": True, "allow_default": False},
            {"id": "cps234_incident_contact", "q": "Incident contact and the 72-hour notification path for a data-security incident in the migrated estate", "required": True, "allow_default": False},
        ],
        "handling": ["Column classification produced on every build is written to the information-asset register (lineage + PII map)",
                     "Guardrail and gate decisions kept per run as control-testing evidence"],
    },
    "APRA_CPG_235": {
        "title": "APRA CPG 235 Managing Data Risk (September 2013 guidance)",
        "applies_to": "APRA-regulated entities; the data-risk expectations for any data platform change, including a migration",
        "requires": [
            "Data quality managed across accuracy, completeness, consistency, timeliness, availability and fitness for purpose (para 12)",
            "Data lineage / traceability from source to report, including transformations (paras 29-32)",
            "Validation and reconciliation when data moves between systems (paras 33-36): the parity run is this control for a migration",
            "Clear ownership of data and of data-quality issues (paras 16-19)",
        ],
        "questions": [
            {"id": "cpg235_data_owner", "q": "Named data owner per subject area for the migrated tables (not the project team)", "required": True, "allow_default": False},
            {"id": "cpg235_quality_rules", "q": "Data-quality rules to carry over or add (which Informatica validations become dbt tests; thresholds that block)", "required": True, "allow_default": True},
            {"id": "cpg235_reconciliation", "q": "Reconciliation approach and sign-off for the parallel run (counts, sums, key checksums, sample tolerance)", "required": True, "allow_default": False},
        ],
        "handling": ["Cross-engine column lineage is the traceability evidence", "Parity results stored per table per run"],
    },
    "AU_PRIVACY_ACT": {
        "title": "Privacy Act 1988 (Cth) — Australian Privacy Principles (APPs), incl. 2024 amendments",
        "applies_to": "APP entities handling personal information; personal data in source systems being migrated or onboarded",
        "requires": [
            "APP 6: personal information used only for the purpose collected (or a permitted secondary purpose) — a new analytical use is a purpose decision",
            "APP 11.1: reasonable steps to protect personal information from misuse, loss, unauthorised access (security, access control, encryption)",
            "APP 11.2: destroy or de-identify when no longer needed — retention decided, not inherited from the legacy platform",
            "APP 8: cross-border disclosure accountability — inference and storage region is a disclosure question (au.* profiles keep inference in Australia)",
            "Notifiable Data Breaches scheme: eligible breaches notified to OAIC and individuals",
        ],
        "questions": [
            {"id": "app_personal_info_inventory", "q": "Which columns are personal information (and which are sensitive information under s6); confirm the detected list", "required": True, "allow_default": False},
            {"id": "app_purpose", "q": "Primary purpose of collection for each personal-data table and whether the new use is within it", "required": True, "allow_default": False},
            {"id": "app_retention", "q": "Retention / de-identification rule per personal-data table (APP 11.2)", "required": True, "allow_default": False},
            {"id": "app_region", "q": "Permitted storage and inference regions (APP 8); default au.* Bedrock profiles, S3 ap-southeast-2", "required": True, "allow_default": True},
        ],
        "handling": ["PII columns masked/tokenised per the answer before any Gold or AI use", "Guardrail PII anonymisation on model output stays on"],
    },
    "AUSTRAC_AML_CTF": {
        "title": "Anti-Money Laundering and Counter-Terrorism Financing Act 2006 — record keeping and reporting",
        "applies_to": "Reporting entities (ADIs, remitters, CFD/FX brokers, casinos…); transaction and customer-identification data",
        "requires": [
            "Records of transactions and customer identification retained for 7 years (Part 10, ss 106-116) — a migration may not shorten retention",
            "Transaction monitoring program and reporting (TTR, IFTI, SMR) — monitoring logic that lives in ETL must be identified and carried over, never silently dropped",
            "Reports must be reproducible from retained data: lineage from report to source transactions",
        ],
        "questions": [
            {"id": "amlctf_retention", "q": "Retention rule for transaction and KYC tables after migration (≥ 7 years) and where the archive lives", "required": True, "allow_default": False},
            {"id": "amlctf_monitoring_logic", "q": "Which legacy mappings/jobs implement transaction-monitoring or reporting logic (TTR/IFTI/SMR) — each must be in the parity scope", "required": True, "allow_default": False},
        ],
        "handling": ["Mappings named here are flagged 'regulatory' in the assessment and cannot be skipped by the agent"],
    },
}


def pack(code: str, config: Dict[str, str]) -> Dict[str, Any]:
    raw = config.get(f"pack.{code}")
    if raw:
        try: return json.loads(raw)
        except Exception: pass
    if code not in PACKS:
        raise KeyError(f"unknown regulation pack {code!r} (known: {sorted(PACKS)})")
    return PACKS[code]


def pack_questions(codes: List[str], config: Dict[str, str]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for c in codes:
        for q in pack(c, config).get("questions", []):
            out.append({**q, "pack": c})
    return out


def render_pack_md(code: str, config: Dict[str, str]) -> str:
    p = pack(code, config)
    lines = [f"# {p['title']}", "", f"**Applies to:** {p['applies_to']}", "", "## What it requires of the data", ""]
    lines += [f"- {r}" for r in p["requires"]]
    lines += ["", "## Gate questions added", ""] + [f"- `{q['id']}` — {q['q']}" for q in p["questions"]]
    lines += ["", "## How the platform handles it", ""] + [f"- {h}" for h in p.get("handling", [])]
    return "\n".join(lines) + "\n"
