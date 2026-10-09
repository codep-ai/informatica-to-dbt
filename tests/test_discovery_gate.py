"""Discovery gate: questions from config (code default offline), answers validated — every required question needs a human value,
'default' only where allowed, choices enforced — written with who/when/hash; convert refuses without an accepted gate."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from informatica_to_dbt.discovery_gate import GateError, accept, questions, status, write_answers  # noqa: E402


@pytest.fixture(autouse=True)
def _offline(monkeypatch):
    from informatica_to_dbt import discovery_gate
    monkeypatch.setattr(discovery_gate, "_load_config", lambda: {})


def _good():
    return {"target_engine": "athena", "parity_policy": "row counts + checksums, full data, 0 tolerance", "cutover_policy": "2 weeks parallel, CDO signs, 7-day rollback",
            "todo_owner": "Jane Doe", "pii_columns": "CUSTOMER.EMAIL mask; CUSTOMER.PHONE mask", "schedule_timezone": "0 3 * * * Australia/Sydney",
            "soft_delete_missing": "default", "incremental_strategy": "merge", "environment_promotion": "git + GitHub Actions to MWAA"}


def test_questions_have_ids_and_required_flags():
    qs = questions("migration")
    assert {q["id"] for q in qs} >= {"target_engine", "parity_policy", "cutover_policy", "todo_owner", "pii_columns", "schedule_timezone"}
    assert all("q" in q and "required" in q for q in qs)


def test_accept_rejects_missing_inferred_and_bad_choice():
    with pytest.raises(GateError) as e:
        accept("migration", {k: v for k, v in _good().items() if k != "todo_owner"}, answered_by="donny")
    assert "todo_owner" in str(e.value)
    with pytest.raises(GateError) as e:
        accept("migration", {**_good(), "target_engine": "default"}, answered_by="donny")     # allow_default=false
    assert "target_engine" in str(e.value)
    with pytest.raises(GateError):
        accept("migration", {**_good(), "target_engine": "oracle"}, answered_by="donny")      # not a choice
    with pytest.raises(GateError):
        accept("migration", _good(), answered_by="")                                           # a human must be named


def test_write_and_status(tmp_path):
    rec = accept("migration", _good(), answered_by="donny")
    p = write_answers(tmp_path, rec)
    assert p.name == "gate.json" and json.loads(p.read_text())["answered_by"] == "donny"
    s = status(tmp_path)
    assert s["gate_answered"] is True and s["scope"] == "migration" and s["answered_by"] == "donny"
    p.write_text(p.read_text().replace("Jane Doe", "Someone Else"))
    assert status(tmp_path)["gate_answered"] is False       # tampered: hash no longer matches
    assert status(tmp_path / "nowhere")["gate_answered"] is False


def test_convert_refuses_without_gate(tmp_path, monkeypatch):
    from informatica_to_dbt.__main__ import main
    from informatica_to_dbt import discovery_gate
    monkeypatch.setattr(discovery_gate, "require_for_convert", lambda: True)
    rc = main(["convert", str(ROOT / "sample_exports" / "SALES_DW.xml"), "--out", str(tmp_path / "p")])
    assert rc == 3 and not (tmp_path / "p" / "dbt_project.yml").exists()
    rc = main(["convert", str(ROOT / "sample_exports" / "SALES_DW.xml"), "--out", str(tmp_path / "p"), "--no-gate"])
    assert rc == 0 and (tmp_path / "p" / "dbt_project.yml").exists()
    write_answers(tmp_path / "g", accept("migration", _good(), answered_by="donny"))
    rc = main(["convert", str(ROOT / "sample_exports" / "SALES_DW.xml"), "--out", str(tmp_path / "p2"), "--gate", str(tmp_path / "g" / ".datapai" / "gate.json")])
    assert rc == 0 and (tmp_path / "p2" / ".datapai" / "gate.json").exists()
