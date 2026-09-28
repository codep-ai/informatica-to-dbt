"""Tests for informatica_to_dbt — week 1: parser, canonical model, registry, classifier, assessment.
Fixtures: sample_exports/*.xml are SYNTHETIC exports written against the public repository DTD (no customer data)."""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from informatica_to_dbt import (assess_export, classify_mapping, parse_export, parse_export_dir,  # noqa: E402
                                          resolve_transformation, render_assessment_md)
from informatica_to_dbt.assess import assess_folder  # noqa: E402

FIXTURES = ROOT / "sample_exports"
SALES = FIXTURES / "SALES_DW.xml"


@pytest.fixture(scope="module")
def sales():
    return parse_export(SALES)


class TestParser:
    def test_folder_counts(self, sales):
        assert sales.name == "SALES_DW" and sales.repository == "REP_DATAPAI"
        s = sales.summary()
        assert s["sources"] == 4 and s["targets"] == 3 and s["reusable_transformations"] == 1
        assert s["mappings"] == 4 and s["sessions"] == 4 and s["workflows"] == 1

    def test_source_fields_and_keys(self, sales):
        cust = next(x for x in sales.sources if x.name == "CUSTOMERS")
        assert cust.dbd_name == "CRM_ORA" and cust.database_type == "Oracle"
        assert [f.name for f in cust.fields][:3] == ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME"]
        assert cust.fields[0].keytype == "PRIMARY KEY"

    def test_instances_resolve_inline_and_reusable(self, sales):
        m = sales.mapping("m_DIM_CUSTOMER")
        exp = m.instance("EXP_CLEAN"); assert exp.transformation is not None and exp.transformation.type == "Expression"
        reuse = m.instance("exp_STATUS_DESC"); assert reuse.reusable and reuse.transformation is not None
        assert reuse.transformation.reusable and "DECODE" in reuse.transformation.ports[1].expression

    def test_ports_and_attributes(self, sales):
        m = sales.mapping("m_DIM_CUSTOMER")
        lkp = m.instance("LKP_COUNTRIES").transformation
        assert lkp.attr("Lookup condition") == "COUNTRY_CODE = IN_COUNTRY_CODE"
        assert lkp.attr("Lookup policy on multiple match") == "Use First Value"
        exp = m.instance("EXP_CLEAN").transformation
        v = [p for p in exp.ports if p.is_variable]; assert [p.name for p in v] == ["v_FIRST", "v_LAST"]
        srt = m.instance("SRT_BY_ID").transformation
        assert srt.ports[0].attrs.get("Sort Key") == "YES"

    def test_connectors_and_router_groups(self, sales):
        m = sales.mapping("m_FACT_ORDER_DAILY")
        assert len(m.connectors) == 48
        rtr = m.instance("RTR_STATUS").transformation
        assert [g["name"] for g in rtr.groups] == ["CANCELLED", "DEFAULT1", "INPUT"]
        assert rtr.groups[0]["expression"] == "STATUS = 'CANCELLED'"
        assert m.transformation_types() == ["Source Qualifier", "Joiner", "Aggregator", "Router", "Update Strategy"]

    def test_sessions_workflow(self, sales):
        s = next(x for x in sales.sessions if x.name == "s_m_DIM_CUSTOMER")
        assert s.mapping_name == "m_DIM_CUSTOMER"
        assert s.connections["CUSTOMERS"]["connection"] == "CRM_ORA_PROD"
        assert s.instance_overrides["DIM_CUSTOMER"]["Truncate target table option"] == "YES"
        assert s.attributes["Parameter Filename"].endswith("sales_dw.par")
        w = sales.workflows[0]
        assert w.name == "wf_NIGHTLY_SALES" and w.scheduler["type"] == "RUN ON DEMAND"
        assert [t.name for t in w.session_tasks] == ["s_m_DIM_CUSTOMER", "s_m_FACT_ORDER_DAILY", "s_m_LEGACY_ADDRESS_NORMALIZE", "s_m_FACT_INVOICE"]
        assert any(l.condition.startswith("$s_m_DIM_CUSTOMER.Status") for l in w.links)
        assert next(t for t in w.tasks if t.name == "s_m_LEGACY_ADDRESS_NORMALIZE").enabled is False

    def test_parse_dir_merges_by_folder(self):
        folders = parse_export_dir(FIXTURES)
        assert [f.name for f in folders] == ["SALES_DW"]

    def test_parse_from_string(self):
        f = parse_export('<POWERMART><REPOSITORY NAME="R"><FOLDER NAME="F"><MAPPING NAME="m" ISVALID="YES"/></FOLDER></REPOSITORY></POWERMART>')
        assert f.name == "F" and f.mappings[0].name == "m"

    def test_not_an_export(self):
        with pytest.raises(ValueError):
            parse_export("<html/>")


class TestRegistry:
    @pytest.mark.parametrize("t,expected,status", [
        ("Expression", "Expression", "convert"), ("Source Qualifier", "Source Qualifier", "convert"),
        ("Lookup Procedure", "Lookup", "convert"), ("Sequence", "Sequence Generator", "convert"),
        ("Update Strategy", "Update Strategy", "todo"), ("Normalizer", "Normalizer", "skip"),
        ("Java Transformation", "Java", "skip"), ("Something New", "Other", "skip"), ("", "Other", "skip")])
    def test_resolve(self, t, expected, status):
        spec = resolve_transformation(t)
        assert spec.name == expected and spec.status == status


class TestClassifier:
    def test_dim_customer_convert_or_todo(self, sales):
        d = classify_mapping(sales.mapping("m_DIM_CUSTOMER"))
        assert d["decision"] in ("convert", "todo") and d["lift"] == "dbt"
        assert d["transformation_types"] == {"Source Qualifier": 1, "Expression": 2, "Filter": 1, "Lookup": 1, "Sorter": 1, "Sequence Generator": 1}
        assert "source_filter" in d["features"]
        assert not any(x.startswith("untranslated_function") for x in d["features"]), d["features"]
        assert d["n_sources"] == 1 and d["n_targets"] == 1

    def test_fact_orders_todo_reasons(self, sales):
        d = classify_mapping(sales.mapping("m_FACT_ORDER_DAILY"))
        assert d["decision"] == "todo"
        assert "aggregator_passthrough" in d["features"]       # CURRENCY / STATUS are not grouped
        assert "dd_insert" in d["features"]
        assert any("Update Strategy" in r for r in d["reasons"])
        assert d["n_targets"] == 2

    def test_legacy_skip(self, sales):
        d = classify_mapping(sales.mapping("m_LEGACY_ADDRESS_NORMALIZE"))
        assert d["decision"] == "skip" and d["lift"] == "native"
        assert "sql_override" in d["features"]
        assert any("Normalizer" in r for r in d["reasons"]) and any("Stored Procedure" in r for r in d["reasons"])

    def test_orphan_convert(self, sales):
        d = classify_mapping(sales.mapping("m_ORPHAN_EXPORT"))
        assert d["decision"] == "convert"


class TestAssessment:
    def test_folder_result_contract(self, sales):
        r = assess_folder(sales)
        for k in ("folder", "dag_id", "n_jobs", "ok", "decisions", "lift_summary", "metadata"):
            assert k in r
        assert r["n_jobs"] == 4 and r["lift_summary"]["skip"] == 1 and r["lift_summary"]["dbt"] == 3
        m = r["metadata"]
        assert m["scheduled_mappings"] == 3
        assert [x["mapping"] for x in m["missing_mappings"]] == ["m_FACT_INVOICE"]
        assert m["orphan_mappings"] == ["m_ORPHAN_EXPORT"]
        assert m["workflow_task_types"]["Session"] == 4 and m["workflow_task_types"]["Command"] == 1
        assert m["transformation_types"]["Source Qualifier"] == 5

    def test_assess_export_writes_reports(self, tmp_path):
        results = assess_export(FIXTURES, report_html=tmp_path / "a.html", report_md=tmp_path / "a.md", results_json=tmp_path / "a.json")
        assert len(results) == 1
        md = (tmp_path / "a.md").read_text()
        assert "| Mappings | 4 |" in md and "m_FACT_INVOICE" in md and "m_ORPHAN_EXPORT" in md
        html = (tmp_path / "a.html").read_text()
        assert "PowerCenter" in html and "</body>" in html
        assert json.loads((tmp_path / "a.json").read_text())[0]["folder"] == "SALES_DW"


    def test_cli_assess(self, capsys):
        from informatica_to_dbt.__main__ import main
        assert main(["assess", str(SALES)]) == 0
        out = capsys.readouterr().out
        assert "Informatica PowerCenter → dbt · assessment" in out and "m_DIM_CUSTOMER" in out
