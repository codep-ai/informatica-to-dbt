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
        assert sorted(f.name for f in folders) == ["FINANCE_DW", "SALES_DW"]

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
        ("Update Strategy", "Update Strategy", "convert"), ("Rank", "Rank", "convert"), ("Normalizer", "Normalizer", "skip"),
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
        assert not any("Update Strategy" in r for r in d["reasons"])   # DD_INSERT-only → plain table, no TODO
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
        assert len(results) == 2 and {r["folder"] for r in results} == {"SALES_DW", "FINANCE_DW"}
        md = (tmp_path / "a.md").read_text()
        assert "| Mappings | 5 |" in md and "m_FACT_INVOICE" in md and "m_ORPHAN_EXPORT" in md
        html = (tmp_path / "a.html").read_text()
        assert "PowerCenter" in html and "</body>" in html
        assert {r["folder"] for r in json.loads((tmp_path / "a.json").read_text())} == {"SALES_DW", "FINANCE_DW"}


    def test_cli_assess(self, capsys):
        from informatica_to_dbt.__main__ import main
        assert main(["assess", str(SALES)]) == 0
        out = capsys.readouterr().out
        assert "Informatica PowerCenter → dbt · assessment" in out and "m_DIM_CUSTOMER" in out


class TestWorkflows:
    def test_workflow_renders_dag(self, sales):
        from informatica_to_dbt.workflows import render_workflow
        r = render_workflow(sales, sales.workflows[0], model_names={"m_DIM_CUSTOMER": ["m_dim_customer__dim_customer"]})
        assert r["ok"], r["validation"]
        assert r["dag_id"] == "infa_wf_nightly_sales" and r["n_jobs"] == 7
        src = r["source"]
        assert "dbt build --select m_dim_customer__dim_customer" in src
        assert "cmd_archive_files" in src and "mv $PMTargetFileDir" in src
        assert ("s_m_dim_customer", "s_m_fact_order_daily", "all_success") in r["edges"]
        assert any("dec_ORDERS_LOADED" in t for t in r["todos"])          # decision task → placeholder TODO
        assert any("Post SQL" in t for t in r["todos"])                   # session post SQL flagged
        assert any("m_FACT_INVOICE" in t or "s_m_FACT_INVOICE" in t for t in r["todos"])

    def test_convert_writes_project_and_dags(self, tmp_path):
        from informatica_to_dbt.project import write_project
        summ = write_project([parse_export(SALES)], tmp_path, project_name="infa_sales_dw", iceberg=False)
        assert summ["models"] == 4 and summ["skipped"] == 1 and summ["dags"] == 1
        assert (tmp_path / "airflow" / "infa_wf_nightly_sales.py").exists()
        assert (tmp_path / "models" / "sales_dw" / "_sources.yml").exists()
        md = (tmp_path / "models" / "sales_dw" / "CONVERSION.md").read_text()
        assert "workflow wf_NIGHTLY_SALES" in md
        compile((tmp_path / "airflow" / "infa_wf_nightly_sales.py").read_text(), "dag", "exec")


class TestTodoRules:
    """2026-09-28 design rules: Update Strategy → merge with soft delete on the target key; Rank → row_number() <= N."""

    def _variant(self, old: str, new: str):
        return parse_export(SALES.read_text(encoding="utf-8").replace(old, new))

    def test_update_strategy_merge_soft_delete(self):
        from informatica_to_dbt.compiler import compile_mapping
        f = self._variant('VALUE="DD_INSERT"', 'VALUE="IIF(GROSS_AMOUNT &gt; 0, DD_UPDATE, DD_DELETE)"')
        m = f.mapping("m_FACT_ORDER_DAILY")
        assert "update_strategy_no_key" not in classify_mapping(m, f)["features"]      # target has PRIMARY KEY ports
        out = {o.target: o for o in compile_mapping(m, f)}["FACT_ORDER_DAILY"]
        assert out.materialization == "incremental" and out.unique_key == ["customer_id", "order_date"]
        for piece in ("incremental_strategy=var('infa_incremental_strategy', 'merge')", "row_hash", "is_deleted",
                      "when _dd_op = 2 then true", "coalesce(_dd_op, 0) <> 3", "t.row_hash <> s.row_hash",
                      "var('infa_soft_delete_missing', true)", "is_incremental()"):
            assert piece in out.sql, piece

    def test_update_strategy_without_key_is_flagged(self):
        f = parse_export(SALES.read_text(encoding="utf-8").replace('VALUE="DD_INSERT"', 'VALUE="DD_UPDATE"')
                         .replace('KEYTYPE="PRIMARY KEY"', 'KEYTYPE="NOT A KEY"'))
        d = classify_mapping(f.mapping("m_FACT_ORDER_DAILY"), f)
        assert "update_strategy_no_key" in d["features"] and d["decision"] == "todo"

    def test_rank_top_n(self):
        from informatica_to_dbt.compiler import compile_mapping
        rank = ('<TRANSFORMATION NAME="RNK_TOP" REUSABLE="NO" TYPE="Rank">'
                '<TRANSFORMFIELD DATATYPE="decimal" NAME="CUSTOMER_ID" PORTTYPE="INPUT/OUTPUT" EXPRESSIONTYPE="GROUPBY"/>'
                '<TRANSFORMFIELD DATATYPE="date/time" NAME="ORDER_DATE" PORTTYPE="INPUT/OUTPUT"/>'
                '<TRANSFORMFIELD DATATYPE="string" NAME="CURRENCY" PORTTYPE="INPUT/OUTPUT"/>'
                '<TRANSFORMFIELD DATATYPE="decimal" NAME="ORDER_COUNT" PORTTYPE="INPUT/OUTPUT"/>'
                '<TRANSFORMFIELD DATATYPE="decimal" NAME="GROSS_AMOUNT" PORTTYPE="INPUT/OUTPUT/MASTER"/>'
                '<TRANSFORMFIELD DATATYPE="decimal" NAME="RANKINDEX" PORTTYPE="OUTPUT"/>'
                '<TABLEATTRIBUTE NAME="Top/Bottom" VALUE="Top"/><TABLEATTRIBUTE NAME="Number Of Ranks" VALUE="3"/></TRANSFORMATION>')
        xml = SALES.read_text(encoding="utf-8")
        start = xml.index('<TRANSFORMATION DESCRIPTION="" NAME="UPD_INSERT"'); end = xml.index("</TRANSFORMATION>", start) + len("</TRANSFORMATION>")
        xml = xml[:start] + rank.replace("RNK_TOP", "UPD_INSERT") + xml[end:]
        xml = xml.replace('TRANSFORMATION_TYPE="Update Strategy"', 'TRANSFORMATION_TYPE="Rank"').replace('TOINSTANCETYPE="Update Strategy"', 'TOINSTANCETYPE="Rank"').replace('FROMINSTANCETYPE="Update Strategy"', 'FROMINSTANCETYPE="Rank"')
        f = parse_export(xml); m = f.mapping("m_FACT_ORDER_DAILY")
        assert classify_mapping(m, f)["transformation_types"].get("Rank") == 1
        out = {o.target: o for o in compile_mapping(m, f)}["FACT_ORDER_DAILY"]
        assert "row_number() over (partition by" in out.sql and "desc)" in out.sql and "<= 3" in out.sql, out.sql


class TestFinanceFixture:
    def test_union_rank_lookup_update_strategy(self):
        from informatica_to_dbt.compiler import compile_mapping
        f = parse_export(FIXTURES / "FINANCE_DW.xml")
        d = classify_mapping(f.mapping("m_GL_TOP_ACCOUNTS"))
        assert d["decision"] == "todo"
        assert {"lookup_unconnected_call", "sql_override", "dd_update", "dd_insert", "dd_reject"} <= set(d["features"])
        outs = compile_mapping(f.mapping("m_GL_TOP_ACCOUNTS"), f)
        assert len(outs) == 1 and outs[0].materialization == "incremental" and outs[0].unique_key == ["account_code", "region"]
        sql = outs[0].sql
        assert "union all" in sql and "row_number() over (partition by" in sql and "<= 5" in sql
        assert "TODO SQL override" in sql and "is_incremental()" in sql
        assert any("LKP_FX_RATE" in t for t in outs[0].todos) and "rankindex" in sql

    def test_finance_workflow_failed_link_and_timer(self):
        from informatica_to_dbt.workflows import render_workflow
        f = parse_export(FIXTURES / "FINANCE_DW.xml")
        r = render_workflow(f, f.workflows[0], model_names={"m_GL_TOP_ACCOUNTS": ["fact_gl_top_accounts"]})
        assert r["ok"] and ("s_m_gl_top_accounts", "email_on_fail", "all_failed") in r["edges"]
        assert any("Timer" in t for t in r["todos"]) and any("Pre SQL" in t for t in r["todos"])
