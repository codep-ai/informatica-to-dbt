"""Provenance headers on every generated artefact; drift verification (edited / stale); determinism (two runs byte-identical
given the same rendered_at)."""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from informatica_to_dbt import parse_export_dir  # noqa: E402
from informatica_to_dbt.project import write_project  # noqa: E402
from informatica_to_dbt.provenance import parse_header, source_hash, verify  # noqa: E402

FIXDIR = ROOT / "sample_exports"


@pytest.fixture()
def project(tmp_path):
    folders = parse_export_dir(FIXDIR)
    out = tmp_path / "p"
    summ = write_project(folders, out, project_name="prov", profile="prov", iceberg=False, rendered_at="2026-10-08T00:00:00Z")
    return out, folders, summ


def test_every_artefact_has_a_header(project):
    out, folders, _ = project
    files = [p for pat in ("**/*.sql", "**/*.sql.skipped", "**/*.yml", "**/*.py", "**/CONVERSION.md") for p in out.glob(pat)
             if p.name not in ("dbt_project.yml", "profiles.yml")]
    assert files
    for f in files:
        h = parse_header(f.read_text(encoding="utf-8"))
        assert h, f"no provenance header: {f}"
        assert h["rendered_at"] == "2026-10-08T00:00:00Z" and h["template_id"].startswith("informatica.") and len(h["source_hash"]) == 64
    exports = {f.name: source_hash([Path(x) for x in f.source_files]) for f in folders}
    rows = verify(out, exports)
    assert rows and all(r.status in ("clean", "no-header") for r in rows), [(r.path, r.status) for r in rows if r.status != "clean"]
    assert all(r.path.endswith((".csv",)) or r.status == "clean" for r in rows if r.status != "no-header")


def test_hand_edit_is_detected_and_stale_is_detected(project, tmp_path):
    out, folders, _ = project
    m = sorted(out.glob("models/*/*.sql"))[0]
    m.write_text(m.read_text(encoding="utf-8") + "\n-- engineer touched this\n", encoding="utf-8")
    exports = {f.name: source_hash([Path(x) for x in f.source_files]) for f in folders}
    by = {r.path: r for r in verify(out, exports)}
    assert by[str(m.relative_to(out))].status == "edited"
    # a changed export → everything from that folder is stale
    exports2 = dict(exports); first = next(iter(exports2)); exports2[first] = "0" * 64
    statuses = {r.status for r in verify(out, exports2) if r.object.startswith(first + "/")}
    assert statuses <= {"stale", "stale+edited"} and statuses


def test_two_runs_are_byte_identical(tmp_path):
    folders = parse_export_dir(FIXDIR)
    a, b = tmp_path / "a", tmp_path / "b"
    write_project(folders, a, project_name="d", profile="d", iceberg=False, rendered_at="2026-10-08T00:00:00Z")
    write_project(folders, b, project_name="d", profile="d", iceberg=False, rendered_at="2026-10-08T00:00:00Z")
    fa = sorted(p.relative_to(a) for p in a.rglob("*") if p.is_file())
    fb = sorted(p.relative_to(b) for p in b.rglob("*") if p.is_file())
    assert fa == fb
    for rel in fa:
        assert (a / rel).read_bytes() == (b / rel).read_bytes(), rel
