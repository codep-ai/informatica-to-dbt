"""
parser — powrmart XML (Repository Manager / `pmrep objectexport`) → canonical model (model.py).

Facts about real exports the parser must live with (from the DTD and the public corpora):
  * `<!DOCTYPE POWERMART SYSTEM "powrmart.dtd">` — the DTD is never fetched; parsing is DTD-less and entity-safe.
  * Attribute spacing varies (`NAME="x"` and `NAME ="x"`); the XML parser normalises that.
  * A file holds one REPOSITORY with one or more FOLDERs; a folder may hold only mappings, only workflows, or everything.
  * Reusable transformations live at folder level and are referenced from mappings by INSTANCE TRANSFORMATION_NAME with
    REUSABLE="YES"; non-reusable ones are defined inline inside the MAPPING.
  * Mapplets are mappings without sources/targets (Input/Output transformations at their edges).
  * Sessions may be defined at folder level (reusable) or inline in a WORKFLOW; a TASKINSTANCE of TASKTYPE "Session" names one.
"""
from __future__ import annotations
import glob
import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Union

from .model import (Connector, Folder, Instance, Mapping, MappingVariable, Port, Session, SourceDef, TargetDef, Task,
                    TaskInstance, Transformation, Workflow, WorkflowLink)

PathLike = Union[str, Path]


def _bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return v.strip().upper() in ("YES", "TRUE", "1")


def _attrs(el: ET.Element) -> Dict[str, str]:
    """TABLEATTRIBUTE / ATTRIBUTE children → {NAME: VALUE}. Repeated names keep the last value (PowerCenter never repeats)."""
    out: Dict[str, str] = {}
    for a in el:
        if a.tag in ("TABLEATTRIBUTE", "ATTRIBUTE") and a.get("NAME") is not None:
            out[a.get("NAME", "")] = a.get("VALUE", "") or ""
    return out


_BOM = "\ufeff"
_BOM_MOJIBAKE = "\u00ef\u00bb\u00bf"      # the same three bytes when the export declares a single-byte encoding (seen: windows-1252)


def _clean(v: Optional[str]) -> str:
    """Attribute text as the engine sees it: real exports carry a UTF-8 BOM inside the first flat-file column name and inside
    expressions that reference it (Lookup condition `CustomerId = \ufeffCustomerId`)."""
    return (v or "").replace(_BOM, "")


def _port(el: ET.Element) -> Port:
    return Port(name=el.get("NAME", ""), datatype=el.get("DATATYPE", ""), precision=el.get("PRECISION", ""),
                scale=el.get("SCALE", ""), porttype=el.get("PORTTYPE", ""), expression=el.get("EXPRESSION", "") or "",
                expressiontype=el.get("EXPRESSIONTYPE", "") or "", default_value=el.get("DEFAULTVALUE", "") or "",
                keytype=el.get("KEYTYPE", "") or "", nullable=el.get("NULLABLE", "") or "",
                group=el.get("GROUP", "") or "", ref_field=el.get("REF_FIELD", "") or "",
                attrs={**({"INPUTGROUPNAME": el.get("INPUTGROUPNAME")} if el.get("INPUTGROUPNAME") else {}),
                       **{a.get("NAME", ""): a.get("VALUE", "") for a in el if a.tag in ("TRANSFORMFIELDATTR", "FIELDATTRIBUTE")}})


# Real exports (seen in public PowerCenter 8.x–10.x exports) emit several built-in types as TYPE="Custom Transformation" with the
# actual kind in TEMPLATENAME. Hand-written samples never do, which is why this went unnoticed until the public corpus was run.
_CUSTOM_TEMPLATES = {"union transformation": "Union", "java transformation": "Java Transformation", "http transformation": "HTTP",
                     "sql transformation": "SQL", "unstructured data transformation": "Unstructured Data",
                     "data masking transformation": "Data Masking", "web services consumer transformation": "Web Service Consumer"}


def _effective_type(el: ET.Element) -> str:
    t = el.get("TYPE", "") or ""
    if t.lower() == "custom transformation":
        tpl = (el.get("TEMPLATENAME", "") or "").strip().lower()
        return _CUSTOM_TEMPLATES.get(tpl, t)
    return t


def _transformation(el: ET.Element) -> Transformation:
    t = Transformation(name=el.get("NAME", ""), type=_effective_type(el), reusable=_bool(el.get("REUSABLE")),
                       description=el.get("DESCRIPTION", "") or "", attributes=_attrs(el),
                       ref_source=el.get("REF_SOURCE_NAME", "") or "", ref_dbd=el.get("REF_DBD_NAME", "") or "")
    t.ports = [_port(f) for f in el if f.tag == "TRANSFORMFIELD"]
    t.groups = [{"name": g.get("NAME", ""), "expression": g.get("EXPRESSION", "") or "", "order": g.get("ORDER", "") or "",
                 "type": g.get("TYPE", "") or ""} for g in el if g.tag == "GROUP"]
    return t


def _mapping(el: ET.Element, is_mapplet: bool = False) -> Mapping:
    m = Mapping(name=el.get("NAME", ""), description=el.get("DESCRIPTION", "") or "", is_valid=_bool(el.get("ISVALID"), True),
                is_mapplet=is_mapplet)
    for c in el:
        if c.tag == "TRANSFORMATION":
            m.transformations.append(_transformation(c))
        elif c.tag == "INSTANCE":
            m.instances.append(Instance(name=c.get("NAME", ""), type=c.get("TYPE", ""),
                                        transformation_type=c.get("TRANSFORMATION_TYPE", "") or "",
                                        transformation_name=c.get("TRANSFORMATION_NAME", "") or "",
                                        dbd_name=c.get("DBDNAME", "") or "", reusable=_bool(c.get("REUSABLE")),
                                        attributes=_attrs(c)))
        elif c.tag == "CONNECTOR":
            m.connectors.append(Connector(c.get("FROMINSTANCE", ""), c.get("FROMINSTANCETYPE", ""), c.get("FROMFIELD", ""),
                                          c.get("TOINSTANCE", ""), c.get("TOINSTANCETYPE", ""), c.get("TOFIELD", "")))
        elif c.tag == "MAPPINGVARIABLE":
            m.variables.append(MappingVariable(name=c.get("NAME", ""), datatype=c.get("DATATYPE", "") or "",
                                               default_value=c.get("DEFAULTVALUE", "") or "", is_param=_bool(c.get("ISPARAM")),
                                               agg_function=c.get("AGGFUNCTION", "") or ""))
        elif c.tag == "TARGETLOADORDER":
            m.target_load_order.append({"order": c.get("ORDER", ""), "target": c.get("TARGETINSTANCE", "")})
    return m


def _source(el: ET.Element) -> SourceDef:
    s = SourceDef(name=el.get("NAME", ""), dbd_name=el.get("DBDNAME", "") or "", database_type=el.get("DATABASETYPE", "") or "",
                  owner=el.get("OWNERNAME", "") or "", attributes=_attrs(el))
    s.fields = [_port(f) for f in el if f.tag == "SOURCEFIELD"]
    ff = next((f for f in el if f.tag == "FLATFILE"), None)
    if ff is not None:
        s.flat_file = dict(ff.attrib)
    return s


def _target(el: ET.Element) -> TargetDef:
    t = TargetDef(name=el.get("NAME", ""), database_type=el.get("DATABASETYPE", "") or "", attributes=_attrs(el))
    t.fields = [_port(f) for f in el if f.tag == "TARGETFIELD"]
    return t


def _session(el: ET.Element) -> Session:
    s = Session(name=el.get("NAME", ""), mapping_name=el.get("MAPPINGNAME", "") or "", reusable=_bool(el.get("REUSABLE")),
                attributes=_attrs(el))
    for c in el:
        if c.tag == "SESSTRANSFORMATIONINST":
            s.instance_overrides[c.get("SINSTANCENAME", "")] = dict(_attrs(c), TRANSFORMATIONTYPE=c.get("TRANSFORMATIONTYPE", "") or "")
        elif c.tag == "SESSIONEXTENSION":
            conn = next((r for r in c if r.tag == "CONNECTIONREFERENCE"), None)
            s.connections[c.get("SINSTANCENAME", "")] = {
                "type": c.get("TYPE", "") or "", "subtype": c.get("SUBTYPE", "") or "",
                "transformation_type": c.get("TRANSFORMATIONTYPE", "") or "",
                "connection": (conn.get("CONNECTIONNAME", "") if conn is not None else "") or "",
                "connection_type": (conn.get("CONNECTIONTYPE", "") if conn is not None else "") or "",
                **{k: v for k, v in _attrs(c).items()}}
    return s


def _task(el: ET.Element) -> Task:
    return Task(name=el.get("NAME", ""), type=el.get("TYPE", ""), attributes=_attrs(el),
                value_pairs={v.get("NAME", ""): v.get("VALUE", "") or "" for v in el if v.tag == "VALUEPAIR"})


def _workflow(el: ET.Element) -> Workflow:
    w = Workflow(name=el.get("NAME", ""), description=el.get("DESCRIPTION", "") or "", enabled=_bool(el.get("ISENABLED"), True),
                 is_valid=_bool(el.get("ISVALID"), True), attributes=_attrs(el))
    for c in el:
        if c.tag == "SCHEDULER":
            info = next((i for i in c if i.tag == "SCHEDULEINFO"), None)
            w.scheduler = {"name": c.get("NAME", ""), **({"type": info.get("SCHEDULETYPE", "")} if info is not None else {})}
            if info is not None:
                for sub in info:
                    w.scheduler.update({f"{sub.tag.lower()}.{k.lower()}": v for k, v in sub.attrib.items()})
        elif c.tag == "TASKINSTANCE":
            w.tasks.append(TaskInstance(name=c.get("NAME", ""), task_type=c.get("TASKTYPE", "") or "", task_name=c.get("TASKNAME", "") or "",
                                        enabled=_bool(c.get("ISENABLED"), True), attributes=_attrs(c),
                                        treat_input_links_as_and=_bool(c.get("TREAT_INPUTLINK_AS_AND"), True)))
        elif c.tag == "WORKFLOWLINK":
            w.links.append(WorkflowLink(c.get("FROMTASK", ""), c.get("TOTASK", ""), c.get("CONDITION", "") or ""))
        elif c.tag == "TASK":
            w.task_defs.append(_task(c))
        elif c.tag == "SESSION":
            w.sessions.append(_session(c))
    return w


def _parse_tree(root: ET.Element, source_file: str = "") -> List[Folder]:
    for el in root.iter():                       # BOM inside attribute values (first flat-file column, expressions naming it)
        for k, v in el.attrib.items():
            if _BOM in v or _BOM_MOJIBAKE in v: el.attrib[k] = v.replace(_BOM, "").replace(_BOM_MOJIBAKE, "")
    folders: List[Folder] = []
    repo_name, repo_ver = "", ""
    if root.tag == "POWERMART":
        repo_ver = root.get("REPOSITORY_VERSION", "") or ""
        repos = [r for r in root if r.tag == "REPOSITORY"]
    elif root.tag == "REPOSITORY":
        repos = [root]
    else:
        raise ValueError(f"not a PowerCenter export (root element {root.tag!r}, expected POWERMART)")
    for repo in repos:
        repo_name = repo.get("NAME", "") or ""
        for fel in repo:
            if fel.tag != "FOLDER":
                continue
            f = Folder(name=fel.get("NAME", ""), repository=repo_name, repository_version=repo_ver,
                       source_files=[source_file] if source_file else [])
            for c in fel:
                if c.tag == "SOURCE": f.sources.append(_source(c))
                elif c.tag == "TARGET": f.targets.append(_target(c))
                elif c.tag == "TRANSFORMATION": f.transformations.append(_transformation(c))
                elif c.tag == "MAPPLET": f.mapplets.append(_mapping(c, is_mapplet=True))
                elif c.tag == "MAPPING": f.mappings.append(_mapping(c))
                elif c.tag == "SESSION": f.sessions.append(_session(c))
                elif c.tag == "WORKFLOW": f.workflows.append(_workflow(c))
                elif c.tag == "TASK": f.tasks.append(_task(c))
            _resolve_instances(f)
            folders.append(f)
    return folders


def _resolve_instances(f: Folder) -> None:
    """Point every TRANSFORMATION instance at its definition: inline (same mapping) first, then the folder's reusable ones."""
    reusable = {t.name: t for t in f.transformations}
    for m in f.mappings + f.mapplets:
        inline = {t.name: t for t in m.transformations}
        for i in m.instances:
            if i.type.upper() != "TRANSFORMATION":
                continue
            i.transformation = inline.get(i.transformation_name) or reusable.get(i.transformation_name) or inline.get(i.name)
            if i.transformation and (not i.transformation_type or i.transformation_type.lower() == "custom transformation"):
                i.transformation_type = i.transformation.type      # the INSTANCE says "Custom Transformation"; the definition knows it is a Union


def _safe_parse(path: PathLike) -> ET.Element:
    # ElementTree does not resolve external entities or fetch the DTD; the DOCTYPE line is ignored. Real exports may declare a
    # code page the platform lacks; read bytes and let the XML declaration decide.
    return ET.parse(str(path)).getroot()


def parse_export(payload: Union[PathLike, str, bytes]) -> Folder:
    """One export file (or an XML string/bytes) → the first FOLDER. Multi-folder files: use parse_export_dir / parse_export_all."""
    folders = parse_export_all(payload)
    if not folders:
        raise ValueError("export holds no FOLDER")
    return folders[0]


def parse_export_all(payload: Union[PathLike, str, bytes]) -> List[Folder]:
    if isinstance(payload, bytes):
        return _parse_tree(ET.fromstring(payload))
    if isinstance(payload, str) and payload.lstrip().startswith("<"):
        return _parse_tree(ET.fromstring(payload))
    p = Path(payload)
    return _parse_tree(_safe_parse(p), source_file=str(p))


def parse_export_dir(directory: PathLike, pattern: str = "**/*.xml") -> List[Folder]:
    """Every export under a directory (case-insensitive .xml), merged by FOLDER name: mappings in one file and workflows in
    another still land in the same Folder. Files that fail to parse are skipped and reported on stderr, not fatal (phData's
    corpus rule)."""
    import sys
    merged: Dict[str, Folder] = {}
    files = sorted({p for p in glob.glob(os.path.join(str(directory), pattern), recursive=True)} |
                   {p for p in glob.glob(os.path.join(str(directory), pattern.replace(".xml", ".XML")), recursive=True)})
    for path in files:
        try:
            for f in parse_export_all(path):
                if f.name in merged:
                    tgt = merged[f.name]
                    for attr in ("sources", "targets", "transformations", "mapplets", "mappings", "sessions", "workflows", "tasks", "source_files"):
                        getattr(tgt, attr).extend(getattr(f, attr))
                    _resolve_instances(tgt)
                else:
                    merged[f.name] = f
        except (ET.ParseError, ValueError) as exc:
            print(f"[informatica_converter] skipped {path}: {exc}", file=sys.stderr)
    return list(merged.values())
