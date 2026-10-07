"""
Canonical model of a PowerCenter repository folder, engine-independent. Field names follow the DTD (powrmart.dtd, grammar 8.x)
so anyone who knows a repository export can read this without a dictionary:

    Folder ─ sources[]        SOURCE  (DBDNAME = the database definition the source belongs to)
           ─ targets[]        TARGET
           ─ transformations[] reusable TRANSFORMATIONs (REUSABLE="YES", referenced by INSTANCE TRANSFORMATION_NAME)
           ─ mapplets[]       MAPPLET (own transformations/instances/connectors)
           ─ mappings[]       MAPPING ─ transformations[] (non-reusable, defined inline)
                                      ─ instances[]       INSTANCE (a node in the dataflow: source, target, transformation)
                                      ─ connectors[]      CONNECTOR (port-to-port edge between instances)
                                      ─ variables[]       MAPPINGVARIABLE (parameters $$X and variables)
           ─ sessions[]       SESSION (runs one mapping; connection + override attributes)
           ─ workflows[]      WORKFLOW ─ tasks[] TASKINSTANCE, links[] WORKFLOWLINK, scheduler
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class Port:
    """A TRANSFORMFIELD / SOURCEFIELD / TARGETFIELD. PORTTYPE: INPUT, OUTPUT, INPUT/OUTPUT, LOCAL VARIABLE, LOOKUP, RETURN …"""
    name: str
    datatype: str = ""
    precision: str = ""
    scale: str = ""
    porttype: str = ""
    expression: str = ""
    expressiontype: str = ""       # GENERAL / GROUPBY / SORTKEY / RETURN … (Aggregator / Sorter / Lookup)
    default_value: str = ""
    keytype: str = ""              # source/target fields: PRIMARY KEY, FOREIGN KEY, NOT A KEY
    nullable: str = ""
    group: str = ""                # Router / Union: the GROUP this port belongs to
    ref_field: str = ""            # Router output ports: the input port they mirror (REF_FIELD)
    attrs: Dict[str, str] = field(default_factory=dict)

    @property
    def is_output(self) -> bool:
        return "OUTPUT" in self.porttype.upper()

    @property
    def is_input(self) -> bool:
        return "INPUT" in self.porttype.upper()

    @property
    def is_variable(self) -> bool:
        return "VARIABLE" in self.porttype.upper()


@dataclass
class Transformation:
    """A TRANSFORMATION element (inline in a mapping/mapplet, or reusable at folder level)."""
    name: str
    type: str                                  # DTD TYPE: "Expression", "Source Qualifier", "Lookup Procedure", …
    reusable: bool = False
    description: str = ""
    ports: List[Port] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)   # TABLEATTRIBUTE NAME→VALUE (Sql Query, Lookup condition, …)
    groups: List[Dict[str, str]] = field(default_factory=list) # Router output groups: {name, expression}
    ref_source: str = ""                        # Source Qualifier: REF_SOURCE_NAME
    ref_dbd: str = ""

    def attr(self, name: str, default: str = "") -> str:
        """TABLEATTRIBUTE by name, case-insensitive ('Number Of Ranks' vs 'Number of Ranks' both occur in real exports)."""
        if name in self.attributes:
            return self.attributes[name]
        low = name.lower()
        for k, v in self.attributes.items():
            if k.lower() == low:
                return v
        return default

    @property
    def output_ports(self) -> List[Port]:
        return [p for p in self.ports if p.is_output]


@dataclass
class Instance:
    """An INSTANCE inside a mapping: the node the CONNECTORs refer to. `transformation` resolves to the definition."""
    name: str
    type: str                                   # INSTANCE TYPE: SOURCE / TARGET / TRANSFORMATION
    transformation_type: str = ""               # for TRANSFORMATION instances: the TRANSFORMATION TYPE
    transformation_name: str = ""               # the TRANSFORMATION (or SOURCE/TARGET) NAME this instance is of
    dbd_name: str = ""                          # SOURCE instances: DBDNAME
    reusable: bool = False
    attributes: Dict[str, str] = field(default_factory=dict)   # instance-level TABLEATTRIBUTE overrides
    transformation: Optional[Transformation] = None


@dataclass
class Connector:
    """A CONNECTOR: one port of one instance feeds one port of another."""
    from_instance: str
    from_instance_type: str
    from_field: str
    to_instance: str
    to_instance_type: str
    to_field: str


@dataclass
class MappingVariable:
    name: str
    datatype: str = ""
    default_value: str = ""
    is_param: bool = False
    agg_function: str = ""


@dataclass
class Mapping:
    name: str
    description: str = ""
    is_valid: bool = True
    transformations: List[Transformation] = field(default_factory=list)   # inline definitions
    instances: List[Instance] = field(default_factory=list)
    connectors: List[Connector] = field(default_factory=list)
    variables: List[MappingVariable] = field(default_factory=list)
    target_load_order: List[Dict[str, str]] = field(default_factory=list)
    is_mapplet: bool = False

    def instance(self, name: str) -> Optional[Instance]:
        for i in self.instances:
            if i.name == name:
                return i
        return None

    @property
    def source_instances(self) -> List[Instance]:
        return [i for i in self.instances if i.type.upper() == "SOURCE"]

    @property
    def target_instances(self) -> List[Instance]:
        return [i for i in self.instances if i.type.upper() == "TARGET"]

    @property
    def transformation_instances(self) -> List[Instance]:
        return [i for i in self.instances if i.type.upper() == "TRANSFORMATION"]

    def transformation_types(self) -> List[str]:
        """Distinct TRANSFORMATION TYPEs used by this mapping, in first-seen order."""
        seen: List[str] = []
        for i in self.transformation_instances:
            t = i.transformation_type or (i.transformation.type if i.transformation else "")
            if t and t not in seen:
                seen.append(t)
        return seen


@dataclass
class SourceDef:
    name: str
    dbd_name: str = ""
    database_type: str = ""
    owner: str = ""
    fields: List[Port] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)
    flat_file: Dict[str, str] = field(default_factory=dict)


@dataclass
class TargetDef:
    name: str
    database_type: str = ""
    fields: List[Port] = field(default_factory=list)
    attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class Session:
    name: str
    mapping_name: str
    reusable: bool = False
    attributes: Dict[str, str] = field(default_factory=dict)   # session ATTRIBUTEs (Pre SQL, Post SQL, Parameter Filename…)
    connections: Dict[str, Dict[str, str]] = field(default_factory=dict)   # instance name → {type, subtype, connection name}
    instance_overrides: Dict[str, Dict[str, str]] = field(default_factory=dict)   # SESSTRANSFORMATIONINST ATTRIBUTEs


@dataclass
class TaskInstance:
    name: str
    task_type: str            # Session / Command / Decision / Email / Event-Wait / Event-Raise / Timer / Assignment / Control / Start / Worklet
    task_name: str = ""
    enabled: bool = True
    attributes: Dict[str, str] = field(default_factory=dict)
    treat_input_links_as_and: bool = True


@dataclass
class WorkflowLink:
    from_task: str
    to_task: str
    condition: str = ""


@dataclass
class Task:
    """A reusable TASK definition at folder/workflow level (Command, Decision, Email …) with its ATTRIBUTEs and VALUEPAIRs."""
    name: str
    type: str
    attributes: Dict[str, str] = field(default_factory=dict)
    value_pairs: Dict[str, str] = field(default_factory=dict)


@dataclass
class Workflow:
    name: str
    description: str = ""
    enabled: bool = True
    is_valid: bool = True
    scheduler: Dict[str, str] = field(default_factory=dict)
    tasks: List[TaskInstance] = field(default_factory=list)
    links: List[WorkflowLink] = field(default_factory=list)
    task_defs: List[Task] = field(default_factory=list)
    sessions: List[Session] = field(default_factory=list)     # sessions defined inline in the workflow
    attributes: Dict[str, str] = field(default_factory=dict)

    @property
    def session_tasks(self) -> List[TaskInstance]:
        return [t for t in self.tasks if t.task_type.lower() == "session"]


@dataclass
class Folder:
    name: str
    repository: str = ""
    repository_version: str = ""
    sources: List[SourceDef] = field(default_factory=list)
    targets: List[TargetDef] = field(default_factory=list)
    transformations: List[Transformation] = field(default_factory=list)   # reusable
    mapplets: List[Mapping] = field(default_factory=list)
    mappings: List[Mapping] = field(default_factory=list)
    sessions: List[Session] = field(default_factory=list)
    workflows: List[Workflow] = field(default_factory=list)
    tasks: List[Task] = field(default_factory=list)
    source_files: List[str] = field(default_factory=list)

    def mapping(self, name: str) -> Optional[Mapping]:
        for m in self.mappings:
            if m.name == name:
                return m
        return None

    def summary(self) -> Dict[str, Any]:
        return {"folder": self.name, "sources": len(self.sources), "targets": len(self.targets),
                "reusable_transformations": len(self.transformations), "mapplets": len(self.mapplets),
                "mappings": len(self.mappings), "sessions": len(self.sessions), "workflows": len(self.workflows)}
