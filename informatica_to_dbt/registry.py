"""
registry — PowerCenter transformation types → conversion family, MVP status and notes.

Mirrors `controlm_converter.job_types` (a registry with aliases and a `family` the lift gate branches on), but the family here is
the SQL-compile strategy, and `status` says what the MVP does with it:

    convert   compiled to SQL by compiler.py (the "easy 80%")
    todo      compiled where the simple variant applies; anything else left as a TODO block in the model (never guessed)
    skip      not compiled in the MVP; the mapping is reported as "needs review" (Tier 2 / human)
    passthru  structural, no SQL of its own (Input/Output of a mapplet, Source/Target definitions)

Variant notes come from the public rule tables (fivetran's transformation-mapping reference, phData's corpus guide) and from
PowerCenter's own transformation reference; they are the checklist the compiler and the classifier share.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


@dataclass(frozen=True)
class TransformationSpec:
    name: str                      # canonical name used in reports
    aliases: Tuple[str, ...]       # DTD TYPE strings (case-insensitive match)
    family: str                    # projection | filter | join | aggregate | lookup | router | sort | union | sequence |
                                   # update_strategy | source | target | mapplet_io | normalizer | procedural | control | rank | other
    status: str                    # convert | todo | skip | passthru
    notes: str = ""
    todo_when: Tuple[str, ...] = ()   # feature flags (from classifier.features) that downgrade convert → todo


TRANSFORMATION_REGISTRY: Dict[str, TransformationSpec] = {}


def _reg(spec: TransformationSpec) -> None:
    TRANSFORMATION_REGISTRY[spec.name] = spec


_reg(TransformationSpec("Source Definition", ("Source Definition", "SOURCE"), "source", "passthru", "A dbt source; the DBDNAME is the source system."))
_reg(TransformationSpec("Target Definition", ("Target Definition", "TARGET"), "target", "passthru", "The dbt model this mapping produces (one model per target instance)."))
_reg(TransformationSpec("Source Qualifier", ("Source Qualifier", "Application Source Qualifier", "MQ Source Qualifier", "XML Source Qualifier"),
                        "source", "convert", "FROM + optional WHERE (Source Filter) + ORDER BY (Number Of Sorted Ports); joins across sources of one qualifier become JOINs.",
                        todo_when=("sql_override", "user_defined_join_complex", "pre_post_sql")))
_reg(TransformationSpec("Expression", ("Expression",), "projection", "convert",
                        "Output ports → SELECT expressions; variable ports (v_) layered in evaluation order; functions via the expression translator.",
                        todo_when=("untranslated_function", "window_function", "lookup_unconnected_call", "flow_control_function")))
_reg(TransformationSpec("Filter", ("Filter",), "filter", "convert", "Filter Condition → WHERE."))
_reg(TransformationSpec("Aggregator", ("Aggregator",), "aggregate", "convert",
                        "GROUPBY ports → GROUP BY; aggregate expressions → aggregate functions; pass-through non-grouped ports → ANY_VALUE with a TODO (Informatica returns the last row's value).",
                        todo_when=("aggregator_passthrough",)))
_reg(TransformationSpec("Joiner", ("Joiner",), "join", "convert", "Join Type Normal/Master Outer/Detail Outer/Full Outer → INNER/LEFT/RIGHT/FULL; Join Condition → ON."))
_reg(TransformationSpec("Sorter", ("Sorter",), "sort", "convert", "SORTKEY ports → ORDER BY in the consuming step; Distinct → SELECT DISTINCT."))
_reg(TransformationSpec("Router", ("Router",), "router", "convert", "Each GROUP EXPRESSION → one filtered branch (one CTE per group); DEFAULT group → NOT (any group)."))
_reg(TransformationSpec("Union", ("Union", "Union Transformation"), "union", "convert", "UNION ALL of the input groups."))
_reg(TransformationSpec("Lookup", ("Lookup Procedure", "Lookup"), "lookup", "convert",
                        "Connected, static-cached lookup with a lookup condition → LEFT JOIN on a deduplicated lookup subquery (Lookup Policy On Multiple Match: first/last → ORDER BY + row_number).",
                        todo_when=("lookup_unconnected", "lookup_dynamic_cache", "lookup_sql_override", "lookup_no_condition", "lookup_return_all")))
_reg(TransformationSpec("Sequence Generator", ("Sequence Generator", "Sequence"), "sequence", "convert",
                        "NEXTVAL → row_number() over () + start value (MVP); CURRVAL → the same value. Cross-run continuity is a TODO.",
                        todo_when=("sequence_shared",)))
_reg(TransformationSpec("Update Strategy", ("Update Strategy",), "update_strategy", "convert",
                        "DD_INSERT-only → table. Otherwise → incremental merge on the target primary key with a row hash: update on hash change, "
                        "insert new keys, soft delete (is_deleted) DD_DELETE rows and keys gone from the source, drop DD_REJECT rows.",
                        todo_when=("update_strategy_no_key",)))
_reg(TransformationSpec("Rank", ("Rank",), "rank", "convert", "Top/Bottom N per group → row_number() over (partition by … order by …) <= N, as RANKINDEX."))
_reg(TransformationSpec("Normalizer", ("Normalizer",), "normalizer", "skip", "Occurs → UNPIVOT / UNION ALL per occurrence; VSAM normalizers are out of scope."))
_reg(TransformationSpec("Transaction Control", ("Transaction Control",), "control", "skip", "Commit/rollback control has no dbt equivalent; batch semantics only."))
_reg(TransformationSpec("Stored Procedure", ("Stored Procedure",), "procedural", "skip", "Pre/post stored procedures → dbt hooks at best; connected ones need a rewrite."))
_reg(TransformationSpec("Java", ("Java Transformation", "Java"), "procedural", "skip", "Code, not dataflow; needs a human."))
_reg(TransformationSpec("Custom", ("Custom Transformation", "Custom", "External Procedure", "SQL", "HTTP", "Unstructured Data", "Data Masking", "Web Service Consumer"), "procedural", "skip", "External code or service call."))
_reg(TransformationSpec("Mapplet Input", ("Input Transformation", "Input"), "mapplet_io", "passthru", "Mapplet boundary."))
_reg(TransformationSpec("Mapplet Output", ("Output Transformation", "Output"), "mapplet_io", "passthru", "Mapplet boundary."))
_reg(TransformationSpec("Mapplet", ("Mapplet",), "mapplet_io", "skip", "MVP flattens nothing: a mapping that instantiates a mapplet is reported for review."))
_reg(TransformationSpec("Expression Macro", ("Expression Macro",), "projection", "skip", "Macro expansion not implemented."))

_ALIAS: Dict[str, TransformationSpec] = {a.lower(): s for s in TRANSFORMATION_REGISTRY.values() for a in s.aliases}
_GENERIC = TransformationSpec("Other", (), "other", "skip", "Transformation type not in the registry.")


def resolve_transformation(type_str: str) -> TransformationSpec:
    """Exact alias match, then longest case-insensitive prefix (`Lookup Procedure` vs `Lookup`), else Other."""
    if not type_str:
        return _GENERIC
    key = type_str.strip().lower()
    if key in _ALIAS:
        return _ALIAS[key]
    best: Optional[TransformationSpec] = None; best_len = 0
    for alias, spec in _ALIAS.items():
        if key.startswith(alias) and len(alias) > best_len:
            best, best_len = spec, len(alias)
    return best or _GENERIC


def list_registry() -> List[Dict[str, str]]:
    return [{"type": s.name, "family": s.family, "status": s.status, "aliases": ", ".join(s.aliases), "notes": s.notes}
            for s in TRANSFORMATION_REGISTRY.values()]
