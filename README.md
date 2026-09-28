# informatica-to-dbt

Informatica PowerCenter repository exports (powrmart XML) → an **assessment** and a **dbt project** (one model per target, on Apache
Iceberg by default); Airflow DAGs for the workflows next. Deterministic first, agent second: the code decides what it can convert, flags what it cannot, and never guesses.

Part of the [DATAP.AI](https://datap.ai) open, engine-neutral data layer. Sibling of the Control-M → Airflow converter. Apache-2.0.

```bash
pip install -e .
informatica-to-dbt assess  exports/ --md assessment.md --html assessment.html --json results.json
informatica-to-dbt convert exports/ --out dbt_project/ --project-name my_estate
informatica-to-dbt inspect exports/SALES_DW.xml --mapping m_DIM_CUSTOMER
informatica-to-dbt registry
```

## What `convert` writes

A dbt project and nothing else: one model per PowerCenter target, `_sources.yml` per folder (one source per DBD or lookup
connection, tables typed from the export, schema and identifier overridable by vars), `_models.yml` (not-null on primary keys,
unique on single keys), `dbt_project.yml` (Iceberg table type by default), `CONVERSION.md` with every TODO, and `.sql.skipped`
stubs for what the MVP does not compile. The SQL is portable dbt: where engines differ it uses dbt's cross-database macros
(`dbt.dateadd`, `dbt.datediff`, `dbt.date_trunc`, `dbt.safe_cast`, …) and your adapter renders it. No engine-specific SQL is
generated here.

A mapping is treated as a row stream: only transformations that change the set of rows become CTEs (Source Qualifier, Joiner,
Aggregator, Router groups, Union); Expression, Lookup (LEFT JOIN), Filter (WHERE), Sorter, Sequence Generator and Update
Strategy are folded into the stream, so the model reads like one an engineer would write.

No PowerCenter licence needed. The parser follows the public repository DTD (`powrmart.dtd`, grammar 8.x); the DTD is never
fetched; nothing leaves your machine.

## What the assessment answers

| Question | Where |
|---|---|
| How much converts as-is, with TODOs, or needs a human? | totals · per-mapping decisions with reasons |
| Which transformation types and features does the estate use? | type histogram · feature histogram |
| Is the export complete? | mappings referenced by workflows but missing · mappings no workflow runs |
| What runs what? | workflows → sessions → mappings · task types · links and conditions |

## Decisions (Tier 1)

- `convert` — every transformation is in the MVP set and no downgrading feature is present.
- `todo` — still generated, with a TODO block per flagged feature: untranslated function, aggregator pass-through, lookup
  variant (SQL override, dynamic cache, no condition, return-all, unconnected), SQL override, `SETVARIABLE`/`ABORT`,
  window-style functions (`LAG`, `LEAD`, `MOVINGAVG`), `:LKP.` calls.
- `skip` — not compiled in the MVP: Normalizer, Stored Procedure, Java, Custom, Transaction Control, mapplet instances, invalid
  or source-less/target-less mappings.

MVP set: Source Qualifier, Expression, Filter, Aggregator, Joiner, Sorter, Router, Union, connected static Lookup, Sequence
Generator; Update Strategy and Rank as `todo`. `informatica_to_dbt/registry.py` is the single place these rules live.

## Roadmap (three weeks, the easy 80%)

1. **Assessment** — done: parser, canonical model, registry, classifier, workflow findings, report.
2. **Compile** — done for the MVP set: expression language → portable dbt SQL with cross-database macros; row-stream compiler →
   one dbt model per target with sources and key tests. Verified by a real `dbt build` of the synthetic project on DuckDB
   (4 models, 8 tests). Next: workflows → Airflow (sessions → dbt run, links → dependencies, command tasks → Bash).
3. **Long tail** — an agent (Claude by default) over the same tools for what Tier 1 flags; parity run when a customer estate exists.

Out of scope for the MVP: unconnected/dynamic lookups, Normalizer, Update Strategy beyond insert, Transaction Control,
Stored Procedure/Java, mapplet flattening, SQL-override rewriting, pushdown, session overrides. Each is flagged, not guessed.

## Corpus and claims

`sample_exports/SALES_DW.xml` is a **synthetic** export written against the DTD: four mappings, one workflow, sessions with
connections and overrides, a reusable transformation, a missing-mapping reference and an orphan. The parser was also run over a
third-party corpus of 57 real-shaped exports / 157 mappings with zero parse failures on genuine export files; that corpus is
proprietary and is not included. Conversion rates on that corpus are not published here: they describe that corpus, not yours.
Run the assessment on your own export; the numbers it prints are the only ones that matter.

## Using it with Claude

`skill/SKILL.md` is a Claude skill that runs the assessment, reads the results and drafts the migration plan in plain language.
It calls this package; it does not replace it.

## Tests

```bash
python -m pytest tests -q                    # parser, registry, classifier, assessment, compiler
pip install dbt-duckdb && python -m pytest tests/test_informatica_to_dbt_build.py -q   # real dbt build on DuckDB (test bench)
```
