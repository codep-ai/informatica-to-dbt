# informatica-to-dbt

Informatica PowerCenter repository exports (powrmart XML) → an **assessment** today, **dbt models on Apache Iceberg + Airflow
DAGs** next. Deterministic first, agent second: the code decides what it can convert, flags what it cannot, and never guesses.

Part of the [DATAP.AI](https://datap.ai) open, engine-neutral data layer. Sibling of the Control-M → Airflow converter. Apache-2.0.

```bash
pip install -e .
informatica-to-dbt assess exports/ --md assessment.md --html assessment.html --json results.json
informatica-to-dbt inspect exports/SALES_DW.xml --mapping m_DIM_CUSTOMER
informatica-to-dbt registry
```

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

1. **Assessment** — this release: parser, canonical model, registry, classifier, workflow findings, report.
2. **Compile** — Informatica expression language → portable SQL (sqlglot per dialect); instance graph → CTEs → one dbt model per
   target, Iceberg materialisation, not-null key tests, sources.yml; workflows → Airflow (sessions → dbt run, links → dependencies,
   command tasks → Bash, parameter files → dbt vars).
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
python -m pytest tests -q
```
