# ADR 006: Data Contracts — Future Enhancement Roadmap

## Status
Proposed

## Date
2026-03-21

## Context
The current pipeline uses dbt schema tests as reactive validation —
problems are caught after data lands in Silver/Gold layers. This is
appropriate for a single-team portfolio project but has limitations
in production environments with multiple consumers or AI/LLM layers.

Data contracts are a formal, versioned agreement between a data producer
(Python ingestion) and a data consumer (dbt models, Tableau, AI agents)
specifying schema, semantics, SLAs, and ownership.

This is a hot topic in 2026 driven by three converging trends:
1. Data mesh adoption — domain-owned data requires explicit interfaces
2. AI/LLM pipelines — need guaranteed schemas to avoid unpredictable results
3. Regulatory pressure — auditability requirements demand transformation audit trails

## Current State
```
Python ingestion → GCS → BigQuery Bronze → dbt tests (reactive)
```
Schema drift from the MTA API would only be caught after corrupting
Bronze and propagating through Silver to Gold before dbt tests fire.

## Proposed Evolution — Three Layers

### Layer 1: Pandera Schema Validation (Python Ingestion)
**Catches:** Upstream schema drift from MTA API changes
**When:** Before data touches the pipeline — proactive not reactive
**Failure mode caught:** MTA renames `transit_timestamp` to `timestamp_utc`

```python
import pandera as pa

mta_schema = pa.DataFrameSchema({
    "transit_timestamp":  pa.Column(str, nullable=False),
    "station_complex_id": pa.Column(str, nullable=False),
    "ridership":          pa.Column(float, pa.Check.ge(0)),
    "borough":            pa.Column(str, pa.Check.isin([
        "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island"
    ]))
})

mta_schema.validate(df)  # raises SchemaError before GCS upload
```

**Does NOT catch:** Transformation logic errors introduced by dbt code.
Layer 1 only validates what the source provides, not what your code does.

### Layer 2: dbt Schema Tests (Current Implementation) ✅
**Catches:** Transformation logic errors introduced by dbt models
**When:** After model runs — runtime validation
**Failure modes caught (real examples from this project):**
- mta_silver_silver schema bug (dbt config error, not MTA)
- Off-Peak hyphen inconsistency (model code, not source data)
- equity_risk_score NULL for 2 stations (join logic edge case)
- Consecutive zero detection returning 0 (window function bug)

None of these would have been caught by Layer 1 — the raw data was
correct, the transformation logic was wrong.

**Current coverage:** 74/75 tests passing, 1 documented warn.
Custom singular tests cover business logic bounds checks.

### Layer 3: dbt Contract Enforcement (dbt 1.5+)
**Catches:** Gold mart output drift from declared interface
**When:** Compile time — before the model even runs
**Failure mode caught:** Model refactor accidentally drops a column
that Tableau or an AI agent depends on

```yaml
models:
  - name: mart_recovery_scorecard
    config:
      contract:
        enforced: true
    columns:
      - name: station_complex_id
        data_type: string
        constraints:
          - type: not_null
      - name: recovery_pct
        data_type: float64
      - name: recovery_tier
        data_type: string
```

**Does NOT replace Layer 2:** Contracts check column names and types.
They do not catch value-level issues (ridership = -500, borough = "New Jersey").
Layer 2 custom tests handle value validation.

## Why All Three Layers Have Distinct Value

```
Layer 1 = Inspect raw materials at the dock (supplier sent wrong parts)
Layer 2 = Inspect during assembly (your worker made a mistake)
Layer 3 = Inspect the finished product spec (output doesn't match design)
```

Each layer catches failure modes the others cannot. They are
orthogonal concerns, not redundant checks.

## Priority Assessment

| Layer | Catches | Effort | Priority |
|-------|---------|--------|----------|
| Layer 1 (Pandera) | Upstream API drift | Medium | High for production |
| Layer 2 (dbt tests) | Logic errors | Low | Done — always required |
| Layer 3 (dbt contracts) | Output schema drift | Low | High when AI consumers added |

## AI/LLM Layer Dependency — Why This Matters Now

This project is being extended with AI engineering layers (RAG pipelines,
MCP servers, LLM agents querying marts). These consumers require:

- Guaranteed column names (agents reference columns by name)
- Guaranteed types (LLM tool definitions specify parameter types)
- Guaranteed value ranges (prompts reference expected values)

Schema drift that a human analyst might notice and work around will
cause an AI agent to silently hallucinate or return wrong results.
Data contracts become the reliability foundation for AI-powered analytics.

## Implementation Roadmap

```
Phase 1 (current):
  Layer 2 complete — 74 dbt tests across all layers

Phase 2 (next sprint):
  Add Pandera to load_to_gcs.py and load_incremental_2025.py
  Schema definitions stored in ingestion/schemas/

Phase 3 (when AI layer added):
  Add dbt contract enforcement to all 4 mart models
  contract: enforced: true + explicit data_type declarations

Phase 4 (data mesh readiness):
  Contract registry (DataHub or custom YAML store)
  Versioned contracts with breaking change approval workflow
  Enables multiple downstream consumers with SLA guarantees
```

## Alternatives Considered
- Great Expectations instead of Pandera — more powerful but significantly
  more complex to configure; Pandera is sufficient for schema validation
- dbt expectations package (metaplane) — already installed, covers Layer 2
  value validation; does not replace compile-time Layer 3 contracts
- Single validation layer only — rejected, each layer catches distinct
  failure modes that others cannot detect

## Interview Talking Point
"The current pipeline uses dbt schema tests as reactive Layer 2 validation —
we catch transformation logic errors after models run. The production evolution
adds Pandera schema validation in Python ingestion as proactive Layer 1 —
catching MTA API changes before they corrupt Bronze. Layer 3 adds dbt compile-time
contract enforcement on mart models, which becomes critical when AI agents query
the marts. An LLM pipeline needs guaranteed schemas — schema drift that a human
analyst notices and works around will cause an agent to silently produce wrong
results. Data contracts are the reliability foundation that makes AI-powered
analytics trustworthy, not just functional."