# Azure IQAir Metadata-Driven Ingestion (PET Project)

Production-like pet project demonstrating an enterprise-style, metadata-driven ingestion pattern on Azure using **ADF + Databricks + Azure SQL + Delta Lake**.

The data source is the **IQAir AirVisual API (Free plan)**: strict rate limits, and only the **current snapshot** (no historical backfill).

---

## Why this project

Intentionally designed to mirror real-world constraints:

- **Snapshot ingestion only** — each API call is a new observation
- **No backfill** — the API has no history; missed data cannot be restored
- **Replay at lake level** — Bronze is immutable; Silver/Gold are recomputable
- **Cost awareness** — API call limits are a first-class concern
- **Metadata-driven orchestration (lightweight)** — metadata controls *what to run and when*

---

## Architecture (Medallion)

### Bronze (Delta)
- Raw API payload stored as-is (JSON string), append-only
- Persists both successful and failed calls
- Idempotency: skip the API call if a **SUCCESS** snapshot already exists for `execution_id + city + state + country`
- Logs every API attempt into the Azure SQL `api_call_registry` (a quota counter)

### Silver (Delta)
- Normalizes Bronze `raw_payload` into trusted tables: `silver_iqair_pollution`, `silver_iqair_weather`
- Type casting, explicit timestamp parsing
- Deduplication: **latest bronze ingestion wins** for the same `(city, state, country, ts)`
- Adds `pollution_date` / `weather_date` (natural keys for future `replaceWhere` patterns)
- Full refresh — simple and deterministic at this data volume

### Gold (Delta)
Business aggregates built only from Silver (full refresh, deterministic):

- **`gold_daily_aqi_metrics`** — daily avg/max AQI per city; rolling 7-day metrics over a
  **calendar** window (`[date - 6 days .. date]`, so a sampling gap shortens coverage rather
  than reaching further back); worst-city rank per day (rank 1 = highest rolling 7-day avg).
  All cities present on a date are ranked; `days_in_window` (1–7) rides along as a
  completeness indicator, so ranks built on few days can be filtered by the consumer rather
  than hidden (same idea as `pairs_cnt`).
- **`gold_aqi_weather_correlation_by_month`** — nearest-timestamp join (±3 h) between
  pollution and weather; monthly correlations per city; includes `pairs_cnt` and
  `join_window_hours = 3`.
- **`gold_aqi_category_days_by_month`** — monthly counts of days by US AQI category, based on
  the **daily max** AQI (the day's worst; a conservative, health-oriented choice).

### Environment-aware schemas (Databricks)

All notebooks receive an `environment` parameter and resolve their Delta schema as
`iqair_{environment}` (e.g. `iqair_dev`). Schema and all six tables are defined in
`databricks/ddl/00_setup_schema_iqair.py` with `CREATE ... IF NOT EXISTS` — idempotent, so it
bootstraps a fresh environment or serves as versioned schema documentation. Tables are
intentionally not partitioned at this volume; the Silver/Gold date columns are the natural
partition keys if that changes.

---

## Current repository structure

```
azure-iqair-metadata-driven-ingestion/
├── adf/
│   ├── dataset/
│   │   └── DS_SQL_Metadata.json
│   ├── linkedService/
│   │   ├── LS_AzureSQL_IQAir.json
│   │   ├── LS_Databricks_IQAir.json
│   │   └── LS_KeyVault_IQAir.json
│   ├── pipeline/
│   │   ├── PL_IQAir_Main_ETL.json
│   │   └── PL_IQAir_MetadataDriven_Ingestion.json
│   └── publish_config.json
├── databricks/
│   ├── ddl/
│   │   └── 00_setup_schema_iqair.py
│   ├── bronze/
│   │   └── 01_bronze_ingestion_iqair.py
│   ├── silver/
│   │   └── 02_silver_transform_iqair.py
│   └── gold/
│       ├── 03_gold_daily_aqi_metrics_iqair.py
│       ├── 04_gold_weather_correlation_monthly_iqair.py
│       └── 05_gold_aqi_category_monthly_iqair.py
├── sql/
│   ├── ddl/
│   │   ├── api_call_registry.sql
│   │   ├── api_limits.sql
│   │   ├── ingestion_entities.sql
│   │   └── pipeline_audit.sql
│   └── procedures/
│       ├── check_api_limits.sql
│       ├── check_minute_limit.sql
│       ├── end_pipeline_run.sql
│       ├── get_execution_context.sql
│       ├── log_activity_end.sql
│       ├── log_activity_start.sql
│       └── start_pipeline_run.sql
└── README.md
```

---

## Data Source — IQAir AirVisual API (Free plan)

- **Rate limits:** 5 calls/min · 500 calls/day · 10,000 calls/month
- **Update frequency:** ~hourly
- **Endpoint:** `GET /v2/city?city={city}&state={state}&country={country}&key={API_KEY}`

---

## Runtime parameters

Notebooks are orchestrated externally by ADF.

- **Common:** `execution_id`, `pipeline_run_id`, `environment` (resolves the Databricks schema
  and is stamped on Azure SQL audit records)
- **Bronze entity:** `city`, `state`, `country`

---

## Orchestration (Azure Data Factory)

Two pipelines, separated by concern:

- **`PL_IQAir_MetadataDriven_Ingestion`** (orchestrator) — resolves execution context,
  starts/ends the run in `pipeline_audit`, calls the execution pipeline via `ExecutePipeline`
- **`PL_IQAir_Main_ETL`** (execution) — metadata-driven Bronze + Silver + Gold

The split keeps run bookkeeping independent from ingestion/transformation logic.

### Execution flow

- Orchestrator resolves `execution_id` — **new on first run / after SUCCESS, reused after
  FAILED/SKIPPED** — so a rerun continues an unfinished run rather than starting over.
- Reads active entities for the environment; validates daily/monthly quota **before** any
  Bronze call.
- Bronze: sequential `ForEach` over cities → Databricks notebook → per-city audit log.
- Silver runs once Bronze **completes** (regardless of per-city outcome — one failed city
  shouldn't block the rest).
- Gold (3 sequential notebooks) runs only if Silver **succeeded** (Gold reads finished Silver
  tables, so a broken Silver must not produce a report on stale data).
- A final status gate makes the overall ADF run status reflect any layer failure.
- Reruns reuse `execution_id` and only process what's actually missing.

### Design decisions

- **Sequential Bronze** (`isSequential = true`) — the Free API's strict limits make
  parallelism risky with no practical benefit here.
- **Quota validated up front** — daily/monthly quota is checked before Bronze starts
  (`check_api_limits`); insufficient quota fails the run before any request, avoiding
  partial Bronze caused purely by exhausted limits. Missing limit config is a hard error
  (fail-fast), not an unbounded quota.
- **Per-minute limit is a checked invariant, not an active throttle** — `check_minute_limit`
  runs per city before each Bronze call (trailing-60-second `COUNT(*)` over the registry).
  Under the current setup (sequential + ~40 s/city) the rate never approaches 5/min, so the
  check by construction never fires; it is kept deliberately so the limit stays *enforced* if
  the workload later speeds up (more cities, faster notebook, parallelism). On a breach the
  city is blocked (no call, no registry row), the run fails via the final gate, and the city
  is picked up on the next rerun.
- **Layer toggles are pipeline parameters** — `bronze/silver/gold/dq_enabled` live on
  `PL_IQAir_MetadataDriven_Ingestion`, since they describe *what to run this time*, not
  reference metadata. `api_limits` holds only `max_calls_*`. `dq_enabled` defaults to `false`
  — a deliberate placeholder for a future DQ layer, not a forgotten flag.
- **Single execution at a time** — a scheduled pipeline; a new run starts only after the
  previous finished. No locking/reservation for concurrent runs.
- **ADF retries disabled on the Bronze notebook** — expected API failures (bad city, outage)
  are substantive, not transient, so a retry would just re-consume quota for a request that
  already failed for a real reason.
- **Interactive cluster instead of a job cluster** — the production pattern is a job cluster
  per run (isolation, clean environment); interactive is chosen deliberately for iteration
  speed and cost within a pet project.

### Error handling & status integrity

API-level errors are captured inside the Bronze notebook and persisted **before** the
notebook fails. Ordering is deliberate — **HTTP → `api_call_registry` → Bronze append →
`raise`** — so a failed call is still counted against quota and captured in Bronze before the
`Failed` surfaces. Raising earlier would lose the quota count and the Bronze row.

Because the `ForEach` is sequential and the failed iteration is absorbed by the Bronze
failure branch (there is no per-city `Fail` activity), a single failed city does not abort
the loop — remaining cities are still processed. The per-city failure sets `run_has_failures`,
which the final gate turns into an overall pipeline failure.

- **`run_has_failures` is a Bronze-only mechanism.** Bronze failures are intentionally
  absorbed (Silver depends on Bronze's *Completed*, not *Succeeded*), so a flag + final gate
  are needed to keep the run from falsely reporting green. Silver/Gold failures instead fail
  the inner pipeline directly (`FAIL_*`) and propagate up through `ExecutePipeline`.
- **Infrastructure failure vs API failure.** Both log as `FAILED` via the same
  `NB_Bronze → Failed` branch. They differ by evidence: an API failure has a registry/Bronze
  row (the call happened, `raise` after the writes); an infrastructure failure (cluster,
  connectivity) fails *before* the writes, so no row exists.

### Idempotency & replay

Idempotency is enforced at two independent layers, both keyed on the full
`city + state + country`, and deliberately using **different criteria**:

- The **notebook** skips the API call if a **SUCCESS** snapshot already exists — record-level
  protection, e.g. a manual rerun bypassing the orchestrator. It checks *success* so a prior
  `FAILED` row never blocks a legitimate manual retry.
- **`LKP_Entities`** only hands the loop cities with **no spent call** in `api_call_registry`
  (a call is a call regardless of result), so a rerun with the same `execution_id` never
  re-requests quota for already-attempted cities.

That is: ADF asks *"was a call spent?"*, the notebook asks *"is there already a successful
snapshot?"*. The orchestration filter lives in an Azure SQL Lookup **on purpose** — so
scheduling never has to wake the Databricks cluster (warm-up + cost) just to decide which
cities are missing. Delta stays the source of truth for *data*, but is not queried for
orchestration decisions. Replay is possible only at the lake level (Bronze → Silver → Gold).

### API call accounting

`api_call_registry` stores one row per request; quota validation counts rows (`COUNT(*)`),
matching the Free plan where every request costs exactly one call. The **call result is
deliberately not duplicated here** — the registry is purely a quota counter ("a call was
spent"), while the full result (`api_result`, `http_status_code`, `api_status`,
`error_message`, `raw_payload`) lives in Bronze. Adding a status column would create a second,
drift-prone source of truth and invite an incorrect `COUNT(*) WHERE status='success'` that
would under-count spent quota (failed calls spend quota too).

All timestamps are **UTC** end to end: the notebook stamps calls with
`datetime.now(timezone.utc)` (Spark session TZ = UTC), and the SQL procedures use
`SYSUTCDATETIME()`. Quota windows are therefore aligned regardless of where the SQL instance
runs.

---

## Known limitations & trade-offs (deliberate for a pet project)

- **Best-effort quota accounting may under-count a call.** "Make the HTTP call" and "record it
  in `api_call_registry`" are two operations without a shared transaction (HTTP to IQAir +
  JDBC to Azure SQL). If the registry write fails after its retries, `COUNT(*)` can fall below
  the real spend. Full solutions (idempotency key, outbox, write-before-call with status
  update) were intentionally not implemented as overkill here.
- **Timeouts / transient errors count as a spent call.** On a timeout or network error before
  the response, the call is still written to the registry and the city is not retried within
  the `execution_id`. This is conservative toward quota; the mirror choice ("don't record,
  allow retry") risks double-counting and re-spending quota on a request that may have already
  reached IQAir — so it is no better. A deliberate default.
- **Orphaned `STARTED` PIPELINE row.** If a run fails before `EP_MainETL`, the PIPELINE row
  stays `STARTED` with no `end_ts`. Data is unaffected (`get_execution_context` reuses such an
  `execution_id`; Bronze idempotency and Silver/Gold recompute prevent duplicates). The
  limitation is purely observational — the audit can't distinguish a truly hanging run from a
  long-closed one whose End simply wasn't stamped.
- **Correlation on small `pairs_cnt`.** AQI↔weather correlations over few pairs are
  statistically weak; `pairs_cnt` is surfaced in the output so a consumer can weight or drop
  low-sample months.
- **Quota reset time zone.** Quota is counted by UTC day/month. If IQAir's real reset is tied
  to a different time zone, the windows may differ at day/month boundaries — a known nuance.
- **No index on `api_call_registry.call_ts`.** Unnecessary at this volume; at scale it would
  become a bottleneck for the windowed `COUNT(*)` in the limit checks.
- **Shared `api_call_registry` across environments is intentional.** The IQAir quota is global
  (one API key), so environments deliberately share the counter and `environment` is **not**
  added to `api_limits`. `environment` *is* carried on `api_call_registry` (attribution only,
  ignored by the limit check), on `ingestion_entities` (city lists may differ), and on
  `pipeline_audit`.

### Auditing granularity

`pipeline_audit` logs activities across all layers, so the audited subject lives in a generic
`entity` column — it holds the city for Bronze and is `null` for Silver/Gold (no per-city
entity). `state` / `country` are that entity's attributes, likewise `null` for Silver/Gold.
This is why the audit key is `entity + state + country`, mirroring `city + state + country` in
the Bronze-only `api_call_registry`.

---

## Prerequisites

Before the first run:

- **Azure SQL** — tables (`api_call_registry`, `api_limits`, `ingestion_entities`,
  `pipeline_audit`) and procedures created; `api_limits` populated with `max_calls_*`;
  `ingestion_entities` populated with active cities.
- **Databricks** — schema `iqair_{environment}` and the six Delta tables created via
  `databricks/ddl/00_setup_schema_iqair.py`.
- **Key Vault / secret scope** (`kv-iqair`) — `iqair-api-key`, `sql-jdbc-url`, `sql-user`,
  `sql-password` (Databricks JDBC), `sql-connection-string` (ADF linked service), and the
  Databricks PAT.

---

## Planned next steps

- Lightweight Data Quality framework (PySpark) — the `dq_enabled` toggle is already wired
- `prod` environment (schema-level: same workspace and Azure SQL, separated by the
  `environment` column already present across the metadata tables)
- CI/CD via GitHub Actions (linting, validation, optional deployment)

---
