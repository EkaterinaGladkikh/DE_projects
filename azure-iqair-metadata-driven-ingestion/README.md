# Azure IQAir Metadata-Driven Ingestion (PET Project)

Production-like pet project that demonstrates an enterprise-style, metadata-driven ingestion pattern on Azure using **ADF + Databricks + Azure SQL + Delta Lake**.

The data source is **IQAir AirVisual API (Free plan)**, which has strict rate limits and returns only the **current snapshot** (no historical backfill).

---

## Why this project

This project is intentionally designed to mirror real-world constraints:

- **Snapshot ingestion only**: each API call is a new observation
- **No backfill**: the API does not provide history, missed data cannot be restored
- **Replay at lake level**: Bronze is immutable; Silver/Gold are recomputable
- **Cost awareness**: API call limits are a first-class concern
- **Metadata-driven orchestration (lightweight)**: metadata controls *what to run and when*

---

## Architecture (Medallion)

### Bronze (Delta)
- Raw API payload stored as-is (JSON string)
- Append-only snapshot table
- Stores both successful and failed calls
- Idempotency check: skip if **SUCCESS already ingested** for `execution_id + city + state + country`
- Logs API call attempts into Azure SQL registry (as a call counter)

### Silver (Delta)
- Normalizes Bronze `raw_payload` into trusted tabular models:
  - `silver_iqair_pollution`
  - `silver_iqair_weather`
- Type casting, null-safe parsing
- Deduplication: **latest bronze ingestion wins** for the same `(city, state, country, ts)`
- Adds `pollution_date` and `weather_date` (useful for future incremental overwrite / replaceWhere patterns)

### Gold (Delta)
Business aggregates built only from Silver (full refresh, deterministic):

- `gold_daily_aqi_metrics`
  - Daily avg/max AQI per city
  - Rolling 7-day metrics over a **calendar** window (`[date - 6 days .. date]`, so a
    gap in sampling shortens the window's coverage rather than reaching further back in time)
  - Worst city rank per day (rank 1 = highest rolling 7-day avg). All cities present on a
    date are ranked; `days_in_window` (1–7) rides along as a completeness indicator, so
    ranks built on few days can be down-weighted or filtered rather than hidden (same idea
    as `pairs_cnt` below)
- `gold_aqi_weather_correlation_by_month`
  - Nearest-timestamp join (±3 hours) between pollution and weather
  - Monthly correlations per city
  - Includes `pairs_cnt` and `join_window_hours=3`
- `gold_aqi_category_days_by_month`
  - Monthly counts of days by US AQI category (based on daily max AQI)

### Environment-aware schemas (Databricks)

All Bronze/Silver/Gold notebooks receive an `environment` parameter from ADF and resolve their Delta schema as `iqair_{environment}` (e.g. `iqair_dev`).

The schema and all six Delta tables are defined in `databricks/ddl/00_setup_schema_iqair.py`,
which creates `iqair_{environment}` and every table with `CREATE ... IF NOT EXISTS`. It is
idempotent — a no-op on an existing environment — so it can be run to bootstrap a fresh
deployment or committed purely as versioned schema documentation. Table definitions mirror
exactly what the notebooks write (so `append` / `overwrite` stay schema-compatible). Tables
are intentionally not partitioned at this data volume; the Silver/Gold date columns
(`pollution_date`, `weather_date`, `date`, `month_start`) are the natural partition keys if
that changes.

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

## Runtime Parameters

Databricks notebooks are designed to be orchestrated externally (Azure Data Factory).

### Common Parameters

- `execution_id`
- `pipeline_run_id`
- `environment` — resolves the Databricks schema as `iqair_{environment}` (e.g. `iqair_dev`);
  also stamped on Azure SQL audit records

### Bronze Entity Parameters

- `city`
- `state`
- `country`

---

## Data Source

### **IQAir AirVisual API (Free plan)**

#### Rate Limits

- 5 calls per minute  
- 500 calls per day  
- 10,000 calls per month  

#### Update Frequency

- Data updates approximately hourly  

#### Endpoint

- GET /v2/city?city={city}&state={state}&country={country}&key={API_KEY}

---

## Current Orchestration Status (Azure Data Factory)

Two pipelines, deliberately separated by concern:

- **`PL_IQAir_MetadataDriven_Ingestion`** (orchestrator) — resolves execution context,
  reads config, starts/ends the pipeline run in `pipeline_audit`, calls the execution
  pipeline via `ExecutePipeline`
- **`PL_IQAir_Main_ETL`** (execution) — metadata-driven Bronze ingestion + Silver + Gold

This split keeps orchestration concerns (execution context, run bookkeeping) independent
from ingestion/transformation logic, and makes it easier to extend either side later
without touching the other.

### Execution Flow

- Orchestrator resolves `execution_id` (reused on FAILED/SKIPPED, new on SUCCESS)
- Reads entity metadata from Azure SQL, validates API quota before any Bronze call
- Bronze: sequential `ForEach` over active cities → Databricks notebook → audit log
  (success/failure) per city
- Silver runs once Bronze **completes** (regardless of per-city outcome — a single failed
  city shouldn't block processing of the rest)
- Gold (3 sequential notebooks) runs only if Silver **succeeded** (Gold reads finished
  Silver tables, so a broken Silver run must not produce a Gold report on stale data)
- A final status check makes the overall ADF run status reflect any layer failure
  (details in Pipeline Status below)
- Every activity start/end is logged into `pipeline_audit`; reruns reuse `execution_id`
  and only request/process what's actually missing

### Orchestration Design Decisions

This project favors simplicity and readability over implementing every possible
production pattern. Some choices were made deliberately, given the project's scope and
the IQAir API's characteristics:

- **Sequential Bronze execution** — `ForEach.isSequential = true`. The IQAir Free API has
  strict limits (5 calls/minute, 500/day); parallel execution would raise the risk of
  hitting quotas without any practical benefit for this workload.
- **API quota validated up front** — required calls are checked against remaining
  daily/monthly quota *before* Bronze starts (`check_api_limits`). If quota is insufficient,
  the pipeline fails before any API request is made, avoiding partially-ingested Bronze data
  caused purely by exhausted limits. Missing limit config is treated as a hard error (fail-
  fast), not an unbounded quota.
- **Per-minute limit enforced inside the loop** — the 5-calls/minute cap is checked per city
  *before* each Bronze call (`check_minute_limit`, a trailing-60-second `COUNT(*)` over the
  registry). Under the current sequential setup (~40 s/city) the rate never approaches the
  cap, so this is a defensive invariant rather than an active throttle: it keeps the limit
  enforced if the workload later speeds up (more cities, faster notebook, parallelism). On a
  breach the city is blocked (no API call, no registry row), the run is failed via the final
  gate, and the blocked city is picked up on the next rerun.
- **Layer toggles are pipeline parameters** — `bronze/silver/gold/dq_enabled` are parameters
  of `PL_IQAir_MetadataDriven_Ingestion` (not rows in a config table), since they describe
  *what to run this time*, not reference metadata. The limits table (`api_limits`) holds only
  `max_calls_*`. `dq_enabled` defaults to `false` — a placeholder for a future DQ layer.
- **Single execution at a time assumed** — this is a scheduled ingestion pipeline; a new
  run starts only after the previous one finished. Execution-context reuse and quota
  validation intentionally do not implement locking/reservation for concurrent runs.
- **ADF retries disabled on the Bronze notebook** — see Error Handling below.

### Error Handling

API-level errors (HTTP errors, unavailable cities, etc.) are captured inside the Bronze
notebook and persisted first — the call is written to `api_call_registry` (quota is spent)
and an `api_result = FAILED` row is appended to Bronze — and only *then* does the notebook
`raise`, surfacing the failure as a `Failed` notebook activity. This ordering (HTTP →
registry → Bronze append → raise) guarantees that a failed call is still counted against
quota and captured in Bronze before the notebook fails, so the run is reported honestly
instead of falsely going green.

Because the Bronze `ForEach` is sequential and the failed iteration is absorbed by the
Bronze failure branch (there is no per-city `Fail` activity), a single failed city does not
abort the loop — remaining cities are still processed. The per-city failure sets
`run_has_failures`, which the final gate turns into an overall pipeline failure.

ADF retry policies on the Bronze notebook activity are intentionally disabled: expected API
failures (bad city, API outage) are substantive, not transient, so retrying would just
re-consume API quota for a request that already failed for a real reason. Unexpected
infrastructure failures (cluster startup, connectivity, platform issues) fail the notebook
*before* the registry/Bronze writes, so they are distinguishable from API failures by the
absence of a registry/Bronze row, and propagate to pipeline status through the same
`Failed` branch.

### API Call Accounting

The API registry stores one record per request; quota validation currently counts rows
(`COUNT(*)`). This matches IQAir's Free-plan pricing, where every request costs exactly one
call. If a provider ever moves to weighted/credit-based pricing, this would need to become
a sum over a cost column instead of a row count.

Limit values live in the `api_limits` table (`max_calls_per_minute` / `_per_day` /
`_per_month`). Both `check_api_limits` and `check_minute_limit` fail-fast if their limit is
missing, so a misconfigured limits table surfaces as an explicit error rather than silently
disabling the guard.

All timestamps are recorded in **UTC** end to end: the Bronze notebook stamps calls in
UTC (Python `datetime.now(timezone.utc)`, Spark `current_timestamp()` with the session
time zone set to UTC), and the Azure SQL procedures use `SYSUTCDATETIME()`. This keeps the
quota windows (`check_api_limits` / `check_minute_limit`, which compare the registry's
`call_ts` against the current time) aligned regardless of where the SQL instance runs.
Azure SQL Database already runs in UTC, so `SYSUTCDATETIME()` is explicit intent rather than
a behavior change, but it keeps the contract correct if the database is ever moved to an
instance with a local time zone.

---

## Planned Next Steps

### Platform Enhancements

- Lightweight Data Quality framework (PySpark)
- `prod` environment (schema-level only — same workspace/catalog, no separate Azure SQL
  instance planned; environment separation there already exists at the row level via
  `pipeline_audit.environment`)
- CI/CD via GitHub Actions (linting, validation, optional deployment)

---

## Notes & Constraints

### API Constraints

- No historical backfill is possible due to API limitations

### Idempotency & Replay

- Idempotency in Bronze is enforced at two independent layers, both keyed on the full
  `execution_id + city + state + country`:
  - the notebook skips the API call if a **SUCCESS** snapshot already exists (protects at
    the record level, e.g. a manual notebook rerun bypassing the orchestrator)
  - `LKP_Entities` only hands the loop cities with **no spent call** in `api_call_registry`
    (a call is a call regardless of result), so a rerun with the same `execution_id`
    doesn't re-request quota for cities already attempted
- The two checks deliberately use different criteria: `LKP_Entities` asks "was a call
  spent?", the notebook asks "is there already a successful snapshot?" — the latter keeps a
  prior `FAILED` row from blocking a legitimate manual retry of that city
- Replay is possible only at the lake level (Bronze → Silver → Gold)

### Pipeline Status

- Layer failures (Bronze per-city, Silver, Gold) are recorded in `pipeline_audit`
  regardless of ADF's own run status
- Because Silver intentionally runs on Bronze's *Completed* (not *Succeeded*) dependency,
  a Bronze failure would otherwise be silently absorbed by ADF and the run would report
  Succeeded — a dedicated failure flag + final gate activity makes the overall pipeline
  status reliable end to end, independent of that dependency choice
- `pipeline_audit` logs activities across all layers, so the audited subject lives in a
  generic `entity` column — it holds the city for Bronze activities and is `null` for
  Silver/Gold (which have no per-city entity). The `state` / `country` columns are that
  entity's attributes and are likewise `null` for Silver/Gold. This is why the audit key is
  `entity + state + country` here, while `api_call_registry` (Bronze-only) names the same
  key `city + state + country`

---
