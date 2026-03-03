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
- Idempotency check: skip if **SUCCESS already ingested** for `execution_id + city`
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
  - Rolling 7-day metrics
  - Worst city rank per day (based on rolling avg)
- `gold_aqi_weather_correlation_by_month`
  - Nearest-timestamp join (±3 hours) between pollution and weather
  - Monthly correlations per city
  - Includes `pairs_cnt` and `join_window_hours=3`
- `gold_aqi_category_days_by_month`
  - Monthly counts of days by US AQI category (based on daily max AQI)

---

## Current repository structure

azure-iqair-metadata-driven-ingestion/
  databricks/
    bronze/
      01_bronze_ingestion_iqair.py
    silver/
      02_silver_transform_iqair.py
    gold/
      03_gold_daily_aqi_metrics_iqair.py
      04_gold_weather_correlation_monthly_iqair.py
      05_gold_aqi_category_monthly_iqair.py

---

## Runtime Parameters

Databricks notebooks are designed to be orchestrated externally (Azure Data Factory).

### Common Parameters

- `execution_id`
- `pipeline_run_id`

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

The Azure Data Factory pipeline is metadata-driven (lightweight, process-oriented design).

### Execution Flow

- Reads entity metadata from Azure SQL  
- Executes ForEach over active cities  
- Runs Databricks notebooks per layer (Bronze → Silver → Gold)  
- Logs execution into `pipeline_audit`  
- Supports restart by `pipeline_run_id`  

> ADF JSON definitions, linked services, and datasets will be added in a later step.

---

## Planned Next Steps

### Orchestration

- ADF pipeline JSON (metadata-driven orchestration)

### Metadata Layer (Azure SQL)

- `ingestion_entities`
- `ingestion_config`
- `pipeline_audit`
- Stored procedures for run control

### Platform Enhancements

- Lightweight Data Quality framework (PySpark)
- Environment setup (dev-only initially, structured as environment-aware)
- CI/CD via GitHub Actions (linting, validation, optional deployment)

---

## Notes & Constraints

### API Constraints

- API call registry is used strictly as a call counter
- No historical backfill is possible due to API limitations

### Idempotency & Replay

- Idempotency in Bronze is based on existence of a **SUCCESS** record for the current `execution_id + city`
- Replay is possible only at the lake level (Bronze → Silver → Gold)

---
