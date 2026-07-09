# Databricks notebook source
# MAGIC %md
# MAGIC ##### Schema Setup — IQAir Delta DDL (Bronze / Silver / Gold)
# MAGIC
# MAGIC **Purpose**  
# MAGIC Bootstrap the Delta schema (`iqair_{environment}`) and all medallion tables so a
# MAGIC fresh environment can be deployed from the repo, and so the schema is versioned
# MAGIC alongside the code.
# MAGIC
# MAGIC **Key characteristics**
# MAGIC - Idempotent: every statement is `CREATE ... IF NOT EXISTS`, so on an existing
# MAGIC   environment this notebook is a no-op and never touches live tables/data.
# MAGIC - Schemas mirror exactly what the Bronze/Silver/Gold notebooks write (types, names,
# MAGIC   order), so the notebooks' `append` / `overwrite` writes stay schema-compatible.
# MAGIC - **No partitioning** — at the project's data volume (tens–hundreds of rows per
# MAGIC   table) partitioning would only create many tiny files and planning overhead. The
# MAGIC   Silver/Gold tables already carry the natural partition keys (`pollution_date`,
# MAGIC   `weather_date`, `date`, `month_start`) for if that ever changes.
# MAGIC
# MAGIC **Layer**: Setup / DDL

# COMMAND ----------

# 0. Spark config
 
spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 1. Runtime parameters (ADF / manual)
 
dbutils.widgets.text("environment", "")
environment = dbutils.widgets.get("environment")
 
schema = f"iqair_{environment}"

# COMMAND ----------

# 2. Schema
 
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

# COMMAND ----------

# 3. Bronze — append-only API snapshot
 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.bronze_iqair_snapshot (
    execution_id     STRING    NOT NULL,
    pipeline_run_id  STRING,
    city             STRING    NOT NULL,
    state            STRING    NOT NULL,
    country          STRING    NOT NULL,
    api_result       STRING    NOT NULL,
    http_status_code INT,
    api_status       STRING,
    api_call_ts      TIMESTAMP,
    raw_payload      STRING,
    error_message    STRING,
    ingestion_ts     TIMESTAMP
) USING DELTA
COMMENT 'Bronze: raw IQAir API snapshots (append-only, one row per API call)'
""")

# COMMAND ----------

# 4. Silver — pollution (full refresh / overwrite)
 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.silver_iqair_pollution (
    city                STRING,
    state               STRING,
    country             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    bronze_ingestion_ts TIMESTAMP,
    pollution_ts        TIMESTAMP,
    aqi_us              INT,
    aqi_cn              INT,
    main_pollutant_us   STRING,
    main_pollutant_cn   STRING,
    pollution_date      DATE,
    silver_ingestion_ts TIMESTAMP,
    execution_id        STRING,
    pipeline_run_id     STRING
) USING DELTA
COMMENT 'Silver: normalized pollution observations (dedup latest-wins)'
""")

# COMMAND ----------

# 5. Silver — weather (full refresh / overwrite)
 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.silver_iqair_weather (
    city                STRING,
    state               STRING,
    country             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    bronze_ingestion_ts TIMESTAMP,
    weather_ts          TIMESTAMP,
    temperature         DOUBLE,
    humidity            INT,
    pressure            INT,
    wind_speed          DOUBLE,
    wind_direction      INT,
    weather_icon_code   STRING,
    heat_index          DOUBLE,
    weather_date        DATE,
    silver_ingestion_ts TIMESTAMP,
    execution_id        STRING,
    pipeline_run_id     STRING
) USING DELTA
COMMENT 'Silver: normalized weather observations (dedup latest-wins)'
""")

# COMMAND ----------

# 6. Gold — daily AQI metrics (full refresh / overwrite)
 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.gold_daily_aqi_metrics (
    city               STRING,
    state              STRING,
    country            STRING,
    date               DATE,
    avg_aqi_us         DOUBLE,
    max_aqi_us         INT,
    observations_cnt   BIGINT,
    avg_aqi_us_7d      DOUBLE,
    max_aqi_us_7d      INT,
    days_in_window     BIGINT,
    rank_avg_aqi_us_7d INT,
    is_worst_city_7d   BOOLEAN,
    gold_ingestion_ts  TIMESTAMP,
    execution_id       STRING,
    pipeline_run_id    STRING
) USING DELTA
COMMENT 'Gold: daily + rolling 7-calendar-day AQI metrics and worst-city rank'
""")

# COMMAND ----------

# 7. Gold — AQI/weather correlation by month (full refresh / overwrite)
 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.gold_aqi_weather_correlation_by_month (
    city                STRING,
    state               STRING,
    country             STRING,
    month_start         TIMESTAMP,
    pairs_cnt           BIGINT,
    aqi_temp_corr       DOUBLE,
    aqi_humidity_corr   DOUBLE,
    aqi_pressure_corr   DOUBLE,
    aqi_wind_speed_corr DOUBLE,
    join_window_hours   INT,
    gold_ingestion_ts   TIMESTAMP,
    execution_id        STRING,
    pipeline_run_id     STRING
) USING DELTA
COMMENT 'Gold: monthly AQI-weather correlations with pairs_cnt reliability signal'
""")

# COMMAND ----------

# 8. Gold — AQI category days by month (full refresh / overwrite)
 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {schema}.gold_aqi_category_days_by_month (
    city                STRING,
    state               STRING,
    country             STRING,
    month_start         TIMESTAMP,
    GOOD                BIGINT,
    MODERATE            BIGINT,
    UNHEALTHY_SENSITIVE BIGINT,
    UNHEALTHY           BIGINT,
    VERY_UNHEALTHY      BIGINT,
    HAZARDOUS           BIGINT,
    days_with_data      BIGINT,
    gold_ingestion_ts   TIMESTAMP,
    execution_id        STRING,
    pipeline_run_id     STRING
) USING DELTA
COMMENT 'Gold: monthly counts of days by US AQI category (from daily max AQI)'
""")
