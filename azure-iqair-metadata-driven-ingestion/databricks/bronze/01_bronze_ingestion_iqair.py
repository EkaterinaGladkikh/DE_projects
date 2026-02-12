# Databricks notebook source
# MAGIC %md
# MAGIC ##### Bronze Ingestion — IQAir API
# MAGIC
# MAGIC **Purpose**  
# MAGIC Bronze ingestion notebook for air quality data (IQAir API).
# MAGIC
# MAGIC **Key characteristics**
# MAGIC - Idempotent API ingestion
# MAGIC - External orchestration (ADF → Databricks)
# MAGIC - API calls logging to Azure SQL
# MAGIC - Append-only Bronze Delta table
# MAGIC
# MAGIC **Layer**: Bronze  
# MAGIC **Pattern**: Snapshot ingestion (API → Delta)

# COMMAND ----------

# 0. Spark config

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 1. Imports

from datetime import datetime, timezone
import requests
import json
import time

from pyspark.sql import Row
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import *

# COMMAND ----------

# 2. Helper functions (pure / utility logic)
# Best-effort JDBC write with retry for API call registry (unstable SQL endpoint)

def write_registry_with_retry(df, jdbc_url, jdbc_properties, retries=3, sleep_sec=5):
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            df.write.jdbc(
                url=jdbc_url,
                table="api_call_registry",
                mode="append",
                properties=jdbc_properties
            )
            return  # success
        except Exception as e:
            last_error = e
            print(f"WARNING: registry write attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(sleep_sec)

    print(f"ERROR: registry write failed after {retries} attempts: {last_error}")


# COMMAND ----------

# 3. Runtime parameters (ADF → Databricks)

dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("pipeline_run_id", "")

dbutils.widgets.text("city", "")
dbutils.widgets.text("state", "")
dbutils.widgets.text("country", "")

execution_id = dbutils.widgets.get("execution_id")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

city = dbutils.widgets.get("city")
state = dbutils.widgets.get("state")
country = dbutils.widgets.get("country")

# COMMAND ----------

# 4. JDBC connection (Azure SQL)
# Used for API calls logging only

jdbc_hostname = "free-sql-server-02-0001.database.windows.net"
jdbc_port = 1433
jdbc_database = "free-sql-db-0870579"

jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};database={jdbc_database}"

jdbc_properties = {
    "user": dbutils.secrets.get(scope="kv-sql-dev", key="sql-user"),
    "password": dbutils.secrets.get(scope="kv-sql-dev", key="sql-password"),
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# 5. Idempotency check
# Skip only if SUCCESS already ingested

already_success = (
    spark.table("bronze_iqair_snapshot")
    .filter(col("execution_id") == execution_id)
    .filter(col("city") == city)
    .filter(col("api_result") == "SUCCESS")
    .limit(1)
)

if already_success.take(1):
    print(f"Skip API call: SUCCESS already ingested for execution_id={execution_id}, city={city}")
    dbutils.notebook.exit("SKIPPED")

# COMMAND ----------

# 6. API call (single attempt by design)

api_key = dbutils.secrets.get("kv-iqair-dev", "iqair-api-key")

base_url = f"https://api.airvisual.com/v2/city"

params = {
    "city": city,
    "state": state,
    "country": country,
    "key": api_key
}

api_call_ts = None
http_status_code = None
api_status = None
api_result = "FAILED"
raw_payload = None
error_message = None

try:
    # Timestamp of HTTP attempt initiation (not guaranteed delivery)
    api_call_ts = datetime.now(timezone.utc)
    response = requests.get(base_url, params=params, timeout=10)

    http_status_code = response.status_code
    response_text = response.text

    try:
        payload = response.json()
        api_status = payload.get("status")
        raw_payload = json.dumps(payload)
    except Exception:
        payload = None
        api_status = "invalid_json"
        raw_payload = response_text

    if response.status_code == 200 and api_status == "success":
        api_result = "SUCCESS"
    else:
        error_message = response_text

except Exception as e:
    api_status = "request_failed"
    error_message = str(e)

# COMMAND ----------

# 7. API call registry (rate-limit guard)

if api_call_ts is not None:
    registry_df = spark.createDataFrame([Row(
        execution_id=execution_id,
        pipeline_run_id=pipeline_run_id,
        city=city,
        call_ts=api_call_ts
    )])

    write_registry_with_retry(registry_df, jdbc_url, jdbc_properties)

# COMMAND ----------

# 8. Write Bronze snapshot (append-only)    
# Both successful and failed API calls are persisted

bronze_schema = StructType([
    StructField("execution_id", StringType(), False),
    StructField("pipeline_run_id", StringType(), True),
    StructField("city", StringType(), False),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("api_result", StringType(), False),
    StructField("http_status_code", IntegerType(), True),
    StructField("api_status", StringType(), True),
    StructField("api_call_ts", TimestampType(), True),
    StructField("raw_payload", StringType(), True),
    StructField("error_message", StringType(), True)
])

bronze_df = spark.createDataFrame(
    [(
        execution_id,
        pipeline_run_id,
        city,
        state,
        country,
        api_result,
        http_status_code,
        api_status,
        api_call_ts,
        raw_payload,
        error_message
    )],
    schema=bronze_schema
).withColumn("ingestion_ts", current_timestamp())

bronze_df.write.format("delta").mode("append").saveAsTable("bronze_iqair_snapshot")
