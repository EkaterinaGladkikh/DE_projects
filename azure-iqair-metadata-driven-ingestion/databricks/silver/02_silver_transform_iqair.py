# Databricks notebook source
# MAGIC %md
# MAGIC ##### Silver Transformation — IQAir (Weather & Pollution)
# MAGIC
# MAGIC **Purpose**  
# MAGIC Normalize Bronze `raw_payload` into trusted Silver tables.
# MAGIC
# MAGIC **Key characteristics**
# MAGIC  - JSON → tabular model
# MAGIC  - Type casting
# MAGIC  - Snapshot deduplication (latest wins)
# MAGIC  - Full refresh (simple & deterministic for small volumes)
# MAGIC
# MAGIC **Layer**: Silver  
# MAGIC **Output tables**: `silver_iqair_pollution`, `silver_iqair_weather`
# MAGIC

# COMMAND ----------

# 0. Spark config

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 1. Imports

from pyspark.sql.functions import (
    col, to_timestamp, get_json_object, to_date,
    row_number, current_timestamp, lit
)
from pyspark.sql.window import Window

# COMMAND ----------

# 2. Runtime parameters (ADF → Databricks)

dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("pipeline_run_id", "")

execution_id = dbutils.widgets.get("execution_id")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# COMMAND ----------

# 3. Read Bronze (trusted only)

bronze = (
    spark.table("bronze_iqair_snapshot")
    .filter(col("api_result") == "SUCCESS")
    .filter(col("raw_payload").isNotNull())
)

base = bronze.select(
    col("ingestion_ts").alias("bronze_ingestion_ts"),
    col("city"),
    col("state"),
    col("country"),
    get_json_object(col("raw_payload"), "$.data.location.coordinates[1]").cast("double").alias("latitude"),
    get_json_object(col("raw_payload"), "$.data.location.coordinates[0]").cast("double").alias("longitude"),
    col("raw_payload")
)

# COMMAND ----------

# 4. Pollution (dedup: latest wins)

pollution_raw = (
    base.select(
        "city", "state", "country",
        "latitude", "longitude",
        "bronze_ingestion_ts",
        to_timestamp(get_json_object(col("raw_payload"), "$.data.current.pollution.ts")).alias("pollution_ts"),
        get_json_object(col("raw_payload"), "$.data.current.pollution.aqius").cast("int").alias("aqi_us"),
        get_json_object(col("raw_payload"), "$.data.current.pollution.aqicn").cast("int").alias("aqi_cn"),
        get_json_object(col("raw_payload"), "$.data.current.pollution.mainus").alias("main_pollutant_us"),
        get_json_object(col("raw_payload"), "$.data.current.pollution.maincn").alias("main_pollutant_cn"),
    )
    .filter(col("pollution_ts").isNotNull())
    .withColumn("pollution_date", to_date(col("pollution_ts")))
)

p_win = (
    Window
    .partitionBy("city", "state", "country", "pollution_ts")
    .orderBy(col("bronze_ingestion_ts").desc())
)

pollution = (
    pollution_raw
    .withColumn("rn", row_number().over(p_win))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("silver_ingestion_ts", current_timestamp())
    .withColumn("execution_id", lit(execution_id))
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
)

# COMMAND ----------

# 5. Weather (dedup: latest wins)

weather_raw = (
    base.select(
        "city", "state", "country",
        "latitude", "longitude",
        "bronze_ingestion_ts",
        to_timestamp(get_json_object(col("raw_payload"), "$.data.current.weather.ts")).alias("weather_ts"),
        get_json_object(col("raw_payload"), "$.data.current.weather.tp").cast("double").alias("temperature"),
        get_json_object(col("raw_payload"), "$.data.current.weather.hu").cast("int").alias("humidity"),
        get_json_object(col("raw_payload"), "$.data.current.weather.pr").cast("int").alias("pressure"),
        get_json_object(col("raw_payload"), "$.data.current.weather.ws").cast("double").alias("wind_speed"),
        get_json_object(col("raw_payload"), "$.data.current.weather.wd").cast("int").alias("wind_direction"),
        get_json_object(col("raw_payload"), "$.data.current.weather.ic").alias("weather_icon_code"),
        get_json_object(col("raw_payload"), "$.data.current.weather.heatIndex").cast("double").alias("heat_index"),
    )
    .filter(col("weather_ts").isNotNull())
    .withColumn("weather_date", to_date(col("weather_ts")))
)

w_win = (
    Window
    .partitionBy("city", "state", "country", "weather_ts")
    .orderBy(col("bronze_ingestion_ts").desc())
)

weather = (
    weather_raw
    .withColumn("rn", row_number().over(w_win))
    .filter(col("rn") == 1)
    .drop("rn")
    .withColumn("silver_ingestion_ts", current_timestamp())
    .withColumn("execution_id", lit(execution_id))
    .withColumn("pipeline_run_id", lit(pipeline_run_id))
)

# COMMAND ----------

# 6. Write Silver (full refresh)

pollution.write.format("delta").mode("overwrite").saveAsTable("silver_iqair_pollution")
weather.write.format("delta").mode("overwrite").saveAsTable("silver_iqair_weather")
