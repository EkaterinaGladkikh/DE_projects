# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold — Daily AQI Metrics (Daily + Rolling 7d + Worst Rank)
# MAGIC **Purpose**  
# MAGIC Build business-ready daily AQI metrics from Silver pollution.
# MAGIC
# MAGIC **Key characteristics**
# MAGIC - Only from Silver (no ingestion logic)
# MAGIC - Fully recomputable (full refresh)
# MAGIC - AQI metrics use only published AQI (`aqi_us is not null`)
# MAGIC
# MAGIC **Layer**: Gold  
# MAGIC **Output tables**: `gold_daily_aqi_metrics`

# COMMAND ----------

# 0. Spark config

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 1. Imports

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# 2. Runtime parameters (ADF → Databricks)

dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("environment", "")

execution_id = dbutils.widgets.get("execution_id")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
environment = dbutils.widgets.get("environment")

schema = f"iqair_{environment}"

# COMMAND ----------

# 3. Read Silver

pollution = spark.table(f"{schema}.silver_iqair_pollution")

# COMMAND ----------

# 4. Daily aggregates (Gold rule: AQI must be published)

daily = (
    pollution
    .filter(F.col("aqi_us").isNotNull())
    .filter(F.col("pollution_date").isNotNull())
    .groupBy(
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("pollution_date").alias("date")
    )
    .agg(
        F.avg("aqi_us").cast("double").alias("avg_aqi_us"),
        F.max("aqi_us").cast("int").alias("max_aqi_us"),
        F.count(F.lit(1)).alias("observations_cnt")
    )
)

# COMMAND ----------

# 5. Rolling 7d per city (7 CALENDAR days)
# rangeBetween works on the date's day-number, so the window is
# [date - 6 days .. date]. days_in_window (1..7) counts how many of those
# calendar days have data — a completeness indicator.

w7 = (
    Window
    .partitionBy("city", "state", "country")
    .orderBy(F.datediff(F.col("date"), F.lit("1970-01-01")))
    .rangeBetween(-6, 0)
)

with_roll = (
    daily
    .withColumn("avg_aqi_us_7d", F.avg("avg_aqi_us").over(w7).cast("double"))
    .withColumn("max_aqi_us_7d", F.max("max_aqi_us").over(w7).cast("int"))
    .withColumn("days_in_window", F.count(F.lit(1)).over(w7))
)

# COMMAND ----------

# 6. Worst rank per date (1 = worst by rolling 7d avg)
# All cities present on a date are ranked. Completeness is not gated:
# days_in_window rides along as a reliability indicator, so
# ranks built on few days can be filtered by the consumer, not hidden.

rank_win = Window.partitionBy("date").orderBy(F.col("avg_aqi_us_7d").desc_nulls_last())

gold_metrics = (
    with_roll
    .withColumn("rank_avg_aqi_us_7d", F.dense_rank().over(rank_win))
    .withColumn("is_worst_city_7d", F.col("rank_avg_aqi_us_7d") == F.lit(1))
    .withColumn("gold_ingestion_ts", F.current_timestamp())
    .withColumn("execution_id", F.lit(execution_id))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# COMMAND ----------

# 7. Write Gold (full refresh)

gold_metrics.write.format("delta").mode("overwrite").saveAsTable(f"{schema}.gold_daily_aqi_metrics")
