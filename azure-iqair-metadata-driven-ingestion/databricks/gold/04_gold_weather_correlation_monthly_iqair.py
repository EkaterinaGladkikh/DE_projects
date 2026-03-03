# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold — AQI ↔ Weather Correlation (By Month)
# MAGIC
# MAGIC **Purpose**  
# MAGIC Match each pollution observation to the nearest weather observation within ±3 hours,
# MAGIC then compute correlations by city and month.
# MAGIC
# MAGIC **Key characteristics**
# MAGIC - Only from Silver
# MAGIC - Fully recomputable (full refresh)
# MAGIC - Use only published AQI (`aqi_us is not null`)
# MAGIC - Only matched pairs participate in correlation
# MAGIC
# MAGIC **Layer**: Gold  
# MAGIC **Output tables**: `gold_aqi_weather_correlation_by_month`

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

execution_id = dbutils.widgets.get("execution_id")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# COMMAND ----------

# 3. Read Silver

pollution = spark.table("silver_iqair_pollution")
weather   = spark.table("silver_iqair_weather")

JOIN_WINDOW_HOURS = 3

# COMMAND ----------

# 4. Prepare datasets

p = (
    pollution
    .filter(F.col("aqi_us").isNotNull())
    .filter(F.col("pollution_ts").isNotNull())
    .select("city", "state", "country", "pollution_ts", "aqi_us")
    .alias("p")
)

w = (
    weather
    .filter(F.col("weather_ts").isNotNull())
    .select(
        "city", "state", "country",
        "weather_ts",
        "temperature", "humidity", "pressure",
        "wind_speed"
    )
    .alias("w")
)

# COMMAND ----------

# 5. Candidate join within ±N hours

candidates = (
    p.join(
        w,
        on=[
            F.col("p.city") == F.col("w.city"),
            F.col("p.state") == F.col("w.state"),
            F.col("p.country") == F.col("w.country"),
            F.expr(
                f"w.weather_ts BETWEEN p.pollution_ts - INTERVAL {JOIN_WINDOW_HOURS} HOURS "
                f"AND p.pollution_ts + INTERVAL {JOIN_WINDOW_HOURS} HOURS"
            )
        ],
        how="left"
    )
    .withColumn(
        "time_diff_sec",
        F.abs(F.unix_timestamp(F.col("p.pollution_ts")) - F.unix_timestamp(F.col("w.weather_ts")))
    )
)

# COMMAND ----------

# 6. Nearest weather per pollution_ts

nearest_win = (
    Window
    .partitionBy("p.city", "p.state", "p.country", "p.pollution_ts")
    .orderBy(F.col("time_diff_sec").asc_nulls_last())
)

nearest = (
    candidates
    .withColumn("rn", F.row_number().over(nearest_win))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

# 7. Correlation by month (only matched pairs)

by_month = (
    nearest
    .filter(F.col("w.weather_ts").isNotNull())
    .withColumn("month_start", F.date_trunc("month", F.col("p.pollution_ts")))
    .groupBy(
        F.col("p.city").alias("city"),
        F.col("p.state").alias("state"),
        F.col("p.country").alias("country"),
        F.col("month_start")
    )
    .agg(
        F.count(F.lit(1)).alias("pairs_cnt"),
        F.corr("aqi_us", "temperature").alias("aqi_temp_corr"),
        F.corr("aqi_us", "humidity").alias("aqi_humidity_corr"),
        F.corr("aqi_us", "pressure").alias("aqi_pressure_corr"),
        F.corr("aqi_us", "wind_speed").alias("aqi_wind_speed_corr")
    )
    .withColumn("join_window_hours", F.lit(JOIN_WINDOW_HOURS))
    .withColumn("gold_ingestion_ts", F.current_timestamp())
    .withColumn("execution_id", F.lit(execution_id))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# COMMAND ----------

# 8. Write Gold (full refresh)

by_month.write.format("delta").mode("overwrite").saveAsTable("gold_aqi_weather_correlation_by_month")
