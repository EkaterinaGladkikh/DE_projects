# Databricks notebook source
# MAGIC %md
# MAGIC ##### Gold — AQI Categories by Month (US AQI)
# MAGIC
# MAGIC **Purpose**  
# MAGIC Aggregate monthly counts of days by US AQI category per city.
# MAGIC
# MAGIC **Key characteristics**
# MAGIC - Only from Silver (no ingestion logic)
# MAGIC - Fully recomputable (full refresh)
# MAGIC - Use only published AQI (`aqi_us is not null`)
# MAGIC - Category is computed from daily AQI (default: daily max AQI)
# MAGIC
# MAGIC **Layer**: Gold  
# MAGIC **Output tables**: `gold_aqi_category_days_by_month`
# MAGIC
# MAGIC **US AQI categories (EPA)**
# MAGIC - Good: 0–50
# MAGIC - Moderate: 51–100
# MAGIC - Unhealthy for Sensitive Groups: 101–150
# MAGIC - Unhealthy: 151–200
# MAGIC - Very Unhealthy: 201–300
# MAGIC - Hazardous: 301+

# COMMAND ----------

# 0. Spark config

spark.conf.set("spark.sql.session.timeZone", "UTC")

# COMMAND ----------

# 1. Imports

from pyspark.sql import functions as F

# COMMAND ----------

# 2. Runtime parameters (ADF → Databricks)

dbutils.widgets.text("execution_id", "")
dbutils.widgets.text("pipeline_run_id", "")

execution_id = dbutils.widgets.get("execution_id")
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")

# COMMAND ----------

# 3. Read Silver

pollution = spark.table("silver_iqair_pollution")

# COMMAND ----------

# 4. Build daily AQI (daily MAX AQI (worst of the day) is used for category classification)

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
        F.max("aqi_us").cast("int").alias("daily_aqi_us"),      # category driver
        F.count(F.lit(1)).alias("observations_cnt")
    )
)

# COMMAND ----------

# 5. Classify US AQI category

daily_cat = (
    daily
    .withColumn(
        "aqi_category_us",
        F.when(F.col("daily_aqi_us").between(0, 50), F.lit("GOOD"))
         .when(F.col("daily_aqi_us").between(51, 100), F.lit("MODERATE"))
         .when(F.col("daily_aqi_us").between(101, 150), F.lit("UNHEALTHY_SENSITIVE"))
         .when(F.col("daily_aqi_us").between(151, 200), F.lit("UNHEALTHY"))
         .when(F.col("daily_aqi_us").between(201, 300), F.lit("VERY_UNHEALTHY"))
         .when(F.col("daily_aqi_us") >= 301, F.lit("HAZARDOUS"))
         .otherwise(F.lit(None))
    )
    .filter(F.col("aqi_category_us").isNotNull())
    .withColumn("month_start", F.date_trunc("month", F.col("date").cast("timestamp")))
)

# COMMAND ----------

# 6. Monthly counts of days by category (pivot)

monthly = (
    daily_cat
    .groupBy("city", "state", "country", "month_start")
    .pivot("aqi_category_us", ["GOOD", "MODERATE", "UNHEALTHY_SENSITIVE", "UNHEALTHY", "VERY_UNHEALTHY", "HAZARDOUS"])
    .agg(F.count(F.lit(1)))
)

# Fill missing categories with 0 and add totals
monthly_filled = (
    monthly
    .na.fill(0, subset=["GOOD", "MODERATE", "UNHEALTHY_SENSITIVE", "UNHEALTHY", "VERY_UNHEALTHY", "HAZARDOUS"])
    .withColumn(
        "days_with_data",
        F.col("GOOD") + F.col("MODERATE") + F.col("UNHEALTHY_SENSITIVE") + F.col("UNHEALTHY") + F.col("VERY_UNHEALTHY") + F.col("HAZARDOUS")
    )
    .withColumn("gold_ingestion_ts", F.current_timestamp())
    .withColumn("execution_id", F.lit(execution_id))
    .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
)

# COMMAND ----------

# 7. Write Gold (full refresh)

monthly_filled.write.format("delta").mode("overwrite").saveAsTable("gold_aqi_category_days_by_month")
