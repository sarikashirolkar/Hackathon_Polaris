# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Spark ETL: Enrich + Score Crops
# MAGIC Joins datasets, engineers features, logs experiment to MLflow.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

spark = spark  # noqa: already available in Databricks

# COMMAND ----------
# MAGIC %md ## Step 1: Load base crop table

df_base = spark.table("kisan_mitra.crop_advisory")
print(f"Base records: {df_base.count()}")
df_base.printSchema()

# COMMAND ----------
# MAGIC %md ## Step 2: Add MSP coverage & water score features

# Water score: high=1, medium=2, low=3 (lower = more water needed = riskier in dry areas)
water_score_map = {"high": 1, "medium": 2, "low": 3}

df_enriched = (
    df_base
    .withColumn("has_msp", F.col("msp_per_quintal").isNotNull().cast("integer"))
    .withColumn("water_score",
        F.when(F.col("water_requirement") == "low", 3)
         .when(F.col("water_requirement") == "medium", 2)
         .otherwise(1)
    )
    # Composite suitability score: has_msp (0/1) * 0.5 + water_score/3 * 0.5
    .withColumn("suitability_score",
        (F.col("has_msp") * 0.5 + F.col("water_score") / 3.0 * 0.5).cast(FloatType())
    )
    # Text document for vector embedding
    .withColumn("document_text",
        F.concat_ws(" | ",
            F.concat_ws(" ", F.lit("State:"), F.col("state")),
            F.concat_ws(" ", F.lit("District:"), F.coalesce(F.col("district"), F.lit("General"))),
            F.concat_ws(" ", F.lit("Crop:"), F.col("crop")),
            F.concat_ws(" ", F.lit("Season:"), F.col("season")),
            F.concat_ws(" ", F.lit("Soil:"), F.col("soil_types")),
            F.concat_ws(" ", F.lit("Water:"), F.col("water_requirement")),
            F.concat_ws(" ", F.lit("MSP Rs:"), F.coalesce(F.col("msp_per_quintal").cast("string"), F.lit("NA"))),
            F.concat_ws(" ", F.lit("Yield per acre:"), F.col("yield_per_acre")),
            F.col("notes")
        )
    )
)

# COMMAND ----------
# MAGIC %md ## Step 3: Save enriched table

df_enriched.write.format("delta").mode("overwrite").saveAsTable("kisan_mitra.crop_enriched")
print(f"✅ Enriched {df_enriched.count()} records")

# COMMAND ----------
# MAGIC %md ## Step 4: Print ETL metrics (MLflow optional on Free Edition)

total = df_enriched.count()
with_msp = df_enriched.filter(F.col("has_msp") == 1).count()
states = df_enriched.select("state").distinct().count()
msp_pct = (with_msp / total * 100) if total else 0.0
print(f"✅ ETL complete — {total} records, {states} states, MSP coverage {with_msp}/{total} ({msp_pct:.1f}%)")

# COMMAND ----------
# MAGIC %md ## Step 5: Top crops by state × season (sanity check)

spark.sql("""
    SELECT state, season, crop, msp_per_quintal, water_requirement, suitability_score
    FROM kisan_mitra.crop_enriched
    ORDER BY state, season, suitability_score DESC
""").show(40, truncate=False)
