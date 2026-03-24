# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Data Ingestion → Delta Lake
# MAGIC Loads crop production, MSP, and soil data into Delta Lake tables.

# COMMAND ----------

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md ## Step 1: Create the crop records dataset (open data, India)

# ── Comprehensive India crop dataset ─────────────────────────────────────────
# Sources: data.gov.in crop production stats, CACP MSP 2024-25, IMD rainfall
# Each record = one crop-district-season combination

CROP_RECORDS = [
    # ── Maharashtra ──────────────────────────────────────────────────────────
    {"state": "Maharashtra", "district": "Nashik", "crop": "Grapes", "season": "Rabi",
     "soil_types": ["loamy", "black"], "water_requirement": "high",
     "msp_per_quintal": None, "yield_per_acre": "8-12 tonnes",
     "notes": "Nashik is India's wine capital. High value export crop. Needs drip irrigation."},

    {"state": "Maharashtra", "district": "Nashik", "crop": "Onion", "season": "Rabi",
     "soil_types": ["loamy", "black"], "water_requirement": "medium",
     "msp_per_quintal": None, "yield_per_acre": "80-120 quintals",
     "notes": "Nashik's famous onion belt. Good storage and market access."},

    {"state": "Maharashtra", "district": "Nashik", "crop": "Wheat", "season": "Rabi",
     "soil_types": ["loamy", "black"], "water_requirement": "medium",
     "msp_per_quintal": 2275, "yield_per_acre": "15-18 quintals",
     "notes": "MSP backed, low risk. Suitable for small farmers."},

    {"state": "Maharashtra", "district": "Vidarbha", "crop": "Cotton", "season": "Kharif",
     "soil_types": ["black"], "water_requirement": "medium",
     "msp_per_quintal": 7121, "yield_per_acre": "5-8 quintals",
     "notes": "Black soil is ideal for cotton. Long duration crop (180 days). MSP secured."},

    {"state": "Maharashtra", "district": "Vidarbha", "crop": "Soybean", "season": "Kharif",
     "soil_types": ["black", "loamy"], "water_requirement": "medium",
     "msp_per_quintal": 4600, "yield_per_acre": "8-12 quintals",
     "notes": "Fastest growing oilseed. Good nitrogen fixation. 90-day crop."},

    {"state": "Maharashtra", "district": "Vidarbha", "crop": "Tur (Arhar)", "season": "Kharif",
     "soil_types": ["black", "red"], "water_requirement": "low",
     "msp_per_quintal": 7550, "yield_per_acre": "5-8 quintals",
     "notes": "Drought tolerant pulse. High MSP. Intercrop with soybean for extra income."},

    {"state": "Maharashtra", "district": "Pune", "crop": "Sugarcane", "season": "Kharif",
     "soil_types": ["black", "loamy"], "water_requirement": "high",
     "msp_per_quintal": 340, "yield_per_acre": "250-350 quintals",
     "notes": "Long duration (12-18 months). High water use but assured procurement."},

    # ── Punjab ────────────────────────────────────────────────────────────────
    {"state": "Punjab", "district": "Ludhiana", "crop": "Wheat", "season": "Rabi",
     "soil_types": ["alluvial", "loamy"], "water_requirement": "medium",
     "msp_per_quintal": 2275, "yield_per_acre": "18-22 quintals",
     "notes": "Punjab's wheat belt. Highest yields in India. Assured procurement at MSP."},

    {"state": "Punjab", "district": "Ludhiana", "crop": "Rice (Paddy)", "season": "Kharif",
     "soil_types": ["alluvial", "loamy"], "water_requirement": "high",
     "msp_per_quintal": 2300, "yield_per_acre": "20-25 quintals",
     "notes": "High yield. Consider water-saving techniques like SRI. Stubble burning issues."},

    {"state": "Punjab", "district": "Amritsar", "crop": "Maize", "season": "Kharif",
     "soil_types": ["alluvial", "sandy"], "water_requirement": "medium",
     "msp_per_quintal": 2090, "yield_per_acre": "18-25 quintals",
     "notes": "Good alternative to paddy. Uses 60% less water. Poultry feed demand rising."},

    {"state": "Punjab", "district": "Patiala", "crop": "Sunflower", "season": "Zaid",
     "soil_types": ["loamy", "sandy"], "water_requirement": "low",
     "msp_per_quintal": 7280, "yield_per_acre": "5-8 quintals",
     "notes": "Short duration oilseed. High MSP. Fits well in Zaid season."},

    # ── Tamil Nadu ────────────────────────────────────────────────────────────
    {"state": "Tamil Nadu", "district": "Coimbatore", "crop": "Groundnut", "season": "Kharif",
     "soil_types": ["red", "sandy"], "water_requirement": "low",
     "msp_per_quintal": 6783, "yield_per_acre": "8-12 quintals",
     "notes": "High oil content. Drought tolerant. Good for red and sandy soils."},

    {"state": "Tamil Nadu", "district": "Coimbatore", "crop": "Cotton", "season": "Kharif",
     "soil_types": ["black", "red"], "water_requirement": "medium",
     "msp_per_quintal": 7121, "yield_per_acre": "4-6 quintals",
     "notes": "Tamil Nadu's cash crop. Bt cotton dominant. 150-180 day duration."},

    {"state": "Tamil Nadu", "district": "Thanjavur", "crop": "Rice (Paddy)", "season": "Kharif",
     "soil_types": ["alluvial", "clay"], "water_requirement": "high",
     "msp_per_quintal": 2300, "yield_per_acre": "22-28 quintals",
     "notes": "Cauvery delta is India's rice bowl. Tamil Nadu's staple crop."},

    {"state": "Tamil Nadu", "district": "Salem", "crop": "Turmeric", "season": "Kharif",
     "soil_types": ["loamy", "clay"], "water_requirement": "medium",
     "msp_per_quintal": None, "yield_per_acre": "20-30 quintals (dry)",
     "notes": "High value spice. Salem is India's turmeric hub. 9 month crop."},

    # ── Rajasthan ─────────────────────────────────────────────────────────────
    {"state": "Rajasthan", "district": "Jaipur", "crop": "Mustard", "season": "Rabi",
     "soil_types": ["sandy", "loamy"], "water_requirement": "low",
     "msp_per_quintal": 5650, "yield_per_acre": "6-10 quintals",
     "notes": "Rajasthan grows 50% of India's mustard. Low water, high MSP."},

    {"state": "Rajasthan", "district": "Jaipur", "crop": "Barley", "season": "Rabi",
     "soil_types": ["sandy", "loamy"], "water_requirement": "low",
     "msp_per_quintal": 1735, "yield_per_acre": "10-14 quintals",
     "notes": "Most drought-tolerant cereal. Malt barley fetches premium prices."},

    {"state": "Rajasthan", "district": "Jodhpur", "crop": "Bajra (Pearl Millet)", "season": "Kharif",
     "soil_types": ["sandy"], "water_requirement": "low",
     "msp_per_quintal": 2625, "yield_per_acre": "8-12 quintals",
     "notes": "Thrives in arid zones. Only 200mm rainfall needed. Food security crop."},

    {"state": "Rajasthan", "district": "Bikaner", "crop": "Guar (Cluster Bean)", "season": "Kharif",
     "soil_types": ["sandy"], "water_requirement": "low",
     "msp_per_quintal": None, "yield_per_acre": "5-8 quintals",
     "notes": "Export commodity for oil & gas industry (guar gum). Desert specialist."},

    # ── Madhya Pradesh ────────────────────────────────────────────────────────
    {"state": "Madhya Pradesh", "district": "Indore", "crop": "Soybean", "season": "Kharif",
     "soil_types": ["black", "loamy"], "water_requirement": "medium",
     "msp_per_quintal": 4600, "yield_per_acre": "10-14 quintals",
     "notes": "MP is India's soybean capital. Black soil ideal. 90-day short season crop."},

    {"state": "Madhya Pradesh", "district": "Bhopal", "crop": "Chickpea (Chana)", "season": "Rabi",
     "soil_types": ["black", "loamy"], "water_requirement": "low",
     "msp_per_quintal": 5440, "yield_per_acre": "8-12 quintals",
     "notes": "MP leads in chickpea production. Residual moisture crop, minimal irrigation."},

    {"state": "Madhya Pradesh", "district": "Gwalior", "crop": "Wheat", "season": "Rabi",
     "soil_types": ["loamy", "alluvial"], "water_requirement": "medium",
     "msp_per_quintal": 2275, "yield_per_acre": "16-20 quintals",
     "notes": "Gwalior division is MP's wheat hub. Assured MSP procurement."},

    # ── Karnataka ─────────────────────────────────────────────────────────────
    {"state": "Karnataka", "district": "Dharwad", "crop": "Sunflower", "season": "Rabi",
     "soil_types": ["red", "black"], "water_requirement": "medium",
     "msp_per_quintal": 7280, "yield_per_acre": "6-9 quintals",
     "notes": "Karnataka leads in sunflower. High MSP and good oil content."},

    {"state": "Karnataka", "district": "Mysuru", "crop": "Ragi (Finger Millet)", "season": "Kharif",
     "soil_types": ["red", "loamy"], "water_requirement": "low",
     "msp_per_quintal": 4290, "yield_per_acre": "8-12 quintals",
     "notes": "Traditional food security crop. Highly nutritious. Drought tolerant."},

    {"state": "Karnataka", "district": "Bidar", "crop": "Tur (Arhar)", "season": "Kharif",
     "soil_types": ["red", "black"], "water_requirement": "low",
     "msp_per_quintal": 7550, "yield_per_acre": "5-7 quintals",
     "notes": "Bidar is Karnataka's pulse hub. Long duration (170 days). High MSP."},

    # ── Gujarat ────────────────────────────────────────────────────────────────
    {"state": "Gujarat", "district": "Surat", "crop": "Sugarcane", "season": "Kharif",
     "soil_types": ["loamy", "alluvial"], "water_requirement": "high",
     "msp_per_quintal": 340, "yield_per_acre": "300-400 quintals",
     "notes": "South Gujarat's river valleys. Assured sugar factory procurement."},

    {"state": "Gujarat", "district": "Junagadh", "crop": "Groundnut", "season": "Kharif",
     "soil_types": ["sandy", "loamy"], "water_requirement": "medium",
     "msp_per_quintal": 6783, "yield_per_acre": "10-14 quintals",
     "notes": "Saurashtra's primary crop. Good market infrastructure. Quality oil."},

    {"state": "Gujarat", "district": "Mehsana", "crop": "Cumin (Jeera)", "season": "Rabi",
     "soil_types": ["sandy", "loamy"], "water_requirement": "low",
     "msp_per_quintal": None, "yield_per_acre": "2-4 quintals",
     "notes": "High value spice. India's largest cumin producer. Fetches ₹20,000+/quintal."},

    # ── Uttar Pradesh ─────────────────────────────────────────────────────────
    {"state": "Uttar Pradesh", "district": "Lucknow", "crop": "Wheat", "season": "Rabi",
     "soil_types": ["alluvial", "loamy"], "water_requirement": "medium",
     "msp_per_quintal": 2275, "yield_per_acre": "16-20 quintals",
     "notes": "UP is India's largest wheat producer. Gangetic plains advantage."},

    {"state": "Uttar Pradesh", "district": "Meerut", "crop": "Sugarcane", "season": "Kharif",
     "soil_types": ["alluvial", "loamy"], "water_requirement": "high",
     "msp_per_quintal": 340, "yield_per_acre": "280-350 quintals",
     "notes": "Western UP sugarcane belt. Multiple mills ensure procurement."},

    {"state": "Uttar Pradesh", "district": "Banda", "crop": "Lentil (Masoor)", "season": "Rabi",
     "soil_types": ["loamy", "clay"], "water_requirement": "low",
     "msp_per_quintal": 6425, "yield_per_acre": "6-9 quintals",
     "notes": "Bundelkhand's pulse crop. High MSP. Minimal inputs required."},

    # ── West Bengal ──────────────────────────────────────────────────────────
    {"state": "West Bengal", "district": "Bardhaman", "crop": "Rice (Paddy)", "season": "Kharif",
     "soil_types": ["alluvial", "clay"], "water_requirement": "high",
     "msp_per_quintal": 2300, "yield_per_acre": "20-26 quintals",
     "notes": "WB's rice bowl. 3 crops/year possible. Excellent irrigation network."},

    {"state": "West Bengal", "district": "Jalpaiguri", "crop": "Tea", "season": "Kharif",
     "soil_types": ["loamy", "sandy"], "water_requirement": "high",
     "msp_per_quintal": None, "yield_per_acre": "8-12 quintals (made tea)",
     "notes": "Darjeeling tea premium. Perennial crop, 50+ year investment."},
]

# COMMAND ----------
# MAGIC %md ## Step 2: Write to Delta Lake

schema = StructType([
    StructField("state", StringType(), True),
    StructField("district", StringType(), True),
    StructField("crop", StringType(), True),
    StructField("season", StringType(), True),
    StructField("soil_types", StringType(), True),          # stored as JSON array string
    StructField("water_requirement", StringType(), True),
    StructField("msp_per_quintal", DoubleType(), True),
    StructField("yield_per_acre", StringType(), True),
    StructField("notes", StringType(), True),
])

# Convert soil_types list → JSON string for Delta Lake storage
rows = []
for r in CROP_RECORDS:
    rows.append((
        r["state"],
        r.get("district"),
        r["crop"],
        r["season"],
        json.dumps(r.get("soil_types", [])),
        r.get("water_requirement"),
        float(r["msp_per_quintal"]) if r.get("msp_per_quintal") else None,
        r.get("yield_per_acre"),
        r.get("notes", ""),
    ))

df = spark.createDataFrame(rows, schema)
df.write.format("delta").mode("overwrite").saveAsTable("kisan_mitra.crop_advisory")
print(f"✅ Wrote {df.count()} crop records to Delta Lake")

# COMMAND ----------
# MAGIC %md ## Step 3: Quick validation

spark.sql("SELECT state, crop, season, msp_per_quintal FROM kisan_mitra.crop_advisory ORDER BY state").show(30)
