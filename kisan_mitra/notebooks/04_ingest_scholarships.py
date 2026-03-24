# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 4 — Scholarship & Scheme Data → Delta Lake
# MAGIC Loads central + state government scholarship schemes for Indian students.

# COMMAND ----------

import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md ## Scholarship dataset (data.gov.in + NSP + Ministry portals)

# Each record = one scholarship scheme
# Sources: scholarships.gov.in, NSP, Ministry of Education, state portals
SCHOLARSHIPS = [

    # ── Central Government — SC/ST ─────────────────────────────────────────
    {
        "scheme_id": "NSP_SC_POST",
        "name": "NSP Post-Matric Scholarship for SC Students",
        "provider": "Ministry of Social Justice & Empowerment",
        "level": ["11th", "12th", "UG", "PG", "Diploma", "ITI"],
        "category": ["SC"],
        "max_income": 250000,
        "amount_per_year": 12000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "scholarships.gov.in (NSP Portal)",
        "deadline": "October–November",
        "description": "Covers tuition, maintenance, and book allowance for SC students studying after Class 10. One of India's largest scholarship programs.",
    },
    {
        "scheme_id": "NSP_ST_POST",
        "name": "NSP Post-Matric Scholarship for ST Students",
        "provider": "Ministry of Tribal Affairs",
        "level": ["11th", "12th", "UG", "PG", "Diploma", "ITI"],
        "category": ["ST"],
        "max_income": 250000,
        "amount_per_year": 12000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "scholarships.gov.in (NSP Portal)",
        "deadline": "October–November",
        "description": "Scholarship for ST students pursuing post-matric education. Covers fees and maintenance allowance.",
    },
    {
        "scheme_id": "NSP_SC_PRE",
        "name": "NSP Pre-Matric Scholarship for SC Students (Class 9-10)",
        "provider": "Ministry of Social Justice & Empowerment",
        "level": ["9th", "10th"],
        "category": ["SC"],
        "max_income": 250000,
        "amount_per_year": 3500,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "scholarships.gov.in (NSP Portal)",
        "deadline": "September–October",
        "description": "Supports SC students in Class 9 and 10 to reduce dropout rates. Day scholars get ₹150/month; hostellers ₹350/month.",
    },
    {
        "scheme_id": "NSP_ST_PRE",
        "name": "NSP Pre-Matric Scholarship for ST Students (Class 9-10)",
        "provider": "Ministry of Tribal Affairs",
        "level": ["9th", "10th"],
        "category": ["ST"],
        "max_income": 250000,
        "amount_per_year": 3500,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "scholarships.gov.in (NSP Portal)",
        "deadline": "September–October",
        "description": "Pre-matric support for ST students with maintenance allowance and book grant.",
    },

    # ── Central Government — OBC / EWS ────────────────────────────────────
    {
        "scheme_id": "YASASVI",
        "name": "PM YASASVI — Young Achievers Scholarship Award Scheme",
        "provider": "Ministry of Social Justice & Empowerment",
        "level": ["9th", "10th", "11th", "12th"],
        "category": ["OBC", "EBC", "DNT"],
        "max_income": 250000,
        "amount_per_year": 75000,
        "farmer_family_bonus": True,
        "states": "all",
        "how_to_apply": "yet.nta.ac.in (entrance test required)",
        "deadline": "August–September (exam in September)",
        "description": "₹75,000/year for Class 9-10, ₹1,25,000/year for Class 11-12. Based on entrance exam. Farmer-family students prioritised in tiebreakers.",
    },
    {
        "scheme_id": "NSP_OBC_POST",
        "name": "NSP Post-Matric Scholarship for OBC Students",
        "provider": "Ministry of Social Justice & Empowerment",
        "level": ["11th", "12th", "UG", "PG", "Diploma"],
        "category": ["OBC"],
        "max_income": 100000,
        "amount_per_year": 10000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "scholarships.gov.in (NSP Portal)",
        "deadline": "October–November",
        "description": "Post-matric support for OBC students with family income below ₹1 lakh. Covers course fees up to ₹5,000 and maintenance allowance.",
    },

    # ── Central Government — Meritorious / General ─────────────────────────
    {
        "scheme_id": "CENTRAL_SECTOR",
        "name": "Central Sector Scholarship for College & University Students",
        "provider": "Ministry of Education",
        "level": ["UG", "PG"],
        "category": ["General", "OBC", "SC", "ST", "EWS"],
        "max_income": 450000,
        "amount_per_year": 20000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "scholarships.gov.in (NSP Portal)",
        "deadline": "October–November",
        "description": "For students in top 20th percentile of Class 12 board exams. ₹10,000/year for UG first 3 years, ₹20,000 for PG. 82,000 scholarships annually.",
    },
    {
        "scheme_id": "INSPIRE",
        "name": "INSPIRE Scholarship (SHE) — Science Higher Education",
        "provider": "Department of Science & Technology",
        "level": ["UG", "PG"],
        "category": ["General", "OBC", "SC", "ST", "EWS"],
        "max_income": None,
        "amount_per_year": 80000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "online-inspire.gov.in",
        "deadline": "October–December",
        "description": "₹80,000/year for top 1% students pursuing BSc/MSc in Natural & Basic Sciences. For students who scored top 1% in Class 12 or JEE/NEET rank.",
    },
    {
        "scheme_id": "VIDYALAXMI",
        "name": "PM Vidyalaxmi — Education Loan Subsidy",
        "provider": "Ministry of Education",
        "level": ["UG", "PG"],
        "category": ["General", "OBC", "SC", "ST", "EWS"],
        "max_income": 800000,
        "amount_per_year": 0,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "vidyalaxmi.co.in",
        "deadline": "Rolling (year-round)",
        "description": "3% interest subvention on education loans up to ₹10 lakh for students in top-ranked institutions. No collateral needed. Farmer-family students get priority.",
    },
    {
        "scheme_id": "BEGUM_HAZRAT",
        "name": "Begum Hazrat Mahal National Scholarship (Minority Girls)",
        "provider": "Maulana Azad Education Foundation",
        "level": ["9th", "10th", "11th", "12th"],
        "category": ["Minority"],
        "max_income": 200000,
        "amount_per_year": 10000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "maef.nic.in",
        "deadline": "September–October",
        "description": "For Muslim, Christian, Sikh, Buddhist, Jain, Parsi girls. Class 9-10: ₹5,000; Class 11-12: ₹6,000. Merit based.",
    },
    {
        "scheme_id": "MAEF_MINORITY",
        "name": "Maulana Azad National Fellowship (Minority PhD Students)",
        "provider": "Ministry of Minority Affairs",
        "level": ["PhD"],
        "category": ["Minority"],
        "max_income": None,
        "amount_per_year": 150000,
        "farmer_family_bonus": False,
        "states": "all",
        "how_to_apply": "minorityaffairs.gov.in",
        "deadline": "November–December",
        "description": "Full fellowship for minority students pursuing PhD — JRF rate + contingency + HRA. 756 slots per year.",
    },

    # ── Farmer-Family Specific ─────────────────────────────────────────────
    {
        "scheme_id": "ICAR_FARMER",
        "name": "ICAR — National Talent Scholarship (Agriculture Students)",
        "provider": "Indian Council of Agricultural Research",
        "level": ["UG"],
        "category": ["General", "OBC", "SC", "ST", "EWS"],
        "max_income": None,
        "amount_per_year": 24000,
        "farmer_family_bonus": True,
        "states": "all",
        "how_to_apply": "icar.org.in (AIEEA entrance exam)",
        "deadline": "May–June (exam)",
        "description": "₹2,000/month for BSc Agriculture students in ICAR-affiliated universities. Priority to children of farmers and rural families.",
    },
    {
        "scheme_id": "PM_KISAN_EDU",
        "name": "Kisan Putra Scholarship (State-level farmer-family schemes)",
        "provider": "Various State Agriculture Departments",
        "level": ["9th", "10th", "11th", "12th", "UG"],
        "category": ["General", "OBC", "SC", "ST", "EWS"],
        "max_income": 200000,
        "amount_per_year": 15000,
        "farmer_family_bonus": True,
        "states": "all",
        "how_to_apply": "State agriculture department portal (varies by state)",
        "deadline": "August–October",
        "description": "Multiple states (Maharashtra, Punjab, Karnataka, MP) offer scholarships specifically for children of registered farmers. Check your state agriculture portal.",
    },

    # ── State-Specific ─────────────────────────────────────────────────────
    {
        "scheme_id": "MH_RAJARSHI",
        "name": "Maharashtra — Rajarshi Chhatrapati Shahu Maharaj Shishyavrutti",
        "provider": "Government of Maharashtra",
        "level": ["11th", "12th", "UG", "PG"],
        "category": ["OBC", "VJNT", "SBC"],
        "max_income": 800000,
        "amount_per_year": 60000,
        "farmer_family_bonus": True,
        "states": ["Maharashtra"],
        "how_to_apply": "mahadbt.maharashtra.gov.in",
        "deadline": "October–November",
        "description": "Up to ₹5,000/month for OBC/VJNT/SBC students in Maharashtra. Priority to students from farming families.",
    },
    {
        "scheme_id": "PB_DR_AMBEDKAR",
        "name": "Punjab — Dr Ambedkar Post Matric Scholarship",
        "provider": "Government of Punjab",
        "level": ["11th", "12th", "UG", "PG", "Diploma"],
        "category": ["SC"],
        "max_income": 250000,
        "amount_per_year": 18000,
        "farmer_family_bonus": False,
        "states": ["Punjab"],
        "how_to_apply": "punjab.gov.in scholarship portal",
        "deadline": "November–December",
        "description": "State top-up over NSP for SC students in Punjab. Covers hostel + maintenance allowance.",
    },
    {
        "scheme_id": "TN_AMMA",
        "name": "Tamil Nadu — Amma Two Wheeler Scheme for Girls",
        "provider": "Government of Tamil Nadu",
        "level": ["UG"],
        "category": ["General", "OBC", "SC", "ST"],
        "max_income": 250000,
        "amount_per_year": 50000,
        "farmer_family_bonus": True,
        "states": ["Tamil Nadu"],
        "how_to_apply": "tnscholarship.net",
        "deadline": "August–September",
        "description": "Free scooter or ₹50,000 cash for girl students pursuing UG from lower-income families. Farmer families given first preference.",
    },
    {
        "scheme_id": "KA_SANDHYA",
        "name": "Karnataka — Sandhya Suraksha & Post Matric Scholarship",
        "provider": "Government of Karnataka",
        "level": ["11th", "12th", "UG", "PG"],
        "category": ["SC", "ST"],
        "max_income": None,
        "amount_per_year": 12000,
        "farmer_family_bonus": False,
        "states": ["Karnataka"],
        "how_to_apply": "karepass.cgg.gov.in",
        "deadline": "October–November",
        "description": "Karnataka top-up for SC/ST students. Additional ₹1,000/month over central NSP scholarship.",
    },
    {
        "scheme_id": "RJ_KALIBAI",
        "name": "Rajasthan — Kalibai Bheel Medhavi Chatra Scooty Yojana",
        "provider": "Government of Rajasthan",
        "level": ["UG"],
        "category": ["SC", "ST", "OBC", "General-EWS"],
        "max_income": 250000,
        "amount_per_year": 0,
        "farmer_family_bonus": True,
        "states": ["Rajasthan"],
        "how_to_apply": "hte.rajasthan.gov.in",
        "deadline": "October",
        "description": "Free scooty for girl students scoring 65%+ in Class 12. SC/ST/OBC farmer-family girls prioritised.",
    },
    {
        "scheme_id": "MP_GAON_BETI",
        "name": "Madhya Pradesh — Gaon ki Beti Scholarship",
        "provider": "Government of Madhya Pradesh",
        "level": ["UG"],
        "category": ["General", "OBC", "SC", "ST", "EWS"],
        "max_income": None,
        "amount_per_year": 6000,
        "farmer_family_bonus": True,
        "states": ["Madhya Pradesh"],
        "how_to_apply": "scholarshipportal.mp.nic.in",
        "deadline": "October–November",
        "description": "₹500/month for 10 months for girls from villages who scored 60%+ in Class 12. No income limit. All rural girls eligible — farmer families get bonus points.",
    },
]

# COMMAND ----------
# MAGIC %md ## Write to Delta Lake

schema = StructType([
    StructField("scheme_id",            StringType(),  False),
    StructField("name",                 StringType(),  True),
    StructField("provider",             StringType(),  True),
    StructField("level",                StringType(),  True),   # JSON array
    StructField("category",             StringType(),  True),   # JSON array
    StructField("max_income",           DoubleType(),  True),
    StructField("amount_per_year",      DoubleType(),  True),
    StructField("farmer_family_bonus",  BooleanType(), True),
    StructField("states",               StringType(),  True),   # JSON array or "all"
    StructField("how_to_apply",         StringType(),  True),
    StructField("deadline",             StringType(),  True),
    StructField("description",          StringType(),  True),
])

rows = []
for s in SCHOLARSHIPS:
    rows.append((
        s["scheme_id"],
        s["name"],
        s["provider"],
        json.dumps(s["level"]),
        json.dumps(s["category"]),
        float(s["max_income"]) if s.get("max_income") else None,
        float(s.get("amount_per_year", 0)),
        bool(s.get("farmer_family_bonus", False)),
        json.dumps(s["states"]) if isinstance(s["states"], list) else s["states"],
        s["how_to_apply"],
        s["deadline"],
        s["description"],
    ))

df = spark.createDataFrame(rows, schema)
df.write.format("delta").mode("overwrite").saveAsTable("kisan_mitra.scholarships")
print(f"✅ Wrote {df.count()} scholarship schemes to Delta Lake")

# COMMAND ----------
# MAGIC %md ## Quick look

spark.sql("""
    SELECT scheme_id, name, category, max_income, amount_per_year, farmer_family_bonus
    FROM kisan_mitra.scholarships
    ORDER BY amount_per_year DESC
""").show(20, truncate=False)
