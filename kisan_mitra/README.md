# 🌾 Kisan Mitra — AI Hyperlocal Farm Advisory

> **Built at Polaris School of Technology Hackathon, March 24 2026**

Kisan Mitra replaces a static form with a conversational AI agent that gathers a
farmer's profile through natural language and returns personalised crop
recommendations — backed by real India data in Databricks.

## What it does (1-2 sentences)

An AI agent talks to Indian farmers in plain English or Hindi, automatically
extracts their farm profile (location, season, soil, water), queries a
Delta Lake crop database via semantic search, and recommends the top crops
with MSP prices and yield estimates — no forms, no dropdowns.

## Architecture

```
Farmer (chat UI)
      │
      ▼
 Claude Haiku Agent  ←── tool call ──►  FAISS Vector Store (DBFS)
      │                                         │
      │                                    Delta Lake
      │                                  (crop_enriched)
      ▼                                         │
 Llama 3.1-70B (Databricks FMI)  ◄─── retrieved docs
      │
      ▼
 Crop Recommendation (MSP + yield + notes)
```

**Databricks components used:**
- **Delta Lake** — structured storage for 32 crop-district-season records
- **Apache Spark / PySpark** — ETL, feature engineering, suitability scoring
- **FAISS on DBFS** — semantic vector search over enriched crop documents
- **MLflow** — experiment tracking for ETL run + vector store build
- **Databricks Foundation Model API** — Llama-3.1-70B for RAG synthesis
- **Databricks App (Gradio)** — deployed UI

## How to run

### Prerequisites
```bash
pip install -r app/requirements.txt
export ANTHROPIC_API_KEY="sk-ant-..."
export DATABRICKS_HOST="https://<your-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

### Step 1 — Run Databricks notebooks (in order)
1. `notebooks/01_ingest_data.py`   → creates `kisan_mitra.crop_advisory` Delta table
2. `notebooks/02_spark_etl.py`    → creates `kisan_mitra.crop_enriched` + MLflow run
3. `notebooks/03_build_vector_store.py` → builds FAISS index on DBFS

### Step 2 — Launch the app
```bash
cd app
python app.py
# Opens on http://localhost:8080
```

### Step 3 — Demo steps
1. Open the app in browser
2. Type: *"Hi, I'm a farmer in Nashik Maharashtra, I have 3 acres of black soil"*
3. Agent asks: *"What season are you planning to plant?"*
4. Type: *"Kharif season"*
5. Agent queries FAISS, calls Llama-3.1, returns top 3 crops with MSP prices

## Demo video

[2-minute demo showing the full conversation flow]

## Project write-up (500 chars)

Kisan Mitra is an AI farm advisor that eliminates the friction of digital
advisory tools for Indian farmers. Instead of complex forms, farmers just
describe their situation in plain language. A Claude-based agent extracts
their profile, queries a Delta Lake crop database via FAISS semantic search,
and uses Llama-3.1-70B to synthesise personalised recommendations with
MSP prices, yield estimates, and planting tips — all grounded in real data.
