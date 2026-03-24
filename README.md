# Kisan Mitra

AI advisory assistant for Indian farming families, built during the Polaris Hackathon (March 2026).

Kisan Mitra combines two practical copilots in one conversational app:
- Crop Advisory: recommends suitable crops using location, season, soil, and farm size.
- Student Schemes: finds scholarships and education schemes for students from farming households.

## Why This Project

Many advisory systems rely on rigid forms and static portals. Kisan Mitra replaces that with chat-first guidance in simple language, while grounding answers in structured data and retrieval pipelines built on Databricks.

## Core Features

- Conversational profile capture (instead of long forms)
- RAG-powered crop recommendations from indexed crop records
- Scholarship eligibility matching using Spark SQL on Delta tables
- Local fallback behavior when Databricks runtime/services are unavailable
- Single Streamlit UI with separate tabs for farming and education support

## System Architecture

### Crop Advisory Flow
1. User chats in Streamlit (`kisan_mitra/app/app.py`).
2. `agent.py` gathers required fields (`state`, `season`, `acres`) and calls tool `query_crop_advisory`.
3. `rag_pipeline.py` performs semantic retrieval from FAISS + documents.
4. Final recommendation is synthesized using Databricks Foundation Model endpoint (or formatted directly in fallback mode).

### Student Schemes Flow
1. User chats in the Student Schemes tab.
2. `student_agent.py` collects profile (`state`, `category`, `level`, etc.) and calls `find_scholarships`.
3. Spark SQL filters scholarship records from Delta table `kisan_mitra.scholarships`.
4. Agent explains top relevant schemes with amount, eligibility, deadline, and application route.

## Tech Stack

- Databricks (Delta Lake, Spark, workspace artifacts)
- MLflow deployments client (embedding + model endpoints)
- FAISS for vector retrieval
- Anthropic Claude tool-calling for agent orchestration
- Streamlit for application UI
- Python 3.10+

## Repository Structure

- `databricks.yml`: Databricks project config
- `kisan_mitra/notebooks/01_ingest_data.py`: crop ingestion
- `kisan_mitra/notebooks/02_spark_etl.py`: crop ETL/enrichment
- `kisan_mitra/notebooks/03_build_vector_store.py`: FAISS build artifacts
- `kisan_mitra/notebooks/04_ingest_scholarships.py`: scholarship ingestion
- `kisan_mitra/app/app.py`: Streamlit app entrypoint
- `kisan_mitra/app/agent.py`: crop advisory agent
- `kisan_mitra/app/student_agent.py`: scholarship advisor agent
- `kisan_mitra/app/rag_pipeline.py`: retrieval + synthesis pipeline
- `kisan_mitra/app/app.yaml`: Databricks app command/env config

## Setup

### 1) Install dependencies

```bash
cd kisan_mitra/app
pip install -r requirements.txt
```

### 2) Set environment variables

```bash
export ANTHROPIC_API_KEY="<your_anthropic_key>"
export DATABRICKS_HOST="https://<your-workspace>.azuredatabricks.net"
export DATABRICKS_TOKEN="<your_databricks_token>"

# Optional: override artifact paths if different in your workspace
export KISAN_FAISS_PATH="/Workspace/Users/<you>/kisan_mitra/faiss_index.bin"
export KISAN_DOCS_PATH="/Workspace/Users/<you>/kisan_mitra/documents.json"
```

## Data + Index Build (Databricks)

Run notebooks in this order:
1. `01_ingest_data.py`
2. `02_spark_etl.py`
3. `03_build_vector_store.py`
4. `04_ingest_scholarships.py`

This creates the Delta tables and retrieval artifacts used by both assistants.

## Run the App

```bash
cd kisan_mitra/app
streamlit run app.py
```

Default local URL: `http://localhost:8501`

## Example Demo Prompts

### Crop Advisory
"I am from Nashik, Maharashtra. I have 3 acres of black soil and I want to plant in Kharif."

### Student Schemes
"I am in 11th class in Maharashtra, category OBC, family income 2 lakh, and my parents are farmers."

## Notes and Limitations

- Recommendation quality depends on freshness and coverage of underlying datasets.
- Scholarship deadlines and amounts may change; verify before final submission.
- For production, use managed secrets for API keys/tokens and stricter data validation.

## Hackathon Context

Built for rapid, practical impact: an AI assistant that supports both farm planning and student opportunity discovery for rural families in one interface.
