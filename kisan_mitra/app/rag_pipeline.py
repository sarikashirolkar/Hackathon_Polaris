"""
Kisan Mitra — RAG Pipeline
Loads the FAISS index + documents from DBFS (built by notebook 03).
Answers crop advisory queries with semantic search + LLM synthesis.
"""
import json
import os
import numpy as np

# ── Lazy imports so the file can be imported even before deps are installed ──
_index = None
_documents = None
_deploy_client = None

FAISS_PATH = os.environ.get("KISAN_FAISS_PATH", "/Workspace/Users/sarikashirolkar@gmail.com/kisan_mitra/faiss_index.bin")
DOCS_PATH  = os.environ.get("KISAN_DOCS_PATH",  "/Workspace/Users/sarikashirolkar@gmail.com/kisan_mitra/documents.json")

# Databricks Foundation Model API endpoint (open-source Llama-3.1-70B)
DATABRICKS_HOST  = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
LLM_ENDPOINT     = "databricks-meta-llama-3-1-70b-instruct"
EMBED_ENDPOINT   = "databricks-bge-large-en"


def _load_artifacts():
    """Load FAISS index and documents (once, lazily)."""
    global _index, _documents, _deploy_client
    if _index is not None:
        return

    import faiss
    import mlflow.deployments

    print(f"[RAG] Loading FAISS index from {FAISS_PATH} …")
    _index = faiss.read_index(FAISS_PATH)

    print(f"[RAG] Loading documents from {DOCS_PATH} …")
    with open(DOCS_PATH, encoding="utf-8") as f:
        _documents = json.load(f)

    _deploy_client = mlflow.deployments.get_deploy_client("databricks")
    print(f"[RAG] Ready — {len(_documents)} crop records indexed.")


def _semantic_search(query: str, k: int = 5) -> list[dict]:
    """Return top-k most relevant crop records."""
    _load_artifacts()
    emb_resp = _deploy_client.predict(
        endpoint=EMBED_ENDPOINT,
        inputs={"input": [query]},
    )
    embedding = np.array([item["embedding"] for item in emb_resp["data"]], dtype=np.float32)
    _, indices = _index.search(embedding, k)
    return [_documents[i] for i in indices[0] if i < len(_documents)]


def _synthesize_with_llm(query: str, docs: list[dict]) -> str:
    """Call Databricks Foundation Model API to synthesize a recommendation."""
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN:
        # Fallback: format results directly without LLM
        return _format_results_directly(docs)

    from openai import OpenAI

    db_client = OpenAI(
        api_key=DATABRICKS_TOKEN,
        base_url=f"{DATABRICKS_HOST}/serving-endpoints",
    )

    context = "\n\n".join(
        f"Crop: {d['crop']}\nState: {d['state']}\nDistrict: {d.get('district','—')}\n"
        f"Season: {d['season']}\nSoil: {d.get('soil_types','—')}\n"
        f"Water: {d.get('water_requirement','—')}\nMSP 2024-25: ₹{d.get('msp_per_quintal','—')}/quintal\n"
        f"Yield/acre: {d.get('yield_per_acre','—')}\nNotes: {d.get('notes','')}"
        for d in docs
    )

    prompt = f"""You are an agricultural expert advisor for Indian farmers.
Based on the following crop data, give a clear recommendation for:
{query}

Crop data:
{context}

Reply in 3-4 sentences. Recommend the top 2-3 crops, why they suit the farmer,
MSP price, and any key tip. Be simple and farmer-friendly."""

    response = db_client.chat.completions.create(
        model=LLM_ENDPOINT,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=512,
        temperature=0.3,
    )
    return response.choices[0].message.content


def _format_results_directly(docs: list[dict]) -> str:
    """Format search results without an LLM (used when Databricks creds absent)."""
    lines = ["**Top Crop Recommendations:**\n"]
    for i, d in enumerate(docs[:3], 1):
        lines.append(
            f"{i}. **{d['crop']}** ({d['season']})\n"
            f"   - Soil: {d.get('soil_types', '—')} | Water: {d.get('water_requirement','—')}\n"
            f"   - MSP: ₹{d.get('msp_per_quintal','N/A')}/quintal | "
            f"Yield: {d.get('yield_per_acre','N/A')}/acre\n"
            f"   - {d.get('notes','')}\n"
        )
    return "\n".join(lines)


def query_crop_advisory(
    state: str,
    season: str,
    acres: float,
    district: str | None = None,
    soil_type: str | None = None,
    water_availability: str | None = None,
) -> dict:
    """
    Main entry point called by the agent tool.
    Returns a dict with recommendations and metadata.
    """
    # Build semantic query
    parts = [f"crops for {state}"]
    if district:
        parts.append(district)
    parts.append(f"{season} season")
    if soil_type:
        parts.append(f"{soil_type} soil")
    if water_availability:
        parts.append(f"{water_availability} water availability")
    if acres:
        size = "small" if acres < 3 else ("medium" if acres < 10 else "large")
        parts.append(f"{size} farm {acres} acres")
    query = " ".join(parts)

    # Search
    relevant_docs = _semantic_search(query, k=5)

    # Filter by season if possible
    season_filtered = [d for d in relevant_docs if d.get("season") == season]
    final_docs = season_filtered if season_filtered else relevant_docs

    # Synthesize
    synthesis = _synthesize_with_llm(query, final_docs)

    return {
        "query": query,
        "top_crops": [d["crop"] for d in final_docs[:3]],
        "recommendation_text": synthesis,
        "source_records": final_docs[:3],
    }
