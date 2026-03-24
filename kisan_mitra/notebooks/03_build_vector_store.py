# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Build FAISS Vector Index on DBFS
# MAGIC Embeds crop documents with Databricks Foundation Model endpoint and persists to DBFS.

# COMMAND ----------

# MAGIC %pip install faiss-cpu mlflow -q

# COMMAND ----------

import json
import os
import numpy as np
import faiss
import mlflow
import mlflow.deployments

spark = spark  # noqa

# COMMAND ----------
# MAGIC %md ## Step 1: Load enriched data from Delta Lake

df = spark.table("kisan_mitra.crop_enriched")
records = [row.asDict() for row in df.collect()]
# Restore soil_types from JSON string
for r in records:
    try:
        r["soil_types"] = json.loads(r["soil_types"])
    except Exception:
        pass
print(f"Loaded {len(records)} records for embedding")

# COMMAND ----------
# MAGIC %md ## Step 2: Generate embeddings

texts = [r["document_text"] for r in records]

print(f"Generating embeddings for {len(texts)} records ...")
deploy_client = mlflow.deployments.get_deploy_client("databricks")
response = deploy_client.predict(
    endpoint="databricks-bge-large-en",
    inputs={"input": texts},
)
embeddings = np.array([item["embedding"] for item in response["data"]], dtype=np.float32)
print(f"Embeddings shape: {embeddings.shape}")

# COMMAND ----------
# MAGIC %md ## Step 3: Build FAISS index

dim = embeddings.shape[1]
index = faiss.IndexFlatL2(dim)         # exact search — small dataset so no need for IVF
index.add(embeddings)
print(f"FAISS index built — {index.ntotal} vectors, dim={dim}")

# Sanity-check: nearest neighbour of first record should be itself
D, I = index.search(embeddings[:1], k=3)
print("Top-3 neighbours of record 0:", [records[i]["crop"] for i in I[0]])

# COMMAND ----------
# MAGIC %md ## Step 4: Save to DBFS

DBFS_DIR = "/dbfs/kisan_mitra"
os.makedirs(DBFS_DIR, exist_ok=True)

INDEX_PATH = f"{DBFS_DIR}/faiss_index.bin"
DOCS_PATH  = f"{DBFS_DIR}/documents.json"

faiss.write_index(index, INDEX_PATH)
with open(DOCS_PATH, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"✅ FAISS index → {INDEX_PATH}")
print(f"✅ Documents   → {DOCS_PATH}")

# COMMAND ----------
# MAGIC %md ## Step 5: Print vector store metadata (MLflow optional on Free Edition)

print("Embedding model endpoint: databricks-bge-large-en")
print(f"Index type: IndexFlatL2 | num_vectors={index.ntotal} | embedding_dim={dim}")

# COMMAND ----------
# MAGIC %md ## Step 6: Test retrieval

def test_query(query_text: str, k: int = 3):
    q_resp = deploy_client.predict(
        endpoint="databricks-bge-large-en",
        inputs={"input": [query_text]},
    )
    q_emb = np.array([item["embedding"] for item in q_resp["data"]], dtype=np.float32)
    D, I = index.search(q_emb, k)
    print(f"\nQuery: '{query_text}'")
    for rank, (dist, idx) in enumerate(zip(D[0], I[0]), 1):
        r = records[idx]
        print(f"  {rank}. {r['crop']} ({r['state']}, {r['season']}) — dist={dist:.3f}")

test_query("best crops for Maharashtra Vidarbha black soil Kharif")
test_query("Rabi season low water Punjab loamy soil")
test_query("drought tolerant crop for sandy soil Rajasthan")
