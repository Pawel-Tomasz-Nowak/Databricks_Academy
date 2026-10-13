# Databricks notebook source
# MAGIC %md
# MAGIC ## Libraries

# COMMAND ----------

# MAGIC %pip install langchain-text-splitters

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %pip install databricks-ai-search

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import re
import requests
from bs4 import BeautifulSoup

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from langchain_text_splitters import RecursiveCharacterTextSplitter

# COMMAND ----------

# MAGIC %md
# MAGIC ## Constants

# COMMAND ----------

volume_path = "/Volumes/dbr_dev_trial/music_analytics/landing_zone/rag_docs"

PBI_ARTICLES = {
    "pbi_star_schema_guidance.md": "https://learn.microsoft.com/en-us/power-bi/guidance/star-schema",
    "pbi_rls_guidance.md": "https://learn.microsoft.com/en-us/power-bi/guidance/rls-guidance",
    "pbi_dax_variables_optimization.md": "https://learn.microsoft.com/en-us/power-bi/guidance/dax-variables",
    "pbi_connect_azure_databricks.md": "https://learn.microsoft.com/en-us/azure/databricks/partners/bi/power-bi/desktop",
    "pbi_bi_directional_relationships.md":"https://learn.microsoft.com/en-us/power-bi/guidance/relationships-bidirectional-filtering"
}

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

catalog = "dbr_dev_trial"
schema = "music_analytics"
volume_path = f"/Volumes/{catalog}/{schema}/landing_zone/rag_docs"
target_table = f"{catalog}.{schema}.powerbi_docs_chunks"

# COMMAND ----------

os.makedirs(volume_path, exist_ok=True)
for filename, url in PBI_ARTICLES.items():
    print(f"Download: {url}...")
    response = requests.get(url, headers=headers, timeout=15)
    
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")
        main_content = soup.find("main", id="main") or soup.find("div", class_="content")
        
        if main_content:
            for unwanted in main_content(["script", "style", "nav", "footer", "form"]):
                unwanted.decompose()
            
            raw_text = main_content.get_text(separator="\n")

            clean_text = re.sub(r"\n\s*\n", "\n\n", raw_text).strip()
            
            target_path = os.path.join(volume_path, filename)
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(f"# Source: {url}\n\n" + clean_text)
            
            print(f" Downloaded: {filename} with ({len(clean_text)} characters)")
        else:
            print(f" Error: Couldn't find content container at{url}")
    else:
        print(f"HTTP error {response.status_code} for {url}")

# COMMAND ----------

display(dbutils.fs.ls("/Volumes/dbr_dev_trial/music_analytics/landing_zone/rag_docs/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Splitting text files into chunks

# COMMAND ----------

splitter = RecursiveCharacterTextSplitter(
    chunk_size=1000,
    chunk_overlap=150,
    separators=["\n\n", "\n", ". ", " ", ""]
)

chunked_records = []

for file_info in dbutils.fs.ls(volume_path):
    if file_info.name.endswith(".md"):
        # Ścieżka POSIX na wolumenie Databricks
        local_path = os.path.join(volume_path, file_info.name)
        
        with open(local_path, "r", encoding="utf-8") as f:
            raw_content = f.read()

        source_url = "unknown"
        clean_body = raw_content
        first_line = raw_content.split("\n", 1)[0]
        if first_line.startswith("# Source:"):
            source_url = first_line.replace("# Source:", "").strip()
            clean_body = raw_content.split("\n", 1)[1].strip()

        doc_chunks = splitter.split_text(clean_body)
        
        for idx, chunk in enumerate(doc_chunks):
            chunk_id = f"{file_info.name.replace('.md', '')}_{idx:04d}"
            chunked_records.append({
                "chunk_id": chunk_id,
                "doc_name": file_info.name,
                "source_url": source_url,
                "chunk_index": idx,
                "content": chunk
            })

print(f"Downloaded and cut {len(chunked_records)} chunks from documentations")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combining chunks into delta table

# COMMAND ----------

schema_def = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("doc_name", StringType(), True),
    StructField("source_url", StringType(), True),
    StructField("chunk_index", IntegerType(), True),
    StructField("content", StringType(), False)
])

df_chunks = spark.createDataFrame(chunked_records, schema=schema_def)

(df_chunks.write
    .format("delta")
    .mode("overwrite")
    .option("delta.enableChangeDataFeed", "true")
    .saveAsTable(target_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imposing constraints and sanity test the output

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {target_table} 
    ALTER COLUMN chunk_id SET NOT NULL
""")


spark.sql(f"""
          ALTER TABLE {target_table}
DROP CONSTRAINT IF EXISTS powerbi_docs_pk
""")

spark.sql(f"""
    ALTER TABLE {target_table} 
    ADD CONSTRAINT powerbi_docs_pk PRIMARY KEY(chunk_id)
""")

display(spark.sql(f"SELECT chunk_id, doc_name, source_url, substring(content, 1, 100) AS preview FROM {target_table} LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining Vector Search endpoint for powerbi docs

# COMMAND ----------

import time
from databricks.ai_search.client import VectorSearchClient

vsc = VectorSearchClient()

endpoint_name = "powerbi_docs_vs_endpoint"
source_table = f"{catalog}.{schema}.powerbi_docs_chunks"
index_name = f"{catalog}.{schema}.powerbi_docs_vs_index"


existing_endpoints = [ep["name"] for ep in vsc.list_endpoints().get("endpoints", [])]

if endpoint_name not in existing_endpoints:
    print(f"Creating vector search endpoint: {endpoint_name}...")
    vsc.create_endpoint(name=endpoint_name, endpoint_type="STANDARD")
else:
    print(f"Endpoint '{endpoint_name}' already exists.")

# Polling loop waiting for endpoint availability
while True:
    endpoint_details = vsc.get_endpoint(endpoint_name)
    state = endpoint_details.get("endpoint_status", {}).get("state")
    print(f"Current endpoint state: {state}")
    if state == "ONLINE":
        print("Vector search endpoint is online and operational.")
        break
    time.sleep(15)

# COMMAND ----------

# Verify if index already exists on the endpoint
existing_indexes = [idx["name"] for idx in vsc.list_indexes(endpoint_name).get("vector_indexes", [])]

if index_name not in existing_indexes:
    print(f"Creating Delta Sync Index: {index_name}...")
    vsc.create_delta_sync_index(
        endpoint_name=endpoint_name,
        index_name=index_name,
        source_table_name=source_table,
        pipeline_type="TRIGGERED",
        primary_key="chunk_id",
        embedding_source_column="content",
        embedding_model_endpoint_name="databricks-bge-large-en"
    )
else:
    print(f"Vector index '{index_name}' already exists.")

# Polling loop waiting for embedding generation and sync pipeline completion
index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)

while True:
    index_description = index.describe()
    is_ready = index_description.get("status", {}).get("ready", False)
    sync_status = index_description.get("status", {}).get("detailed_state", "UNKNOWN")
    
    if is_ready:
        print("Vector index synchronization complete. Ready for queries.")
        break
    
    print(f"Indexing status: {sync_status}. Waiting 20 seconds...")
    time.sleep(20)

# COMMAND ----------


query = "What is the guidance on star schema dimension tables and snowflake schemas?"

search_results = index.similarity_search(
    query_text=query,
    columns=["chunk_id", "doc_name", "source_url", "content"],
    num_results=3
)

for rank, hit in enumerate(search_results.get("result", {}).get("data_array", []), 1):
    chunk_id, doc_name, source_url, content, score = hit[0], hit[1], hit[2], hit[3], hit[4]
    print(f"--- RANK {rank} (Score: {score:.4f}) ---")
    print(f"Source: {doc_name} ({source_url})")
    print(f"Content:\n{content[:300]}...\n")

# COMMAND ----------

import mlflow.deployments
from databricks.ai_search.client import VectorSearchClient

vsc = VectorSearchClient()
deploy_client = mlflow.deployments.get_deploy_client("databricks")

index_name = f"{catalog}.{schema}.powerbi_docs_vs_index"
llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

index = vsc.get_index(endpoint_name=endpoint_name, index_name=index_name)


def retrieve_context(query_text: str, top_k: int = 3):
    """
    Queries the Databricks Vector Search index and extracts matching context chunks.
    """
    search_results = index.similarity_search(
        query_text=query_text,
        columns=["chunk_id", "doc_name", "source_url", "content"],
        num_results=top_k
    )
    
    hits = search_results.get("result", {}).get("data_array", [])
    retrieved_chunks = []
    
    for hit in hits:
        chunk_id, doc_name, source_url, content = hit[0], hit[1], hit[2], hit[3]
        retrieved_chunks.append({
            "chunk_id": chunk_id,
            "doc_name": doc_name,
            "source_url": source_url,
            "content": content
        })
    return retrieved_chunks

def generate_rag_response(user_query: str, top_k: int = 5):
    """
    Executes end-to-end RAG with enriched top-k retrieval and technical synthesis.
    """
    # Step A: Retrieve semantic context chunks
    context_data = retrieve_context(user_query, top_k=top_k)
    
    # Step B: Format context blocks explicitly including document metadata
    formatted_blocks = []
    for item in context_data:
        block = (
            f"[Document: {item['doc_name']} | Source: {item['source_url']}]\n"
            f"{item['content']}"
        )
        formatted_blocks.append(block)
        
    context_text = "\n\n---\n\n".join(formatted_blocks)
    
    # Step C: Construct instructions favoring comprehensive technical synthesis
    system_prompt = (
        "You are a Principal Power BI and Data Modeling Architect. "
        "Answer the user's inquiry thoroughly by synthesizing all relevant technical points, "
        "trade-offs, and design caveats found in the provided documentation context below. "
        "Explain the underlying mechanisms (e.g., filter propagation, ambiguity, query plans) "
        "mentioned in the text. "
        "Strict Rule: Rely ONLY on the provided context. Do not invent facts or unmentioned features. "
        "At the end of your response, list all unique Source URLs cited in the context.\n\n"
        f"CONTEXT:\n{context_text}"
    )
    
    # Step D: Invoke the serving endpoint
    response = deploy_client.predict(
        endpoint=llm_endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query}
            ],
            "temperature": 0.1,
            "max_tokens": 800
        }
    )
    
    generated_answer = response["choices"][0]["message"]["content"]
    return generated_answer, context_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## RAG-assisted LLM vs. Base LLM

# COMMAND ----------

test_query = (
    "When should bi-directional relationship filters be avoided in Power BI, "
    "and what specific performance or ambiguity risks do they introduce?"
)

print(f"=== TEST QUERY ===\n{test_query}\n")
rag_answer, sources = generate_rag_response(test_query, top_k=6)

print("=" * 60)
print("1. ANSWER WITH RAG (Grounded in Microsoft Documentation):")
print("=" * 60)
print(rag_answer)
print("\nRetrieved Source Documents:")
for src in sources:
    print(f"- {src['doc_name']} -> {src['source_url']}")

raw_response = deploy_client.predict(
    endpoint=llm_endpoint,
    inputs={
        "messages": [
            {
                "role": "system", 
                "content": "You are a general technical assistant. Answer the user's question based on your general training data."
            },
            {"role": "user", "content": test_query}
        ],
        "temperature": 0.1,
        "max_tokens": 600
    }
)
raw_answer = raw_response["choices"][0]["message"]["content"]

print("\n" + "=" * 60)
print("2. ANSWER WITHOUT RAG (Baseline LLM - Potential Hallucinations/Generics):")
print("=" * 60)
print(raw_answer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt & Inference Audit Logging

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType, ArrayType
import datetime
import time

# 1. Define audit log table schema
audit_table_name = f"{catalog}.{schema}.rag_inference_logs"

audit_schema = StructType([
    StructField("log_id", StringType(), False),
    StructField("timestamp", TimestampType(), False),
    StructField("user_query", StringType(), False),
    StructField("generated_response", StringType(), False),
    StructField("retrieved_chunk_ids", ArrayType(StringType()), True),
    StructField("retrieved_urls", ArrayType(StringType()), True),
    StructField("latency_seconds", FloatType(), False)
])
def query_and_log_rag(user_query: str, top_k: int = 5):
    """
    Executes the RAG pipeline, measures execution latency, and logs output to Delta table.
    """
    start_time = time.perf_counter()
    
    answer, sources = generate_rag_response(user_query, top_k=top_k)
    
    elapsed_time = time.perf_counter() - start_time

    chunk_ids = [src["chunk_id"] for src in sources]
    unique_urls = list(set([src["source_url"] for src in sources]))
    log_id = f"log_{int(time.time() * 1000)}"
    
    log_data = [{
        "log_id": log_id,
        "timestamp": datetime.datetime.now(datetime.UTC),
        "user_query": user_query,
        "generated_response": answer,
        "retrieved_chunk_ids": chunk_ids,
        "retrieved_urls": unique_urls,
        "latency_seconds": float(elapsed_time)
    }]
    
    df_log = spark.createDataFrame(log_data, schema=audit_schema)
    (df_log.write
        .format("delta")
        .mode("append")
        .saveAsTable(audit_table_name))
        
    print(f"Logged inference request {log_id} (Latency: {elapsed_time:.2f}s).")
    return answer

# COMMAND ----------

sample_queries = [
"When should bi-directional relationship filters be avoided in Power BI and what specific performance or ambiguity risks do they introduce?",
"What are the performance impacts of using DAX variables compared to repeating expressions?"
]

# COMMAND ----------

for query in sample_queries:
    answer = query_and_log_rag(query, top_k=5)
    print(f"Query: {query}\nAnswer: {answer}\n")

# COMMAND ----------

# DBTITLE 1,Discussion
# MAGIC %md
# MAGIC # Discussion
# MAGIC ## 1. Why RAG Beats Prompting Alone
# MAGIC
# MAGIC A standard language model relies entirely on what it memorized during pre-training (its frozen "parametric memory"). It has zero visibility into internal, proprietary, or recently updated documents. When asked about specific technical nuances, it tends to guess, producing plausible-sounding generic advice or outright hallucinations (such as claiming that relationship filters create "data redundancy").
# MAGIC
# MAGIC **RAG works like giving the model an open textbook with an exact reference bookmark.** Instead of forcing the model to recite facts from memory, the system first searches your knowledge base for relevant passages, and the model merely synthesizes the answer based on that verified context. This provides three concrete advantages:
# MAGIC - **Grounded in facts:** The model is constrained to the supplied text and cannot invent unsupported features.
# MAGIC - **Full auditability:** Every response cites its exact source URL (e.g., specific Microsoft Learn articles), allowing users to verify claims in seconds.
# MAGIC - **Instant updates:** When documentation changes, you simply append or update a row in the underlying Delta table. There is no need for slow, expensive model fine-tuning or retraining.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 2. Chunk Size vs. Retrieval Quality
# MAGIC
# MAGIC Splitting long articles into smaller pieces (*chunks*) requires finding a practical balance between context and search accuracy:
# MAGIC
# MAGIC | Chunk Size | Practical Impact |
# MAGIC |---|---|
# MAGIC | **Too small** (< 300 chars) | Chunks are chopped into isolated sentences. The vector represents sentence fragments without surrounding context, leading to **information starvation** where the model lacks the background needed to answer. |
# MAGIC | **Too large** (> 2,500 chars) | The vector loses semantic focus (it averages an entire multi-topic article). Irrelevant noise floods the prompt, token costs rise, and the model risks overlooking critical details buried in the text (**lost-in-the-middle effect**). |
# MAGIC | **Optimal** (800–1,200 chars, 10–15% overlap) | The sweet spot for technical docs: each chunk holds roughly one coherent concept or paragraph. The small overlap ensures thoughts are not severed mid-sentence across chunk boundaries. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 3. Governance and Cost for GenAI Workloads
# MAGIC
# MAGIC ### Data Governance & Security
# MAGIC - The source Delta table, the vector search index, and the inference audit log all sit under unified **Unity Catalog** governance.
# MAGIC - Access permissions are enforced at the catalog and table level: the system will never expose or cite proprietary documents to users who do not possess the required read permissions.
# MAGIC
# MAGIC ### Cost Management (FinOps)
# MAGIC - **Query efficiency:** Vector search similarity queries cost fractions of a cent. Retrieving only the top 3–5 relevant chunks is exponentially cheaper than stuffing entire documentation manuals into the prompt on every request.
# MAGIC - **Triggered synchronization:** Setting the vector index sync pipeline to **TRIGGERED** mode ensures embeddings are calculated on demand in micro-batches rather than keeping compute instances running 24/7 in the background, protecting cloud budgets during development and scheduled workloads.