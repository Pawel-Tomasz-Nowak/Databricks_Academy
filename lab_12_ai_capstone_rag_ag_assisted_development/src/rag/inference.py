import argparse
import sys
import time
from datetime import datetime
import mlflow.deployments
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_session() -> SparkSession:
    """
    Retrieves or initializes the active SparkSession.
    """
    return SparkSession.builder.getOrCreate()


def retrieve_context(index, query_text: str, top_k: int = 5) -> list:
    """
    Queries the Vector Search index and parses matches into structured dictionaries.
    """
    search_results = index.similarity_search(
        query_text=query_text,
        columns=["chunk_id", "doc_name", "source_url", "content"],
        num_results=top_k,
    )

    hits = search_results.get("result", {}).get("data_array", [])
    chunks = []
    for hit in hits:
        chunks.append({
            "chunk_id": hit[0],
            "doc_name": hit[1],
            "source_url": hit[2],
            "content": hit[3],
        })
    return chunks


def build_augmented_prompt(query: str, chunks: list) -> str:
    """
    Builds a grounded system prompt combining retrieved chunks with strict guardrails.
    """
    formatted_blocks = [
        f"[Document: {c['doc_name']} | Source: {c['source_url']}]\n{c['content']}"
        for c in chunks
    ]
    context_text = "\n\n---\n\n".join(formatted_blocks)

    return (
        "You are a Principal Power BI and Data Modeling Architect. "
        "Answer the user query by synthesizing all technical details and trade-offs "
        "present in the context below. Rely ONLY on the provided context. "
        "Do not invent unmentioned facts. Always cite the exact source URLs at the end.\n\n"
        f"CONTEXT:\n{context_text}"
    )


def execute_inference(
    client, model_endpoint: str, system_prompt: str, user_query: str
) -> str:
    """
    Calls the Databricks Model Serving foundation model endpoint.
    """
    response = client.predict(
        endpoint=model_endpoint,
        inputs={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_query},
            ],
            "temperature": 0.1,
            "max_tokens": 800,
        },
    )
    return response["choices"][0]["message"]["content"]


def log_telemetry_to_delta(
    spark: SparkSession,
    table_name: str,
    log_id: str,
    query: str,
    response: str,
    chunks: list,
    latency: float,
) -> None:
    """
    Persists inference telemetry and lineage into the Unity Catalog audit table.
    """
    audit_schema = StructType([
        StructField("log_id", StringType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("user_query", StringType(), False),
        StructField("generated_response", StringType(), False),
        StructField("retrieved_chunk_ids", ArrayType(StringType()), True),
        StructField("retrieved_urls", ArrayType(StringType()), True),
        StructField("latency_seconds", FloatType(), False),
    ])

    record = [{
        "log_id": log_id,
        "timestamp": datetime.utcnow(),
        "user_query": query,
        "generated_response": response,
        "retrieved_chunk_ids": [c["chunk_id"] for c in chunks],
        "retrieved_urls": list(set(c["source_url"] for c in chunks)),
        "latency_seconds": float(latency),
    }]

    df = spark.createDataFrame(record, schema=audit_schema)
    df.write.format("delta").mode("append").saveAsTable(table_name)


def main():
    parser = argparse.ArgumentParser(description="Production RAG Inference Pipeline")
    parser.add_argument("--catalog", required=True, help="Unity Catalog name")
    parser.add_argument("--schema", required=True, help="Schema name")
    parser.add_argument(
        "--endpoint",
        default="powerbi_docs_vs_endpoint",
        help="Vector Search endpoint name",
    )
    parser.add_argument(
        "--model",
        default="databricks-meta-llama-3-1-70b-instruct",
        help="Serving endpoint name",
    )
    parser.add_argument(
        "--query",
        default="When should bi-directional relationship filters be avoided in Power BI?",
        help="Input user query",
    )
    args = parser.parse_args()

    spark = get_spark_session()
    vsc = VectorSearchClient()
    deploy_client = mlflow.deployments.get_deploy_client("databricks")

    index_name = f"{args.catalog}.{args.schema}.powerbi_docs_vs_index"
    audit_table = f"{args.catalog}.{args.schema}.rag_inference_logs"

    start_time = time.perf_counter()
    index = vsc.get_index(endpoint_name=args.endpoint, index_name=index_name)

    chunks = retrieve_context(index, args.query, top_k=5)
    prompt = build_augmented_prompt(args.query, chunks)
    answer = execute_inference(deploy_client, args.model, prompt, args.query)
    duration = time.perf_counter() - start_time

    log_id = f"job_eval_{int(time.time())}"
    log_telemetry_to_delta(
        spark, audit_table, log_id, args.query, answer, chunks, duration
    )
    print(f"RAG execution logged successfully ({log_id}). Latency: {duration:.2f}s")


if __name__ == "__main__":
    main()