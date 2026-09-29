import math
import os
import random
import time
import uuid
from datetime import datetime, timezone
import requests
import base64
from databricks.sdk import WorkspaceClient

# -------------------------------------------------------------------------
# 1. Credentials & Environment Configuration 
# -------------------------------------------------------------------------

w = WorkspaceClient()
secret_scope = "pawelnowak2004pri219_scope"

def fetch_credential(env_var_name: str, scope: str, key: str) -> str:
    """
    Pobiera poświadczenia priorytetowo ze zmiennych środowiskowych.
    Jeśli ich brak, pobiera z Databricks Secret Scope i dekoduje z Base64.
    """
    val = os.environ.get(env_var_name)
    if val is not None:
        return val
    
    # Pobranie przez SDK zwraca wartość zakodowaną w Base64
    b64_val = w.secrets.get_secret(scope=scope, key=key).value
    return base64.b64decode(b64_val).decode("utf-8")

HOST = fetch_credential("DATABRICKS_HOST", secret_scope, "pawelnowak-host")
TOKEN = fetch_credential("DATABRICKS_OAUTH_TOKEN", secret_scope, "pawelnowak-oauth-token")
WAREHOUSE_ID = fetch_credential("DATABRICKS_WAREHOUSE_ID", secret_scope, "pawelnowak-warehouse-id")

if not HOST or not TOKEN or not WAREHOUSE_ID:
    raise ValueError(
        "Missing required credentials: "
        "HOST, OAUTH_TOKEN, or WAREHOUSE_ID"
    )

HOST = HOST.strip().rstrip("/")
if not HOST.startswith("http"):
    HOST = f"https://{HOST}"

CATALOG = "dbr_dev"
SCHEMA = "music_analytics"
TABLE_NAME = "zerobus_music_events"
FULL_TABLE_NAME = f"{CATALOG}.{SCHEMA}.{TABLE_NAME}"

URL = f"{HOST}/api/2.0/sql/statements"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# -------------------------------------------------------------------------
# 2. Simulation & Batching Configuration
# -------------------------------------------------------------------------
N_RECORDS = 21
BATCH_SIZE = 10
N_BATCHES = math.ceil(N_RECORDS / BATCH_SIZE)

MAX_RETRIES = 3
BASE_DELAY = 1.0  # Base wait time for Exponential Backoff (seconds)

random.seed(2137)


def main():
    print(f"Starting Zerobus ingestion pipeline: {N_RECORDS} records across {N_BATCHES} batches...")
    # -------------------------------------------------------------------------
    # 3. Batch Generation and Dispatch Loop
    # -------------------------------------------------------------------------
    for batch_id in range(N_BATCHES):
        start = batch_id * BATCH_SIZE
        end = min(N_RECORDS, (batch_id + 1) * BATCH_SIZE)

        batch_rows: list[dict] = []
        
        # Safe duplicate calculation: prevent ZeroDivisionError
        batch_records_count = end - start
        retry_freq = max(1, int(0.2 * batch_records_count))

        for record_idx, record_id in enumerate(range(start, end)):
            # Generate idempotency key at the SOURCE prior to network transmission
            event_id = str(uuid.uuid4())
            event_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            payload = {
                "event_id": event_id,
                "video_id": "dQw4w9WgXcQ",
                "event_timestamp": event_timestamp,
                "view_count": int(5 * 10e6 * random.uniform(0, 1)),
                "like_count": int(5 * 10e5 * random.uniform(0, 1)),
                "comment_count": int(5 * 10e4 * random.uniform(0, 1)),
                "_ingested_at": event_timestamp
            }

            batch_rows.append(payload)

            # simulate network retries by appending duplicate payloads
            if record_idx % retry_freq == 0:
                batch_rows.append(payload)

        values_clauses = []
        for r in batch_rows:
            row_str = (
                f"('{r['event_id']}', '{r['video_id']}', "
                f"TIMESTAMP '{r['event_timestamp']}', "
                f"{r['view_count']}, {r['like_count']}, {r['comment_count']}, "
                f"TIMESTAMP '{r['_ingested_at']}')"
            )
            values_clauses.append(row_str)

        sql_statement = (
            f"INSERT INTO {FULL_TABLE_NAME} "
            f"(event_id, video_id, event_timestamp, view_count, like_count, comment_count, _ingested_at) "
            f"VALUES {', '.join(values_clauses)}"
        )

        request_body = {
            "warehouse_id": WAREHOUSE_ID,
            "statement": sql_statement,
            "wait_timeout": "30s"
        }

        print(f"\n[Batch {batch_id + 1}/{N_BATCHES}] Prepared {len(batch_rows)} rows (including simulated retries). Sending...")


        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = requests.post(
                    URL,
                    json=request_body,
                    headers=HEADERS,
                    timeout=35
                )

                # Databricks SQL API returns HTTP 200 with status SUCCEEDED on successful query execution
                if response.status_code == 200:
                    res_data = response.json()
                    exec_state = res_data.get("status", {}).get("state")
                    if exec_state == "SUCCEEDED":
                        print(f" -> Batch {batch_id + 1} successfully executed and committed (HTTP 200).")
                        break
                    else:
                        print(f" -> Query execution non-terminal state: {exec_state}. Response: {response.text}")
                else:
                    print(f" -> Upstream API error (HTTP {response.status_code}): {response.text}")
                    
            except requests.exceptions.RequestException as e:
                print(f" -> Network connection failure during attempt {attempt}: {e}")

            # Compute backoff interval if retries remain
            if attempt < MAX_RETRIES:
                wait_time = BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0.1, 0.5)
                print(f"    Backing off... retrying in {wait_time:.2f} seconds.")
                time.sleep(wait_time)
        else:
            raise RuntimeError(
                f"Critical failure: Batch {batch_id + 1} failed after {MAX_RETRIES} attempts. "
                "Terminating producer to avoid data inconsistency."
            )

    print("\nProducer run completed successfully. All payloads dispatched.")


if __name__ == "__main__":
    main()