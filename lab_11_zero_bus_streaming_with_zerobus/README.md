# Lab 11: Direct Lakehouse Ingestion & Idempotent Processing (Zerobus Pattern)

## Overview
Implementation of a brokerless push ingestion architecture (the Zerobus Pattern) into Databricks Delta Lake, bypassing dedicated messaging middleware (Kafka / Event Hubs) for single-sink workloads.

## Architecture
```
[ External Producer / Python ]
  - Source-side UUID v4 idempotency keys
  - Micro-batched JSON payloads
  - Exponential backoff with jitter (handling 429/503)
        |
        v
HTTP POST (/api/2.0/sql/statements)
        |
        v
[ Databricks SQL Warehouse ]
        |
        v
[ Bronze: Raw Landing Buffer ] (`zerobus_music_events`)

- Append-only Delta table
- Captures 100% of attempts (including network retries)

        |
        v
[ Silver: Target State ] (`target_music_metrics`)
- Deduplicated & idempotent via MERGE INTO
- Deduplication via: ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _ingested_at DESC)
```

## Key Technical Concepts
1. **Informational Constraints in Delta Lake:** `PRIMARY KEY` is unenforced in Delta/Unity Catalog to avoid distributed locking overhead on write. Idempotency is enforced logically at the ingestion boundary.
2. **Client-Side Idempotency:** The producer generates an immutable `event_id` (UUID v4) prior to network transit, enabling downstream deduplication after retried transmissions.
3. **Resilience Pattern:** Exponential backoff with randomized jitter prevents self-inflicted thundering herd issues against the SQL Execution API.

## Quickstart

### 1. Environment Configuration
```bash
export DATABRICKS_HOST="https://<workspace-id>.azuredatabricks.net"
export DATABRICKS_OAUTH_TOKEN="<your-token>"
export DATABRICKS_WAREHOUSE_ID="<warehouse-id>"
```

### 2. DDL Setup (Databricks SQL)
```sql
CREATE TABLE IF NOT EXISTS dbr_dev.music_analytics.zerobus_music_events (
    event_id STRING NOT NULL,
    video_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    view_count BIGINT,
    like_count BIGINT,
    comment_count INT,
    _ingested_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;

CREATE TABLE IF NOT EXISTS dbr_dev.music_analytics.target_music_metrics (
    event_id STRING NOT NULL,
    video_id STRING NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    view_count BIGINT,
    like_count BIGINT,
    comment_count INT,
    _ingested_at TIMESTAMP,
    _processed_at TIMESTAMP DEFAULT current_timestamp()
) USING DELTA;
```

### 3. Run Producer
```bash
python src/producer/producer.py
```

### 4. Run Deduplication (Silver Reconciliation)
```sql
MERGE INTO dbr_dev.music_analytics.target_music_metrics AS target
USING (
  SELECT * EXCEPT(row_num)
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY _ingested_at DESC, event_timestamp DESC
      ) AS row_num
    FROM dbr_dev.music_analytics.zerobus_music_events
  )
  WHERE row_num = 1
) AS source
ON target.event_id = source.event_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```
