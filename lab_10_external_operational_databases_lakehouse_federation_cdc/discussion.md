# Lab 10 Part B Notes: Change Data Capture (CDC) & Incremental Merges

## 1. Implementation Overview
- **CDC Simulation in Neon PostgreSQL:** Added metadata audit columns (`updated_at`, `is_deleted`) alongside a PostgreSQL row-level trigger automating timestamp updates upon every `UPDATE`.
- **Target Delta Table:** Configured `target_artists_cdc` inside Unity Catalog (`dbr_dev.music_analytics`).
- **Incremental Synchronization:** Implemented an idempotent `MERGE INTO` operation handling:
  - Deletions: Flagging soft-deleted rows (`WHEN MATCHED AND source.is_deleted = TRUE`).
  - Updates: Modifying altered records (`WHEN MATCHED AND source.updated_at > target.updated_at`).
  - Insertions: Ingesting newly contracted artists (`WHEN NOT MATCHED AND source.is_deleted = FALSE`).

---

## 2. Technical Discussion & Architectural Trade-offs

### A. Batch vs. CDC Ingestion
- **Full Batch Ingestion (Truncate & Load / Append Dump):**
  - *Mechanism:* Ingests the full operational table on every run.
  - *Drawbacks:* Computation scales linearly with historical data size; causes network saturation, high cluster runtime, and storage bloat from redundant rows.
- **Change Data Capture (CDC):**
  - *Mechanism:* Identifies and transmits strictly modified or newly inserted rows between processing boundaries.
  - *Advantages:* Minimal network transfer, bounded micro-batch compute runtime, and immediate reflection of operational state in the lakehouse.

### B. Slowly Changing Dimensions (SCD Type 1 vs. Type 2)
- **SCD Type 1 (Current State Overwrite):**
  - The pattern implemented in this lab (`MERGE INTO` overwriting the matching `artist_id`).
  - *Trade-off:* Preserves the latest operational state with minimal storage footprint, but permanently loses historical state prior to the update.
- **SCD Type 2 (Historical Tracking):**
  - Employs validity windows (`valid_from`, `valid_to`, `is_current`) to close existing records and append updated records as new active rows.
  - *Trade-off:* Essential for historical point-in-time metrics and auditing, at the expense of higher table cardinality and join complexity.

### C. Handling Late-Arriving Data
- **Problem:** Operational updates or external events arriving out of order due to network partitions or async queuing.
- **Mitigation Pattern:**
  - Enforce monotonic ordering in the `MERGE` predicate:
    ```sql
    WHEN MATCHED AND source.updated_at > target.updated_at THEN UPDATE ...
    ```
  - This prevents an out-of-order, stale payload from overwriting newer, already-committed state in the Delta table.