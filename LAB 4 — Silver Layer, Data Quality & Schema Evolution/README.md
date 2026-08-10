# LAB 4 — Silver Layer, Data Quality & Schema Evolution

This laboratory demonstrates implementing a **Silver Layer** data pipeline with data quality enforcement and schema evolution using **Slowly Changing Dimensions (SCD)** Type I and Type II patterns in Databricks.

## 📁 Directory Structure

```
LAB 4 — Silver Layer, Data Quality & Schema Evolution/
├── Lab 4 (notebook)           # Main execution notebook
├── config.py                   # Table name configurations
├── data_generators.py          # Synthetic data generation
├── data_cleaner.py            # Data quality and cleansing logic
├── data_mergers.py            # Delta merge operations
├── preprocessing.py           # Orchestration pipeline
└── README.md                  # This documentation
```

## 🎯 Lab Objectives

1. **Bronze Layer**: Generate and ingest raw streaming event data
2. **Silver Layer**: Apply data quality transformations and deduplication
3. **SCD Type I**: Maintain only current state (overwrites historical data)
4. **SCD Type II**: Maintain full historical records with versioning
5. **Schema Evolution**: Handle incremental batch processing with overlapping data

## 📋 Components Description

### 1. `config.py`
Centralized configuration for table names across both SCD patterns.

**Tables defined:**
- Bronze SCD Type I: `workspace.lab_4_schema.bronze_streaming_events_SCD_I`
- Silver SCD Type I: `workspace.lab_4_schema.silver_streaming_events_SCD_I`
- Bronze SCD Type II: `workspace.lab_4_schema.bronze_streaming_events_SCD_II`
- Silver SCD Type II: `workspace.lab_4_schema.silver_streaming_events_SCD_II`

**Structure:**
```python
tables = {
    "I": {"bronze": ..., "silver": ...},
    "II": {"bronze": ..., "silver": ...}
}
```

### 2. `data_generators.py`
Generates synthetic streaming music event data with intentional overlaps between batches.

**Key function:** `generate_bronze_data(NUM_ROWS=1_000_000, batch_offset=0)`

**Generated columns:**
- `event_id` — Primary key (with modulo to create overlaps)
- `user_id`, `track_id` — Identifiers
- `genre` — Music genre (includes NULL values)
- `play_time_seconds` — Duration (includes invalid negative values)
- `royalty_rate`, `is_skipped` — Metrics
- `country_code`, `device_type` — Dimensions
- `event_timestamp` — Event time (varies per event_id)
- `_ingested_at`, `_source_file` — Metadata

**Important feature:** The `batch_offset` parameter ensures consecutive batches have ~5% overlapping records, simulating real-world late-arriving data.

### 3. `data_cleaner.py`
Implements data quality transformations for the Silver layer.

**Key function:** `clean_bronze_data(bronze_table, scd_type="I")`

**Quality checks & transformations:**

1. **Negative value handling:**
   - Identifies negative `play_time_seconds` values
   - Replaces with 0 (valid minimum)

2. **Missing value handling:**
   - Filters out NULL `event_id` (primary key)
   - Replaces NULL `genre` with "Unknown"

3. **Deduplication:**
   - Uses window function to keep only the most recent record per `event_id`
   - Partition by `event_id`, order by `event_timestamp DESC`
   - Keeps row_number = 1

4. **SCD Type II enhancements (when `scd_type="II"`):**
   - Adds `valid_from` (timestamp when record became active)
   - Adds `is_current` (boolean flag for current version)
   - Adds `valid_to` (NULL for current records)

**Result:** Typically ~5% reduction in row count due to deduplication.

### 4. `data_mergers.py`
Handles incremental updates using Delta Lake MERGE operations.

**Key function:** `merge_new_batch(batch, table_name, scd_type="I")`

**Merge logic:**

**Match condition:** `target.event_id == source.event_id`

**For SCD Type I:**
- **When matched AND source is newer:** UPDATE all columns
- **When not matched:** INSERT new record

**For SCD Type II:**
- **When matched AND source is newer:** 
  - UPDATE target: set `is_current=False`, `valid_to=current_timestamp`
  - INSERT source as new current record
- **When not matched:** INSERT new record

**Condition for updates:** `source.event_timestamp > target.event_timestamp`

### 5. `preprocessing.py`
Orchestrates the complete ETL pipeline from Bronze to Silver.

**Key function:** `preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=0, scd_type="I")`

**Pipeline steps:**
1. Generate bronze batch data
2. Write to bronze table (overwrite if first batch, append otherwise)
3. Clean and transform the batch
4. Write to silver table (overwrite if first batch, merge otherwise)

### 6. `Lab 4` (Notebook)
Interactive demonstration of the entire pipeline.

**Sections:**

1. **General tests** — Validates batch overlap behavior
2. **SCD Type I workflow:**
   - Initialize bronze and silver tables
   - Ingest first batch (1M records)
   - Ingest second batch (1M records, offset by 750K)
   - Observe: Bronze has 2M records, Silver has ~1.7M (due to overlaps)

3. **SCD Type II workflow:**
   - Similar initialization and ingestion
   - Observe: Maintains historical versions with `is_current` flag
   - Final query shows distribution of active vs inactive records

## 🔄 Data Flow

```
┌─────────────────────┐
│  generate_bronze    │  Synthetic data with overlaps
│      _data()        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Bronze Table      │  Raw data (append-only)
│  Delta Lake         │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  clean_bronze       │  Quality checks, deduplication
│      _data()        │  SCD-specific columns
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│   Silver Table      │  Curated data
│  Delta Lake MERGE   │  SCD Type I or II
└─────────────────────┘
```

## 🚀 Usage

### Prerequisites
Set notebook parameters:
- `catalog_name` = "workspace"
- `schema_name` = "lab_4_schema"

### Initialize Tables
```python
from preprocessing import preprocess_new_batch

# For SCD Type I
preprocess_new_batch()

# For SCD Type II
preprocess_new_batch(scd_type="II")
```

### Ingest New Batch
```python
# Ingest overlapping batch (offset determines overlap)
preprocess_new_batch(batch_offset=750_000, scd_type="I")
```

### Query Results
```python
# Check row counts
row_count = spark.sql(f"SELECT COUNT(*) FROM {silver_table}").collect()[0][0]

# For SCD Type II: Check current vs historical
results = spark.sql(f"""
    SELECT is_current, COUNT(*) 
    FROM {silver_table} 
    GROUP BY is_current
""")
```

## 📊 Expected Results

### SCD Type I (Second Batch)
- **Bronze table:** 2,000,000 records (1M + 1M)
- **Silver table:** ~1,700,000 records (overlapping records updated, not duplicated)

### SCD Type II (Second Batch)
- **Bronze table:** 2,000,000 records
- **Silver table:** >1,700,000 records (keeps both old and new versions)
- Records have `is_current` flag distinguishing active from historical versions

## 🔍 Key Learning Points

1. **Data Quality Enforcement:**
   - Handling invalid values (negatives)
   - Managing missing data
   - Deduplication strategies

2. **SCD Patterns:**
   - **Type I:** Simple, space-efficient, loses history
   - **Type II:** Complete audit trail, more storage

3. **Delta Lake Features:**
   - MERGE operation for UPSERT logic
   - Schema evolution with `overwriteSchema`
   - ACID transactions

4. **Incremental Processing:**
   - Handling late-arriving data
   - Overlapping batch windows
   - Idempotent pipeline (can replay batches)

## 🛠️ Maintenance

### Drop Tables (if needed)
```python
for tbl in [bronze_I_tbl, silver_I_tbl, bronze_II_tbl, silver_II_tbl]:
    if spark.catalog.tableExists(tbl):
        spark.sql(f"DROP TABLE IF EXISTS {tbl}")
```

### Monitor Data Quality
```python
# Check for negative play times
spark.sql(f"""
    SELECT MIN(play_time_seconds), MAX(play_time_seconds)
    FROM {silver_table}
""")

# Check for NULL genres (should be replaced with "Unknown")
spark.sql(f"""
    SELECT COUNT(*) 
    FROM {silver_table} 
    WHERE genre IS NULL
""")
```

## 📚 References

- [Delta Lake MERGE Documentation](https://docs.databricks.com/delta/merge.html)
- [Slowly Changing Dimensions Best Practices](https://docs.databricks.com/lakehouse/data-modeling.html)
- [Data Quality Patterns in Databricks](https://docs.databricks.com/data-engineering/data-quality.html)