# LAB 4: Silver Layer, Data Quality & Schema Evolution - Python Scripts Documentation

## Overview
This lab demonstrates a medallion architecture (Bronze → Silver) data pipeline for music streaming events, 
featuring Slowly Changing Dimension (SCD) strategies, data quality controls, and schema evolution patterns.

---

## 1. config.py
**Purpose:** Centralized configuration management for Unity Catalog table references.

**Key Components:**
- **Catalog:** `workspace`
- **Schema:** `lab_4_schema`
- **Table Naming Convention:** `{layer}_streaming_events_SCD_{I|II}`

**Configuration Structure:**
```python
tables = {
    "I": {
        "bronze": "workspace.lab_4_schema.bronze_streaming_events_SCD_I",
        "silver": "workspace.lab_4_schema.silver_streaming_events_SCD_I"
    },
    "II": {
        "bronze": "workspace.lab_4_schema.bronze_streaming_events_SCD_II",
        "silver": "workspace.lab_4_schema.silver_streaming_events_SCD_II"
    }
}
```

**Usage Pattern:** 
- Access via `tables[scd_type][layer]` for dynamic table resolution
- Supports both SCD Type I (simple upsert) and Type II (historical tracking)

---

## 2. data_generators.py
**Purpose:** Synthetic data generation with intentional quality issues for testing.

**Key Features:**

### Data Generation Parameters:
- **NUM_ROWS:** Number of events to generate (default: 1,000,000)
- **batch_offset:** Creates overlapping event_ids between batches for SCD testing

### Schema:
| Column | Type | Notes |
|--------|------|-------|
| event_id | string | Format: \`evt_N\`, with modulo for 5% overlap |
| user_id | string | Random from 100k users |
| track_id | string | Random from 5k tracks |
| genre | string | Metal/Rock/Pop/Hip-Hop/Indie/Electronic/null (~14% null) |
| play_time_seconds | int | 10-310s, **3% negative values (-100)** for testing |
| royalty_rate | decimal(10,4) | Random 0.0-0.05 |
| is_skipped | boolean | ~20% true |
| country_code | string | PL/US/DE/UK/FR/JP/BR |
| device_type | string | mobile/desktop/smart_tv/web_player |
| event_timestamp | timestamp | Distributed over past 30 days |
| _ingested_at | timestamp | Current timestamp |
| _source_file | string | Static landing path |

### Intentional Data Quality Issues:
1. **Negative values:** 3% of \`play_time_seconds\` are -100
2. **Null genres:** ~14% missing genre values
3. **Duplicate event_ids:** Same event_id with different timestamps
4. **Batch overlap:** Modulo threshold creates 5% event_id overlap

### Batch Overlap Mechanism:
```python
# Example: NUM_ROWS=1000, batch_offset=750
# Batch 1: evt_0 to evt_949
# Batch 2: evt_750 to evt_1699 (overlaps evt_750-949)
modulo_threshold = int(0.95*NUM_ROWS) + batch_offset
```

**Use Cases:**
- Testing SCD merge logic with late-arriving events
- Validating data quality cleaning pipelines
- Simulating production data issues

---

## 3. data_cleaner.py
**Purpose:** Bronze-to-Silver transformation with data quality rules.

**Cleaning Pipeline (4 Steps):**

### Step 1: Fix Negative Values
- **Target:** \`play_time_seconds\` column
- **Rule:** Replace negative values with 0
- **Implementation:** \`F.when(col < 0, 0).otherwise(col)\`

### Step 2: Handle Missing Values
- **event_id:** Filter out nulls (primary key constraint)
- **genre:** Replace nulls with "Unknown" for clearer interpretation
- **Implementation:** \`F.when(col.isNull(), "Unknown").otherwise(col)\`

### Step 3: Deduplication
- **Strategy:** Keep most recent event by \`event_timestamp\`
- **Method:** Window function partitioned by \`event_id\`
```python
event_window = Window.partitionBy("event_id").orderBy(F.desc("event_timestamp"))
df.withColumn("row_number", F.row_number().over(event_window))
  .filter(F.col("row_number") == 1)
```

### Step 4: Add Metadata
- **_processed_at:** Current timestamp for tracking
- **SCD Type II columns (conditional):**
  - \`valid_from\`: Set to \`_processed_at\`
  - \`valid_to\`: NULL (current records)
  - \`is_current\`: TRUE

**Monitoring Output:**
- Reports before/after row counts
- Calculates percentage change
- Typical reduction: ~5% (due to deduplication)

**Function Signature:**
```python
def clean_bronze_data(bronze_table: DataFrame, scd_type: str = "I") -> DataFrame
```

---

## 4. data_mergers.py
**Purpose:** Merge cleaned batches into Silver tables using SCD strategies.

### SCD Type I Implementation

**Behavior:**
- Matches on \`event_id\`
- Updates target if new \`event_timestamp\` is later
- No history preservation

**Merge Logic:**
```python
delta_table.merge(batch, "target.event_id = source.event_id")
  .whenMatchedUpdate(
      condition="source.event_timestamp > target.event_timestamp",
      set={all_columns}
  )
```

**Use Case:** Current-state tables where only latest values matter

---

### SCD Type II Implementation

**Behavior:**
- Matches on \`event_id\`
- Tracks full history with validity periods
- Identifies three record categories:

#### Record Categories:
1. **Brand New Records:**
   - No match in target
   - Insert with \`is_current=True\`, \`valid_to=NULL\`

2. **Changed Records:**
   - Dimensions differ AND newer timestamp
   - Insert new version with \`is_current=True\`
   - Expire old version: \`is_current=False\`, \`valid_to=new_event_timestamp\`

3. **Unchanged Records:**
   - Identical dimensions
   - No action (skip)

#### Staging Pattern:
Uses \`merge_id\` column for precise control:
- **Brand new/changed records:** \`merge_id = NULL\` (triggers INSERT)
- **Old versions to expire:** \`merge_id = event_id\` (triggers UPDATE)

**Change Detection:**
```python
something_has_changed = (
    (tgt.user_id != src.user_id) |
    (tgt.track_id != src.track_id) |
    # ... all dimension columns
)
```

**Merge Conditions:**
```python
delta_table.merge(staging, "tgt.event_id = merge_id AND tgt.is_current = true")
  .whenMatchedUpdate(set={"is_current": False, "valid_to": src.event_timestamp})
  .whenNotMatchedInsert(values={all_columns, "is_current": True, "valid_to": NULL})
```

**Use Case:** Historical tracking, audit trails, time-travel queries

---

## 5. preprocessing.py
**Purpose:** Orchestrates the complete Bronze → Silver pipeline.

**Workflow:**

### Step 1: Generate Bronze Data
```python
bronze_batch = generate_bronze_data(NUM_ROWS, batch_offset)
```

### Step 2: Write to Bronze Table
- **First run:** Overwrite with schema evolution
- **Subsequent runs:** Append mode
```python
if not table_exists:
    .mode("overwrite").option("overwriteSchema", "true")
else:
    .mode("append")
```

### Step 3: Clean Data
```python
silver_batch = clean_bronze_data(bronze_batch, scd_type)
```

### Step 4: Write/Merge to Silver Table
- **First run:** Create table with overwrite
- **Subsequent runs:** Merge using SCD strategy
```python
if not table_exists:
    .mode("overwrite").option("overwriteSchema", "true")
else:
    merge_new_batch(silver_batch, silver_table, scd_type)
```

**Function Signature:**
```python
def preprocess_new_batch(
    NUM_ROWS: int = 1_000_000,
    batch_offset: int = 0,
    scd_type: str = "I"
)
```

**Parameters:**
- **NUM_ROWS:** Batch size
- **batch_offset:** For creating overlapping batches (SCD testing)
  - Set to \`NUM_ROWS * 0.75\` for 25% overlap
- **scd_type:** "I" or "II"

**Example Usage:**
```python
# Initial load
preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=0, scd_type="I")

# Incremental load with 25% overlap (simulates late-arriving data)
preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=750_000, scd_type="II")
```

---

## Data Flow Summary

```
[data_generators.py]
    ↓ (synthetic events with quality issues)
[Bronze Delta Table] (append-only)
    ↓
[data_cleaner.py]
    ↓ (fix negatives, nulls, duplicates)
[Cleaned DataFrame]
    ↓
[data_mergers.py] (SCD I or II merge logic)
    ↓
[Silver Delta Table] (analytics-ready)
```

---

## Key Design Patterns

### 1. Medallion Architecture
- **Bronze:** Raw, append-only landing zone
- **Silver:** Cleaned, deduplicated, business-ready

### 2. Configuration-Driven
- Single source of truth in \`config.py\`
- Supports multiple SCD strategies via dictionary lookup

### 3. Intentional Test Data
- Synthetic data with realistic quality issues
- Enables validation of cleaning logic

### 4. Incremental Processing
- Batch offset mechanism for overlapping events
- Tests merge behavior with late-arriving data

### 5. SCD Pattern Flexibility
- Type I: Current-state snapshots
- Type II: Full historical tracking with validity periods

---

## Testing Scenarios Enabled

1. **Data Quality:** Negative values, nulls, duplicates
2. **Schema Evolution:** Extra columns, type changes, renames
3. **SCD Merges:** New inserts, updates, historical versioning
4. **Late-Arriving Data:** Overlapping batches with batch_offset
5. **Deduplication:** Same event_id with multiple timestamps

---

## Dependencies

- **PySpark:** DataFrame operations, Window functions
- **Delta Lake:** ACID transactions, time travel, merge operations
- **Databricks Runtime:** Serverless compute environment

---

## Author
Paweł Nowak

## Date
2026-08-10

---

## Notes

- All tables use Delta Lake format for ACID guarantees
- SCD Type II requires additional columns: \`valid_from\`, \`valid_to\`, \`is_current\`
- Batch overlap creates realistic late-arrival scenarios for testing
- Cleaning pipeline reduces row count by ~5% (typical deduplication impact)
- Type II merge uses staging pattern with \`merge_id\` for precise control

---

## Error Screenshot Mappings

All error screenshots in the notebook have been properly named and mapped to their corresponding cells:

1. **Cell 35** → \`error_delta_metadata_mismatch_extra_column.png\`
   - Error when writing DataFrame with extra column to existing Delta table
   
2. **Cell 41** → \`error_cast_invalid_input_string_to_int.png\`
   - Error when attempting to cast string column with non-numeric values to integer
   
3. **Cell 52** → \`error_schema_merge_incompatible_types.png\`
   - Error when mergeSchema=true cannot find common type between integer and string
   
4. **Cell 57** → \`error_schema_mismatch_renamed_column.png\`
   - Error when attempting to write DataFrame with renamed column
   
5. **Cell 65** → \`error_integer_overflow.png\`
   - Error showing integer overflow when multiplying by 2147483647
   
6. **Cell 68** → \`error_type_widening_narrowing.png\`
   - Error when attempting type widening from int to long
   
7. **Cell 82** → \`error_column_mapping_mode_required.png\`
   - Error when attempting to rename column without enabling delta.columnMapping.mode

