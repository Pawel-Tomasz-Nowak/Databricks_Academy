# LAB 4 Documentation Summary

**Date:** 2026-08-10  
**Author:** Paweł Nowak

## Overview
This lab demonstrates Silver Layer data quality management and schema evolution patterns using Delta Lake on Databricks, implementing Slowly Changing Dimension (SCD) Type I and Type II strategies.

---

## Python Scripts Documentation

All Python scripts have been comprehensively documented with:
- Module-level docstrings explaining purpose and usage
- Function-level docstrings with complete parameter descriptions
- Examples and usage patterns
- Notes on implementation details

### 1. **data_generators.py**
Synthetic streaming event data generator.
- **Purpose:** Generate music streaming events with intentional data quality issues
- **Key Features:** 
  - Configurable batch sizes and overlap
  - 3% negative play_time_seconds values
  - Null genres (~14%)
  - Duplicate event_ids with different timestamps
- **Main Function:** `generate_bronze_data(NUM_ROWS, batch_offset)`

### 2. **data_cleaner.py**
Bronze-to-silver data transformation pipeline.
- **Purpose:** Clean raw data and prepare for analytics
- **Transformations:**
  1. Fix negative play_time_seconds → 0
  2. Handle nulls (filter event_id nulls, replace genre nulls with "Unknown")
  3. Deduplicate by event_id (keep most recent timestamp)
  4. Add processing timestamp
  5. Add SCD Type II tracking columns (valid_from, valid_to, is_current)
- **Main Function:** `clean_bronze_data(bronze_table, scd_type)`

### 3. **data_mergers.py**
Delta Lake merge operations using SCD strategies.
- **Purpose:** Merge new batches into existing Delta tables
- **SCD Type I:** Simple upsert by event_id, no history
- **SCD Type II:** Historical tracking with validity periods
  - Brand new records → insert with is_current=True
  - Changed records → insert new version, expire old
  - Unchanged records → no action
- **Main Function:** `merge_new_batch(batch, table_name, scd_type)`

### 4. **preprocessing.py**
Orchestration module for end-to-end pipeline.
- **Purpose:** Entry point for bronze→silver transformation
- **Pipeline Steps:**
  1. Generate synthetic bronze data
  2. Write/append to bronze Delta table
  3. Clean and transform data
  4. Write/merge into silver Delta table
- **Main Function:** `preprocess_new_batch(NUM_ROWS, batch_offset, scd_type)`

### 5. **config.py**
Centralized configuration for table names.
- **Purpose:** Unity Catalog table name management
- **Structure:** Nested dictionary `tables[scd_type][layer]`
- **Catalog:** workspace
- **Schema:** lab_4_schema

---

## Error Screenshots Renamed

All error screenshots have been renamed from timestamp-based names to descriptive error names and updated in the notebook:

| Old Name | New Name | Error Type | Cell |
|----------|----------|------------|------|
| `image_1786358771421.png` | `error_delta_metadata_mismatch_extra_column.png` | DELTA_METADATA_MISMATCH - Extra column | 36 |
| `image_1786358818155.png` | `error_cast_invalid_input_string_to_int.png` | CAST_INVALID_INPUT - String to int cast | 42 |
| `image_1786358852777.png` | `error_schema_merge_incompatible_types.png` | Schema merge with incompatible types | 53 |
| `image_1786358882638.png` | `error_schema_mismatch_renamed_column.png` | Schema mismatch from renamed column | 58 |
| `image_1786358913959.png` | `error_integer_overflow.png` | Integer overflow (multiplier 2147483647) | 66 |
| `image_1786301943265.png` | `error_type_widening_narrowing.png` | Type widening/narrowing (int→long) | 69 |
| `image_1786359573468.png` | `error_column_mapping_mode_required.png` | Column rename requires mapping mode | 83 |

### Error Categories Demonstrated

1. **Schema Enforcement Errors** (cells 35-42)
   - Extra column metadata mismatch
   - Invalid cast operations
   
2. **Schema Evolution Errors** (cells 52-58)
   - Type incompatibility during merge
   - Column renaming without column mapping

3. **Type System Errors** (cells 65-69)
   - Integer overflow
   - Type widening restrictions

4. **Delta Feature Requirements** (cell 82)
   - Column mapping mode for renames/drops

---

## File Structure

```
LAB 4 — Silver Layer, Data Quality & Schema Evolution/
├── Lab 4.ipynb                                    # Main notebook (113 cells)
├── config.py                                       # ✓ Documented
├── data_generators.py                              # ✓ Documented
├── data_cleaner.py                                 # ✓ Documented  
├── data_mergers.py                                 # ✓ Documented
├── preprocessing.py                                # ✓ Documented
├── README.md                                       # (existing)
├── DOCUMENTATION_SUMMARY.md                        # This file
├── error_delta_metadata_mismatch_extra_column.png # ✓ Renamed
├── error_cast_invalid_input_string_to_int.png     # ✓ Renamed
├── error_schema_merge_incompatible_types.png      # ✓ Renamed
├── error_schema_mismatch_renamed_column.png       # ✓ Renamed
├── error_integer_overflow.png                      # ✓ Renamed
├── error_type_widening_narrowing.png               # ✓ Renamed
├── error_column_mapping_mode_required.png          # ✓ Renamed
├── image_1786358960386.png                         # (not referenced in notebook)
├── image_1786358984166.png                         # (not referenced in notebook)
└── image_1786359036995.png                         # (not referenced in notebook)
```

---

## Key Concepts Demonstrated

### 1. Slowly Changing Dimensions (SCD)
- **Type I:** Overwrites old values, no history
- **Type II:** Maintains full history with validity tracking

### 2. Schema Enforcement vs. Evolution
- **Enforcement:** Strict validation, rejects incompatible changes
- **Evolution:** `mergeSchema=true` allows compatible changes
- **Type Widening:** Requires explicit enablement (`delta.enableTypeWidening`)

### 3. Delta Lake Features
- **Column Mapping:** Required for rename/drop operations
- **Type Widening:** Int→Long requires feature flag
- **Constraints:** CHECK constraints, NOT NULL, UNIQUE (informational)

### 4. Data Quality Patterns
- **Bronze Layer:** Raw data, append-only
- **Silver Layer:** Cleaned, deduplicated, business-rule validated
- **Quarantine Pattern:** Mentioned in notebook (cell 85)

---

## Usage Examples

### Initial Load
```python
from preprocessing import preprocess_new_batch

# Generate 1M records, SCD Type I
preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=0, scd_type="I")
```

### Incremental Load with Overlap
```python
# Generate 1M records with 25% overlap for merge testing
preprocess_new_batch(NUM_ROWS=1_000_000, batch_offset=750_000, scd_type="II")
```

### Direct Module Usage
```python
from data_generators import generate_bronze_data
from data_cleaner import clean_bronze_data

# Generate and clean
bronze = generate_bronze_data(NUM_ROWS=100_000)
silver = clean_bronze_data(bronze, scd_type="II")
```

---

## Tables Created

### SCD Type I
- **Bronze:** `workspace.lab_4_schema.bronze_streaming_events_SCD_I`
- **Silver:** `workspace.lab_4_schema.silver_streaming_events_SCD_I`

### SCD Type II  
- **Bronze:** `workspace.lab_4_schema.bronze_streaming_events_SCD_II`
- **Silver:** `workspace.lab_4_schema.silver_streaming_events_SCD_II`

---

## Notes

- All Python scripts follow PEP 257 docstring conventions
- Error screenshots provide visual reference for common Delta Lake errors
- Three unreferenced image files remain (may be orphaned or used elsewhere)
- Notebook contains 113 cells demonstrating complete data engineering workflow

---

**Documentation completed:** 2026-08-10  
**Scripts documented:** 5  
**Error screenshots renamed:** 7  
**Notebook cells examined:** 113
