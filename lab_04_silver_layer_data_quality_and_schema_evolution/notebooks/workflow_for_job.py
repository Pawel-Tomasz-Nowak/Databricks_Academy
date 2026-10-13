# Databricks notebook source
# DBTITLE 1,Setup: Add LAB 4 root to Python path
# Setup: Add LAB 4 root directory to Python path for src.* imports (using relative path)
import sys
import os

# Get the notebook path using dbutils and navigate to LAB 4 root
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_dir = os.path.dirname(notebook_path)  # .../LAB 4.../notebooks
lab_root = os.path.dirname(notebook_dir)  # .../LAB 4...
lab_root_path = f"/Workspace{lab_root}"

if lab_root_path not in sys.path:
    sys.path.insert(0, lab_root_path)
    print(f"✓ Added LAB 4 root to Python path: {lab_root_path}")

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from src.preprocessing import preprocess_new_batch

# COMMAND ----------

from src.config import tables

bronze_II_tbl:str = tables['II']['bronze']
silver_II_tbl:str = tables['II']['silver']

bronze_count = spark.table(bronze_II_tbl).count()

preprocess_new_batch(NUM_ROWS=5_000, batch_offset=bronze_count, scd_type="II")