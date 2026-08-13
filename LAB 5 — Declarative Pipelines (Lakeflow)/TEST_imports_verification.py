# Databricks notebook source
# DBTITLE 1,Test import from src/setup
import sys

# Add src/setup to path
sys.path.insert(0, "../src/setup")

print("Testing import from music_pipeline_setup...")
from music_pipeline_setup import *

print("✓ Import successful!")
print(f"\nCatalog: {catalog_name}")
print(f"Schema: {music_schema}")
print(f"Volume: {volume_name}")

# COMMAND ----------

# DBTITLE 1,List all available imports
# List all functions and variables available
import music_pipeline_setup

print("Available in music_pipeline_setup:")
print("="*60)

for name in dir(music_pipeline_setup):
    if not name.startswith('_'):
        obj = getattr(music_pipeline_setup, name)
        if callable(obj):
            print(f"  FUNCTION: {name}()")
        else:
            print(f"  VARIABLE: {name} = {repr(obj)[:50]}..." if len(repr(obj)) > 50 else f"  VARIABLE: {name} = {repr(obj)}")

# COMMAND ----------

