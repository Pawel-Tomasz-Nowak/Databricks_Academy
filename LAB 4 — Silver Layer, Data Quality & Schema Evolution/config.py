"""Configuration Module

Centralized configuration for LAB 4 data pipeline.
Defines Unity Catalog table names for bronze and silver layers,
organized by Slowly Changing Dimension (SCD) type.

Table Structure:
    Bronze Layer: Raw streaming events (append-only)
    Silver Layer: Cleaned, deduplicated events (SCD I or II)

Catalog: workspace
Schema: lab_4_schema

Usage:
    >>> from config import tables
    >>> bronze_table = tables["I"]["bronze"]
    >>> silver_table = tables["II"]["silver"]

Author: Paweł Nowak
Date: 2026-08-10
"""

# SCD Type I table names - simple upsert, no history
target_bronze_table_SCD_I = "workspace.lab_4_schema.bronze_streaming_events_SCD_I"
target_silver_table_SCD_I = "workspace.lab_4_schema.silver_streaming_events_SCD_I"

# SCD Type II table names - historical tracking with validity periods
target_bronze_table_SCD_II = "workspace.lab_4_schema.bronze_streaming_events_SCD_II"
target_silver_table_SCD_II = "workspace.lab_4_schema.silver_streaming_events_SCD_II"

# Nested dictionary for easy lookup: tables[scd_type][layer]
# Example: tables["II"]["silver"] returns the Type II silver table name
tables = {
    "I": {
        "bronze": target_bronze_table_SCD_I,
        "silver": target_silver_table_SCD_I
    },
    "II": {
        "bronze": target_bronze_table_SCD_II,
        "silver": target_silver_table_SCD_II
    }
}