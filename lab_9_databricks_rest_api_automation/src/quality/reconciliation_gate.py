"""Run post-pipeline reconciliation checks for lab_7.

This task acts as the final job gate. It verifies that quarantine handling does
not hide silent data loss and that the gold aggregates preserve the same total
views as the underlying fact table.
"""

import os
import sys
from pyspark.sql import SparkSession, functions as F

# Resolve the project root in the same way as the pipeline files so ``src.*``
# imports work in both bundle deployments and ad hoc execution contexts.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
bundle_root = spark.conf.get("bundle.root", "")

possible_roots = [
    bundle_root,
    os.getcwd(),
    os.path.abspath(os.path.join(os.getcwd(), "..", "..")),
]

for path in possible_roots:
    if path and os.path.isdir(os.path.join(path, "src")):
        if path not in sys.path:
            sys.path.insert(0, path)
        break

from src.setup.music_pipeline_setup import (
    music_metadata_tables,
    music_stats_tables,
)


def run_reconciliation_metadata() -> None:
    """Validate that every unique bronze metadata record is either kept or quarantined."""
    try:
        bronze_df = spark.read.table(music_metadata_tables["bronze"])
        silver_clean_df = spark.read.table(music_metadata_tables["silver"])
        silver_quarantine = spark.read.table(music_metadata_tables["silver_quarantine"])
    except Exception as exc:
        print(f"Error reading metadata tables: {exc}")
        sys.exit(1)

    bronze_count = bronze_df.select("url").distinct().count()
    silver_clean_count = silver_clean_df.count()
    silver_quarantine_count = silver_quarantine.count()

    print(f"Metadata counts: Bronze={bronze_count}, Silver={silver_clean_count}, Quarantine={silver_quarantine_count}")

    if bronze_count != (silver_clean_count + silver_quarantine_count):
        sys.exit(
            f"SILENT DATA LOSS DETECTED!\n"
            f"Bronze ({bronze_count}) != Silver ({silver_clean_count}) + Quarantine ({silver_quarantine_count})"
        )


def run_reconciliation_gold_aggregates() -> None:
    """Validate that all gold aggregates preserve the same total view volume."""
    try:
        video_df = spark.read.table(music_stats_tables["fact"])
        author_df = spark.read.table(music_stats_tables["gold_author"])
        album_df = spark.read.table(music_stats_tables["gold_album"])
    except Exception as exc:
        sys.exit(f"Error reading gold tables: {exc}")

    video_views = video_df.agg(F.sum("total_views")).first()[0] or 0
    author_views = author_df.agg(F.sum("total_views")).first()[0] or 0
    album_views = album_df.agg(F.sum("total_views")).first()[0] or 0

    print(f"Views sums: Fact={video_views}, Author={author_views}, Album={album_views}")

    if not (video_views == author_views == album_views):
        sys.exit(
            f"SILENT DATA LOSS DETECTED ACROSS GOLD AGGREGATES!\n"
            f"Fact Table Views:   {video_views}\n"
            f"Author Views:       {author_views}\n"
            f"Album Views:        {album_views}"
        )


if __name__ == "__main__":
    print("Starting data quality and reconciliation verification...")
    run_reconciliation_metadata()
    run_reconciliation_gold_aggregates()
    print("All reconciliation checks passed successfully!")