import os
import sys

from pyspark.sql import SparkSession
import pyspark.pipelines as dp
import pyspark.sql.functions as F

# The SDP runtime evaluates pipeline files dynamically — __file__ is not set.
# bundle.root is injected via spark.conf by the pipeline cluster configuration.
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
bundle_root = spark.conf.get("bundle.root", "")
possible_roots = [
    bundle_root, 
    os.getcwd(), 
    os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
]

for path in possible_roots:
    if path and os.path.isdir(os.path.join(path, "src")):
        if path not in sys.path:
            sys.path.insert(0, path)
        break

from src.setup.music_pipeline_setup import (
    music_metadata_tables
)

@dp.table(
    name=music_metadata_tables["gold"],
    comment="Gold table for music metadata",
    table_properties={"quality": "gold", "table_type":"dimension"},
    cluster_by=["author","album"]
)
def golden_music_metadata():
    silver_tbl = spark.read.table(music_metadata_tables["silver"])

    return silver_tbl.drop(F.col("url"))


