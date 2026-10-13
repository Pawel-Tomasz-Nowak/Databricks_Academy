# Databricks notebook source
# MAGIC %md
# MAGIC # First step – generating synthetic partitioned data

# COMMAND ----------

# DBTITLE 1,declare widgets
dbutils.widgets.text("catalog_name", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("volume_name", "")

# COMMAND ----------

# DBTITLE 1,get widgets values
catalog_name = dbutils.widgets.get("catalog_name")
schema_name = dbutils.widgets.get("schema_name")
volume_name = dbutils.widgets.get("volume_name")

# COMMAND ----------

# DBTITLE 1,define paths for volumes
volume_path      = f"{catalog_name}.{schema_name}.{volume_name}"       
volume_full_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}" 

# COMMAND ----------

# DBTITLE 1,Define ingestion workflow class
class workflow():
    """
    End-to-end helper for this lab's synthetic streaming ingestion demo.

    The class is responsible for four things:
    1. Preparing paths, checkpoints, and target table names.
    2. Generating synthetic CSV files with gradually evolving schemas.
    3. Reading those files with Auto Loader.
    4. Writing the ingested data into a Delta bronze table and printing progress stats.

    This makes it easy to rerun the same experiment with different settings such as
    schema evolution mode, number of directories, or number of files.
    """

    def __init__(self, subVolume_name: str = "default",
        NUM_ALL_POSSIBLE_COLUMNS: int = 15,
        NUM_SUBDIRECTORIES: int = 3,
        MAX_FILES_PER_DIR: int = 150,
        schemaEvolutionMode: str = "rescue",
        trigger_mode: str = "availableNow",  # "availableNow" or "once"
        mergeSchema: str = "true",
        NUM_FILES: int = 50,
    reset_resources: bool = False) -> None:
        """
        Initialize the workflow configuration.

        Parameters define how much synthetic data is created, how the files are
        organized inside the volume, and how Auto Loader should react to schema
        changes when new columns appear.
        """
        self.subVolume_name = subVolume_name
        self.NUM_ALL_POSSIBLE_COLUMNS = NUM_ALL_POSSIBLE_COLUMNS
        self.NUM_SUBDIRECTORIES = NUM_SUBDIRECTORIES
        self.MAX_FILES_PER_DIR = MAX_FILES_PER_DIR
        self.schemaEvolutionMode = schemaEvolutionMode
        self.trigger_mode = trigger_mode
        self.mergeSchema = mergeSchema
        self.NUM_FILES = NUM_FILES

        # Build the logical and physical paths for this workflow run.
        # Example: one subvolume may represent one ingestion scenario.
        self.subvolume_path = volume_path + f".{self.subVolume_name}"
        self.subvolume_full_path = volume_full_path + f"/{self.subVolume_name}"

        # Auto Loader stores inferred schema history here.
        # Keeping this separate is important so schema tracking survives across runs.
        self.source_path = f"{self.subvolume_full_path}/batch_dir_[1-{self.NUM_SUBDIRECTORIES}]/*"
        self.schema_metadata_path = f"{self.subvolume_full_path}/_schema_metadata/"
        # dbutils.fs.rm(schema_metadata_path, recurse=True)

        # The checkpoint tracks streaming progress, while the target table stores
        # the ingested bronze-layer data.
        self.checkpoint_path = f"{self.subvolume_full_path}/_checkpoints/bronze_sensor_stream/"
        self.target_table_name = f"{catalog_name}.{schema_name}_bronze.bronze_sensor_data_{self.subVolume_name}"

        # Optional cleanup path for repeatable demos.
        # When enabled, old files and the old target table are removed first.
        if reset_resources:
            self.remove_folder()
            self.remove_table()

        self.create_volume()
        self.create_table()

    def remove_folder(self):
        """
        Delete the workflow-specific folder from the volume.

        Useful when you want to regenerate the synthetic input data from scratch.
        """
        dbutils.fs.rm(self.subvolume_full_path, recurse=True)

    def remove_table(self):
        """
        Drop the target Delta table if it already exists.
        """
        spark.sql(f"DROP TABLE IF EXISTS {self.target_table_name}")

    def create_table(self):
        """
        Create the target table if it does not already exist.

        In this notebook the table is used as the bronze destination for the
        streaming ingestion output.
        """
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.target_table_name}")

    def create_volume(self):
        """
        Create the base Unity Catalog volume if it does not already exist.
        """
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_path}")

    def generate_data(self):
        """
        Generate the initial batch of synthetic CSV files.

        Each directory simulates a separate arrival batch with a slightly wider
        schema than the previous one. This is what makes the later Auto Loader
        schema evolution demo meaningful.
        """
        import random
        from pyspark.sql.functions import lit, rand, current_timestamp
        from pyspark.sql.types import IntegerType, TimestampType

        # Create several batch directories. Each next directory gets more columns,
        # which simulates a source system whose schema evolves over time.
        for dir_idx in range(1, self.NUM_SUBDIRECTORIES + 1):
            num_cols_in_this_dir = max(2, int((dir_idx / self.NUM_SUBDIRECTORIES) * self.NUM_ALL_POSSIBLE_COLUMNS))
            active_columns = [f"X{i}" for i in range(1, num_cols_in_this_dir + 1)]
            dir_path = f"{self.subvolume_full_path}/batch_dir_{dir_idx}/"

            # Re-create the directory so the demo remains deterministic between runs.
            dbutils.fs.rm(dir_path, recurse=True)
            dbutils.fs.mkdirs(dir_path)

            print(f"Directory index: {dir_idx} | Columns: ['id', 'timestamp'] + {active_columns}")

            # More files means more micro-batches to observe in Auto Loader stats.
            num_files = random.randint(10, self.MAX_FILES_PER_DIR)
            n_records = num_files * 10

            # Start with the core columns that are always present in every file.
            df = (spark.range(1, n_records + 1)
                    .withColumn("id", lit(dir_idx).cast(IntegerType()))
                    .withColumn("timestamp", current_timestamp().cast(TimestampType()))
            )

            # Add synthetic sensor-like columns for this specific directory.
            for col_name in active_columns:
                df = df.withColumn(col_name, (rand() * 100).cast(IntegerType()))

            # Write one directory worth of CSV files.
            (df.repartition(num_files)
                .write
                .mode("overwrite")
                .option("header", "true")
                .format("csv")
                .save(dir_path)
            )

    def generate_new_data(self):
        """
        Generate one additional batch directory with even more columns.

        This is used after the first ingestion to explicitly test how Auto Loader
        behaves once the schema expands beyond the original set of columns.
        """
        from pyspark.sql.functions import lit, rand, current_timestamp
        from pyspark.sql.types import IntegerType, TimestampType

        # Create a brand-new directory that arrives after the initial load.
        new_dir_idx = self.NUM_SUBDIRECTORIES + 1
        new_dir_path = f"{self.subvolume_full_path}/batch_dir_{new_dir_idx}/"
        dbutils.fs.mkdirs(new_dir_path)

        n_records = self.NUM_FILES * 10
        df_new = (spark.range(1, n_records + 1)
            .withColumn("id", lit(new_dir_idx).cast(IntegerType()))
            .withColumn("timestamp", current_timestamp().cast(TimestampType()))
        )

        # Deliberately add three extra columns beyond the original maximum.
        # This creates a clear schema evolution scenario for the second ingestion.
        for i in range(1, self.NUM_ALL_POSSIBLE_COLUMNS + 4):  # 3 extra columns beyond the original design
            df_new = df_new.withColumn(f"X{i}", (rand() * 100).cast(IntegerType()))

        df_new.repartition(self.NUM_FILES).write.mode("overwrite").option("header", "true").format("csv").save(new_dir_path)

    def read_write_data(self, source_path = "before", show_statistics: bool = False, when: str = "Before schema evolution"):
        """
        Read files from the volume with Auto Loader and append them to Delta.

        Parameters
        ----------
        source_path:
            "before" reads only the initial set of directories.
            "after" reads all directories, including the newly added one.
        show_statistics:
            When True, print streaming progress information after the write finishes.
        when:
            A label used in printed statistics so it is obvious which phase is being reported.
        """
        # Translate the friendly phase name into the actual file pattern to ingest.
        if source_path == "before":
            source_path = f"{self.subvolume_full_path}/batch_dir_[1-{self.NUM_SUBDIRECTORIES}]/*"
        elif source_path == "after":
            source_path = f"{self.subvolume_full_path}/batch_dir_*/*"

        from pyspark.sql.functions import col, current_timestamp

        # Configure Auto Loader to read CSV files and track schema changes over time.
        df_stream = (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.schemaLocation", self.schema_metadata_path)
            .option("cloudFiles.schemaHints", "id INT, timestamp TIMESTAMP")
            .option("cloudFiles.schemaEvolutionMode", self.schemaEvolutionMode)
            .load(source_path)
        )

        # Add ingestion metadata so the bronze table records both when a record
        # was loaded and which exact file it came from.
        df_stream_enriched = (
            df_stream
            .withColumn("ingestion_time", current_timestamp())
            .withColumn("source_file_name", col("_metadata.file_path"))
        )

        # Choose the trigger style. availableNow processes all currently available
        # files and then stops; once processes one micro-batch and stops.
        trigger_options = {}
        if self.trigger_mode == "availableNow":
            trigger_options["availableNow"] = True
        elif self.trigger_mode == "once":
            trigger_options["once"] = True

        # Write the ingested stream into the bronze Delta table.
        query = (df_stream_enriched.writeStream
            .outputMode("append")
            .option("mergeSchema", self.mergeSchema)
            .format("delta")
            .trigger(**trigger_options)
            .option("checkpointLocation", self.checkpoint_path)
            .toTable(self.target_table_name)
        )

        # Wait until the finite streaming run completes before continuing.
        query.awaitTermination()

        if show_statistics:
            self.show_statistics(query, when)

    def show_statistics(self, query, when):
        """
        Print a compact summary of Auto Loader progress for each processed batch.

        The goal is to make the ingestion behavior easy to inspect during the lab,
        especially before and after schema evolution.
        """
        history_stats_all = query.recentProgress
        num_of_batches_all = len(history_stats_all)
        print(f"Number of batches processed {when}: {num_of_batches_all}")

        for idx, batch in enumerate(history_stats_all):
            num_rows = batch.get('numInputRows', 0)
            sources = batch.get('sources', [])
            num_files = 0

            # Each batch can include source metrics. If file counts are available,
            # surface them together with the processed row counts.
            for source in sources:
                metrics = source.get('metrics', {})
                if 'numFiles' in metrics:
                    num_files = metrics['numFiles']

            print(f"Batch [{idx}] -> Processed files: {num_files} | Processed records: {num_rows}")
        

# COMMAND ----------

# DBTITLE 1,Define workflow runner
def run_workflow(subVolume_name: str = "default",
        NUM_ALL_POSSIBLE_COLUMNS: int = 15,
        NUM_SUBDIRECTORIES: int = 3,
        MAX_FILES_PER_DIR: int = 50,
        schemaEvolutionMode: str = "rescue",
        trigger_mode: str = "availableNow",  # "availableNow" or "once"
        mergeSchema: str = "true",
        NUM_FILES: int = 50,
    reset_resources: bool = False):
    """
    Run the full synthetic ingestion demo from start to finish.

    High-level flow:
    1. Create a configured workflow object.
    2. Generate the initial synthetic CSV batches.
    3. Ingest those files into the bronze Delta table.
    4. Generate one more batch with extra columns.
    5. Re-run ingestion to observe schema evolution behavior.

    This helper is the easiest entry point for the lab because it hides the
    lower-level method calls behind one readable function.
    """

    # Build one workflow instance using the provided configuration.
    work_flow = workflow(subVolume_name=subVolume_name,
        NUM_ALL_POSSIBLE_COLUMNS=NUM_ALL_POSSIBLE_COLUMNS,
        NUM_SUBDIRECTORIES=NUM_SUBDIRECTORIES,
        MAX_FILES_PER_DIR=MAX_FILES_PER_DIR,
        schemaEvolutionMode=schemaEvolutionMode,
        trigger_mode=trigger_mode,
        mergeSchema=mergeSchema,
        NUM_FILES=NUM_FILES)

    # Phase 1: create the initial landing files and ingest them.
    work_flow.generate_data()
    work_flow.read_write_data(show_statistics=True)

    # Phase 2: introduce a new directory with extra columns, then ingest again
    # so the impact of schema evolution is easy to compare.
    work_flow.generate_new_data()
    work_flow.read_write_data(source_path="after", show_statistics=True, when="After schema evolution")


# COMMAND ----------

# DBTITLE 1,default workflow
run_workflow(reset_resources=True) # Run the default workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final remarks (possible questions to Experts)
# MAGIC * Why is the upper script showing there's zero processed files in the batch? Does it imply PySpark didn't handle any files?
# MAGIC