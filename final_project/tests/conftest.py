import pytest

# ------------------------------------------------------------------------------
# SHARED PYTEST FIXTURES (DATABRICKS CONNECT & LOCAL PYSPARK FALLBACK)
# ------------------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
  """Provides a Spark session for running unit tests.

  Attempts to connect via Databricks Connect first (for local IDE development).
  Falls back to a lightweight local PySpark session for headless CI pipelines.
  """
  try:
    from databricks.connect import DatabricksSession

    # Remote session execution against an active Databricks cluster
    return DatabricksSession.builder.getOrCreate()
  except (ImportError, Exception):
    from pyspark.sql import SparkSession

    # Headless local execution for fast CI execution without cluster dependencies
    return (
        SparkSession.builder.master("local[1]")
        .appName("Lab8-UnitTests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )