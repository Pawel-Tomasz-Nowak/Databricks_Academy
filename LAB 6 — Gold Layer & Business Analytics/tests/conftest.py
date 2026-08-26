import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provides a remote SparkSession via Databricks Connect."""
    return DatabricksSession.builder.getOrCreate()