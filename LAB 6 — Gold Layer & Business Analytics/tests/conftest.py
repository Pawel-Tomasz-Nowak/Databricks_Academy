import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provides a remote SparkSession via Databricks Connect with hardcoded cluster ID."""
    return (
        DatabricksSession.builder
        .clusterId("0702-132442-toro5spu")
        .getOrCreate()
    )