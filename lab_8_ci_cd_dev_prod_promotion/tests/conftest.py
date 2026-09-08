import pytest
from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a shared Databricks Connect Spark session for pytest runs.

    The tests target the same cluster-backed environment as the lab assets so
    transformation logic can be validated against the Databricks runtime.
    """
    return (
        DatabricksSession.builder
        .clusterId("0702-132442-toro5spu")
        .getOrCreate()
    )