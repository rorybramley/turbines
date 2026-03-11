import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    active_spark = SparkSession.getActiveSession()
    if active_spark is not None:
        yield active_spark
        return

    local_spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("turbine-pipeline-tests")
        .getOrCreate()
    )
    yield local_spark
    local_spark.stop()