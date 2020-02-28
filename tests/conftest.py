from contextlib import contextmanager
import pytest

@pytest.fixture
def not_raises() :
    @contextmanager
    def not_raises(ExpectedException):
        try:
            yield

        except ExpectedException:
            raise AssertionError(
                "Did raise exception {0} when it should not!".format(
                    repr(ExpectedException)
                )
            )

        except Exception:
            raise AssertionError(
                "An unexpected exception {0} raised.".format(repr(Exception))
            )
    return not_raises


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession, SQLContext

    spark = (
        SparkSession.builder.master("local")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.host", "localhost")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def df(spark):

    l = [["Patrik", 1.92], ["Tanja", 1.62], ["Tobi", None]] * 10
    df = spark.createDataFrame(l, ["name", "height"])
    return df




