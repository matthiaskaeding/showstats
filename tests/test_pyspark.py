"""Optional PySpark tests.

Run these tests with:

    uv run --with "pyspark>=3.5" python -m pytest tests/test_pyspark.py -m integration -v
"""

from datetime import date, datetime

import pyarrow as pa
import pytest

from showstats.showstats import make_stats_tbl
from tests.helpers import stats_frame

pytestmark = pytest.mark.integration


def _spark_timestamp(day: int, hour: int) -> datetime:
    """Build a value for Spark's timestamp type, which has no time zone."""
    return datetime(2020, 1, day, hour)  # noqa: DTZ001


@pytest.fixture(scope="module")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("showstats-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def test_pyspark_lazy_input_matches_pyarrow(spark):
    from pyspark.sql import functions as sf

    rows = [
        (1, True, "a", date(2020, 1, 1), _spark_timestamp(1, 1)),
        (2, False, "a", date(2020, 1, 2), _spark_timestamp(2, 2)),
        (3, True, "a", date(2020, 1, 3), _spark_timestamp(3, 3)),
        (4, False, "b", date(2020, 1, 4), _spark_timestamp(4, 4)),
        (None, True, "b", None, None),
        (6, None, "c", date(2020, 1, 6), _spark_timestamp(6, 6)),
    ]
    columns = ["number", "active", "category", "day", "moment"]
    spark_rows = [
        (*row[:-1], row[-1].isoformat(sep=" ") if row[-1] is not None else None)
        for row in rows
    ]
    spark_frame = spark.createDataFrame(spark_rows, columns).withColumn(
        "moment", sf.col("moment").cast("timestamp")
    )
    arrow_frame = pa.table(
        {name: [row[position] for row in rows] for position, name in enumerate(columns)}
    )

    spark_result = make_stats_tbl(spark_frame, "all")
    arrow_result = make_stats_tbl(arrow_frame, "all")

    assert list(spark_result) == list(arrow_result)
    for table_type in arrow_result:
        assert isinstance(spark_result[table_type], pa.Table)
        assert (
            stats_frame(spark_result[table_type]).rows()
            == stats_frame(arrow_result[table_type]).rows()
        )
