import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from showstats.showstats import make_stats_tbl
from tests.helpers import stats_frame

MIXED_PL = pl.DataFrame(
    {
        "int_col": [1, 2, 3, 4, 5],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "str_col": ["a", "b", "c", "a", "b"],
    }
)

BACKENDS = [
    pytest.param(MIXED_PL, pl.DataFrame, id="polars"),
    pytest.param(MIXED_PL.to_pandas(), pd.DataFrame, id="pandas"),
    pytest.param(MIXED_PL.to_arrow(), pa.Table, id="pyarrow"),
]


def test_make_stats_tbl(sample_df):
    # make_stats_tbl returns the input's native type (#37), so assert on the
    # shape of the result rather than on a particular dataframe class.
    res_num = stats_frame(make_stats_tbl(sample_df, "num"))
    assert res_num.shape[0] > 0
    res_cat = stats_frame(make_stats_tbl(sample_df, "cat"))
    assert res_cat.shape[0] > 0


def test_make_stats_tbl_quantiles(sample_df):
    res_num = stats_frame(make_stats_tbl(sample_df, "num", quantiles=[0.25, 0.75]))
    assert "Q25" in res_num.columns
    assert "Q75" in res_num.columns


@pytest.mark.parametrize("table_type", ["num", "cat"])
def test_make_stats_tbl_collects_polars_lazy_input(table_type):
    result = make_stats_tbl(MIXED_PL.lazy(), table_type)
    assert isinstance(result, pl.DataFrame)
    assert result.equals(make_stats_tbl(MIXED_PL, table_type))


def test_make_stats_tbl_collects_deferred_scan(tmp_path):
    path = tmp_path / "input.parquet"
    MIXED_PL.write_parquet(path)

    result = make_stats_tbl(pl.scan_parquet(path), "num")
    assert isinstance(result, pl.DataFrame)
    assert result.equals(make_stats_tbl(MIXED_PL, "num"))


@pytest.mark.parametrize(("df", "native_type"), BACKENDS)
def test_make_stats_tbl_returns_the_input_type(df, native_type):
    """The return follows the input, rather than always being polars.

    Signed off on #37: a dataframe-agnostic library that hands back one
    particular library's frame makes its caller depend on that library.
    """
    assert isinstance(make_stats_tbl(df, "num"), native_type)


def test_make_stats_tbl_pandas_result_is_indexed_from_zero():
    """A returned pandas frame must be usable as a pandas frame.

    The table is assembled from one subframe per var-type, and pandas
    carries each subframe's own index through a vertical concat — so the
    result came back indexed (0, 0), where `.loc[0]` returns two rows.
    """
    result = make_stats_tbl(MIXED_PL.to_pandas(), "num")
    assert list(result.index) == [0, 1]


@pytest.mark.parametrize(("df", "native_type"), BACKENDS)
def test_top_cols_ordering(df, native_type):
    """top_cols used to work by casting to pl.Enum and sorting.

    narwhals has no categorical-ordering trick to lean on, so the rank is
    made explicit — and it has to keep working, natively, whatever frame
    was passed in.
    """
    result = make_stats_tbl(df, "num", top_cols="float_col")
    assert isinstance(result, native_type)
    frame = stats_frame(result)
    assert frame[frame.columns[0]].to_list() == ["float_col", "int_col"]
