import narwhals as nw
import polars as pl
import pytest

from showstats._table import (
    _check_input_maybe_try_transform,
    _map_cols_and_funs_for_var_type,
)
from showstats.showstats import make_stats_tbl


@pytest.mark.parametrize("bad", [1, 1.0, None, [], {}, {"a": []}])
def test_input_check_rejects_non_frames(bad):
    """Each of these must raise — none is a narwhals-native frame.

    These used to sit in a single `pytest.raises(Exception)` block, where only
    the first line ever ran: the rest were unreachable once it raised.
    """
    with pytest.raises(TypeError):
        _check_input_maybe_try_transform(bad)


def test_input_check(sample_df):
    df2 = _check_input_maybe_try_transform(sample_df)
    # Now returns a narwhals DataFrame
    assert isinstance(df2, nw.DataFrame)
    assert df2.shape == sample_df.shape
    # Test with valid dict input (convert through polars first)
    result2 = _check_input_maybe_try_transform(pl.DataFrame({"x": [1, 2, 3]}))
    assert isinstance(result2, nw.DataFrame)

    sample_df_pandas = sample_df.to_pandas()
    sample_df_from_pandas = _check_input_maybe_try_transform(sample_df_pandas)
    # Those wont be the same in general but only roughly
    assert sample_df.shape == sample_df_from_pandas.shape
    assert list(sample_df.columns) == list(sample_df_from_pandas.columns)

    # Check the values are the same by converting both to narwhals and comparing
    import pandas as pd

    native_from_pandas = nw.to_native(sample_df_from_pandas)
    if isinstance(native_from_pandas, pd.DataFrame):
        # pandas backend - use pandas methods
        assert (
            sample_df.get_column("float_mean_2").to_list()
            == native_from_pandas["float_mean_2"].tolist()
        )
        assert (
            sample_df.get_column("float_std_2").to_list()
            == native_from_pandas["float_std_2"].tolist()
        )
        assert (
            sample_df.get_column("bool_col").to_list()
            == native_from_pandas["bool_col"].tolist()
        )
    else:
        # polars backend - use polars methods
        assert sample_df.get_column("float_mean_2").equals(
            native_from_pandas.get_column("float_mean_2")
        )
        assert sample_df.get_column("float_std_2").equals(
            native_from_pandas.get_column("float_std_2")
        )
        assert sample_df.get_column("bool_col").equals(
            native_from_pandas.get_column("bool_col")
        )


def test_lazy_input_stays_lazy_during_preparation():
    lf = pl.LazyFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]})
    df = _check_input_maybe_try_transform(lf)
    assert isinstance(df, nw.LazyFrame)
    assert df.collect_schema().names() == ["a", "b"]


def test_an_empty_lazy_frame_is_still_rejected():
    with pytest.raises(ValueError, match="must have rows and columns"):
        make_stats_tbl(pl.LazyFrame({"a": []}))


def test_input_check_pyarrow():
    import pyarrow as pa

    tbl = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    df = _check_input_maybe_try_transform(tbl)
    assert isinstance(df, nw.DataFrame)
    assert df.shape == (3, 2)
    assert list(df.columns) == ["a", "b"]


def test_mapping(sample_df):
    # Wrap in narwhals since _map_cols_and_funs_for_var_type expects narwhals DataFrame
    nw_df = nw.from_native(sample_df, eager_only=True)
    res_lag = None
    for var_type in (
        "num_float",
        "num_bool",
        "num_int",
        "null",
        "cat",
        "date",
        "datetime",
    ):
        res = _map_cols_and_funs_for_var_type(nw_df, var_type)
        assert len(res[0]) > 0, f"{var_type} errs"
        assert len(res[1]) > 0, f"{var_type} errs"
        assert res_lag != res
        res_lag = res
