import narwhals as nw
import polars as pl
import pytest
from showstats._table import (
    _check_input_maybe_try_transform,
    _map_cols_and_funs_for_var_type,
)


def test_input_check(sample_df):
    df2 = _check_input_maybe_try_transform(sample_df)
    # Now returns a narwhals DataFrame
    assert isinstance(df2, nw.DataFrame)
    assert df2.shape == sample_df.shape
    with pytest.raises(Exception):
        # All of those are wrong inputs
        _check_input_maybe_try_transform(1)
        _check_input_maybe_try_transform(1.0)
        _check_input_maybe_try_transform(None)
        _check_input_maybe_try_transform([])
        _check_input_maybe_try_transform(dict())
        _check_input_maybe_try_transform(dict(a=[]))

    # Test with valid dict input (convert through polars first)
    result2 = _check_input_maybe_try_transform(pl.DataFrame(dict(x=[1, 2, 3])))
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
