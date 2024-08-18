import polars as pl
import pytest
from showstats._table import (
    _check_input_maybe_try_transform,
    _map_cols_and_funs_for_var_type,
)


def test_input_check(sample_df):
    df2 = _check_input_maybe_try_transform(sample_df)
    assert hex(id(df2)) == hex(id(sample_df))
    with pytest.raises(Exception):
        # All of those are wrong inputs
        _check_input_maybe_try_transform(1)
        _check_input_maybe_try_transform(1.0)
        _check_input_maybe_try_transform(None)
        _check_input_maybe_try_transform([])
        _check_input_maybe_try_transform(dict())
        _check_input_maybe_try_transform(dict(a=[]))

    assert isinstance(_check_input_maybe_try_transform([1]), pl.DataFrame)
    assert isinstance(_check_input_maybe_try_transform(dict(x=[1, 2, 3])), pl.DataFrame)

    sample_df_pandas = sample_df.to_pandas()
    sample_df_from_pandas = _check_input_maybe_try_transform(sample_df_pandas)
    # Those wont be the same in general but onky roughly
    assert sample_df.shape == sample_df_from_pandas.shape
    assert list(sample_df.columns) == list(sample_df_from_pandas.columns)

    assert sample_df.get_column("float_mean_2").equals(
        sample_df_from_pandas.get_column("float_mean_2")
    )
    assert sample_df.get_column("float_std_2").equals(
        sample_df_from_pandas.get_column("float_std_2")
    )
    assert sample_df.get_column("bool_col").equals(
        sample_df_from_pandas.get_column("bool_col")
    )


def test_mapping(sample_df):
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
        res = _map_cols_and_funs_for_var_type(sample_df, var_type)
        assert len(res[0]) > 0, f"{var_type} errs"
        assert len(res[1]) > 0, f"{var_type} errs"
        assert res_lag != res
        res_lag = res
