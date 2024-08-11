import polars as pl
import pytest
from showstats.utils import _check_input_maybe_try_transform


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
