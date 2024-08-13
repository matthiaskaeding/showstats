import polars as pl
from showstats.showstats import make_stats_tbl


def test_make_stats_tbl(sample_df):
    res = make_stats_tbl(sample_df)
    assert isinstance(res, pl.DataFrame)
