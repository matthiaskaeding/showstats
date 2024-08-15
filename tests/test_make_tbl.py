import polars as pl
from showstats.showstats import make_stats_tbl


def test_make_stats_tbl(sample_df):
    res_num = make_stats_tbl(sample_df, "num")
    assert isinstance(res_num, pl.DataFrame)
    res_cat = make_stats_tbl(sample_df, "cat")
    assert isinstance(res_cat, pl.DataFrame)
