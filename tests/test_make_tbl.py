from showstats.showstats import make_stats_tbl
from tests.helpers import stats_frame


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
