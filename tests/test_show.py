from showstats.showstats import show_cat_stats, show_stats


def test_metatable(sample_df):
    show_stats(sample_df)
    show_cat_stats(sample_df)
