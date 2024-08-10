import polars as pl
from showstats.metatable import Metatable


def test_metatable(sample_df):
    mt = Metatable(sample_df)

    all_stat_names = []
    for vt in mt.stat_names:
        all_stat_names.extend(mt.stat_names[vt])
    assert mt.stats.keys() == set(all_stat_names)

    df_num = mt.make_dt("num")
    df_cat = mt.make_dt("cat")
    df_date = mt.make_dt("date")
    df_datetime = mt.make_dt("datetime")
    df_null = mt.make_dt("null")

    assert isinstance(df_num, pl.LazyFrame)
    assert isinstance(df_cat, pl.LazyFrame)
    assert isinstance(df_date, pl.LazyFrame)
    assert isinstance(df_datetime, pl.LazyFrame)
    assert isinstance(df_null, pl.LazyFrame)

    desired_schema = pl.Schema(
        [
            ("Variable", pl.String),
            ("null_count", pl.String),
            ("mean", pl.String),
            ("median", pl.String),
            ("std", pl.String),
            ("min", pl.String),
            ("max", pl.String),
        ]
    )

    assert df_num.collect_schema() == desired_schema
    assert df_cat.collect_schema() == desired_schema
    assert df_date.collect_schema() == desired_schema
    assert df_datetime.collect_schema() == desired_schema
    assert df_null.collect_schema() == desired_schema

    mt = Metatable(sample_df, "cat_special")
    assert mt.stats.keys() == set(mt.stat_names["cat_special"])
