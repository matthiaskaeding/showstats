import polars as pl
from showstats._table import _Table


def test_metatable(sample_df):
    mt = _Table(sample_df)

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


def test_that_statistics_are_correct(sample_df):
    table = _Table(sample_df)
    table.form_stat_df()
    stat_df = table.stat_df
    var_0 = pl.col(stat_df.columns[0])
    assert stat_df.filter(var_0 == "float_mean_2").item(0, "Mean") == "2.0"
    assert stat_df.filter(var_0 == "float_std_2").item(0, "Std.") == "2.0"
    assert stat_df.filter(var_0 == "float_min_7").item(0, "Min") == "7.0"
    assert stat_df.filter(var_0 == "float_max_17").item(0, "Max") == "17.0"


def test_top_cols(sample_df):
    table_no_top_cols = _Table(sample_df)
    table_no_top_cols.form_stat_df()
    table_top_cols = _Table(sample_df, top_cols="U")
    table_top_cols.form_stat_df()

    assert table_top_cols.stat_df.item(0, 0) == "U"

    table_top_cols = _Table(sample_df, top_cols=["enum_col", "int_col"])
    table_top_cols.form_stat_df()
    assert table_top_cols.stat_df.item(0, 0) == "enum_col"
    assert table_top_cols.stat_df.item(1, 0) == "int_col"
    assert table_top_cols.stat_df.shape == table_no_top_cols.stat_df.shape

    name_col_0 = table_top_cols.stat_df.columns[0]
    col_0_top_cols = table_top_cols.stat_df.get_column(name_col_0)
    col_0_no_top_cols = table_no_top_cols.stat_df.get_column(name_col_0)
    assert col_0_top_cols.equals(col_0_no_top_cols) is False
    assert sorted(col_0_top_cols.to_list()) == sorted(col_0_no_top_cols.to_list())


def test_single_columns():
    null_df = pl.DataFrame({"null_col": [None] * 10})
    mt = _Table(null_df)
    mt.form_stat_df()
    desired_shape = (1, 7)
    assert mt.stat_df.item(0, 0) == "null_col"
    assert mt.stat_df.shape == desired_shape
    assert mt.stat_df.item(0, 0) == "null_col"
    assert mt.stat_df.item(0, 1) == "100%"

    flt_df = pl.DataFrame({"flt_col": [1.3, 1.9]})
    flt_table = _Table(flt_df)
    flt_table.form_stat_df()
    assert flt_table.stat_df.item(0, 0) == "flt_col"
    assert flt_table.stat_df.shape == desired_shape
    assert flt_table.stat_df.item(0, "Mean") == "1.6"
    assert flt_table.stat_df.item(0, 1) == "0%"
    assert flt_table.stat_df.columns[0] == "Var. N=2"
