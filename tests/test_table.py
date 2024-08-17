import polars as pl
from polars.testing import assert_frame_equal
from showstats._table import _Table


def test_make_dt_num(sample_df):
    _table = _Table(sample_df, "all")

    df_num_float = _table.make_dt("num_float")
    df_num_int = _table.make_dt("num_int")
    df_num_bool = _table.make_dt("num_bool")
    df_datetime = _table.make_dt("datetime")
    df_date = _table.make_dt("date")

    assert isinstance(df_num_int, pl.LazyFrame)
    assert isinstance(df_num_float, pl.LazyFrame)
    assert isinstance(df_num_bool, pl.LazyFrame)
    assert isinstance(df_date, pl.LazyFrame)
    assert isinstance(df_datetime, pl.LazyFrame)

    desired_names = [
        "Variable",
        "null_count",
        "mean",
        "std",
        "median",
        "min",
        "max",
    ]
    desired_dtypes = [
        pl.String,
        pl.Int16,
        pl.String,
        pl.String,
        pl.String,
        pl.String,
        pl.String,
    ]

    df_num_float = df_num_float.collect()
    assert df_num_float.columns == desired_names
    assert df_num_float.dtypes == desired_dtypes

    df_num_int = df_num_int.collect()
    assert df_num_int.columns == desired_names
    assert df_num_int.dtypes == desired_dtypes

    df_num_bool = df_num_bool.collect()
    assert df_num_bool.columns == desired_names
    assert df_num_bool.dtypes == desired_dtypes

    df_datetime = df_datetime.collect()
    assert df_datetime.columns == ["Variable", "null_count", "median", "min", "max"]
    assert df_datetime.dtypes == [pl.String, pl.Int16, pl.String, pl.String, pl.String]


def test_make_dt_cat(sample_df):
    _table = _Table(sample_df, "cat")

    df_cat = _table.make_dt("cat")

    assert isinstance(df_cat, pl.LazyFrame)

    desired_names = [
        "Variable",
        "NA%",
        "N distinct",
        "Top values",
    ]
    desired_dtypes = [pl.String, pl.Int16, pl.Int64, pl.String]

    df_cat = df_cat.collect()
    assert df_cat.columns == desired_names
    assert df_cat.dtypes == desired_dtypes


def test_that_statistics_are_correct(sample_df):
    table = _Table(sample_df, "num")
    table.form_stat_df("num")
    stat_df = table.stat_dfs["num"]
    var_0 = pl.col(stat_df.columns[0])
    assert stat_df.filter(var_0 == "float_mean_2").item(0, "Avg") == "2.0"
    assert stat_df.filter(var_0 == "float_std_2").item(0, "SD") == "2.0"
    assert stat_df.filter(var_0 == "float_min_-7").item(0, "Min") == "-7.0"
    assert stat_df.filter(var_0 == "float_max_17").item(0, "Max") == "17.0"


1


def test_top_cols(sample_df):
    table_no_top_cols = _Table(sample_df, "num")
    table_no_top_cols.form_stat_df("num")
    table_top_cols = _Table(sample_df, "num", top_cols="U")
    table_top_cols.form_stat_df("num")

    assert table_top_cols.stat_dfs["num"].item(0, 0) == "U"

    table_top_cols = _Table(sample_df, "num", top_cols=["bool_col", "int_col"])
    table_top_cols.form_stat_df("num")
    assert table_top_cols.stat_dfs["num"].item(0, 0) == "bool_col"
    assert table_top_cols.stat_dfs["num"].item(1, 0) == "int_col"
    assert (
        table_top_cols.stat_dfs["num"].shape == table_no_top_cols.stat_dfs["num"].shape
    )

    name_col_0 = table_top_cols.stat_dfs["num"].columns[0]
    col_0_top_cols = table_top_cols.stat_dfs["num"].get_column(name_col_0)
    col_0_no_top_cols = table_no_top_cols.stat_dfs["num"].get_column(name_col_0)
    assert col_0_top_cols.equals(col_0_no_top_cols) is False
    assert sorted(col_0_top_cols.to_list()) == sorted(col_0_no_top_cols.to_list())

    assert (
        table_no_top_cols.stat_dfs["num"].height
        == sample_df.select(
            pl.selectors.exclude(
                pl.Enum, pl.String, pl.Categorical, pl.Date, pl.Datetime
            )
        ).width
    ), "Each row in table_no_top_cols-stat_df must be one column in sample_df"


def test_single_columns():
    null_df = pl.DataFrame({"null_col": [None] * 10})
    mt = _Table(null_df, "num")
    mt.form_stat_df("num")
    desired_shape = (1, 7)
    assert mt.stat_dfs["num"].item(0, 0) == "null_col"
    assert mt.stat_dfs["num"].shape == desired_shape
    assert mt.stat_dfs["num"].item(0, 0) == "null_col"
    assert mt.stat_dfs["num"].item(0, 1) == 100

    flt_df = pl.DataFrame({"flt_col": [1.3, 1.9]})
    flt_table = _Table(flt_df, "num")
    flt_table.form_stat_df("num")
    assert flt_table.stat_dfs["num"].item(0, 0) == "flt_col"
    assert flt_table.stat_dfs["num"].shape == desired_shape
    assert flt_table.stat_dfs["num"].item(0, "Avg") == "1.6"
    assert flt_table.stat_dfs["num"].item(0, 1) == 0
    assert flt_table.stat_dfs["num"].columns[0] == "Var. N=2"


def test_char_table():
    import string

    data = {}
    data["x0"] = list(string.ascii_uppercase)
    data["x1"] = ["A"] * 26
    data["x2"] = ["A"] * 25 + ["B"]
    data["x3"] = ["A"] * 24 + ["B", "C"]
    data["x4"] = ["A"] * 23 + ["B", "C", "D"]
    data["x5"] = ["A"] * 22 + [None] + ["B", "C", "D"]
    data["x6"] = ["A"] * 21 + [None, None] + ["B", "C", "D"]

    df = pl.DataFrame(data)
    _table = _Table(df, "cat")
    _table.form_stat_df("cat")
    stat_df = _table.stat_dfs["cat"]
    col0 = pl.col(stat_df.columns[0])

    assert _table.stat_dfs["cat"].filter(col0 == "x1").item(0, "NA%") == 0
    assert _table.stat_dfs["cat"].filter(col0 == "x1").item(0, "N distinct") == 1
    assert (
        _table.stat_dfs["cat"].filter(col0 == "x1").item(0, "Top values") == "A (100%)"
    )
    assert _table.stat_dfs["cat"].filter(col0 == "x3").item(0, "N distinct") == 3
    assert (
        _table.stat_dfs["cat"].filter(col0 == "x3").item(0, "Top values")
        == "A (92%)\nB (4%)\nC (4%)"
    )
    assert stat_df.get_column(stat_df.columns[0]).to_list() == list(data.keys())
    assert stat_df.columns == ["Var. N=26", "NA%", "N distinct", "Top values"]


def test_pandas(sample_df):
    tmp = pl.DataFrame({"a": [1, 2, 3], "b": ["A", "B", "C"]})

    tmp_pandas = tmp.to_pandas()
    _table_pandas = _Table(tmp, "num")
    _table_polars = _Table(tmp_pandas, "num")
    _table_pandas.form_stat_df("num")
    _table_polars.form_stat_df("num")

    assert_frame_equal(_table_pandas.stat_dfs["num"], _table_polars.stat_dfs["num"])
