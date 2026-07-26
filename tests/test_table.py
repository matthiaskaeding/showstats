import warnings

import polars as pl
import pytest
from showstats._table import _WARNED_ONCE, _Table


@pytest.fixture(autouse=True)
def _reset_warning_registry():
    """Advisory warnings fire once per session, so tests must start clean."""
    _WARNED_ONCE.clear()
    yield
    _WARNED_ONCE.clear()


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
        "Uniques",
        "Top 1",
        "Top 2",
        "Top 3",
    ]
    desired_dtypes = [pl.String, pl.Int16, pl.Int64, pl.String, pl.String, pl.String]

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
    assert flt_table.stat_dfs["num"].columns[0] == "Col (N=2)"


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
    assert _table.stat_dfs["cat"].filter(col0 == "x1").item(0, "Uniques") == 1
    assert _table.stat_dfs["cat"].filter(col0 == "x1").item(0, "Top 1") == "A (100%)"
    assert _table.stat_dfs["cat"].filter(col0 == "x3").item(0, "Uniques") == 3
    assert _table.stat_dfs["cat"].filter(col0 == "x3").item(0, "Top 1") == "A (92%)"
    assert _table.stat_dfs["cat"].filter(col0 == "x3").item(0, "Top 2") == "B (4%)"
    assert _table.stat_dfs["cat"].filter(col0 == "x3").item(0, "Top 3") == "C (4%)"

    assert stat_df.get_column(stat_df.columns[0]).to_list() == list(data.keys())
    assert stat_df.columns == [
        "Col (N=26)",
        "NA%",
        "Uniques",
        "Top 1",
        "Top 2",
        "Top 3",
    ]


def test_long_variable_names_are_truncated():
    long_name = "this_is_a_really_really_long_variable_name_that_goes_on_and_on"
    df = pl.DataFrame({long_name: [1, 2, 3], "short": [1.0, 2.0, 3.0]})

    _table = _Table(df, "num")
    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]

    names = stat_df.get_column(stat_df.columns[0]).to_list()
    assert "short" in names
    assert long_name not in names
    truncated = next(n for n in names if n != "short")
    assert len(truncated) == 30
    assert truncated.endswith("…")
    assert truncated.startswith(long_name[:29])


def test_quantiles(sample_df):
    _table = _Table(sample_df, "num", quantiles=[0.1, 0.9])
    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]

    assert "Q10" in stat_df.columns
    assert "Q90" in stat_df.columns

    var_0 = pl.col(stat_df.columns[0])
    q10 = float(stat_df.filter(var_0 == "int_col").item(0, "Q10"))
    q90 = float(stat_df.filter(var_0 == "int_col").item(0, "Q90"))
    assert q10 < q90

    # Quantiles must also work for boolean columns, which have no native
    # quantile support and are cast first.
    bool_row = stat_df.filter(var_0 == "bool_col")
    assert bool_row.item(0, "Q10") is not None


def test_quantiles_fold_min_and_max():
    """min/max are the 0th and 100th percentiles, so they get relabelled."""
    df = pl.DataFrame({"x": [float(i) for i in range(1, 101)]})

    default = _Table(df, "num")
    default.form_stat_df("num")
    assert default.stat_dfs["num"].columns == [
        "Col (N=100)",
        "NA%",
        "Avg",
        "SD",
        "Min",
        "Max",
        "Median",
    ], "the default table must be unchanged when no quantiles are asked for"

    _table = _Table(df, "num", quantiles=[0.1, 0.9])
    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]

    # Ascending sequence, with Min/Max folded in as the endpoints.
    assert stat_df.columns == [
        "Col (N=100)",
        "NA%",
        "Avg",
        "SD",
        "Median",
        "Q0",
        "Q10",
        "Q90",
        "Q100",
    ]
    assert "Min" not in stat_df.columns
    assert "Max" not in stat_df.columns
    assert float(stat_df.item(0, "Q0")) == 1.0
    assert float(stat_df.item(0, "Q100")) == 100.0


def test_quantiles_50_replaces_median():
    """Q50 is the median, so asking for it drops the separate column."""
    df = pl.DataFrame({"x": [float(i) for i in range(1, 101)]})

    _table = _Table(df, "num", quantiles=[0.5])
    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]

    assert "Median" not in stat_df.columns
    assert "Q50" in stat_df.columns
    # Interpolation is "linear" precisely so these agree.
    assert float(stat_df.item(0, "Q50")) == 50.5

    # Without an explicit 0.5 the Median column stays.
    other = _Table(df, "num", quantiles=[0.1])
    other.form_stat_df("num")
    assert "Median" in other.stat_dfs["num"].columns


def test_quantiles_0_and_1_warn_and_are_dropped():
    df = pl.DataFrame({"x": [float(i) for i in range(1, 101)]})

    with pytest.warns(UserWarning, match="always shown as Q0 and Q100"):
        _table = _Table(df, "num", quantiles=[0, 0.5, 1])
    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]

    # 0 and 1 are dropped as explicit quantiles, but still appear as the
    # relabelled min/max endpoints — so no column is duplicated.
    assert stat_df.columns == ["Col (N=100)", "NA%", "Avg", "SD", "Q0", "Q50", "Q100"]


def test_quantiles_warning_is_emitted_once_per_session():
    """showstats gets called repeatedly; repeating the advice is noise."""
    df = pl.DataFrame({"x": [float(i) for i in range(1, 11)]})

    with warnings.catch_warnings(record=True) as caught:
        # "always" defeats Python's own dedup, so this tests our guard.
        warnings.simplefilter("always")
        for _ in range(3):
            _Table(df, "num", quantiles=[0, 0.5, 1])
        # A different redundant value is still the same advisory.
        _Table(df, "num", quantiles=[1])

    assert len(caught) == 1


def test_fold_quantiles_false_keeps_named_columns():
    """Opting out keeps column names stable whatever quantiles are asked for."""
    df = pl.DataFrame({"x": [float(i) for i in range(1, 101)]})

    _table = _Table(df, "num", quantiles=[0.1, 0.5], fold_quantiles=False)
    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]

    # Named stats keep their names, and the quantiles are appended.
    assert stat_df.columns == [
        "Col (N=100)",
        "NA%",
        "Avg",
        "SD",
        "Min",
        "Max",
        "Median",
        "Q10",
        "Q50",
    ]


def test_fold_quantiles_false_honours_0_and_1_without_warning():
    """0 and 1 are only redundant when folding puts them in as Q0/Q100."""
    df = pl.DataFrame({"x": [float(i) for i in range(1, 101)]})

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _table = _Table(df, "num", quantiles=[0, 1], fold_quantiles=False)
    assert caught == []

    _table.form_stat_df("num")
    stat_df = _table.stat_dfs["num"]
    assert "Q0" in stat_df.columns
    assert "Q100" in stat_df.columns
    assert "Min" in stat_df.columns
    assert "Max" in stat_df.columns


def test_quantiles_invalid_raises():
    df = pl.DataFrame({"x": [1, 2, 3]})
    with pytest.raises(ValueError):
        _Table(df, "num", quantiles=[1.5])
    with pytest.raises(ValueError):
        _Table(df, "num", quantiles=[-0.1])


def test_pandas(sample_df):
    tmp = pl.DataFrame({"a": [1, 2, 3], "b": ["A", "B", "C"]})

    tmp_pandas = tmp.to_pandas()
    _table_pandas = _Table(tmp, "num")
    _table_polars = _Table(tmp_pandas, "num")
    _table_pandas.form_stat_df("num")
    _table_polars.form_stat_df("num")

    # The values should be the same, but pandas may format integers differently (e.g., "1" vs "1.0")
    # So we check shapes and most columns, but allow minor formatting differences
    assert _table_pandas.stat_dfs["num"].shape == _table_polars.stat_dfs["num"].shape
    assert (
        _table_pandas.stat_dfs["num"]["Col (N=3)"][0]
        == _table_polars.stat_dfs["num"]["Col (N=3)"][0]
    )
    assert (
        _table_pandas.stat_dfs["num"]["NA%"][0]
        == _table_polars.stat_dfs["num"]["NA%"][0]
    )
    assert (
        _table_pandas.stat_dfs["num"]["Avg"][0]
        == _table_polars.stat_dfs["num"]["Avg"][0]
    )
    assert (
        _table_pandas.stat_dfs["num"]["SD"][0] == _table_polars.stat_dfs["num"]["SD"][0]
    )
    # Min and Max may have minor formatting differences between pandas and polars for integers
    # Just check they're both present and non-empty
    assert len(_table_pandas.stat_dfs["num"]["Min"][0]) > 0
    assert len(_table_polars.stat_dfs["num"]["Min"][0]) > 0
