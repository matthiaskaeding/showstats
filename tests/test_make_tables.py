import polars as pl
from dfstats.dfstats import _make_tables


def test_make_tables(sample_df):
    result = _make_tables(sample_df)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"num", "cat", "datetime", "date", "null"}

    for key, df in result.items():
        assert isinstance(df, pl.DataFrame)
        assert "Variable" in df.columns
        assert "null_count" in df.columns
        if key != "null":
            assert "min" in df.columns
            assert "max" in df.columns

    # Check specific data-frames
    num_df = result["num"]
    assert set(num_df["Variable"]) == {
        "int_col",
        "float_col",
        "bool_col",
        "int_with_missing",
        "float_col_with_mean_2",
        "float_col_with_std_2",
        "float_col_with_min_7",
        "float_col_with_max_17",
    }
    assert "mean" in num_df.columns
    assert "median" in num_df.columns
    assert "std" in num_df.columns

    cat_df = result["cat"]
    assert set(cat_df.get_column("Variable")) == {
        "str_col",
        "categorical_col",
        "enum_col",
    }
    assert "n_unique" in cat_df.columns

    datetime_df = result["datetime"]
    assert set(datetime_df.get_column("Variable")) == {"datetime_col", "datetime_col_2"}
    assert "mean" in datetime_df.columns
    assert "median" in datetime_df.columns

    date_df = result["date"]
    assert set(date_df["Variable"]) == {"date_col", "date_col_2"}


def test_all_null_column():
    null_df = pl.DataFrame({"null_col": [None] * 10})
    result = _make_tables(null_df)
    assert isinstance(result, dict)
    assert "cat" not in result
    assert len(result) == 1
    assert "null" in result
    assert "null_col" in result["null"].get_column("Variable")
    assert (
        result["null"].filter(pl.col("Variable") == "null_col").item(0, "null_count")
        == 10
    )


def test_single_column_dataframe():
    single_col_df = pl.DataFrame({"test_col": range(10)})
    result = _make_tables(single_col_df)
    assert isinstance(result, dict)
    assert "num" in result
    assert "test_col" in result["num"]["Variable"]
