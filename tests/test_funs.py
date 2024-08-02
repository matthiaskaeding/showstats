from datetime import date, datetime

import polars as pl
import pytest
from polars_summary_tables.utils import (
    make_tables,
    print_summary,
)


@pytest.fixture
def sample_df():
    # Generate sample data
    import random

    random.seed(a=1, version=2)
    n = 100
    int_data = range(n)
    float_data = [i / 100 for i in range(n)]
    bool_data = [i % 2 == 0 for i in range(n)]
    str_data = random.choices(["foo", "bar", "baz", "ABC"], k=n)
    date_data = pl.date_range(start=date(2022, 1, 1), end=date(2022, 4, 10), eager=True)
    datetime_data = pl.datetime_range(
        start=datetime(2022, 1, 1), end=datetime(2022, 4, 10), interval="1d", eager=True
    )
    categorical_data = random.choices(["low", "medium", "high"], k=n)

    return pl.DataFrame(
        {
            "int_col": int_data,
            "float_col": float_data,
            "bool_col": bool_data,
            "str_col": str_data,
            "date_col": pl.Series(date_data).cast(pl.Date),
            "datetime_col": pl.Series(datetime_data).cast(pl.Datetime),
            "categorical_col": pl.Series(categorical_data).cast(pl.Categorical),
        }
    )


def test_make_tables(sample_df):
    result = make_tables(sample_df)

    assert isinstance(result, dict)
    assert set(result.keys()) == {"num", "cat", "datetime", "date"}

    for key, df in result.items():
        assert isinstance(df, pl.DataFrame)
        assert "Variable" in df.columns
        assert "null_count" in df.columns
        if key != "null":
            assert "min" in df.columns
            assert "max" in df.columns

    # Check specific data-frames
    num_df = result["num"]
    assert set(num_df["Variable"]) == {"int_col", "float_col", "bool_col"}
    assert "mean" in num_df.columns
    assert "median" in num_df.columns
    assert "std" in num_df.columns

    cat_df = result["cat"]
    assert set(cat_df.get_column("Variable")) == {
        "str_col",
        "categorical_col",
    }
    assert "n_unique" in cat_df.columns

    datetime_df = result["datetime"]
    assert set(datetime_df.get_column("Variable")) == {"datetime_col"}
    assert "mean" in datetime_df.columns
    assert "median" in datetime_df.columns

    date_df = result["date"]
    assert set(date_df["Variable"]) == {"date_col"}


def test_print_summary(sample_df, capsys):
    pl.Config.set_fmt_str_lengths(n=10000).set_tbl_width_chars(10000)
    print_summary(sample_df)
    captured = capsys.readouterr()
    output = captured.out
    # Check if the output contains expected column names
    expected_columns = ["Variable", "Missings", "Mean", "Median", "Std.", "Min", "Max"]
    for col in expected_columns:
        assert col in output, f"{col} not in output"

    # Check if all variable names are in the output
    for col in sample_df.columns:
        assert col in output

    # Check if the output is formatted as an ASCII markdown table
    assert "|" in output
    assert "-" in output


def test_empty_dataframe():
    empty_df = pl.DataFrame()
    result = make_tables(empty_df)
    assert isinstance(result, dict)
    assert all(isinstance(df, pl.DataFrame) for df in result.values())
    assert all(df.is_empty() for df in result.values())


def test_single_column_dataframe():
    single_col_df = pl.DataFrame({"test_col": range(10)})
    result = make_tables(single_col_df)
    assert isinstance(result, dict)
    assert "num" in result
    assert "test_col" in result["num"]["Variable"]


def test_all_null_column():
    null_df = pl.DataFrame({"null_col": [None] * 10})
    result = make_tables(null_df)
    assert isinstance(result, dict)
    assert "cat" not in result
    assert len(result) == 1
    assert "null" in result
    assert "null_col" in result["null"].get_column("Variable")
    assert (
        result["null"].filter(pl.col("Variable") == "null_col").item(0, "null_count")
        == 10
    )


if __name__ == "__main__":
    df = sample_df()
    print(df)
