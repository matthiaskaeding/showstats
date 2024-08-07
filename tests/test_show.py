import polars as pl
import pytest
from showstats.showstats import show_stats


def test_print_summary(sample_df, capsys):
    pl.Config.set_fmt_str_lengths(n=10000).set_tbl_width_chars(10000)
    show_stats(sample_df)
    captured = capsys.readouterr()
    output = captured.out

    # Check if the output contains expected column names
    expected_columns = ["Null %", "Mean", "Median", "Std.", "Min", "Max"]
    for col in expected_columns:
        assert col in output, f"{col} not in output"
    assert "Var" in output

    # Check if all variable names are in the output
    for col in sample_df.columns:
        assert col in output

    # Check if the output is formatted as an ASCII markdown table
    assert "|" in output
    assert "-" in output


def test_empty_dataframe():
    with pytest.raises(ValueError) as err:
        empty_df = pl.DataFrame()
        show_stats(empty_df)
        assert "Input data frame must have rows and columns" in str(err.value)
