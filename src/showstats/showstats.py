# Central functions for table making
from typing import TYPE_CHECKING, List, Union

import polars as pl

from showstats._table import _Table
from showstats.utils import _check_input_maybe_try_transform

if TYPE_CHECKING:
    import pandas


def show_stats(
    df: Union[pl.DataFrame, "pandas.DataFrame"],
    top_cols: Union[List[str], str, None] = None,
) -> None:
    """
    Print a table of summary statistics for the given DataFrame.

    This function generates and prints a formatted table of summary statistics
    using the make_stats_df function. It configures the output format for
    optimal readability.

    Args:
        df (Union[pl.DataFrame, pandas.DataFrame]): The input DataFrame.
        top_cols (Union[List[str], str, None], optional): Column or list of columns
            that should appear at the top of the summary table. Defaults to None.

    Raises:
        ValueError: If the input DataFrame has no rows or columns.

    Note:
        - The output is formatted as an ASCII Markdown table with left-aligned cells
          and no column data types displayed.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Percentage of missing values is grouped into categories for easier interpretation.
        - Datetime columns are formatted as strings in the output.
    """
    df = _check_input_maybe_try_transform(df)
    _table = _Table(df, top_cols=top_cols)
    _table.show()


def show_cat_stats(
    df: Union[pl.DataFrame, "pandas.DataFrame"],
    top_cols: Union[List[str], str, None] = None,
) -> None:
    """
    Print a table of summary statistics for categorical varibles of the  DataFrame.

    This function generates and prints a formatted table of summary statistics
    using the make_stats_df function. It configures the output format for
    optimal readability.

    Args:
        df (Union[pl.DataFrame, pandas.DataFrame]): The input DataFrame.
        top_cols (Union[List[str], str, None], optional): Column or list of columns
            that should appear at the top of the summary table. Defaults to None.

    Raises:
        ValueError: If the input DataFrame has no rows or columns.

    Note:
        - The output is formatted as an ASCII Markdown table with left-aligned cells
          and no column data types displayed.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Percentage of missing values is grouped into categories for easier interpretation.
        - Datetime columns are formatted as strings in the output.
    """
    df = _check_input_maybe_try_transform(df)
    _table = _Table(df, ("cat_special",), top_cols)
    _table.show()
