# Central functions for table making
from typing import TYPE_CHECKING, List, Union

import polars as pl

from showstats._table import _Table

if TYPE_CHECKING:
    import pandas


def show_stats(
    df: Union[pl.DataFrame, "pandas.DataFrame"],
    type: str = "all",
    top_cols: Union[List[str], str, None] = None,
) -> None:
    """
    Print a table of summary statistics for the given DataFrame, configured
    for for optimal readability.

    Args:
        df (Union[pl.DataFrame, pandas.DataFrame]): The input DataFrame.
        top_cols (Union[List[str], str, None], optional): Column or list of columns
            that should appear at the top of the summary table. Defaults to None.
        type (str): All variables (default) = "num" or categorical = "cat"
    Raises:
        ValueError: If the input DataFrame has no rows or columns.

    Note:
        - The output is formatted as an ASCII Markdown table with left-aligned cells
          and no column data types displayed.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Percentage of missing values is grouped into categories for easier interpretation.
        - Datetime columns are formatted as strings in the output.
    """
    if type == "all":
        _table = _Table(df, top_cols=top_cols)
    elif type == "cat":
        _table = _Table(df, var_types=("cat_special",), top_cols=top_cols)
    else:
        raise ValueError("Type must be either 'all' or 'cat'")
    _table.show()


def make_stats_tbl(
    df: Union[pl.DataFrame, "pandas.DataFrame"],
    type: str = "all",
    top_cols: Union[List[str], str, None] = None,
) -> None:
    """
    Builds table of summary statistics for the given DataFrame, configured
    for for optimal readability.

    Args:
        df (Union[pl.DataFrame, pandas.DataFrame]): The input DataFrame.
        top_cols (Union[List[str], str, None], optional): Column or list of columns
            that should appear at the top of the summary table. Defaults to None.
        type (str): All variables (default) = "num" or categorical = "cat"
    Raises:
        ValueError: If the input DataFrame has no rows or columns.

    Note:
        - The output is formatted as an ASCII Markdown table with left-aligned cells
          and no column data types displayed.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Percentage of missing values is grouped into categories for easier interpretation.
        - Datetime columns are formatted as strings in the output.
    """
    if type == "all":
        _table = _Table(df, top_cols=top_cols)
    elif type == "cat":
        _table = _Table(df, var_types=("cat_special",), top_cols=top_cols)
    else:
        raise ValueError("Type must be either 'all' or 'cat'")
    _table.form_stat_df()
    return _table.stat_df
