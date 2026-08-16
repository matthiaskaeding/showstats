# Central functions for table making
from __future__ import annotations

from narwhals.typing import IntoDataFrame, IntoFrame

from showstats._table import (
    TableType,
    _check_input_maybe_try_transform,
    build_summary_plan,
    compute_summary,
    format_tables,
    normalize_config,
    render_tables,
)


def show_stats(
    df: IntoFrame,
    table_type: TableType = "all",
    top_cols: list[str] | str | None = None,
    quantiles: list[float] | None = None,
    fold_quantiles: bool = True,
) -> None:
    """
    Print a table of summary statistics for the given DataFrame, configured
    for for optimal readability.

    Args:
        df: The input frame — polars, pandas, pyarrow or any other
            narwhals-compatible frame. Lazy frames are accepted and
            collected.
        top_cols (list[str] | str | None, optional): Column or list of columns
            that should appear at the top of the summary table. Defaults to None.
        table_type (str): All variables (default) = "num" or categorical = "cat"
        quantiles (list[float] | None, optional): Extra quantiles (values in
            [0, 1]) to compute for numerical columns, shown as extra "Q<pct>"
            columns. Defaults to None.
        fold_quantiles (bool, optional): When quantiles are given, relabel Min,
            Max and (if 0.5 is requested) Median as Q0, Q100 and Q50 and fold
            them into the quantile sequence, rather than repeating the same
            statistic under two names. Set to False to keep Min/Max/Median as
            separate columns, which keeps the column names stable regardless of
            which quantiles are requested. Defaults to True.
    Raises:
        ValueError: If the input DataFrame has no rows or columns, or if a
            requested quantile is outside [0, 1].

    Note:
        - The output is formatted as an ASCII Markdown table with left-aligned cells
          and no column data types displayed.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Percentage of missing values is grouped into categories for easier interpretation.
        - Datetime columns are formatted as strings in the output.
    """
    config = normalize_config(table_type, top_cols, quantiles, fold_quantiles)
    frame = _check_input_maybe_try_transform(df)
    plan = build_summary_plan(frame.schema, config)
    summary = compute_summary(frame, plan)
    tables = format_tables(summary)
    render_tables(tables, config, summary.num_rows)


def make_stats_tbl(
    df: IntoFrame,
    table_type: TableType = "num",
    top_cols: list[str] | str | None = None,
    quantiles: list[float] | None = None,
    fold_quantiles: bool = True,
) -> IntoDataFrame | dict[str, IntoDataFrame] | None:
    """
    Builds table of summary statistics for the given DataFrame, configured
    for for optimal readability.

    The result is always eager. An eager input returns the same native frame
    type. A lazy input returns the eager frame type chosen by narwhals when it
    collects the input. For example, a polars LazyFrame returns a polars
    DataFrame, while a DuckDB relation returns a pyarrow Table.

    For `table_type="all"`, the result is a dictionary containing each
    nonempty table under its `"time"`, `"num"`, or `"cat"` key. The function
    returns None when the input has no columns of a requested single type.

    Args:
        df: The input frame — polars, pandas, pyarrow or any other
            narwhals-compatible frame. Lazy frames are accepted and
            collected.
        top_cols (list[str] | str | None, optional): Column or list of columns
            that should appear at the top of the summary table. Defaults to None.
        type (str): All variables (default) = "num" or categorical = "cat"
        quantiles (list[float] | None, optional): Extra quantiles (values in
            [0, 1]) to compute for numerical columns, shown as extra "Q<pct>"
            columns. Defaults to None.
        fold_quantiles (bool, optional): When quantiles are given, relabel Min,
            Max and (if 0.5 is requested) Median as Q0, Q100 and Q50 and fold
            them into the quantile sequence, rather than repeating the same
            statistic under two names. Set to False to keep Min/Max/Median as
            separate columns, which keeps the column names stable regardless of
            which quantiles are requested. Defaults to True.
    Raises:
        ValueError: If the input DataFrame has no rows or columns, or if a
            requested quantile is outside [0, 1].

    Note:
        - The output is formatted as an ASCII Markdown table with left-aligned cells
          and no column data types displayed.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Percentage of missing values is grouped into categories for easier interpretation.
        - Datetime columns are formatted as strings in the output.
    """
    config = normalize_config(table_type, top_cols, quantiles, fold_quantiles)
    frame = _check_input_maybe_try_transform(df)
    plan = build_summary_plan(frame.schema, config)
    summary = compute_summary(frame, plan)
    tables = format_tables(summary)
    if table_type == "all":
        return {
            name: tables[name].to_native()
            for name in ("time", "num", "cat")
            if name in tables
        }
    # Return None if no columns of this type were found
    stat_df = tables.get(table_type)
    if stat_df is None:
        return None
    return stat_df.to_native()
