# Central functions for table making
from __future__ import annotations

from typing import TYPE_CHECKING, Literal, get_args

import narwhals as nw
from narwhals.typing import IntoDataFrame, IntoFrame

from showstats._table import (
    SummaryConfig,
    TableType,
    build_summary_plan,
    compute_summary,
    format_tables,
    normalize_config,
    prepare_input,
    render_tables,
)

if TYPE_CHECKING:
    from great_tables import GT

Out = Literal["text", "gt"]


def _build_tables(
    df: IntoFrame,
    table_type: TableType,
    top_cols: list[str] | str | None,
    quantiles: list[float] | None,
    fold_quantiles: bool,
) -> tuple[dict[str, nw.DataFrame], SummaryConfig, int]:
    """Build formatted tables and the information needed to render them."""
    config = normalize_config(table_type, top_cols, quantiles, fold_quantiles)
    prepared = prepare_input(df)
    plan = build_summary_plan(prepared.schema, config)
    summary = compute_summary(prepared.frame, plan)
    return format_tables(summary), config, summary.num_rows


def show_stats(
    df: IntoFrame,
    table_type: TableType = "all",
    top_cols: list[str] | str | None = None,
    quantiles: list[float] | None = None,
    fold_quantiles: bool = True,
    out: Out = "text",
) -> None | GT | dict[str, GT]:
    """
    Show compact summary statistics for the given frame.

    Text output is printed. Great Tables output is returned so a notebook can
    display it, or so the caller can apply more Great Tables methods.

    Args:
        df: The input frame. Polars, pandas, PyArrow, and other
            Narwhals compatible frames are accepted. For a lazy input,
            only the planned summary results are collected.
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
        out (str): Use ``"text"`` for the compact terminal table, or
            ``"gt"`` for a styled Great Tables object. ``table_type="all"``
            returns one Great Tables object per nonempty section. Defaults to
            ``"text"``.
    Raises:
        ValueError: If the input DataFrame has no rows or columns, or if a
            requested quantile is outside [0, 1], or if output is unsupported.
        ImportError: If ``out="gt"`` is requested without the optional
            ``gt`` extra installed.

    Note:
        - Text output uses a fixed-width table with left-aligned cells.
        - Great Tables output uses bold text for ``NA%`` values of 25 percent
          or more.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Datetime columns are formatted as strings in the output.
    """
    if out not in get_args(Out):
        raise ValueError(f"out {out!r} not supported; expected one of {get_args(Out)}")

    tables, config, num_rows = _build_tables(
        df, table_type, top_cols, quantiles, fold_quantiles
    )
    if out == "gt":
        from showstats._gt import make_gt_tables

        return make_gt_tables(tables, config, num_rows)
    render_tables(tables, config, num_rows)
    return None


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
    type. A lazy input returns the eager frame type chosen by Narwhals when it
    collects the summary results. For example, a Polars LazyFrame returns a
    Polars DataFrame, while a DuckDB relation returns a PyArrow table.

    For `table_type="all"`, the result is a dictionary containing each
    nonempty table under its `"time"`, `"num"`, or `"cat"` key. The function
    returns None when the input has no columns of a requested single type.

    Args:
        df: The input frame. Polars, pandas, PyArrow, and other
            Narwhals compatible frames are accepted. For a lazy input,
            only the planned summary results are collected.
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
    tables, _, _ = _build_tables(df, table_type, top_cols, quantiles, fold_quantiles)
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
