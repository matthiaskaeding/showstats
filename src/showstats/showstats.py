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

Format = Literal["text", "gt"]


def _build_tables(
    df: IntoFrame,
    table_type: TableType,
    quantiles: list[float] | None,
    fold_quantiles: bool,
) -> tuple[dict[str, nw.DataFrame], SummaryConfig, int]:
    """Build formatted tables and the information needed to render them."""
    config = normalize_config(table_type, quantiles, fold_quantiles)
    prepared = prepare_input(df)
    plan = build_summary_plan(prepared.schema, config)
    summary = compute_summary(prepared.frame, plan)
    return format_tables(summary), config, summary.num_rows


def show_stats(
    df: IntoFrame,
    table_type: TableType = "all",
    quantiles: list[float] | None = None,
    fold_quantiles: bool = True,
    fmt: Format = "text",
    color_missing: bool = False,
) -> None | GT | dict[str, GT]:
    """
    Show compact summary statistics for the given frame.

    Text output is printed. Great Tables output is returned so a notebook can
    display it, or so the caller can apply more Great Tables methods.

    Args:
        df: The input frame. Polars, pandas, PyArrow, and other
            Narwhals compatible frames are accepted. For a lazy input,
            only the planned summary results are collected.
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
        fmt (str): Use ``"text"`` for the compact terminal table, or
            ``"gt"`` for a styled Great Tables object. ``table_type="all"``
            returns one Great Tables object per nonempty section. Defaults to
            ``"text"``.
        color_missing (bool): For Great Tables output, show ``NA%`` on a white
            to dark gray background scale. Defaults to False.
    Raises:
        ValueError: If the input DataFrame has no rows or columns, or if a
            requested quantile is outside [0, 1], or if output is unsupported.
        ImportError: If ``fmt="gt"`` is requested without the optional
            ``gt`` extra installed.

    Note:
        - Text output uses a fixed-width table with left-aligned cells.
        - Great Tables output uses bold text for ``NA%`` values of 20 percent
          or more. Set ``color_missing=True`` to add a white to dark gray scale.
        - For large DataFrames (>100,000 rows), the row count is displayed in scientific notation.
        - Datetime columns are formatted as strings in the output.
    """
    if fmt not in get_args(Format):
        raise ValueError(
            f"fmt {fmt!r} not supported; expected one of {get_args(Format)}"
        )
    if color_missing and fmt != "gt":
        raise ValueError('color_missing=True requires fmt="gt"')

    tables, config, num_rows = _build_tables(df, table_type, quantiles, fold_quantiles)
    if fmt == "gt":
        from showstats._gt import make_gt_tables

        return make_gt_tables(tables, config, num_rows, color_missing)
    render_tables(tables, config, num_rows)
    return None


def make_stats_tbl(
    df: IntoFrame,
    table_type: TableType = "num",
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
    tables, _, _ = _build_tables(df, table_type, quantiles, fold_quantiles)
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
