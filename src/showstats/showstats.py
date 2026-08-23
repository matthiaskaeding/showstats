# Central functions for table making
from __future__ import annotations

from typing import TYPE_CHECKING, Literal, get_args

import narwhals as nw
from narwhals.typing import IntoDataFrame, IntoFrame

from showstats._table import (
    SummaryConfig,
    SummaryResult,
    TableOneType,
    TableType,
    build_summary_plan,
    compute_summary,
    format_table_one,
    format_tables,
    normalize_config,
    prepare_input,
    render_table_one,
    render_tables,
)

if TYPE_CHECKING:
    from great_tables import GT

Format = Literal["text", "gt"]

_GROUP_COUNT = "__showstats_group_count"


def _build_summary(
    df: IntoFrame,
    table_type: TableType,
    quantiles: list[float] | None,
    fold_quantiles: bool,
    table_one_style: TableOneType | None = None,
    n_categories: int = 3,
) -> tuple[SummaryResult, SummaryConfig]:
    """Compute a summary and return its normalized configuration."""
    config = normalize_config(
        table_type,
        quantiles,
        fold_quantiles,
        table_one_style,
        n_categories,
    )
    prepared = prepare_input(df)
    plan = build_summary_plan(prepared.schema, config)
    return compute_summary(prepared.frame, plan), config


def _build_tables(
    df: IntoFrame,
    table_type: TableType,
    quantiles: list[float] | None,
    fold_quantiles: bool,
) -> tuple[dict[str, nw.DataFrame], SummaryConfig, int]:
    """Build formatted tables and the information needed to render them."""
    summary, config = _build_summary(df, table_type, quantiles, fold_quantiles)
    return format_tables(summary), config, summary.num_rows


def _merge_grouped_table_one(
    overall: nw.DataFrame,
    grouped: list[nw.DataFrame],
    backend: object,
    show_missing: bool,
) -> nw.DataFrame:
    """Join Table 1 values by their displayed row label."""
    labels = overall["Col"].to_list()
    seen = set(labels)
    for table in grouped:
        for label in table["Col"].to_list():
            if label not in seen:
                labels.append(label)
                seen.add(label)

    result = {"Col": labels}
    if show_missing:
        missing_by_label = dict(
            zip(overall["Col"].to_list(), overall["NA%"].to_list(), strict=True)
        )
        result["NA%"] = [missing_by_label.get(label, "") for label in labels]

    for table in [overall, *grouped]:
        value_column = table.columns[-1]
        values_by_label = dict(
            zip(table["Col"].to_list(), table[value_column].to_list(), strict=True)
        )
        result[value_column] = [
            values_by_label.get(
                label,
                "0 (0%)" if " = " in label and label.endswith(" (%)") else "",
            )
            for label in labels
        ]

    return nw.from_dict(result, backend=backend)


def _build_table_one(
    df: IntoFrame,
    style: TableOneType,
    show_missing: bool,
    n_categories: int,
    group: str | None,
) -> tuple[nw.DataFrame | None, int]:
    """Build an overall Table 1 and optional columns for each group."""
    prepared = prepare_input(df)
    config = normalize_config(
        "all",
        quantiles=None,
        fold_quantiles=True,
        table_one=style,
        n_categories=n_categories,
    )

    if group is not None and not isinstance(group, str):
        raise TypeError("group must be a column name")
    if group is not None and group not in prepared.schema:
        raise ValueError(f"group column {group!r} not found")

    columns = [name for name in prepared.schema if name != group]
    schema = {name: prepared.schema[name] for name in columns}
    summary_frame = prepared.frame.select(*(nw.col(name) for name in columns))
    plan = build_summary_plan(schema, config)
    overall_summary = compute_summary(summary_frame, plan)
    overall = format_table_one(overall_summary, show_missing=True)
    if overall is None or group is None:
        if overall is not None and not show_missing:
            overall = overall.select("Col", "Overall")
        return overall, overall_summary.num_rows

    count_column = _GROUP_COUNT
    while count_column in prepared.schema:
        count_column = f"{count_column}_"
    counts = prepared.frame.group_by(group, drop_null_keys=False).agg(
        nw.len().alias(count_column)
    )
    if isinstance(counts, nw.LazyFrame):
        counts = counts.collect()
    counts = counts.sort(group, nulls_last=True)

    grouped = []
    for value, count in counts.select(group, count_column).iter_rows():
        condition = (
            nw.col(group).is_null() if value is None else nw.col(group) == nw.lit(value)
        )
        group_frame = prepared.frame.filter(condition).select(
            *(nw.col(name) for name in columns)
        )
        group_summary = compute_summary(group_frame, plan)
        display_value = "Missing" if value is None else str(value)
        label = f"{group} = {display_value} (N={count})"
        group_table = format_table_one(
            group_summary,
            show_missing=True,
            value_label=label,
        )
        if group_table is not None:
            grouped.append(group_table)

    return (
        _merge_grouped_table_one(
            overall,
            grouped,
            overall_summary.backend,
            show_missing,
        ),
        overall_summary.num_rows,
    )


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
        - Great Tables output can color ``NA%`` values with a white to dark gray
          scale when ``color_missing=True``.
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


def table_one(
    df: IntoFrame,
    style: TableOneType = "mean_sd",
    show_missing: bool = True,
    n_categories: int = 3,
    group: str | None = None,
    fmt: Format = "text",
    color_missing: bool = False,
) -> None | GT:
    """Show a numerical and categorical Table 1 summary.

    Text output is printed. Great Tables output is returned so a notebook can
    display it, or so the caller can apply more Great Tables methods.

    Args:
        df: The input frame. Polars, pandas, PyArrow, and other Narwhals
            compatible frames are accepted. Lazy inputs stay lazy while the
            summary statistics are computed.
        style: The numerical summary to show. Use "mean_sd", "median_mad",
            or "median_iqr". Defaults to "mean_sd".
        show_missing: Include the NA% column. A categorical variable shows its
            percentage only on its first category row. Defaults to True.
        n_categories: The maximum number of values to show for each categorical
            variable. Defaults to 3.
        group: A column used to split the statistics into separate columns. The
            group column is not summarized as a row. Defaults to None.
        fmt: Use "text" for the fixed width table, or "gt" for a Great Tables
            object. Defaults to "text".
        color_missing: For Great Tables output, show NA% on a white to dark gray
            background scale. Defaults to False.

    Raises:
        ValueError: If the input is empty, an option is unsupported, or the
            group column does not exist.
        TypeError: If n_categories is not an integer, or group is not a string.
        ImportError: If fmt="gt" is requested without the optional gt extra.
    """
    if fmt not in get_args(Format):
        raise ValueError(
            f"fmt {fmt!r} not supported; expected one of {get_args(Format)}"
        )
    if color_missing and fmt != "gt":
        raise ValueError('color_missing=True requires fmt="gt"')
    if color_missing and not show_missing:
        raise ValueError("color_missing=True requires show_missing=True")

    table, num_rows = _build_table_one(
        df,
        style,
        show_missing,
        n_categories,
        group,
    )
    if fmt == "gt":
        if table is None:
            return None
        from showstats._gt import make_gt_table

        return make_gt_table(table, "table_one", num_rows, color_missing)

    render_table_one(table, num_rows)
    return None
