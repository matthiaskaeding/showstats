from __future__ import annotations

from collections.abc import Mapping

import narwhals as nw

from showstats._table import SummaryConfig, row_count

_HIGH_MISSING_PERCENTAGE = 20
_LIGHT_MISSING_CHANNEL = 255
_DARK_MISSING_CHANNEL = 74


def _great_tables_api():
    try:
        from great_tables import GT, loc, style
    except ModuleNotFoundError as exc:
        if exc.name != "great_tables":
            raise
        raise ImportError(
            "Great Tables output requires the optional 'gt' extra. "
            'Install it with `uv add "showstats[gt]"`.'
        ) from exc
    return GT, loc, style


def _missing_background(percentage: float) -> tuple[str, str]:
    """Return the gray background and readable text color for one percentage."""
    bounded = min(max(percentage, 0), 100)
    channel = round(
        _LIGHT_MISSING_CHANNEL
        + (_DARK_MISSING_CHANNEL - _LIGHT_MISSING_CHANNEL) * bounded / 100
    )
    background = f"#{channel:02x}{channel:02x}{channel:02x}"
    text = "#FFFFFF" if channel < 128 else "#000000"
    return background, text


def make_gt_table(
    table: nw.DataFrame,
    table_type: str,
    num_rows: int,
    color_missing: bool = False,
):
    """Build one styled Great Tables object from a formatted summary table."""
    GT, loc, style = _great_tables_api()
    titles = {
        "time": "Date and datetime columns",
        "cat": "Categorical columns",
        "num": "Numerical columns",
    }
    high_missing_rows = [
        index
        for index, percentage in enumerate(table["NA%"].to_list())
        if percentage >= _HIGH_MISSING_PERCENTAGE
    ]

    result = (
        GT(table.to_native(), rowname_col="Col")
        .tab_header(title=titles[table_type], subtitle=f"{row_count(num_rows)} rows")
        .tab_stubhead(label="Column")
        .opt_align_table_header(align="left")
        .tab_options(
            table_font_size="14px",
            data_row_padding="5px",
            data_row_padding_horizontal="8px",
        )
    )
    if table_type == "num":
        result = result.cols_align(align="right", columns=table.columns[1:])
    if color_missing:
        result = result.data_color(
            columns="NA%",
            palette=["#FFFFFF", "#4A4A4A"],
            domain=[0, 100],
            autocolor_text=True,
            truncate=True,
        )
    if high_missing_rows:
        result = result.tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(columns="NA%", rows=high_missing_rows),
        )
    return result


def make_gt_tables(
    tables: Mapping[str, nw.DataFrame],
    config: SummaryConfig,
    num_rows: int,
    color_missing: bool = False,
):
    """Return one GT object, or a section dictionary for ``table_type='all'``."""
    if config.table_type != "all":
        table = tables.get(config.table_type)
        if table is None:
            return None
        return make_gt_table(table, config.table_type, num_rows, color_missing)

    return {
        table_type: make_gt_table(
            tables[table_type], table_type, num_rows, color_missing
        )
        for table_type in ("time", "num", "cat")
        if table_type in tables
    }


def make_gt_table_one(
    table: nw.DataFrame | None,
    num_rows: int,
    color_missing: bool = False,
):
    """Build one styled Great Tables object for a Table 1 summary."""
    if table is None:
        return None

    GT, loc, style = _great_tables_api()
    has_missing = "NA%" in table.columns
    missing_cells = (
        [
            (index, float(value))
            for index, value in enumerate(table["NA%"].to_list())
            if value not in (None, "")
        ]
        if has_missing
        else []
    )
    high_missing_rows = [
        index
        for index, percentage in missing_cells
        if percentage >= _HIGH_MISSING_PERCENTAGE
    ]

    result = (
        GT(table.to_native(), rowname_col="Col")
        .tab_header(title="Table 1", subtitle=f"N = {row_count(num_rows)}")
        .tab_stubhead(label="Characteristic")
        .opt_align_table_header(align="left")
        .cols_align(align="right", columns=table.columns[1:])
        .tab_options(
            table_font_size="14px",
            data_row_padding="5px",
            data_row_padding_horizontal="8px",
        )
    )
    if color_missing:
        for index, percentage in missing_cells:
            background, text = _missing_background(percentage)
            result = result.tab_style(
                style=[style.fill(color=background), style.text(color=text)],
                locations=loc.body(columns="NA%", rows=[index]),
            )
    if high_missing_rows:
        result = result.tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(columns="NA%", rows=high_missing_rows),
        )
    return result
