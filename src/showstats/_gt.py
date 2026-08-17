from __future__ import annotations

from collections.abc import Mapping

import narwhals as nw

from showstats._table import SummaryConfig, row_count

_HIGH_MISSING_PERCENTAGE = 20


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


def make_gt_table(table: nw.DataFrame, table_type: str, num_rows: int):
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
        .data_color(
            columns="NA%",
            palette=["#FFFFFF", "#F4D35E", "#B91C1C"],
            domain=[0, 100],
            autocolor_text=True,
            truncate=True,
        )
        .opt_align_table_header(align="left")
        .tab_options(
            table_font_size="14px",
            data_row_padding="5px",
            data_row_padding_horizontal="8px",
        )
    )
    if high_missing_rows:
        result = result.tab_style(
            style=style.text(weight="bold"),
            locations=loc.body(columns="NA%", rows=high_missing_rows),
        )
    return result


def make_gt_tables(
    tables: Mapping[str, nw.DataFrame], config: SummaryConfig, num_rows: int
):
    """Return one GT object, or a section dictionary for ``table_type='all'``."""
    if config.table_type != "all":
        table = tables.get(config.table_type)
        if table is None:
            return None
        return make_gt_table(table, config.table_type, num_rows)

    return {
        table_type: make_gt_table(tables[table_type], table_type, num_rows)
        for table_type in ("time", "num", "cat")
        if table_type in tables
    }
