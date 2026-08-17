import sys

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest
from great_tables import GT

from showstats import show_stats

MISSING = {
    "complete": list(range(20)),
    "some_missing": [None] * 4 + list(range(16)),
    "threshold_missing": [None] * 5 + list(range(15)),
    "mostly_missing": [None] * 16 + [1, 2, 3, 4],
}


@pytest.mark.parametrize(
    "frame",
    [
        pytest.param(pl.DataFrame(MISSING), id="polars"),
        pytest.param(pd.DataFrame(MISSING), id="pandas"),
        pytest.param(
            pa.table(MISSING),
            id="pyarrow",
            marks=pytest.mark.filterwarnings(
                "ignore:PyArrow Table support is currently experimental"
            ),
        ),
    ],
)
def test_gt_output_supports_input_backends(frame, capsys):
    result = show_stats(frame, table_type="num", out="gt")

    assert isinstance(result, GT)
    assert capsys.readouterr().out == ""
    html = result.as_raw_html()
    assert "Numerical columns" in html
    assert "20 rows" in html
    assert '<td class="gt_row gt_right">20</td>' in html
    assert '<td style="font-weight: bold;" class="gt_row gt_right">25</td>' in html
    assert '<td class="gt_row gt_right">9.5</td>' in html
    assert 'gt_columns_bottom_border gt_right" rowspan="1"' in html


def test_gt_output_returns_one_table_per_section(capsys):
    frame = pl.DataFrame(
        {
            "number": [1, 2],
            "category": ["a", None],
            "date": [None, None],
        },
        schema={"number": pl.Int64, "category": pl.String, "date": pl.Date},
    )

    result = show_stats(frame, out="gt")

    assert list(result) == ["time", "num", "cat"]
    assert all(isinstance(table, GT) for table in result.values())
    assert capsys.readouterr().out == ""


def test_gt_output_returns_none_for_an_empty_section():
    result = show_stats(pl.DataFrame({"category": ["a"]}), "num", out="gt")

    assert result is None


def test_gt_output_explains_how_to_install_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "great_tables", None)

    with pytest.raises(ImportError, match=r'uv add "showstats\[gt\]"'):
        show_stats(pl.DataFrame({"number": [1]}), "num", out="gt")
