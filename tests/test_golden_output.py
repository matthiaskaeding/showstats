"""Pin the rendered output before the rewrite touches rendering.

`show_one_table` prints through `pl.Config`. #37 replaces that with a
hand-written formatter, and there is no narwhals renderer to fall back on —
so the printed layout is entirely re-implemented by the last slice. These
snapshots are what makes that re-implementation checkable: without them the
first place a spacing change would surface is a README diff, since
README.md embeds this output verbatim.

None of these are marked `rewrite`. They describe behaviour that must
survive the rewrite unchanged, so they are expected to pass throughout.

Two current warts are pinned here deliberately rather than fixed:

1. `QUANTILES_FOLDED` is missing its Q0 column — polars elides columns that
   do not fit `set_tbl_width_chars=80`, replacing them with `…`. So asking
   for enough quantiles silently drops the minimum.
2. `TIME` wraps the datetime median onto a second, unaligned line for the
   same reason.

These goldens are all polars input. That the same data prints identically
from pandas and pyarrow is
`test_backends.py::test_rendering_does_not_depend_on_the_input_backend`;
together the two say the rendering is fixed *and* backend-independent.

Changing any of these is a deliberate act: update the golden in its own
commit, with the reason, plus a changelog entry. Never quietly, alongside
an implementation change.
"""

from __future__ import annotations

from datetime import date, datetime

import polars as pl
import pytest

from showstats.showstats import show_stats, table_one
from tests import _golden

MIXED = pl.DataFrame(
    {
        "int_col": [1, 2, 3, 4, None],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "bool_col": [True, False, True, False, True],
    }
)

CAT = pl.DataFrame(
    {
        "cat_col": ["A"] * 5 + ["B"] * 3 + ["C"] * 2,
        "other_col": ["x", "y", None, "x", "x", "y", "y", "x", "x", "y"],
    }
)

TIME = pl.DataFrame(
    {
        "date_col": [date(2020, 1, d) for d in range(1, 6)],
        # naive on purpose: a tz-aware value would change the column's dtype
        # and so the rendered string
        "datetime_col": [datetime(2020, 1, d, 12, 30, 15) for d in range(1, 6)],  # noqa: DTZ001
    }
)

ALL = pl.DataFrame(
    {
        "int_col": [1, 2, 3, 4, 5],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "str_col": ["a", "b", "c", "a", "b"],
        "date_col": [date(2020, 1, d) for d in range(1, 6)],
    }
)

LONG = pl.DataFrame(
    {
        "a_very_long_column_name_that_exceeds_the_limit": [1, 2, 3],
        "short": [1.0, 2.0, 3.0],
    }
)

BIG = pl.DataFrame({"big_col": [1e-7, 1.0, 12345.0, 9.87e9]})

NULLS = pl.DataFrame({"null_col": [None] * 5, "int_col": [1, 2, 3, 4, 5]})

MANY_ROWS = pl.DataFrame({"int_col": range(150_000)})

TABLE_ONE = pl.DataFrame(
    {
        "age": [34.0, 45.0, None, 52.0],
        "score": [7.5, 8.0, 9.5, 7.0],
        "group": ["control", "treated", "control", "placebo"],
    }
)


def render(capsys, df, **kwargs) -> str:
    show_stats(df, **kwargs)
    return capsys.readouterr().out


CASES = [
    ("num table", _golden.NUM, MIXED, {"table_type": "num"}),
    ("cat table", _golden.CAT, CAT, {"table_type": "cat"}),
    ("time table", _golden.TIME, TIME, {"table_type": "time"}),
    ("all three sections", _golden.ALL, ALL, {"table_type": "all"}),
    ("name truncation", _golden.LONG_NAMES, LONG, {"table_type": "num"}),
    ("scientific notation", _golden.SCIENTIFIC, BIG, {"table_type": "num"}),
    ("all-null column", _golden.NULL_COLUMN, NULLS, {"table_type": "num"}),
    ("scientific row count", _golden.BIG_N, MANY_ROWS, {"table_type": "num"}),
    (
        "folded quantiles",
        _golden.QUANTILES_FOLDED,
        MIXED,
        {"table_type": "num", "quantiles": [0.25, 0.5, 0.75]},
    ),
    (
        "unfolded quantiles",
        _golden.QUANTILES_UNFOLDED,
        MIXED,
        {"table_type": "num", "quantiles": [0.25], "fold_quantiles": False},
    ),
]


@pytest.mark.parametrize(
    ("expected", "df", "kwargs"),
    [pytest.param(*case[1:], id=case[0]) for case in CASES],
)
def test_rendered_output_matches_golden(capsys, expected, df, kwargs):
    assert render(capsys, df, **kwargs) == expected


def test_table_one_rendered_output_matches_golden(capsys):
    table_one(TABLE_ONE)
    assert capsys.readouterr().out == _golden.TABLE_ONE


def test_every_row_of_a_table_is_the_same_width(capsys):
    """Alignment, checked as an invariant rather than only in snapshots.

    Every line of one table must be exactly as wide as its header row —
    that is what "aligned columns" means, and it is the property a careless
    golden update is most likely to break. The `-Section----` rules are
    their own fixed 80 wide and are excluded.

    Not an 80-character cap: a table only as wide as its content is the
    point of the hand-written formatter. The old renderer wrapped anything
    wider onto a second, unaligned line, which is what this now forbids.
    """
    for name, _expected, df, kwargs in CASES:
        table = None
        for line in render(capsys, df, **kwargs).splitlines():
            if line.startswith("-"):
                assert len(line) == 80, f"{name}: rule is {len(line)} chars"
                table = None
                continue
            if table is None:
                table = len(line)
            assert len(line) == table, f"{name}: {len(line)} vs {table}: {line!r}"
