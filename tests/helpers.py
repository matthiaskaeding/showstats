"""Assertion helpers shared across the test suite.

`make_stats_tbl` returns the input's native type (#37) — pandas in, pandas
out — so tests must not reach for polars methods on its result. Routing
assertions through narwhals keeps one set of expectations that hold both on
today's polars-returning implementation and on the native-returning one.
"""

from __future__ import annotations

import narwhals as nw


def stats_frame(result) -> nw.DataFrame:
    """Wrap a `make_stats_tbl` result so assertions do not assume a backend."""
    return nw.from_native(result, eager_only=True)


def cell(result, row: int, column: str):
    """Read one cell from a `make_stats_tbl` result, backend-agnostically."""
    return stats_frame(result)[column][row]


def row_for(result, variable: str) -> nw.DataFrame:
    """The stats row describing `variable`, backend-agnostically.

    The first column holds the variable names but is titled with the row count
    (``Col (N=10)``), so it is addressed by position rather than by name.
    Since #75 that column is simply ``Col``, but addressing it by position
    keeps these helpers indifferent to the name.
    """
    frame = stats_frame(result)
    return frame.filter(nw.col(frame.columns[0]) == variable)
