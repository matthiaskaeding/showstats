"""Property-based checks that the printed statistics are the real ones.

The rest of the suite asserts particular numbers for particular frames.
These generate the frame instead and check a relationship that has to hold
for every one of them: each cell of the numerical table is the statistic it
claims to be, computed independently from the raw column with Python's own
`statistics` module.

Every frame is checked twice — as polars, and converted to pandas — because
the whole point of the narwhals work is that the answer must not depend on
which library is holding the data.

Two things are compared *numerically* rather than as text, on purpose.
`DataFrame.to_pandas()` is not dtype-preserving: an integer column with
nulls comes back float64, so where polars prints `1` pandas prints `1.0`.
That is the conversion's doing, not showstats', and asserting on the
rendered string would be asserting on pandas' nullable-dtype behaviour.
`tests/test_backends.py` covers byte-identical rendering separately, on
frames that do round-trip.
"""

from __future__ import annotations

import math
import statistics

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from showstats.showstats import make_stats_tbl
from tests.helpers import stats_frame

# Bounded well inside the range where a float is exactly representable and
# formatting stays in its ordinary path. NaN and infinity are excluded:
# "realistic" data does not contain them, and they have their own
# assertions in test_scientific_conversion.py.
FLOATS = st.floats(
    min_value=-1e9,
    max_value=1e9,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
    width=64,
)
INTS = st.integers(min_value=-(10**9), max_value=10**9)


def _maybe_null(strategy):
    """The same values, with roughly one null in five."""
    return st.one_of(st.just(None), strategy, strategy, strategy, strategy)


@st.composite
def realistic_frames(draw) -> pl.DataFrame:
    """A polars frame of numerical columns, some with nulls.

    At least two rows, because the standard deviation of a single value is
    undefined and prints blank — that case is asserted directly in
    `test_single_row_has_no_standard_deviation` rather than being drawn
    into every example.

    Booleans are drawn without nulls: `to_pandas()` turns a nullable
    boolean column into `object`, which showstats does not classify as
    numerical at all, so the column would simply vanish from the pandas
    table. That is worth knowing but it is not a fact about statistics.
    """
    n_rows = draw(st.integers(min_value=2, max_value=30))
    kinds = draw(
        st.lists(st.sampled_from(["float", "int", "bool"]), min_size=1, max_size=4)
    )

    data = {}
    schema = {}
    for i, kind in enumerate(kinds):
        name = f"col_{i}_{kind}"
        if kind == "float":
            values = draw(_column(_maybe_null(FLOATS), n_rows))
            schema[name] = pl.Float64
        elif kind == "int":
            values = draw(_column(_maybe_null(INTS), n_rows))
            schema[name] = pl.Int64
        else:
            values = draw(_column(st.booleans(), n_rows))
            schema[name] = pl.Boolean
        data[name] = values

    # An explicit schema so an all-null column keeps the dtype it was drawn
    # as; inferred, it would come out as Null and land in a different part
    # of the table.
    return pl.DataFrame(data, schema=schema)


def _column(values, n_rows):
    return st.lists(values, min_size=n_rows, max_size=n_rows)


def _parse(text: str):
    """A rendered cell back to the value it represents, or None if blank."""
    if text == "":
        return None
    if text == "true":
        return 1.0
    if text == "false":
        return 0.0
    return float(text)


# The rendered number keeps two decimal places, or two decimal places of a
# mantissa in [1, 10). So it is within 0.005 absolutely, or 0.005
# relatively, of the statistic — this leaves a little room on top of that
# for accumulation-order differences between backends. Tight enough to
# catch the mistakes that matter: a population/sample standard deviation
# mix-up differs by 1.3% at 40 rows, well outside it.
REL_TOL = 6e-3
ABS_TOL = 6e-3


def assert_is(rendered: str, expected: float, label: str) -> None:
    parsed = _parse(rendered)
    assert parsed is not None, f"{label}: blank, expected {expected}"
    assert math.isclose(parsed, expected, rel_tol=REL_TOL, abs_tol=ABS_TOL), (
        f"{label}: printed {rendered!r}, expected {expected!r}"
    )


def check_column(row, values, label: str) -> None:
    """Every cell of one row against the column it summarises."""
    n_rows = len(values)
    present = [float(v) for v in values if v is not None]

    # Integer arithmetic, no floats: `ceil(a / b)` is `-(-a // b)`. Going
    # via a float here would reproduce the very rounding bug this is meant
    # to catch — ceil(14 / 25 * 100) is 57 in IEEE754, and 56 in fact.
    missing = n_rows - len(present)
    expected_na = -(-missing * 100 // n_rows)
    assert row["NA%"] == expected_na, f"{label}: NA% for {missing}/{n_rows}"

    if not present:
        for stat in ("Avg", "SD", "Median", "Min", "Max"):
            assert row[stat] == "", f"{label}: {stat} of an all-null column"
        return

    assert_is(row["Avg"], statistics.fmean(present), f"{label}: Avg")
    assert_is(row["Median"], statistics.median(present), f"{label}: Median")
    assert_is(row["Min"], min(present), f"{label}: Min")
    assert_is(row["Max"], max(present), f"{label}: Max")

    if len(present) >= 2:
        # Sample standard deviation, matching polars, pandas and pyarrow,
        # all of which default to ddof=1.
        assert_is(row["SD"], statistics.stdev(present), f"{label}: SD")
    else:
        assert row["SD"] == "", f"{label}: SD of a single value"


def rows_by_variable(result) -> dict:
    frame = stats_frame(result)
    name_column = frame.columns[0]
    return {row[name_column]: row for row in frame.rows(named=True)}, frame.columns[0]


@settings(
    max_examples=75,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(frame=realistic_frames())
def test_numerical_statistics_are_correct(frame):
    for backend, df in (("polars", frame), ("pandas", frame.to_pandas())):
        rows, name_column = rows_by_variable(make_stats_tbl(df, "num"))

        assert name_column == f"Col (N={frame.height})"
        assert set(rows) == set(frame.columns), f"{backend}: one row per column"

        for name in frame.columns:
            values = frame.get_column(name).to_list()
            check_column(rows[name], values, f"{backend}/{name}")


@settings(max_examples=25, deadline=None)
@given(
    values=st.lists(FLOATS, min_size=2, max_size=20),
    extra=st.lists(FLOATS, min_size=2, max_size=20),
)
def test_column_order_does_not_change_a_columns_statistics(values, extra):
    """A column is summarised on its own, whatever it sits next to."""
    n = min(len(values), len(extra))
    alone = pl.DataFrame({"x": values[:n]})
    together = pl.DataFrame({"x": values[:n], "y": extra[:n]})

    rows_alone, _ = rows_by_variable(make_stats_tbl(alone, "num"))
    rows_together, _ = rows_by_variable(make_stats_tbl(together, "num"))

    assert rows_alone["x"] == rows_together["x"]


def test_single_row_has_no_standard_deviation():
    """Drawn out of the generated frames, so asserted directly instead."""
    for df in (pl.DataFrame({"x": [1.5]}), pl.DataFrame({"x": [1.5]}).to_pandas()):
        rows, _ = rows_by_variable(make_stats_tbl(df, "num"))
        assert rows["x"]["SD"] == ""
        assert rows["x"]["Avg"] == "1.5"


@pytest.mark.parametrize("backend", ["polars", "pandas"])
def test_all_null_column_prints_blank_statistics(backend):
    df = pl.DataFrame({"x": [None, None, None]}, schema={"x": pl.Float64})
    if backend == "pandas":
        df = df.to_pandas()
    rows, _ = rows_by_variable(make_stats_tbl(df, "num"))
    assert rows["x"]["NA%"] == 100
    for stat in ("Avg", "SD", "Median", "Min", "Max"):
        assert rows["x"][stat] == ""
