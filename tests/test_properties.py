"""Property-based checks that the printed statistics are the real ones.

The rest of the suite asserts particular numbers for particular frames.
These generate the frame instead and check a relationship that has to hold
for every one of them: each cell of the table is the statistic it claims to
be, recomputed from the raw column in plain Python.

Every dtype showstats classifies is drawn — every float and integer width,
booleans, strings, categoricals, enums, dates, datetimes, and the all-null
Null dtype — because the classification decides which of the three tables a
column lands in, and each table reports different statistics.

Every frame is checked twice, as polars and converted to pandas, since the
point of the narwhals work is that the answer must not depend on which
library holds the data.

Two deliberate accommodations for the conversion, neither of them about
showstats:

`DataFrame.to_pandas()` is not dtype-preserving. An integer column with
nulls comes back float64 and prints `1.0` where polars prints `1`; a Date
column comes back Datetime; a Decimal column comes back `object`, which is
not a dtype showstats classifies at all, so it drops out of the pandas
table. So values are compared numerically rather than as text, and which
table a column belongs to is read off the result rather than assumed.
`tests/test_backends.py` covers byte-identical rendering separately, on
frames that do round-trip.

Ties in the Top N columns have no defined order — `value_counts` may break
them differently on each backend — so those are asserted as a multiset of
(value, count) pairs rather than a sequence.
"""

from __future__ import annotations

import datetime as dt
import io
import math
import statistics
from contextlib import redirect_stdout
from decimal import Decimal

import narwhals as nw
import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from showstats.showstats import make_stats_tbl, show_stats
from tests.helpers import stats_frame

# Bounded well inside the range where a float is exactly representable and
# formatting stays in its ordinary path. NaN and infinity are excluded:
# realistic data does not contain them, and they have their own assertions
# in test_scientific_conversion.py.
FLOAT64 = st.floats(
    min_value=-1e9,
    max_value=1e9,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
    width=64,
)
FLOAT32 = st.floats(
    min_value=-1e6,
    max_value=1e6,
    allow_nan=False,
    allow_infinity=False,
    allow_subnormal=False,
    width=32,
)
# A small alphabet on purpose: it makes ties in the Top N columns common,
# which is the interesting case rather than one to avoid.
WORDS = ["alpha", "beta", "gamma", "delta"]
EPOCH = dt.date(1970, 1, 1)


def _ints(bits: int, signed: bool):
    """Every value the width can hold, except uint64 above int64's range.

    A uint64 over 2**63 - 1 does not fit numpy's int64, and the pinned old
    stack in `noxfile.py` (polars 1.4.1 with pandas 1.5.3) raises
    `OverflowError: Python int too large to convert to C long` on it.
    Current polars and pandas handle it — `18446744073709551615` prints
    fine — so this is the old stack's limit, not showstats', and the
    generator stops short of it rather than the matrix being narrowed.
    """
    if signed:
        return st.integers(min_value=-(2 ** (bits - 1)), max_value=2 ** (bits - 1) - 1)
    return st.integers(min_value=0, max_value=min(2**bits, 2**63) - 1)


def _decimals():
    return st.integers(min_value=-(10**6), max_value=10**6).map(
        lambda n: Decimal(n) / 100
    )


def _dates():
    return st.dates(min_value=dt.date(1900, 1, 1), max_value=dt.date(2100, 1, 1))


def _datetimes():
    return st.datetimes(
        min_value=dt.datetime(1900, 1, 1),  # noqa: DTZ001 — naive, as the column is
        max_value=dt.datetime(2100, 1, 1),  # noqa: DTZ001
    )


# Every dtype `_get_cols_for_var_type` knows about, by the name used in the
# generated column names so a failing example says what it was drawing.
KINDS = {
    "float32": (pl.Float32, FLOAT32),
    "float64": (pl.Float64, FLOAT64),
    "decimal": (pl.Decimal(16, 2), _decimals()),
    "int8": (pl.Int8, _ints(8, True)),
    "int16": (pl.Int16, _ints(16, True)),
    "int32": (pl.Int32, _ints(32, True)),
    "int64": (pl.Int64, _ints(64, True)),
    "uint8": (pl.UInt8, _ints(8, False)),
    "uint16": (pl.UInt16, _ints(16, False)),
    "uint32": (pl.UInt32, _ints(32, False)),
    "uint64": (pl.UInt64, _ints(64, False)),
    "bool": (pl.Boolean, st.booleans()),
    "string": (pl.String, st.sampled_from(WORDS)),
    "categorical": (pl.Categorical, st.sampled_from(WORDS)),
    "enum": (pl.Enum(WORDS), st.sampled_from(WORDS)),
    "date": (pl.Date, _dates()),
    "datetime": (pl.Datetime("us"), _datetimes()),
    "null": (pl.Null, st.none()),
}

# `to_pandas()` turns a nullable boolean into `object`, which showstats does
# not classify, so the column would simply vanish from the pandas table.
# That is a fact about the conversion, not about statistics, and it is
# asserted directly in `test_nullable_boolean_survives_as_polars`.
NEVER_NULL = {"bool"}


def _maybe_null(strategy):
    """The same values, with roughly one null in five."""
    return st.one_of(st.just(None), strategy, strategy, strategy, strategy)


@st.composite
def realistic_frames(draw) -> pl.DataFrame:
    """A polars frame mixing dtypes, most columns carrying some nulls.

    At least two rows: the standard deviation of a single value is
    undefined and prints blank, which is asserted directly in
    `test_single_row_has_no_standard_deviation` rather than drawn into
    every example.
    """
    n_rows = draw(st.integers(min_value=2, max_value=25))
    kinds = draw(st.lists(st.sampled_from(sorted(KINDS)), min_size=1, max_size=5))

    data = {}
    schema = {}
    for i, kind in enumerate(kinds):
        dtype, values = KINDS[kind]
        if kind not in NEVER_NULL:
            values = _maybe_null(values)
        name = f"col_{i}_{kind}"
        data[name] = draw(st.lists(values, min_size=n_rows, max_size=n_rows))
        # An explicit schema so an all-null column keeps the dtype it was
        # drawn as; inferred, every one of them would come out as Null.
        schema[name] = dtype
    return pl.DataFrame(data, schema=schema)


def _parse_number(text: str):
    if text == "":
        return None
    if text == "true":
        return 1.0
    if text == "false":
        return 0.0
    return float(text)


def _parse_moment(text: str) -> dt.datetime:
    """A rendered date or datetime back to a datetime.

    Both forms appear: a polars Date column prints `2020-01-01`, and the
    same column after `to_pandas()` is a Datetime and prints
    `2020-01-01 00:00:00`.
    """
    return dt.datetime.fromisoformat(text)


def _to_seconds(value) -> dt.datetime:
    """A drawn date or datetime at the resolution the table prints.

    `make_dt` slices the rendered timestamp to 19 characters, which is
    `YYYY-MM-DD HH:MM:SS` — anything finer is truncated away, so the
    expectation has to be truncated the same way rather than rounded.
    """
    if not isinstance(value, dt.datetime):
        value = dt.datetime(value.year, value.month, value.day)  # noqa: DTZ001
    return value.replace(microsecond=0)


# The rendered number keeps two decimal places, or two decimal places of a
# mantissa in [1, 10). So it is within 0.005 absolutely, or 0.005
# relatively, of the statistic — with a little room on top for
# accumulation-order differences between backends and for float32's
# narrower mantissa. Tight enough to catch what matters: a
# population/sample standard deviation mix-up differs by 1.3% at 40 rows.
REL_TOL = 6e-3
ABS_TOL = 6e-3


def assert_is(rendered: str, expected: float, label: str) -> None:
    parsed = _parse_number(rendered)
    assert parsed is not None, f"{label}: blank, expected {expected}"
    assert math.isclose(parsed, expected, rel_tol=REL_TOL, abs_tol=ABS_TOL), (
        f"{label}: printed {rendered!r}, expected {expected!r}"
    )


def assert_na_percent(row, values, label: str) -> None:
    """The missing share, rounded up.

    Integer arithmetic, no floats: `ceil(a / b)` is `-(-a // b)`. Going via
    a float here would reproduce the very rounding bug this is meant to
    catch — `ceil(14 / 25 * 100)` is 57 in IEEE754, and 56 in fact.
    """
    missing = sum(v is None for v in values)
    expected = -(-missing * 100 // len(values))
    assert row["NA%"] == expected, f"{label}: NA% for {missing}/{len(values)}"


def check_numerical(row, values, label: str) -> None:
    present = [float(v) for v in values if v is not None]
    assert_na_percent(row, values, label)

    if not present:
        for stat in ("Avg", "SD", "Median", "Min", "Max"):
            assert row[stat] == "", f"{label}: {stat} of an all-null column"
        return

    assert_is(row["Avg"], statistics.fmean(present), f"{label}: Avg")
    assert_is(row["Median"], statistics.median(present), f"{label}: Median")
    assert_is(row["Min"], min(present), f"{label}: Min")
    assert_is(row["Max"], max(present), f"{label}: Max")

    if len(present) >= 2:
        # Sample standard deviation: polars, pandas and pyarrow all default
        # to ddof=1.
        assert_is(row["SD"], statistics.stdev(present), f"{label}: SD")
    else:
        assert row["SD"] == "", f"{label}: SD of a single value"


def check_categorical(row, values, label: str) -> None:
    present = [v for v in values if v is not None]
    assert_na_percent(row, values, label)

    assert row["Uniques"] == len(set(present)), f"{label}: Uniques"

    counts = {v: present.count(v) for v in set(present)}
    ranked = sorted(counts.values(), reverse=True)

    reported = [row[f"Top {i}"] for i in (1, 2, 3) if f"Top {i}" in row]
    shown = [cell for cell in reported if cell != ""]
    assert len(shown) == min(3, len(counts)), f"{label}: {len(shown)} of top 3"

    seen = []
    for cell in shown:
        value, _, share = cell.rpartition(" (")
        count = counts.get(value)
        assert count is not None, f"{label}: {value!r} is not in the column"
        assert value not in seen, f"{label}: {value!r} listed twice"
        seen.append(value)
        assert share == f"{count / len(values):.0%})", f"{label}: share of {value!r}"

    # Ties have no defined order and the backends may break them
    # differently, so what is checked is that the counts shown are the
    # largest ones — not which of several equal values was picked.
    assert sorted((counts[v] for v in seen), reverse=True) == ranked[: len(seen)], (
        f"{label}: Top N are not the most frequent values"
    )


def check_temporal(row, values, label: str) -> None:
    present = [v for v in values if v is not None]
    assert_na_percent(row, values, label)

    if not present:
        for stat in ("Median", "Min", "Max"):
            assert row[stat] == "", f"{label}: {stat} of an all-null column"
        return

    as_moments = [_to_seconds(v) for v in present]
    low, high = min(as_moments), max(as_moments)

    assert _parse_moment(row["Min"]) == low, f"{label}: Min"
    assert _parse_moment(row["Max"]) == high, f"{label}: Max"
    # Not an exact value: the median of an even number of instants falls
    # between two of them, and where it lands depends on the column's time
    # unit — which `to_pandas()` changes, turning Date into Datetime(ms).
    # Pinning it would pin the conversion.
    assert low <= _parse_moment(row["Median"]) <= high, f"{label}: Median"


def tables_for(df) -> dict:
    """Every table showstats produces, as {table_type: {variable: row}}."""
    tables = {}
    for table_type in ("num", "cat", "time"):
        result = make_stats_tbl(df, table_type)
        if result is None:
            continue
        frame = stats_frame(result)
        name_column = frame.columns[0]
        tables[table_type] = {row[name_column]: row for row in frame.rows(named=True)}
    return tables


CHECKS = {"num": check_numerical, "cat": check_categorical, "time": check_temporal}


@settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
@given(frame=realistic_frames())
def test_every_column_is_summarised_correctly(frame):
    for backend, df in (("polars", frame), ("pandas", frame.to_pandas())):
        tables = tables_for(df)
        schema = nw.from_native(df, eager_only=True).schema

        for name in frame.columns:
            values = frame.get_column(name).to_list()
            found = [tt for tt, rows in tables.items() if name in rows]

            if schema[name] == nw.Object:
                # Decimal does not survive `to_pandas()`; it arrives as
                # `object`, which showstats does not classify. Nothing to
                # check beyond it not having landed somewhere wrong.
                assert found == [], f"{backend}/{name}: Object column was summarised"
                continue

            assert len(found) == 1, (
                f"{backend}/{name} ({schema[name]}): appears in {found or 'no table'}"
            )
            CHECKS[found[0]](tables[found[0]][name], values, f"{backend}/{name}")


@settings(max_examples=30, deadline=None)
@given(frame=realistic_frames())
def test_showing_every_table_never_raises(frame):
    """`show_stats(df)` has to cope with whatever mixture it is given.

    Output is captured with `redirect_stdout` rather than the `capsys`
    fixture: a function-scoped fixture is not reset between the inputs
    `@given` generates, so it would accumulate across examples.
    """
    for df in (frame, frame.to_pandas()):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            show_stats(df, "all")
        printed = buffer.getvalue()
        assert printed, "printed nothing"

        # A line starting with "-" is a section rule and its own fixed 80
        # wide; every row between two rules belongs to one table and must
        # be the same width as its header.
        table = None
        for line in printed.splitlines():
            if line.startswith("-"):
                assert len(line) == 80, f"rule is {len(line)} chars"
                table = None
                continue
            if table is None:
                table = len(line)
            assert len(line) == table, f"ragged table: {len(line)} vs {table}"


@settings(max_examples=25, deadline=None)
@given(
    values=st.lists(FLOAT64, min_size=2, max_size=20),
    extra=st.lists(FLOAT64, min_size=2, max_size=20),
)
def test_column_order_does_not_change_a_columns_statistics(values, extra):
    """A column is summarised on its own, whatever it sits next to."""
    n = min(len(values), len(extra))
    alone = pl.DataFrame({"x": values[:n]})
    together = pl.DataFrame({"x": values[:n], "y": extra[:n]})

    assert tables_for(alone)["num"]["x"] == tables_for(together)["num"]["x"]


def test_single_row_has_no_standard_deviation():
    """Drawn out of the generated frames, so asserted directly instead."""
    for df in (pl.DataFrame({"x": [1.5]}), pl.DataFrame({"x": [1.5]}).to_pandas()):
        row = tables_for(df)["num"]["x"]
        assert row["SD"] == ""
        assert row["Avg"] == "1.5"


@pytest.mark.parametrize("backend", ["polars", "pandas"])
def test_all_null_column_prints_blank_statistics(backend):
    df = pl.DataFrame({"x": [None, None, None]}, schema={"x": pl.Float64})
    if backend == "pandas":
        df = df.to_pandas()
    row = tables_for(df)["num"]["x"]
    assert row["NA%"] == 100
    for stat in ("Avg", "SD", "Median", "Min", "Max"):
        assert row[stat] == ""


def test_nullable_boolean_survives_as_polars():
    """Drawn without nulls above, so the asymmetry is recorded here.

    A boolean column containing nulls is `object` after `to_pandas()`, and
    showstats classifies `object` as nothing at all — so the column is
    summarised from polars and silently absent from pandas. That is the
    conversion's doing; narwhals reports the dtype it is given.
    """
    df = pl.DataFrame({"b": [True, False, None]})
    assert "b" in tables_for(df)["num"]
    assert "b" not in tables_for(df.to_pandas()).get("num", {})
