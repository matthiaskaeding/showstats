"""The behaviour #37 is aiming at, declared up front and expected to fail.

Every test here is `@pytest.mark.rewrite` + a strict xfail: it describes
something the narwhals migration is supposed to make true and that is not
true yet. `xfail_strict = true` turns each one into a ratchet — the moment a
slice makes one pass, the build fails until its markers are removed. So the
markers cannot rot, and `make burndown` is an honest count of what is left.

Tests are grouped by the implementation slice that should retire them, in
the order the slices land. Nothing here is marked `integration`: CI
deselects that marker, and hiding a rewrite test from CI would defeat the
whole arrangement.

Behaviour that already works is *not* here — it lives in the ordinary test
files, and in `test_golden_output.py`, as the regression net.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

import pandas as pd
import polars as pl
import pyarrow as pa
import pytest

from showstats.showstats import make_stats_tbl, show_stats
from tests import _golden

pytestmark = pytest.mark.rewrite

# The same data in three backends. Going through polars' own converters
# keeps the dtypes corresponding — a hand-built pandas frame would differ in
# ways that have nothing to do with the rewrite (int64 vs float64 for a
# column with nulls, say), and the tests would then be measuring the
# conversion rather than showstats.
MIXED_PL = pl.DataFrame(
    {
        "int_col": [1, 2, 3, 4, 5],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "str_col": ["a", "b", "c", "a", "b"],
    }
)
MIXED_PD = MIXED_PL.to_pandas()
MIXED_PA = MIXED_PL.to_arrow()


def render(capsys, df, **kwargs) -> str:
    show_stats(df, **kwargs)
    return capsys.readouterr().out


def run_without_polars(body: str) -> subprocess.CompletedProcess:
    """Run `body` in a fresh interpreter where importing polars raises.

    A subprocess rather than monkeypatching `sys.modules`: `showstats` and
    `polars` are both already imported by the time a test runs, so an
    in-process block would only prove that the *cached* modules still work.
    The finder has to be in place before showstats is imported at all.
    """
    script = textwrap.dedent("""
        import sys

        class NoPolars:
            def find_spec(self, fullname, path=None, target=None):
                if fullname == "polars" or fullname.startswith("polars."):
                    raise ImportError("polars is not installed")
                return None

        sys.meta_path.insert(0, NoPolars())
        assert "polars" not in sys.modules
    """) + textwrap.dedent(body)
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )


# Slice 1 — the backend fork in _Table.__init__ — is done. Its tests now
# pass, so they have moved to test_backends.py, where they belong as
# ordinary regression tests rather than as a to-do list.

# Slice 2 — _utils.convert_df_scientific — is done. Its tests now pass, so
# they have moved to test_scientific_conversion.py alongside the polars
# assertions they were written against.

# Slice 3 — _Table.make_dt — is done. Its tests now pass, so they have
# moved to test_table.py, next to the other make_dt assertions.

# --------------------------------------------------------------------------
# Slice 4 — _Table.form_stat_df and the return type
#
# make_stats_tbl returns a pl.DataFrame whatever it is given. The agreed
# outcome (see the sign-off on #37) is that the return follows the input.
# The polars case already holds today and so is asserted in
# test_make_tbl.py, unmarked, rather than here.
# --------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True, reason="#37: form_stat_df collects to polars whatever came in"
)
@pytest.mark.parametrize(
    ("df", "native_type"),
    [
        pytest.param(MIXED_PD, pd.DataFrame, id="pandas"),
        pytest.param(MIXED_PA, pa.Table, id="pyarrow"),
    ],
)
def test_make_stats_tbl_returns_the_input_type(df, native_type):
    assert isinstance(make_stats_tbl(df, "num"), native_type)


@pytest.mark.xfail(
    strict=True, reason="#37: form_stat_df orders top_cols by casting to pl.Enum"
)
def test_top_cols_ordering_survives_the_backend_change():
    """top_cols currently works by casting to pl.Enum and sorting.

    narwhals has no equivalent, so the ordering needs a different
    implementation — and it has to keep working, natively, for a
    non-polars input.
    """
    result = make_stats_tbl(MIXED_PD, "num", top_cols="float_col")
    assert isinstance(result, pd.DataFrame)
    assert result[result.columns[0]].tolist() == ["float_col", "int_col"]


# --------------------------------------------------------------------------
# Slice 5 — _Table.show_one_table
#
# Printing goes through pl.Config, so there is no polars-free path to the
# output at all: polars stops being importable-or-bust only once this
# lands.
#
# Rendering no longer varies by input backend — that turned out to belong
# to slice 1 rather than here, and its test now lives in test_backends.py.
#
# Which rendering wins is not open: the polars one, because README.md is
# generated from it and is pinned in test_golden_output.py. So the last
# test below compares against the golden, not merely against itself.
# --------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="#37: showstats._table imports polars at import")
def test_showstats_imports_without_polars():
    result = run_without_polars("""
        import showstats
        assert "polars" not in sys.modules
        print("ok")
    """)
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


@pytest.mark.xfail(strict=True, reason="#37: rendering goes through pl.Config")
def test_show_stats_renders_without_polars():
    result = run_without_polars(f"""
        import pandas as pd
        from showstats import show_stats

        df = pd.DataFrame({MIXED_PL.to_dict(as_series=False)!r})
        show_stats(df, "num")
        assert "polars" not in sys.modules
    """)
    assert result.returncode == 0, result.stderr
    assert "int_col" in result.stdout


@pytest.mark.xfail(
    strict=True, reason="#37: form_stat_df needs polars to build a frame"
)
def test_make_stats_tbl_works_without_polars():
    result = run_without_polars(f"""
        import pandas as pd
        from showstats.showstats import make_stats_tbl

        df = pd.DataFrame({MIXED_PL.to_dict(as_series=False)!r})
        out = make_stats_tbl(df, "num")
        assert isinstance(out, pd.DataFrame), type(out)
        assert "polars" not in sys.modules
        print("ok")
    """)
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


# --------------------------------------------------------------------------
# The finish line — what "#37 is done" means, in one test.
# --------------------------------------------------------------------------


@pytest.mark.xfail(strict=True, reason="#37: polars is still a hard runtime dependency")
def test_the_readme_table_renders_without_polars():
    """The whole point, end to end: the documented output, no polars.

    Compared against the golden captured from the current renderer, so
    "works without polars" cannot be achieved by rendering something else.
    """
    data = {
        "cat_col": ["A"] * 5 + ["B"] * 3 + ["C"] * 2,
        "other_col": ["x", "y", None, "x", "x", "y", "y", "x", "x", "y"],
    }
    result = run_without_polars(f"""
        import pandas as pd
        from showstats import show_stats

        show_stats(pd.DataFrame({data!r}), "cat")
    """)
    assert result.returncode == 0, result.stderr
    assert result.stdout == _golden.CAT
