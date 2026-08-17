"""showstats works with no polars installed at all.

This is what #37 was for. `_table.py` imported polars at module scope and
printed through `pl.Config`, so polars was a hard runtime dependency even
for someone whose data was in pandas — installing showstats meant
installing a second dataframe library to render a summary of the first.

The check is a subprocess with an import blocker on `sys.meta_path`, not
`monkeypatch.setitem(sys.modules, ...)`: by the time a test runs both
showstats and polars are already imported, so an in-process block would
only prove the cached modules still work. The finder has to be installed
before showstats is imported at all.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import polars as pl

import showstats
from tests import _golden

# Where the subprocess should look for showstats. `pythonpath = ["src"]` in
# pyproject.toml only applies to the pytest process itself, and `uv run`
# installs the project editable — neither reaches a bare `python -c`, so
# under `nox`, which does neither, the subprocess could not import
# showstats at all. Taking the path from the module this process imported
# works however the environment was put together.
_IMPORT_ROOT = str(Path(showstats.__file__).resolve().parent.parent)

MIXED_PL = pl.DataFrame(
    {
        "int_col": [1, 2, 3, 4, 5],
        "float_col": [1.5, 2.5, 3.5, 4.5, 5.5],
        "str_col": ["a", "b", "c", "a", "b"],
    }
)


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
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(
        [_IMPORT_ROOT, *([env["PYTHONPATH"]] if env.get("PYTHONPATH") else [])]
    )
    return subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )


# Slice 1 — the backend fork in _Table.__init__ — is done. Its tests now
# pass, so they have moved to test_backends.py, where they belong as
# ordinary regression tests rather than as a to-do list.

# Slice 2 — _utils.convert_df_scientific — is done. Its tests now pass, so
# they have moved to test_scientific_conversion.py alongside the polars
# assertions they were written against.

# Slice 3 — _Table.make_dt — is done. Its tests now pass, so they have
# moved to test_table.py, next to the other make_dt assertions.

# Slice 4 — _Table.form_stat_df and the return type — is done. Its tests
# now pass, so they have moved to test_make_tbl.py.


def test_showstats_imports_without_polars():
    result = run_without_polars("""
        import showstats
        assert "polars" not in sys.modules
        print("ok")
    """)
    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


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


def test_table_one_renders_without_polars():
    result = run_without_polars(f"""
        import pandas as pd
        from showstats import table_one

        df = pd.DataFrame({MIXED_PL.to_dict(as_series=False)!r})
        table_one(df, style="median_iqr", show_missing=False)
        assert "polars" not in sys.modules
    """)
    assert result.returncode == 0, result.stderr
    assert "Median [Q1, Q3]" in result.stdout
    assert "NA%" not in result.stdout


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
