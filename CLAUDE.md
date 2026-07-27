# CLAUDE.md

Guidance for Claude Code (and other agents) working in this repository.

## Project

`showstats` prints compact, vertically-oriented summary statistic tables for
data frames. Core logic lives in `src/showstats/`:

- `_table.py` — `_Table` class: maps columns to var-types (num_float,
  num_int, num_bool, cat, date, datetime, null) and builds the stat
  DataFrame.
- `_utils.py` — scientific-notation formatting (`convert_df_scientific`).
- `showstats.py` — public `show_stats` / `make_stats_tbl` functions.

`show_stats` and `make_stats_tbl` are the whole public API — there is no
DataFrame accessor. A polars-only `.stats` namespace existed until #53 and
was removed as incompatible with the narwhals direction.

Input and statistic computation go through `narwhals`, so any
narwhals-compatible frame (polars, pandas, pyarrow, ...) is accepted
directly. Note that `_check_input_maybe_try_transform` returns a
`nw.DataFrame`, not a polars one, and raises `TypeError` for plain Python
literals (list/dict) — those are not narwhals-native frames.

Formatting, table assembly and rendering are still polars-internal
(`make_dt`, `form_stat_df` and `show_one_table` build a `pl.LazyFrame`
and print via `pl.Config`), so `polars` remains a hard dependency. See
issue #37 for the tracked plan to finish the migration.

## Dev workflow

**Always use `uv`.** Never `pip install`, `python -m build`, or a bare
`pytest`/`ruff` — those pick up whatever happens to be on `PATH` rather
than the project environment.

```
uv sync              # create/refresh .venv from pyproject.toml
uv run pytest        # tests (pythonpath is set to `src` via pyproject.toml)
uv run ruff check .  # lint
uv run ruff format   # format
uv build             # sdist + wheel
```

- Dependencies live in `pyproject.toml` only: runtime deps under
  `[project.dependencies]`, dev/test deps in the `dev` group under
  `[dependency-groups]` (PEP 735). There is no `dev-requirements.txt` —
  it was removed in #69. A runtime dependency that tests also need does
  **not** have to be repeated in the dev group; `uv sync` installs the
  project itself.
- `uv.lock` is committed, so CI and local dev resolve identically. Run
  `uv sync` after changing dependencies and commit the updated lock.
- `.python-version` pins the interpreter (3.11.9) so CI does not silently
  drift to a newer Python; `uv` provisions it automatically.
- Lint/format uses `ruff==0.5.6`. A newer ruff enables extra rules (e.g.
  `RUF013`, `FA100`) that aren't part of this project's lint gate and would
  produce false positives, so `required-version = "==0.5.6"` in
  `[tool.ruff]` makes ruff refuse to run under any other version — however
  it's invoked. If you bump it, update all three: `required-version`, the
  dev group, and `rev:` in `.pre-commit-config.yaml`. CI lints via
  `astral-sh/ruff-action` rather than syncing the dev environment, since
  ruff is a standalone tool.
- `README.md` is generated from `README.qmd` via Quarto: `uv run quarto
  render README.qmd`. `uv run` matters here — it puts the project
  environment on `PATH` so Quarto's jupyter engine uses the synced
  interpreter. CI re-renders on every push to `main` that touches
  `README.qmd` or `src/**`, so hand-editing `README.md` will be
  overwritten.
- Update `changelog.md` (Keep a Changelog format) for user-facing changes.

## Issue work — always open a PR

When asked to work an issue (or issues) from this repo's GitHub tracker:
push the branch **and open a pull request** referencing the issue
number(s). Don't stop at a branch push — a finished unit of issue work
ends with a PR, not just pushed commits.
