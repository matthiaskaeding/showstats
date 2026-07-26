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
- `pl_namespace.py` — registers the `.stats` polars DataFrame namespace.

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

- Dependencies for local dev/test are pinned in `dev-requirements.txt`
  (**not** `pyproject.toml`, which only lists the runtime deps) — CI
  installs from there. If you add a runtime dependency, update both
  `pyproject.toml` and `dev-requirements.txt`, plus `noxfile.py`'s
  `session.install(...)` calls.
- Run tests: `pytest tests/` (pythonpath is set to `src` via
  `pyproject.toml`).
- Lint/format: use the pinned `ruff==0.5.6` from `dev-requirements.txt`
  specifically — a newer ruff enables extra rules (e.g. `RUF013`,
  `FA100`) that aren't part of this project's actual lint gate and will
  produce false positives. `ruff check .` and `ruff format` mirror
  `.github/workflows/lint.yaml` and `.pre-commit-config.yaml`.
- `README.md` is generated from `README.qmd` via Quarto; when editing one,
  keep the other in sync (render with `quarto render README.qmd` if
  Quarto/Jupyter are available, otherwise hand-edit `README.md` to match).
- Update `changelog.md` (Keep a Changelog format) for user-facing changes.

## Issue work — always open a PR

When asked to work an issue (or issues) from this repo's GitHub tracker:
push the branch **and open a pull request** referencing the issue
number(s). Don't stop at a branch push — a finished unit of issue work
ends with a PR, not just pushed commits.
