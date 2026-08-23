# AGENTS.md

Guidance for coding agents working in this repository.

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
directly. `prepare_input` returns the Narwhals frame and its schema. It keeps
lazy inputs lazy and raises `TypeError` for plain Python literals such as lists
and dictionaries, because they are not Narwhals frames.

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
- `uv.lock` is ignored. This is a library, so CI resolves the allowed
  dependency versions on each run and can catch new incompatibilities. Run
  `uv sync` after changing dependencies, but do not commit the generated lock.
- `.python-version` pins the interpreter (3.11.9) so CI does not silently
  drift to a newer Python; `uv` provisions it automatically.
- Lint/format uses `ruff==0.16.0`. `required-version = "==0.16.0"` in
  `[tool.ruff]` makes ruff refuse to run under any other version, however
  it's invoked — ruff's default rule set grows between releases, so a
  mismatch lints under different rules than CI enforces. Bumping it means
  updating **four** places: `required-version`, the `dev` group, `rev:` in
  `.pre-commit-config.yaml`, and `version:` in `.github/workflows/lint.yaml`
  — and then fixing whatever the new defaults flag. `noxfile.py`'s lint
  session is deliberately not a fifth: it shells out to `uv run ruff` so it
  picks up the dev group's pin. It used to install ruff unpinned, which
  broke the day 0.16.1 shipped. CI lints via
  `astral-sh/ruff-action` rather than syncing the dev environment, since
  ruff is a standalone tool.
- `README.md` is generated from `README.qmd` via Quarto: `uv run quarto
  render README.qmd`. `uv run` matters here — it puts the project
  environment on `PATH` so Quarto's jupyter engine uses the synced
  interpreter. CI re-renders on every push to `main` that touches
  `README.qmd` or `src/**`, so hand-editing `README.md` will be
  overwritten.
- Update `changelog.md` (Keep a Changelog format) for user-facing changes.

## No polars

`narwhals` is the only runtime dependency. Statistics, formatting, table
assembly and printing all go through it, so showstats adds nothing to
whichever dataframe library the caller already has — that was #37, finished
across PRs #72–#83.

Consequences worth knowing before editing `src/`:

- `make_stats_tbl` returns a frame of the **input's** type, never polars.
- Printing is `_utils.render_table`, a hand-written fixed-width formatter,
  not a dataframe library's `__repr__`. `tests/_golden.py` pins its exact
  output, byte for byte, and `README.md` embeds that output.
- Backends disagree about how values become text — Arrow drops a trailing
  `.0`, pandas renders booleans as `True` — so `tests/test_backends.py`
  asserts pandas and pyarrow print byte-identically to polars. Add a case
  there when touching formatting.
- The narwhals floor is a *feature* floor, checked rather than guessed, and
  it moves with `requires-python`: 1.42.1 and below need Python 3.8, 1.43.0
  to 2.21.0 need 3.9, 2.21.2 and above need 3.10. Raising one without the
  other is what #78 was about.

`xfail_strict = true` stays: an unexpectedly passing xfail fails the build.

## Issue work — always open a PR

When asked to work an issue (or issues) from this repo's GitHub tracker:
push the branch **and open a pull request** referencing the issue
number(s). Don't stop at a branch push — a finished unit of issue work
ends with a PR, not just pushed commits.
