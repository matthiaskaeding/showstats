# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- Tables are printed by showstats itself rather than by polars' dataframe
  formatter. The layout is unchanged; two of polars' display behaviours are
  not reproduced, both listed under Fixed below (#37)
- **Breaking:** `make_stats_tbl` now returns a frame of the same kind as its
  input — pandas in, pandas out; pyarrow in, pyarrow out — instead of always
  returning a polars DataFrame. Code that called polars methods on the
  result of a non-polars input needs updating (#37)
- Minimum `narwhals` raised from 1.0.0 to 1.40.0, which is where
  `Expr.log` arrived. The old floor was never checked against anything —
  see #78, the suite in fact needs 2.0.0 for the pandas backend (#37)

### Fixed

- pyarrow tables raised `AttributeError: 'pyarrow.lib.Table' object has no
  attribute 'iloc'`. They were accepted as input and then summarised through
  a pandas-only code path, so no pyarrow frame worked (#37)
- Integer statistics from a pandas frame were printed as floats — `Min` as
  `1.0` rather than `1`, `Uniques` as `3.00` rather than `3`. The same data
  now prints identically whatever backend carries it (#37)
- pyarrow tables with a boolean column raised `ArrowNotImplementedError:
  Function 'stddev' has no kernel matching input types (bool)` (#37)
- Booleans printed as `True`/`False` from a pandas frame and `true`/`false`
  from polars and pyarrow; floats ending in `.0` lost the decimal from a
  pyarrow table. Both now follow the polars rendering (#37)
- Tables with more than eight columns silently dropped one and printed `…`
  in its place — so `quantiles=[0.25, 0.5, 0.75]` lost the `Q0` column, and
  the quantiles example in the README lost `Median`. Every column is now
  shown (#37)
- A value wider than the table wrapped onto a second, unaligned line, which
  is how a datetime median printed. Columns are now sized to their contents,
  so a wide table is wide rather than misaligned (#37)

## [0.1.0] - 2026-07-27

### Removed

- **Breaking:** the `.stats` polars DataFrame namespace and the `StatsFrame`
  class. `show_stats(df)` and `make_stats_tbl(df)` replace
  `df.stats.show()` and `df.stats.make_tbl()`. The accessor was polars-only,
  which does not fit the narwhals direction (#53)

### Changed

- `Min` and `Max` now come last in the numerical and time tables, after
  `Median` and any quantile columns — the central statistics lead and the
  extremes close (#35)

### Added

- `quantiles` argument on `show_stats`/`make_stats_tbl` to compute extra
  quantile columns for numerical columns. When given, `Min` and `Max` are
  relabelled `Q0`/`Q100` and folded into the quantile sequence, and an
  explicit `0.5` replaces the `Median` column (#4)
- `fold_quantiles` argument to turn that folding off, keeping
  `Min`/`Max`/`Median` as separate columns so column names stay stable
  whatever quantiles are requested (#4)

### Fixed

- Long variable names no longer wrap onto a misaligned line; they are now
  truncated with an ellipsis (#26)
- Off-by-one exponent in scientific notation formatting caused by
  floating-point imprecision in `log10().floor()`, which could print e.g.
  "10.0E5" instead of "1.0E6" (#27)

## [0.0.3]

### Changed

- Better formatting for booleans and integer stats
- By default now prints 3 seperate tables for time, num and cat
- show_cat has argument table_type (num, cat, time and all)
- Scientific notation for numbers with many decimals

### Added 

- Tests
  
### Changed

- Use internal class 

## [0.0.2] - 2024-08-07

### Added

- Parameter "top_cols" which puts selected columns at front

### Changed

- Missings only printed as upper bound percentage
- Rounding uses round_sig_figs
- Quicker simulation of data-frames (only internal) 
- Improvements in README and docstrings

### Fixed

- Some typos in test-files


## [0.0.1] - 2024-08-06

### Changed

- Initial release
