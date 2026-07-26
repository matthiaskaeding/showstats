# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `quantiles` argument on `show_stats`/`make_stats_tbl` to compute extra
  quantile columns for numerical columns (#4)

### Fixed

- Off-by-one exponent in scientific notation formatting caused by
  floating-point imprecision in `log10().floor()`, which could print e.g.
  "10.0E5" instead of "1.0E6" (#27)
- Long variable names no longer wrap onto a misaligned line; they are now
  truncated with an ellipsis (#26)

### Changed

- Input handling and statistic computation now go through `narwhals`, so any
  narwhals-compatible data frame (polars, pandas, pyarrow, ...) is accepted
  directly rather than relying on `pl.DataFrame`'s implicit pandas support
  (#33, #34). Formatting and rendering are still polars-internal; see #37 for
  the remaining steps.

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
