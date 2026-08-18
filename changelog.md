# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `show_stats(..., fmt="gt")` returns styled Great Tables output. Great Tables
  is available through the optional `gt` extra. `NA%` values of 25 percent or
  more use bold text. Set `color_missing=True` to add a white to dark gray
  background scale. Numerical statistics are right aligned (#103).
- Lazy frames are accepted. `show_stats(df.lazy())` used to raise
  `TypeError: Cannot only use eager_only ... with polars.LazyFrame`; the
  schema is read once and used to build the summary plan. Scalar statistics
  and the row count are collected as one row. Categorical top values and
  temporal medians use small queries that collect no more than three rows per
  column. `make_stats_tbl` always returns an eager frame, but showstats does
  not materialize the full lazy input (#85)

### Fixed

- `make_stats_tbl(..., table_type="all")` now returns a dictionary of the
  nonempty numerical, categorical, and temporal tables instead of returning
  `None`.

### Fixed

- Date and datetime columns raised `ArrowNotImplementedError: Unsupported
  cast from date32[day] to int64` on a pyarrow table — every one of them.
  The median went through Int64, which is the detour pandas needs, and Arrow
  refuses that cast and has no quantile kernel for dates either. It is
  computed from the sorted values now, which needs no cast (#86)
- The median was approximate on pyarrow. `median()` maps to Arrow's
  `approximate_median`, a t-digest: for the two values 0.00 and 0.02 it
  answered 0.0. The median is the 50th percentile now, which is exact on
  every backend — and the same call the `Q50` column makes, so the two agree
  by construction rather than by coincidence (#86)
- Quantiles of a narrow integer column overflowed: pandas answered 64.0 for
  the median of an `Int8` column holding 0 and -128, where it is -64.0.
  Interpolation happens in Int64 now. This affected the `Q50` column too,
  not only `Median` (#86)
- A statistic missing for every row — the standard deviation of a one-row
  column, say — raised `ArrowInvalid: Invalid null value` on a pyarrow
  table, because Arrow types an all-null column as `null` rather than as an
  absent float (#86)

### Changed

- CI now runs the stable suite with Python 3.10 and Narwhals 2.20.0, so the
  declared minimum versions are checked on every PR. The pandas extra now
  requires PyArrow 13, which is the minimum supported by Narwhals 2.20.0
  (#78)
- **Breaking:** `requires-python` is now `>= 3.10`, up from `>= 3.8`, and the
  minimum `narwhals` is 2.20.0. The two are linked: `requires-python` caps
  which narwhals is installable at all — 1.42.1 and below need 3.8, 1.43.0
  to 2.21.0 need 3.9, 2.21.2 and above need 3.10 — so claiming 3.8 pinned
  the project to a narwhals nothing had ever been tested against. Python 3.8
  and 3.9 are both past end of life. Four internal workarounds go with it:
  `Expr.floor`, `Expr.ceil`, `str.len_chars` and chained `.when()` are used
  directly now instead of being hand-rolled (#78)
- **Breaking:** the numerical table now reads `Avg`, `Median`, `SD`, then the
  extremes. The two measures of location sit together with the spread beside
  them, rather than `SD` splitting them apart (#74)
- **Breaking:** the row count moved from the first column's header to the
  section rule — `-Numerical columns (N=1461)---` with the column simply
  named `Col`. The header was usually wider than the variable names and
  padded every row of the first column out to its own length: twelve
  characters of `Col (N=1461)` against a seven-character `weather`, or
  fifteen once N passes 100,000 and the count goes scientific. A side
  benefit for callers of `make_stats_tbl`: the first column's name no longer
  changes with the row count, so it can be addressed by name (#75)

## [0.2.0] - 2026-07-29

### Changed

- **Breaking:** `polars` is no longer a dependency. `narwhals` is the only
  one — statistics, formatting, table assembly and printing all go through
  it, so showstats adds nothing to whichever dataframe library you already
  have. Installing showstats no longer pulls polars in; if you want it,
  `pip install showstats[polars]` (#37, #42)
- Tables are printed by showstats itself rather than by polars' dataframe
  formatter. The layout is unchanged; two of polars' display behaviours are
  not reproduced, both listed under Fixed below (#37)
- **Breaking:** `make_stats_tbl` now returns a frame of the same kind as its
  input — pandas in, pandas out; pyarrow in, pyarrow out — instead of always
  returning a polars DataFrame. Code that called polars methods on the
  result of a non-polars input needs updating (#37)
- Minimum `narwhals` raised from 1.0.0 to 1.40.0, which is where
  `Expr.log` arrived. The old floor was never checked against anything;
  the whole `noxfile.py` matrix — Python 3.8 to 3.12, polars 0.20.21 and
  1.4.1 — now passes, where the 3.8 and 3.9 legs did not before. See #78
  for the remaining tension between `requires-python` and narwhals (#37)

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
- `NA%` could be one percentage point too high: the missing share was
  computed as `count / rows * 100`, which polars evaluates as
  `60.00000000000001` for 6 of 10, so a column exactly 60% missing was
  reported as 61%. Multiplying before dividing is exact — checked over every
  count/rows pair up to 60 rows on all three backends (#37)
- The median of a date or datetime column containing nulls was wrong on the
  pandas backend. pandas represents a missing timestamp as `NaT`, and
  casting that to an integer gives the int64 minimum rather than null, so
  the missing rows joined the median as enormous negative numbers — two
  nulls alongside 2020-01-01, 2020-06-01 and 2020-12-01 reported a median
  of 2020-01-01. Not blank, simply wrong (#37)
- `Uniques` counted null as a distinct value, so a column of `a`, `b` and
  two nulls read as three uniques with only two ever listed beside it —
  disagreeing with both `NA%` and the `Top N` columns, which treat null as
  missing (#37)
- A pandas categorical or enum column padded its `Top N` columns with
  categories it does not actually contain, at 0%: a column holding only
  `alpha` listed `alpha (67%)`, `beta (0%)`, `gamma (0%)` (#37)
- A frame holding a `Decimal` column beside an ordinary float column raised
  `TypeError: unexpected value while building Series of type Decimal(38, 2)`.
  Decimal columns are now summarised as floats. A Decimal column on its own
  always worked, which is why this went unnoticed (#37)
- An all-null date or datetime column printed the literal `NaT` as its
  median on pandas 1.5, where newer pandas printed blank. Whether a
  statistic is missing is now decided from the value rather than from what
  the backend renders it as (#37)
- `show_stats(df)` printed absolute silence when no column had a dtype
  showstats summarises, rather than saying so as every other table type
  does (#37)
- `Min` and `Max` of an all-null integer or boolean column came back as null
  from `make_stats_tbl` where the equivalent float column gives `""`. Both
  printed blank either way (#37)
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
