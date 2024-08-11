# showstats: quick and compact summary statistics


**showstats** produces summary statistic tables with vertical
orientation.

``` python
from showstats import show_stats, show_cat_stats

show_stats(df)
```

    | Var.          | Null % | Mean          | Median        | Std.     | Min           | Max          |
    | N=1.00E+6     |        |               |               |          |               |              |
    |---------------|--------|---------------|---------------|----------|---------------|--------------|
    | float_mean_2  | 0%     | 2.0           | 2.0           | 1.0      | -2.6          | 6.6          |
    | float_std_2   | 0%     | 0.0013        | 0.00024       | 2.0      | -9.3          | 9.3          |
    | float_min_7   | 0%     | 12.0          | 12.0          | 1.0      | 7.0           | 16.0         |
    | float_max_17  | 0%     | 12.0          | 12.0          | 1.0      | 7.7           | 17.0         |
    | float_col     | 0%     | 5000.0        | 5000.0        | 2900.0   | 0.0           | 10000.0      |
    | U             | 0%     | 0.5           | 0.5           | 0.29     | 5.1e-7        | 1.0          |
    | int_col       | 0%     | 500000.0      | 500000.0      | 290000.0 | 0             | 999999       |
    | int_with_miss | <20%   | 500000.0      | 500000.0      | 290000.0 | 1             | 999999       |
    | ings          |        |               |               |          |               |              |
    | bool_col      | <40%   | 0.5           | 0.0           | 0.5      | false         | true         |
    | str_col       | <60%   |               |               |          | ABC           | foo          |
    | categorical_c | 0%     |               |               |          | Fara          | Car          |
    | ol            |        |               |               |          |               |              |
    | enum_col      | 0%     |               |               |          | worst         | best         |
    | datetime_col  | 0%     | 1750-01-30    | 1750-03-18    |          | 1500-01-01    | 1999-12-31   |
    |               |        | 01:54:50      | 04:52:16      |          | 04:17:28      | 21:39:20     |
    | datetime_col_ | 0%     | 1750-01-12    | 1750-01-22    |          | 1500-01-01    | 1999-12-31   |
    | 2             |        | 23:02:09      | 06:33:24      |          | 06:19:48      | 17:41:57     |
    | date_col      | 0%     |               |               |          | 1500-01-01    | 1999-12-31   |
    | date_col_2    | 0%     |               |               |          | 1500-01-01    | 1999-12-31   |
    | null_col      | 100%   |               |               |          |               |              |

``` python
show_cat_stats(df)
```

    | Var. N=1.00E+6  | Null % | N uniq. | Top values   |
    |-----------------|--------|---------|--------------|
    | str_col         | <60%   | 5       | None (55%)   |
    |                 |        |         | baz (11%)    |
    |                 |        |         | ABC (11%)    |
    | categorical_col | 0%     | 2       | Fara (50%)   |
    |                 |        |         | Car (50%)    |
    | enum_col        | 0%     | 3       | worst (33%)  |
    |                 |        |         | medium (33%) |
    |                 |        |         | best (33%)   |

Primarily built for polars data frames, **showstats** converts other
inputs. For full compatibility with pandas.DataFrames install as
`pip install showstats[pandas]`.

Because **showstats** uses polars as backend, its really fast: \<1
second for a 1,000,000 × 1,000 data frame, running on a M1 MacBook.
