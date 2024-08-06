# showstats: quick and compact summary statistics


**showstats** produces summary statistic tables with vertical
orientation.

``` python
from showstats import show_stats

show_stats(df)
```

    | Var; N = 100 | Missing      | Mean          | Median       | Std.  | Min          | Max          |
    |--------------|--------------|---------------|--------------|-------|--------------|--------------|
    | float_col    | 0 (0.0%)     | 0.5           | 0.5          | 0.29  | 0.0          | 0.99         |
    | X            | 0 (0.0%)     | 49.5          | 49.5         | 29.01 | 0.0          | 99.0         |
    | Y            | 20 (20.0%)   | 57.0          | 59.5         | 27.6  | 0.0          | 99.0         |
    | datetime     | 0 (0.0%)     | 2022-01-01    | 2022-01-01   |       | 2022-01-01   | 2022-01-01   |
    |              |              | 00:00:49      | 00:00:49     |       | 00:00:00     | 00:01:39     |
    | date         | 0 (0.0%)     |               |              |       | 2022-01-01   | 2022-04-10   |
    | string       | 0 (0.0%)     |               |              |       | ABC          | foo          |
    | null_col     | 100 (100.0%) |               |              |       |              |              |

Primarily built for polars data frames. **showstats** converts other
inputs, for compatibility with pandas.DataFrames install as
`pip install showstats[pandas]`
