# dfstats: quick and compact summary statistics


**dfstats** produces summary statistic tables with vertical orientation.

``` python
from dfstats import show_stats

show_stats(df)
```

    | Var; N = 100  | Missing      | Mean         | Median       | Std.  | Min          | Max          |
    |---------------|--------------|--------------|--------------|-------|--------------|--------------|
    | int_col       | 0 (0.0%)     | 49.5         | 49.5         | 29.01 | 0.0          | 99.0         |
    | int_with_miss | 20 (20.0%)   | 57.0         | 59.5         | 27.6  | 0.0          | 99.0         |
    | ing           |              |              |              |       |              |              |
    | float_col     | 0 (0.0%)     | 0.5          | 0.5          | 0.29  | 0.0          | 0.99         |
    | float_col_wit | 0 (0.0%)     | 2.0          | 2.06         | 0.94  | -0.79        | 4.44         |
    | h_mean_2      |              |              |              |       |              |              |
    | float_col_wit | 0 (0.0%)     | -0.11        | 0.02         | 2.0   | -6.01        | 5.06         |
    | h_std_2       |              |              |              |       |              |              |
    | float_col_wit | 0 (0.0%)     | 9.79         | 9.85         | 0.94  | 7.0          | 12.22        |
    | h_min_7       |              |              |              |       |              |              |
    | float_col_wit | 0 (0.0%)     | 14.56        | 14.62        | 0.94  | 11.78        | 17.0         |
    | h_max_17      |              |              |              |       |              |              |
    | bool_col      | 0 (0.0%)     | 0.5          | 0.5          | 0.5   | 0.0          | 1.0          |
    | datetime_col  | 0 (0.0%)     | 2022-01-01   | 2022-01-01   |       | 2022-01-01   | 2022-01-01   |
    |               |              | 00:00:49     | 00:00:49     |       | 00:00:00     | 00:01:39     |
    | datetime_col_ | 0 (0.0%)     | 1995-01-01   | 1995-01-01   |       | 1995-01-01   | 1995-01-01   |
    | 2             |              | 00:00:49     | 00:00:49     |       | 00:00:00     | 00:01:39     |
    | date_col      | 0 (0.0%)     |              |              |       | 2022-01-01   | 2022-04-10   |
    | date_col_2    | 0 (0.0%)     |              |              |       | 1500-01-01   | 1500-04-10   |
    | str_col       | 0 (0.0%)     |              |              |       | ABC          | foo          |
    | enum_col      | 0 (0.0%)     |              |              |       | low          | high         |
    | categorical_c | 0 (0.0%)     |              |              |       | low          | medium       |
    | ol            |              |              |              |       |              |              |
    | null_col      | 100 (100.0%) |              |              |       |              |              |

Primarily built for polars data frames. **dfstats** converts other
inputs, for compatibility with pandas.DataFrames install as
`pip install dfstats[pandas]`
