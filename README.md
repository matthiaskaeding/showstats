# showstats: quick and compact summary statistics


**showstats** produces summary statistic tables with vertical
orientation.

``` python
from showstats import show_stats

show_stats(df)
```

    | Var; N=100          | Null % | Mean       | Median     | Std. | Min        | Max                 |
    |---------------------|--------|------------|------------|------|------------|---------------------|
    | float_mean_2        | 0%     | 2.0        | 2.0        | 0.89 | -0.36      | 4.1                 |
    | float_std_2         | 0%     | 0.14       | 0.14       | 2.0  | -5.2       | 4.9                 |
    | float_min_7         | 0%     | 9.4        | 9.4        | 0.89 | 7.0        | 11.0                |
    | float_max_17        | 0%     | 15.0       | 15.0       | 0.89 | 13.0       | 17.0                |
    | bool_col            | <40%   | 0.46       | 0.0        | 0.5  | 0.0        | 1.0                 |
    | int_col             | 0%     | 50.0       | 50.0       | 29.0 | 0.0        | 99.0                |
    | float_col           | 0%     | 0.5        | 0.5        | 0.29 | 0.0        | 0.99                |
    | U                   | 0%     | 0.5        | 0.55       | 0.31 | 0.013      | 0.99                |
    | int_with_missings   | <20%   | 51.0       | 52.0       | 29.0 | 0.0        | 99.0                |
    | datetime_col        | 0%     | 1756-03-09 | 1755-07-20 |      | 1501-01-20 | 1996-04-09 06:29:29 |
    |                     |        | 11:19:09   | 10:10:59   |      | 14:37:46   |                     |
    | datetime_col_2      | 0%     | 1776-05-02 | 1776-03-03 |      | 1511-12-06 | 1999-05-05 14:12:20 |
    |                     |        | 10:44:13   | 13:25:50   |      | 23:40:13   |                     |
    | date_col            | 0%     |            |            |      | 1501-01-20 | 1996-04-09          |
    | date_col_2          | 0%     |            |            |      | 1511-12-06 | 1999-05-05          |
    | str_col             | <60%   |            |            |      | ABC        | foo                 |
    | enum_col            | 0%     |            |            |      | low        | high                |
    | categorical_col     | 0%     |            |            |      | medium     | low                 |
    | null_col            | 100%   |            |            |      |            |                     |

Primarily built for polars data frames, **showstats** converts other
inputs.

For compatibility with pandas.DataFrames install as
`pip install showstats[pandas]`.
