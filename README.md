# dfstats: quick and compact summary statistics


Polars offers a good way to summarize a dataframe `df` via the
`df.describe` method, however this returns the output in a wide (same
number of columns as `df`) format.

``` python
import polars as pl

# Polars
print(df.describe())
```

    shape: (9, 17)
    ┌────────────┬───────────┬───────────┬───────────┬───┬───────────┬───────────┬──────────┬──────────┐
    │ statistic  ┆ int_col   ┆ int_with_ ┆ float_col ┆ … ┆ datetime_ ┆ categoric ┆ enum_col ┆ null_col │
    │ ---        ┆ ---       ┆ missing   ┆ ---       ┆   ┆ col_2     ┆ al_col    ┆ ---      ┆ ---      │
    │ str        ┆ f64       ┆ ---       ┆ f64       ┆   ┆ ---       ┆ ---       ┆ str      ┆ f64      │
    │            ┆           ┆ f64       ┆           ┆   ┆ str       ┆ str       ┆          ┆          │
    ╞════════════╪═══════════╪═══════════╪═══════════╪═══╪═══════════╪═══════════╪══════════╪══════════╡
    │ count      ┆ 100.0     ┆ 80.0      ┆ 100.0     ┆ … ┆ 100       ┆ 100       ┆ 100      ┆ 0.0      │
    │ null_count ┆ 0.0       ┆ 20.0      ┆ 0.0       ┆ … ┆ 0         ┆ 0         ┆ 0        ┆ 100.0    │
    │ mean       ┆ 49.5      ┆ 57.0      ┆ 0.495     ┆ … ┆ 1995-01-0 ┆ null      ┆ null     ┆ null     │
    │            ┆           ┆           ┆           ┆   ┆ 1 00:00:4 ┆           ┆          ┆          │
    │            ┆           ┆           ┆           ┆   ┆ 9.500000  ┆           ┆          ┆          │
    │ std        ┆ 29.011492 ┆ 27.595633 ┆ 0.290115  ┆ … ┆ null      ┆ null      ┆ null     ┆ null     │
    │ min        ┆ 0.0       ┆ 0.0       ┆ 0.0       ┆ … ┆ 1995-01-0 ┆ null      ┆ null     ┆ null     │
    │            ┆           ┆           ┆           ┆   ┆ 1         ┆           ┆          ┆          │
    │            ┆           ┆           ┆           ┆   ┆ 00:00:00  ┆           ┆          ┆          │
    │ 25%        ┆ 25.0      ┆ 40.0      ┆ 0.25      ┆ … ┆ 1995-01-0 ┆ null      ┆ null     ┆ null     │
    │            ┆           ┆           ┆           ┆   ┆ 1         ┆           ┆          ┆          │
    │            ┆           ┆           ┆           ┆   ┆ 00:00:25  ┆           ┆          ┆          │
    │ 50%        ┆ 50.0      ┆ 60.0      ┆ 0.5       ┆ … ┆ 1995-01-0 ┆ null      ┆ null     ┆ null     │
    │            ┆           ┆           ┆           ┆   ┆ 1         ┆           ┆          ┆          │
    │            ┆           ┆           ┆           ┆   ┆ 00:00:50  ┆           ┆          ┆          │
    │ 75%        ┆ 74.0      ┆ 79.0      ┆ 0.74      ┆ … ┆ 1995-01-0 ┆ null      ┆ null     ┆ null     │
    │            ┆           ┆           ┆           ┆   ┆ 1         ┆           ┆          ┆          │
    │            ┆           ┆           ┆           ┆   ┆ 00:01:14  ┆           ┆          ┆          │
    │ max        ┆ 99.0      ┆ 99.0      ┆ 0.99      ┆ … ┆ 1995-01-0 ┆ null      ┆ null     ┆ null     │
    │            ┆           ┆           ┆           ┆   ┆ 1         ┆           ┆          ┆          │
    │            ┆           ┆           ┆           ┆   ┆ 00:01:39  ┆           ┆          ┆          │
    └────────────┴───────────┴───────────┴───────────┴───┴───────────┴───────────┴──────────┴──────────┘

This package prints a summary table, mapping each input column to one
row.

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

Primarily intended for polars data frame, dfstats will try to convert
input such as pandas.DataFrame to a polars.DataFrame.
