summtable
================

Polars and pandas offer a good way to summarize a dataframe `df` via the
`df.describe` method, however this returns the output in a wide (same
number of columns as `df`) format.

``` python
import polars as pl

# Polars
df.describe()
```

<div><style>
.dataframe > thead > tr,
.dataframe > tbody > tr {
  text-align: right;
  white-space: pre-wrap;
}
</style>
<small>shape: (9, 11)</small><table border="1" class="dataframe"><thead><tr><th>statistic</th><th>int_col</th><th>int_with_missing</th><th>float_col</th><th>bool_col</th><th>str_col</th><th>date_col</th><th>datetime_col</th><th>categorical_col</th><th>enum_col</th><th>null_col</th></tr><tr><td>str</td><td>f64</td><td>f64</td><td>f64</td><td>f64</td><td>str</td><td>str</td><td>str</td><td>str</td><td>str</td><td>f64</td></tr></thead><tbody><tr><td>&quot;count&quot;</td><td>100.0</td><td>80.0</td><td>100.0</td><td>100.0</td><td>&quot;100&quot;</td><td>&quot;100&quot;</td><td>&quot;100&quot;</td><td>&quot;100&quot;</td><td>&quot;100&quot;</td><td>0.0</td></tr><tr><td>&quot;null_count&quot;</td><td>0.0</td><td>20.0</td><td>0.0</td><td>0.0</td><td>&quot;0&quot;</td><td>&quot;0&quot;</td><td>&quot;0&quot;</td><td>&quot;0&quot;</td><td>&quot;0&quot;</td><td>100.0</td></tr><tr><td>&quot;mean&quot;</td><td>49.5</td><td>57.0</td><td>0.495</td><td>0.5</td><td>null</td><td>&quot;2022-02-19 12:00:00&quot;</td><td>&quot;2022-01-01 00:00:49.500000&quot;</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;std&quot;</td><td>29.011492</td><td>27.595633</td><td>0.290115</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;min&quot;</td><td>0.0</td><td>0.0</td><td>0.0</td><td>0.0</td><td>&quot;ABC&quot;</td><td>&quot;2022-01-01&quot;</td><td>&quot;2022-01-01 00:00:00&quot;</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;25%&quot;</td><td>25.0</td><td>40.0</td><td>0.25</td><td>null</td><td>null</td><td>&quot;2022-01-26&quot;</td><td>&quot;2022-01-01 00:00:25&quot;</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;50%&quot;</td><td>50.0</td><td>60.0</td><td>0.5</td><td>null</td><td>null</td><td>&quot;2022-02-20&quot;</td><td>&quot;2022-01-01 00:00:50&quot;</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;75%&quot;</td><td>74.0</td><td>79.0</td><td>0.74</td><td>null</td><td>null</td><td>&quot;2022-03-16&quot;</td><td>&quot;2022-01-01 00:01:14&quot;</td><td>null</td><td>null</td><td>null</td></tr><tr><td>&quot;max&quot;</td><td>99.0</td><td>99.0</td><td>0.99</td><td>1.0</td><td>&quot;foo&quot;</td><td>&quot;2022-04-10&quot;</td><td>&quot;2022-01-01 00:01:39&quot;</td><td>null</td><td>null</td><td>null</td></tr></tbody></table></div>

This package prints a summary table, mapping each input column to one
row.

``` python
from summtable import show_summary

show_summary(df)
```

    | Var; N = 100  | Missing      | Mean         | Median       | Std.  | Min          | Max          |
    |---------------|--------------|--------------|--------------|-------|--------------|--------------|
    | int_col       | 0 (0.0%)     | 49.5         | 49.5         | 29.01 | 0.0          | 99.0         |
    | int_with_miss | 20 (20.0%)   | 57.0         | 59.5         | 27.6  | 0.0          | 99.0         |
    | ing           |              |              |              |       |              |              |
    | float_col     | 0 (0.0%)     | 0.5          | 0.5          | 0.29  | 0.0          | 0.99         |
    | bool_col      | 0 (0.0%)     | 0.5          | 0.5          | 0.5   | 0.0          | 1.0          |
    | datetime_col  | 0 (0.0%)     | 2022-01-01   | 2022-01-01   |       | 2022-01-01   | 2022-01-01   |
    |               |              | 00:00:49     | 00:00:49     |       | 00:00:00     | 00:01:39     |
    | date_col      | 0 (0.0%)     |              |              |       | 2022-01-01   | 2022-04-10   |
    | str_col       | 0 (0.0%)     |              |              |       | ABC          | foo          |
    | enum_col      | 0 (0.0%)     |              |              |       | low          | high         |
    | categorical_c | 0 (0.0%)     |              |              |       | low          | medium       |
    | ol            |              |              |              |       |              |              |
    | null_col      | 100 (100.0%) |              |              |       |              |              |
