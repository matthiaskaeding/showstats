# showstats: quick and compact summary statistics


**showstats** quickly produces compact summary statistic tables with
vertical orientation.

``` python
import polars as pl
from showstats import show_stats, table_one

# Daily weather in Seattle, 2012-2015
df = pl.read_csv("docs/data/seattle-weather.csv", try_parse_dates=True)

show_stats(df)
```

    -Date and datetime columns (N=1461)---------------------------------------------
     Col   NA%  Median      Min         Max        
     date  0    2013-12-31  2012-01-01  2015-12-31 
    -Numerical columns (N=1461)-----------------------------------------------------
     Col            NA%  Avg    Median  SD    Min   Max  
     precipitation  0    3.03   0.0     6.68  0.0   55.9 
     temp_max       0    16.44  15.6    7.35  -1.6  35.6 
     temp_min       0    8.23   8.3     5.02  -7.1  18.3 
     wind           0    3.24   3.0     1.44  0.4   9.5  
    -Categorical columns (N=1461)---------------------------------------------------
     Col      NA%  Uniques  Top 1       Top 2      Top 3    
     weather  0    5        rain (44%)  sun (44%)  fog (7%) 

``` python
# Only one type
show_stats(df, "cat")  # Other are num, time
```

    -Categorical columns (N=1461)---------------------------------------------------
     Col      NA%  Uniques  Top 1       Top 2      Top 3    
     weather  0    5        rain (44%)  sun (44%)  fog (7%) 

``` python
# Any subset of columns works
show_stats(df.select("temp_max", "wind"))
```

    -Numerical columns (N=1461)-----------------------------------------------------
     Col       NA%  Avg    Median  SD    Min   Max  
     temp_max  0    16.44  15.6    7.35  -1.6  35.6 
     wind      0    3.24   3.0     1.44  0.4   9.5  

``` python
# Add extra quantiles for numerical columns
show_stats(df.select("temp_max", "wind"), "num", quantiles=[0.1, 0.9])
```

    -Numerical columns (N=1461)-----------------------------------------------------
     Col       NA%  Avg    Median  SD    Q0    Q10  Q90   Q100 
     temp_max  0    16.44  15.6    7.35  -1.6  7.2  26.7  35.6 
     wind      0    3.24   3.0     1.44  0.4   1.7  5.2   9.5  

Use the standalone `table_one` function for a numerical and categorical
Table 1 summary. Set `style` to `mean_sd`, `median_mad`, or
`median_iqr`. Use `group` to report the statistics in separate columns
for each value of another variable. The grouping variable is not
included as a row.

``` python
table_one_df = pl.DataFrame(
    {
        "age": [34.0, 45.0, None, 52.0],
        "score": [7.5, 8.0, 9.5, 7.0],
        "outcome": ["yes", "no", "yes", "yes"],
        "arm": ["control", "control", "treated", "treated"],
    }
)

table_one(table_one_df, style="mean_sd", group="arm")
```

    -Table 1 (N=4)------------------------------------------------------------------
     Col                NA%  Overall       arm = control (N=2)  arm = treated (N=2) 
     age (mean (SD))    25   43.67 (9.07)  39.5 (7.78)          52.0                
     score (mean (SD))  0    8.0 (1.08)    7.75 (0.35)          8.25 (1.77)         
     outcome = yes (%)  0    3 (75%)       1 (50%)              2 (100%)            
     outcome = no (%)        1 (25%)       1 (50%)              0 (0%)              

The `NA%` column gives the percentage of missing values. Set
`show_missing=False` to omit it. Categorical variables show their three
most common values by default. Use `n_categories` to change that number.
Set `fmt="gt"` to return a Great Tables object.

``` python
# pandas, pyarrow and other narwhals-supported frames work the same way
import pandas as pd

show_stats(pd.read_csv("docs/data/seattle-weather.csv")[["temp_max", "wind"]])
```

    -Numerical columns (N=1461)-----------------------------------------------------
     Col       NA%  Avg    Median  SD    Min   Max  
     temp_max  0    16.44  15.6    7.35  -1.6  35.6 
     wind      0    3.24   3.0     1.44  0.4   9.5  

## Command line

Install the command with `uv tool install "showstats[cli]"`. In a
project, use `uv add "showstats[cli]"` and run it with
`uv run showstats`.

Pass a CSV or Parquet file to print the usual summary table:

``` sh
showstats file.csv
showstats file.parquet
```

Use `--type cat` for categorical columns or `--type num` for numerical
columns:

``` sh
showstats file.csv --type cat
showstats file.parquet --type num
```

The default is `--type all`. The `--type` option applies to summary
tables and cannot be combined with head, tail, or offset previews.

Use `-h` or `--head` to preview the first three rows, or `-t` or
`--tail` to preview the last three rows. You can also specify the number
of rows:

``` sh
showstats file.csv -h
showstats file.parquet -h 10
showstats file.csv -t
showstats file.parquet -t 10
```

Row numbers start at 0 and refer to positions in the data, excluding the
CSV header. Use `--offset` to start at a specific row. An offset alone
previews three rows, and `-h N` changes the count:

``` sh
showstats file.csv --offset 4       # Rows 4, 5, and 6
showstats file.csv --offset 4 -h 2  # Rows 4 and 5
showstats file.csv --offset -3      # Last three rows
```

Negative offsets count from the end. Offsets before the beginning start
at row 0, and offsets past the end show an empty preview. `--offset`
cannot be combined with `--tail`.

The default layout is `auto`. Previews use a normal table when it fits
the terminal width, or an expanded view with one record per block when
it does not. Use `--layout` to control the preview:

``` sh
showstats file.csv -h --layout auto
showstats file.csv -h --layout table
showstats file.csv -h --layout expanded
```

Expanded output keeps each field name beside its value, with long values
wrapped onto continuation lines. Full names and values are retained. The
`table` layout always prints a normal table, even if it exceeds the
terminal width. Missing values appear as blank cells in both layouts.
`--layout` requires `--head`, `--tail`, or `--offset`.

For example, preview one weather record in the expanded layout:

``` sh
showstats docs/data/seattle-weather.csv --offset 4 -h 1 --layout expanded
```

    Rows (1 of 1461 rows)

    Row 4
    date           2012-01-05
    precipitation  1.3
    temp_max       8.9
    temp_min       2.8
    wind           6.1
    weather        rain

Use `showstats --help` for help, because `-h` means head. You can also
run `uv run python -m showstats` with the same arguments.

The CLI reads the file into memory. It uses Polars, PyArrow, or pandas,
in that order, depending on which is installed. The `cli` extra includes
PyArrow for reading both file formats. If you already have a suitable
dataframe library, you can use the CLI without the extra. CSV column
types are inferred by that library, while Parquet files carry their own
schema.

## Optional Great Tables output

Install the optional output package with `uv add "showstats[gt]"`. The
`gt` output returns a Great Tables object that displays as HTML in a
notebook. Set `color_missing=True` to add an optional white to dark gray
background scale.

``` python
table = show_stats(df, "num", fmt="gt")
table
```

![Great Tables output](docs/images/gt-output.png)

- **showstats** works with any data frame
  [narwhals](https://github.com/narwhals-dev/narwhals) supports —
  polars, pandas, pyarrow and more. They all work directly, with no
  extra installs or conversion step.

- Heavily inspired by the great R-packages
  [skimr](https://github.com/ropensci/skimr) and
  [modelsummary](https://modelsummary.com/vignettes/datasummary.html).

- Numbers with many digits are automatically converted to scientific
  notation.

- The example above uses [Seattle daily
  weather](https://github.com/vega/vega-datasets/blob/main/data/seattle-weather.csv)
  from vega-datasets (BSD-3-Clause).

- Fast: under a second to summarise a 1,000,000 × 1,000 data frame on an
  M1 MacBook.
