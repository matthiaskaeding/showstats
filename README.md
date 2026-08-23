# showstats: quick and compact summary statistics


**showstats** quickly produces compact summary statistic tables with
vertical orientation.

``` python
import duckdb
import pandas as pd
import polars as pl

from showstats import show_stats, table_one
```

``` python
# Daily weather in Seattle, 2012-2015
weather_path = "docs/data/seattle-weather.csv"
df = pl.read_csv(weather_path, try_parse_dates=True)

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
# Select a table type, columns, or extra quantiles.
show_stats(df, "cat")  # Other options are num and time.
show_stats(df.select("temp_max", "wind"), "num", quantiles=[0.1, 0.9])
```

Use `table_one` for a numerical and categorical Table 1 summary. The
`style` can be `mean_sd`, `median_mad`, or `median_iqr`. Use `group` to
report the statistics for each value of another variable.

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
`show_missing=False` to omit it. Use `n_categories` to change the number
of categorical values shown.

``` python
# Polars detects dates. pandas needs the date column name.
pandas_df = pd.read_csv(weather_path, parse_dates=["date"])
show_stats(pandas_df[["date", "temp_max", "wind"]])

# DuckDB relations are lazy.
duckdb_df = duckdb.read_csv(weather_path).select("date, temp_max, wind")
show_stats(duckdb_df)
```

## Optional Great Tables output

Install the optional package with `uv add "showstats[gt]"`. Set
`fmt="gt"` in `show_stats` or `table_one` to return a Great Tables
object. Set `color_missing=True` to add a white to dark gray background
scale.

``` python
table = show_stats(df, "num", fmt="gt")
table
```

![Great Tables output](docs/images/gt-output.png)

- **showstats** works with data frames supported by
  [narwhals](https://github.com/narwhals-dev/narwhals), including
  polars, pandas, and lazy DuckDB relations.

- Inspired by the R packages [skimr](https://github.com/ropensci/skimr)
  and
  [modelsummary](https://modelsummary.com/vignettes/datasummary.html).

- Numbers with many digits are automatically converted to scientific
  notation.

- The example above uses [Seattle daily
  weather](https://github.com/vega/vega-datasets/blob/main/data/seattle-weather.csv)
  from vega-datasets (BSD-3-Clause).

- Fast: under a second to summarise a 1,000,000 × 1,000 data frame on an
  M1 MacBook.
