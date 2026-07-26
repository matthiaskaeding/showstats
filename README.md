# showstats: quick and compact summary statistics


**showstats** quickly produces compact summary statistic tables with
vertical orientation.

``` python
import polars as pl
from showstats import show_stats

# Daily weather in Seattle, 2012-2015
df = pl.read_csv("docs/data/seattle-weather.csv", try_parse_dates=True)

show_stats(df)
```

    -Date and datetime columns------------------------------------------------------
     Col (N=1461)  NA%  Median      Min         Max        
     date          0    2013-12-31  2012-01-01  2015-12-31 
    -Numerical columns--------------------------------------------------------------
     Col (N=1461)   NA%  Avg    SD    Median  Min   Max  
     precipitation  0    3.03   6.68  0.0     0.0   55.9 
     temp_max       0    16.44  7.35  15.6    -1.6  35.6 
     temp_min       0    8.23   5.02  8.3     -7.1  18.3 
     wind           0    3.24   1.44  3.0     0.4   9.5  
    -Categorical columns------------------------------------------------------------
     Col (N=1461)  NA%  Uniques  Top 1       Top 2      Top 3    
     weather       0    5        rain (44%)  sun (44%)  fog (7%) 

``` python
# Only one type
show_stats(df, "cat")  # Other are num, time
```

    -Categorical columns------------------------------------------------------------
     Col (N=1461)  NA%  Uniques  Top 1       Top 2      Top 3    
     weather       0    5        rain (44%)  sun (44%)  fog (7%) 

``` python
# Any subset of columns works
show_stats(df.select("temp_max", "wind"))
```

    -Numerical columns--------------------------------------------------------------
     Col (N=1461)  NA%  Avg    SD    Median  Min   Max  
     temp_max      0    16.44  7.35  15.6    -1.6  35.6 
     wind          0    3.24   1.44  3.0     0.4   9.5  

``` python
# Add extra quantiles for numerical columns
show_stats(df.select("temp_max", "wind"), "num", quantiles=[0.1, 0.9])
```

    -Numerical columns--------------------------------------------------------------
     Col (N=1461)  NA%  Avg    SD    …  Q0    Q10  Q90   Q100 
     temp_max      0    16.44  7.35  …  -1.6  7.2  26.7  35.6 
     wind          0    3.24   1.44  …  0.4   1.7  5.2   9.5  

``` python
# pandas, pyarrow and other narwhals-supported frames work the same way
import pandas as pd

show_stats(pd.read_csv("docs/data/seattle-weather.csv")[["temp_max", "wind"]])
```

    -Numerical columns--------------------------------------------------------------
     Col (N=1461)  NA%  Avg    SD    Median  Min   Max  
     temp_max      0    16.44  7.35  15.6    -1.6  35.6 
     wind          0    3.24   1.44  3.0     0.4   9.5  

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
