# showstats: quick and compact summary statistics


**showstats** quickly produces compact summary statistic tables with
vertical orientation.

``` python
from showstats import show_stats

show_stats(df)
```

    -Date and datetime columns------------------------------------------------------
     Var. N=100      NA%  Min         Max                     Median                
     date_col        0    1501-01-20  1996-04-09              1755-07-20 00:00:00   
     date_col_2      0    1511-12-06  1999-05-05              1776-03-03 00:00:00   
     datetime_col    0    1501-01-20  1996-04-09 06:29:29     1755-07-20 10:10:59   
                          14:37:46                                                  
     datetime_col_2  0    1511-12-06  1999-05-05 14:12:20     1776-03-03 13:25:50   
                          23:40:13                                                  
    -Numerical columns--------------------------------------------------------------
     Var. N=100         NA%  Avg      SD     Min      Max      Median  
     float_mean_2       0    2.0      0.89   -0.36    4.12     2.0     
     float_std_2        0    0.14     2.0    -5.17    4.91     0.14    
     float_min_-7       0    -4.64    0.89   -7.0     -2.51    -4.63   
     float_max_17       0    14.88    0.89   12.51    17.0     14.88   
     float_big          0    1.235E6  0.89   1.235E6  1.235E6  1.235E6 
     float_col          0    0.5      0.29   0.0      0.99     0.5     
     U                  0    0.54     0.26   0.02     0.98     0.57    
     int_col            0    49.5     29.01  0        99       49.5    
     int_with_missings  5    48.32    28.8   0        99       49.0    
     bool_col           26   0.5      0.5    false    true     0.5     
     null_col           100                                            
    -Categorical columns------------------------------------------------------------
     Var. N=100       NA%  Uniques  Top 1       Top 2        Top 3        
     str_col          48   5        foo (15%)   ABC (13%)    bar (12%)    
     categorical_col  0    2        Fara (57%)  Car (43%)                 
     enum_col         0    3        best (36%)  worst (35%)  medium (29%) 

``` python
# Only one type
show_stats(df, "cat")  # Other are num, time
```

    -Categorical columns------------------------------------------------------------
     Var. N=100       NA%  Uniques  Top 1       Top 2        Top 3        
     str_col          48   5        foo (15%)   ABC (13%)    bar (12%)    
     categorical_col  0    2        Fara (57%)  Car (43%)                 
     enum_col         0    3        best (36%)  worst (35%)  medium (29%) 

``` python
# Importing showstats adds the stats_tbl namespace
df.select("U", "int_col").stats_tbl.show()
```

    -Numerical columns--------------------------------------------------------------
     Var. N=100  NA%  Avg   SD     Min   Max   Median 
     U           0    0.54  0.26   0.02  0.98  0.57   
     int_col     0    49.5  29.01  0     99    49.5   

- Primarily built for polars data frames, **showstats** converts other
  inputs.

  - For full compatibility with pandas.DataFrames install as
    `pip install showstats[pandas]`.

- Heavily inspired by the great R-packages
  [skimr](https://github.com/ropensci/skimr) and
  [modelsummary](https://modelsummary.com/vignettes/datasummary.html).

- Numbers with many digits are automatically converted to scientific
  notation.

- Because **showstats** uses polars as backend, its really fast: \<1
  second for a 1,000,000 × 1,000 data frame, running on a M1 MacBook.
