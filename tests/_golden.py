"""Golden renderings of the printed table, captured before #37 touches it.

Generated from the current renderer, not transcribed by hand. The exact
spacing is a real contract: it is what appears verbatim in README.md, and it
comes from the ``pl.Config`` block in ``_Table.show_one_table``
(``float_precision=2``, ``set_tbl_width_chars=80``,
``tbl_cell_alignment="LEFT"``). The rewrite replaces that renderer with a
hand-written formatter, so any drift has to show up here rather than in a
README diff.

Warts are pinned along with everything else — see the notes in
``tests/test_golden_output.py``. If a slice deliberately changes one of
these, the golden changes in its own commit with the reason, never quietly
alongside the implementation.
"""

NUM = (
    "-Numerical columns (N=5)--------------------------------------------------------\n"
    " Col        NA%  Avg  Median  SD    Min    Max  \n"
    " float_col  0    3.5  3.5     1.58  1.5    5.5  \n"
    " int_col    20   2.5  2.5     1.29  1      4    \n"
    " bool_col   0    0.6  1.0     0.55  false  true \n"
)

CAT = (
    "-Categorical columns (N=10)-----------------------------------------------------\n"
    " Col        NA%  Uniques  Top 1    Top 2    Top 3   \n"
    " cat_col    0    3        A (50%)  B (30%)  C (20%) \n"
    " other_col  10   2        x (50%)  y (40%)          \n"
)

TIME = (
    "-Date and datetime columns (N=5)------------------------------------------------\n"
    " Col           NA%  Median               Min                  Max                 \n"
    " date_col      0    2020-01-03           2020-01-01           2020-01-05          \n"
    " datetime_col  0    2020-01-03 12:30:15  2020-01-01 12:30:15  2020-01-05 12:30:15 \n"
)

ALL = (
    "-Date and datetime columns (N=5)------------------------------------------------\n"
    " Col       NA%  Median      Min         Max        \n"
    " date_col  0    2020-01-03  2020-01-01  2020-01-05 \n"
    "-Numerical columns (N=5)--------------------------------------------------------\n"
    " Col        NA%  Avg  Median  SD    Min  Max \n"
    " float_col  0    3.5  3.5     1.58  1.5  5.5 \n"
    " int_col    0    3.0  3.0     1.58  1    5   \n"
    "-Categorical columns (N=5)------------------------------------------------------\n"
    " Col      NA%  Uniques  Top 1    Top 2    Top 3   \n"
    " str_col  0    3        a (40%)  b (40%)  c (20%) \n"
)

LONG_NAMES = (
    "-Numerical columns (N=3)--------------------------------------------------------\n"
    " Col                             NA%  Avg  Median  SD   Min  Max \n"
    " short                           0    2.0  2.0     1.0  1.0  3.0 \n"
    " a_very_long_column_name_that_…  0    2.0  2.0     1.0  1    3   \n"
)

SCIENTIFIC = (
    "-Numerical columns (N=4)--------------------------------------------------------\n"
    " Col      NA%  Avg     Median  SD      Min  Max    \n"
    " big_col  0    2.47E9  6173.0  4.93E9  0.0  9.87E9 \n"
)

NULL_COLUMN = (
    "-Numerical columns (N=5)--------------------------------------------------------\n"
    " Col       NA%  Avg  Median  SD    Min  Max \n"
    " int_col   0    3.0  3.0     1.58  1    5   \n"
    " null_col  100                              \n"
)

BIG_N = (
    "-Numerical columns (N=1.50E+5)--------------------------------------------------\n"
    " Col      NA%  Avg      Median   SD        Min  Max    \n"
    " int_col  0    74999.5  74999.5  43301.41  0    149999 \n"
)

QUANTILES_FOLDED = (
    "-Numerical columns (N=5)--------------------------------------------------------\n"
    " Col        NA%  Avg  SD    Q0     Q25   Q50  Q75   Q100 \n"
    " float_col  0    3.5  1.58  1.5    2.5   3.5  4.5   5.5  \n"
    " int_col    20   2.5  1.29  1      1.75  2.5  3.25  4    \n"
    " bool_col   0    0.6  0.55  false  0.0   1.0  1.0   true \n"
)

QUANTILES_UNFOLDED = (
    "-Numerical columns (N=5)--------------------------------------------------------\n"
    " Col        NA%  Avg  Median  SD    Q25   Min    Max  \n"
    " float_col  0    3.5  3.5     1.58  2.5   1.5    5.5  \n"
    " int_col    20   2.5  2.5     1.29  1.75  1      4    \n"
    " bool_col   0    0.6  1.0     0.55  0.0   false  true \n"
)

TOP_COLS = (
    "-Numerical columns (N=5)--------------------------------------------------------\n"
    " Col        NA%  Avg  Median  SD    Min  Max \n"
    " int_col    0    3.0  3.0     1.58  1    5   \n"
    " float_col  0    3.5  3.5     1.58  1.5  5.5 \n"
)

TABLE_ONE = (
    "-Table 1 (N=4)------------------------------------------------------------------\n"
    " Col                  NA%  Overall      \n"
    " age (mean (SD))      25   43.67 (9.07) \n"
    " score (mean (SD))    0    8.0 (1.08)   \n"
    " group = control (%)  0    2 (50%)      \n"
    " group = treated (%)       1 (25%)      \n"
    " group = placebo (%)       1 (25%)      \n"
)
