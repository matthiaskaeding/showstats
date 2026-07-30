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
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=5)  NA%  Avg  SD    Median  Min    Max  \n"
    " float_col  0    3.5  1.58  3.5     1.5    5.5  \n"
    " int_col    20   2.5  1.29  2.5     1      4    \n"
    " bool_col   0    0.6  0.55  1.0     false  true \n"
)

CAT = (
    "-Categorical columns------------------------------------------------------------\n"
    " Col (N=10)  NA%  Uniques  Top 1    Top 2    Top 3   \n"
    " cat_col     0    3        A (50%)  B (30%)  C (20%) \n"
    " other_col   10   3        x (50%)  y (40%)          \n"
)

TIME = (
    "-Date and datetime columns------------------------------------------------------\n"
    " Col (N=5)     NA%  Median               Min                  Max                 \n"
    " date_col      0    2020-01-03           2020-01-01           2020-01-05          \n"
    " datetime_col  0    2020-01-03 12:30:15  2020-01-01 12:30:15  2020-01-05 12:30:15 \n"
)

ALL = (
    "-Date and datetime columns------------------------------------------------------\n"
    " Col (N=5)  NA%  Median      Min         Max        \n"
    " date_col   0    2020-01-03  2020-01-01  2020-01-05 \n"
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=5)  NA%  Avg  SD    Median  Min  Max \n"
    " float_col  0    3.5  1.58  3.5     1.5  5.5 \n"
    " int_col    0    3.0  1.58  3.0     1    5   \n"
    "-Categorical columns------------------------------------------------------------\n"
    " Col (N=5)  NA%  Uniques  Top 1    Top 2    Top 3   \n"
    " str_col    0    3        a (40%)  b (40%)  c (20%) \n"
)

LONG_NAMES = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=3)                       NA%  Avg  SD   Median  Min  Max \n"
    " short                           0    2.0  1.0  2.0     1.0  3.0 \n"
    " a_very_long_column_name_that_…  0    2.0  1.0  2.0     1    3   \n"
)

SCIENTIFIC = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=4)  NA%  Avg     SD      Median  Min  Max    \n"
    " big_col    0    2.47E9  4.93E9  6173.0  0.0  9.87E9 \n"
)

NULL_COLUMN = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=5)  NA%  Avg  SD    Median  Min  Max \n"
    " int_col    0    3.0  1.58  3.0     1    5   \n"
    " null_col   100                              \n"
)

BIG_N = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=1.50E+5)  NA%  Avg      SD        Median   Min  Max    \n"
    " int_col          0    74999.5  43301.41  74999.5  0    149999 \n"
)

QUANTILES_FOLDED = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=5)  NA%  Avg  SD    Q0     Q25   Q50  Q75   Q100 \n"
    " float_col  0    3.5  1.58  1.5    2.5   3.5  4.5   5.5  \n"
    " int_col    20   2.5  1.29  1      1.75  2.5  3.25  4    \n"
    " bool_col   0    0.6  0.55  false  0.0   1.0  1.0   true \n"
)

QUANTILES_UNFOLDED = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=5)  NA%  Avg  SD    Median  Q25   Min    Max  \n"
    " float_col  0    3.5  1.58  3.5     2.5   1.5    5.5  \n"
    " int_col    20   2.5  1.29  2.5     1.75  1      4    \n"
    " bool_col   0    0.6  0.55  1.0     0.0   false  true \n"
)

TOP_COLS = (
    "-Numerical columns--------------------------------------------------------------\n"
    " Col (N=5)  NA%  Avg  SD    Median  Min  Max \n"
    " int_col    0    3.0  1.58  3.0     1    5   \n"
    " float_col  0    3.5  1.58  3.5     1.5  5.5 \n"
)
