# Utility functions
from datetime import date, datetime

import polars as pl


def _sample_df(n: int = 100) -> pl.DataFrame:
    """
    Generate a sample DataFrame with various data types.

    Args:
        n (int): Number of rows to generate. Default is 100.

    Returns:
        pl.DataFrame: A DataFrame with sample data.
    """
    import random

    assert n >= 100, "There must be >= 100 rows"

    random.seed(a=1, version=2)
    int_data = range(n)
    float_data = [i / 100 for i in range(n)]
    bool_data = [i % 2 == 0 for i in range(n)]
    str_data = random.choices(["foo", "bar", "baz", "ABC"], k=n)
    date_col = pl.date_range(
        start=date(2022, 1, 1),
        end=date(2022, 1, 1) + pl.duration(days=n - 1),
        eager=True,
    )
    date_col_2 = pl.date_range(
        start=date(1500, 1, 1),
        end=date(1500, 1, 1) + pl.duration(days=n - 1),
        eager=True,
    )
    datetime_col = pl.datetime_range(
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1) + pl.duration(seconds=n - 1),
        interval="1s",
        eager=True,
    )
    datetime_col_2 = pl.datetime_range(
        start=datetime(1995, 1, 1),
        end=datetime(1995, 1, 1) + pl.duration(seconds=n - 1),
        interval="1s",
        eager=True,
    )

    cats = ["low", "medium", "high"]
    categorical_data = random.choices(cats, k=n)
    null_data = [None] * n

    int_with_missing_data = list(int_data)
    for i in range(10, 30):
        int_with_missing_data[i] = None

    return pl.DataFrame(
        {
            "int_col": int_data,
            "int_with_missing": int_with_missing_data,
            "float_col": float_data,
            "bool_col": bool_data,
            "str_col": str_data,
            "date_col": date_col,
            "date_col_2": date_col_2,
            "datetime_col": datetime_col,
            "datetime_col_2": datetime_col_2,
            "categorical_col": pl.Series(categorical_data).cast(pl.Categorical),
            "enum_col": pl.Series(categorical_data).cast(pl.Enum(cats)),
            "null_col": pl.Series(null_data),
        }
    )


def _format_num_rows(num: int, thr: float) -> str:
    """
    Formats a number nicely, using scientific notation for large numbers.

    Args:
        num (int): The number to format.
        thr (int): The threshold above which to use scientific notation.

    Returns:
        str: The formatted number as a string.
    """
    import math

    if num < thr:
        return f"{num:,.0f}"

    exponent = int(math.floor(math.log10(abs(num))))
    coefficient = num / 10**exponent

    # Unicode superscript digits
    superscripts = "⁰¹²³⁴⁵⁶⁷⁸⁹"

    # Convert exponent to superscript
    exp_superscript = "".join(superscripts[int(d)] for d in str(abs(exponent)))
    if exponent < 0:
        exp_superscript = "⁻" + exp_superscript

    return f"{coefficient:.2f}×10{exp_superscript}"
