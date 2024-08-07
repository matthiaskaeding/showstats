# Utility functions
from typing import List, Union

import polars as pl


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


def _is_pkg_available(pkg: str) -> None:
    import importlib

    return importlib.util.find_spec(pkg) is not None


def _add_empty_strings(
    df: Union[pl.LazyFrame, pl.DataFrame], col_names: List[str]
) -> pl.LazyFrame:
    return df.with_columns([pl.lit("").alias(x) for x in col_names])
