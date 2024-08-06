# Utility functions


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
