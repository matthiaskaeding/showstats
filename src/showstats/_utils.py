from __future__ import annotations

import math
from typing import Iterable

import narwhals as nw

NAN = float("nan")

TABLE_WIDTH = 80


def _cell_text(value) -> str:
    """One cell as it should print.

    Missing shows as blank rather than "None" or "nan" — which of those a
    null turns into depends on the backend, and neither belongs in a
    summary table.
    """
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    return str(value)


def render_table(frame: nw.DataFrame) -> str:
    """Render a frame as the fixed-width, left-aligned table showstats prints.

    This replaces printing through `pl.Config`, which is what tied the
    output to polars. The layout it produces is the one the goldens in
    tests/test_golden_output.py captured from that renderer: a leading
    space, cells padded to the widest entry in their column, two spaces
    between columns, one trailing space.

    Two of polars' behaviours are deliberately not reproduced, because
    both lose information:

    - polars prints at most 8 columns and replaces the rest with "…",
      whatever the width. Nine columns is `quantiles=[0.25, 0.5, 0.75]`,
      so asking for a few quantiles silently dropped the Q0 column.
    - polars wraps a cell wider than the table onto a second, unaligned
      line. Every column is shown in full here instead; a table wider
      than 80 characters is wider than 80 characters.
    """
    columns = frame.columns
    rows = [[_cell_text(value) for value in row] for row in frame.rows()]
    widths = [
        max(len(name), *(len(row[i]) for row in rows)) if rows else len(name)
        for i, name in enumerate(columns)
    ]

    def line(cells) -> str:
        padded = "  ".join(cell.ljust(width) for cell, width in zip(cells, widths))
        return f" {padded} "

    return "\n".join([line(columns), *(line(row) for row in rows)]) + "\n"


def _as_string(expr: nw.Expr) -> nw.Expr:
    """String form of a numeric expression.

    `pl.format` was doing this before the port; casting reaches the same
    rendering and is the one thing narwhals offers on every backend.
    """
    return expr.cast(nw.String)


def _floor(expr: nw.Expr) -> nw.Expr:
    """Floor, as floor division.

    `Expr.floor` only arrived in narwhals 2.20, which needs Python 3.9+ —
    and this package promises 3.8. `// 1` floors identically on polars,
    pandas and pyarrow, negatives included.
    """
    return expr // 1


def _float_as_string(expr: nw.Expr) -> nw.Expr:
    """String form of a float, with the trailing ".0" kept.

    Arrow's double-to-string cast drops it — 3.0 becomes "3" where polars
    and pandas give "3.0" — so without this the printed table would depend
    on which backend happened to hold the number. polars is the reference,
    because README.md is generated from it.

    Only meaningful for finite values. Infinities and NaN reach a "no
    decimal point" verdict here too, but every caller resolves those in an
    earlier branch, so the "inf.0" this would produce is never selected.
    """
    text = expr.cast(nw.String)
    return _branch(
        (text.str.contains(".", literal=True), text),
        otherwise=nw.concat_str([text, nw.lit(".0")]),
    )


def _ceil(expr: nw.Expr) -> nw.Expr:
    """Ceiling, as `-floor(-x)`, for the same version reason as `_floor`.

    Written with `* -1` rather than unary `-`: narwhals only gave `Expr` a
    `__neg__` recently, and on Python 3.9 the newest resolvable narwhals
    does not have it (#78).
    """
    return ((expr * -1) // 1) * -1


def _branch(*cases, otherwise: nw.Expr) -> nw.Expr:
    """A when/then/otherwise chain, written as nesting.

    polars lets `.when()` be chained onto a `Then`; narwhals only grew that
    in 2.x, so the same shape is expressed by nesting each remaining case
    inside the previous `otherwise`.
    """
    expr = otherwise
    for condition, value in reversed(cases):
        expr = nw.when(condition).then(value).otherwise(expr)
    return expr


def convert_df_scientific(df, varnames: Iterable[str], thr: int = 4):
    """
    Converts the given columns of a dataframe to scientific notation.

    Backend-agnostic: whatever kind of frame goes in comes back out — a
    polars LazyFrame stays a polars LazyFrame, a pandas DataFrame stays a
    pandas DataFrame, a narwhals frame stays a narwhals frame.

    Args:
        df: any narwhals-compatible frame, eager or lazy.
        varnames Iterable[str]: The names of the column to convert.
        thr (int): The threshold exponent for using scientific notation, entries
        white more decimals than 10 ^ thr are converted

    Returns:
        A frame of the same kind as `df`, with the named columns replaced by
        their string renderings.
    """
    varnames = list(varnames)
    already_narwhals = isinstance(df, (nw.DataFrame, nw.LazyFrame))
    frame = df if already_narwhals else nw.from_native(df)

    exprs_ex = []
    name_exponents = []
    for varname in varnames:
        var = nw.col(varname).fill_null(NAN)  # Somewhat hacky way to deal with nulls:
        # Convert to nan, which have more methods defined. Otherwise the when - then
        # function will fail
        name_exponent = f"____EXPONENT____{varname}"
        name_exponents.append(name_exponent)
        # Zero, the infinities and NaN have no meaningful exponent, and all
        # of them are resolved by an earlier branch when the string is
        # built — so any placeholder does. It has to be a number rather
        # than null, though: pandas' int16 cannot hold NA, and leaving the
        # branch open raised IntCastingNaNError on the first frame that
        # contained a zero.
        # 1.0 rather than the value itself for those rows: pandas evaluates
        # both arms of a when/then, so feeding log() a zero printed
        # "RuntimeWarning: divide by zero encountered in log" out of an
        # ordinary show_stats call. log(1) is 0, which is the placeholder
        # wanted anyway.
        has_exponent = var.is_finite() & (var != 0)
        magnitude = _branch((has_exponent, var.abs()), otherwise=nw.lit(1.0))
        exp_ex = (
            _branch(
                (has_exponent, _floor(magnitude.log(base=10))),
                otherwise=nw.lit(0.0),
            )
            .cast(nw.Int16)
            .alias(name_exponent)
        )
        exprs_ex.append(exp_ex)
    frame = frame.with_columns(exprs_ex)

    # log10().floor() can land one off near exact powers of ten because of
    # floating-point error, which pushes the mantissa out of [1, 10) and
    # prints e.g. "10.0E5" instead of "1.0E6". Nudge the exponent back in
    # range based on the rounded mantissa it actually produces.
    exprs_correct = []
    for varname, name_exponent in zip(varnames, name_exponents):
        var = nw.col(varname).fill_null(NAN)
        var_exponent = nw.col(name_exponent)
        mantissa = (var.abs() / (nw.lit(10.0) ** var_exponent)).round(2)
        corrected = (
            _branch(
                (mantissa >= 10, var_exponent + 1),
                ((mantissa < 1) & (var != 0), var_exponent - 1),
                otherwise=var_exponent,
            )
            .cast(nw.Int16)
            .alias(name_exponent)
        )
        exprs_correct.append(corrected)
    frame = frame.with_columns(exprs_correct)

    exprs_scient = []
    for varname, name_exponent in zip(varnames, name_exponents):
        var = nw.col(varname).fill_null(NAN)
        var_exponent = nw.col(name_exponent)
        exp_scient = _branch(
            (var.is_nan(), nw.lit("")),
            (~var.is_finite(), _as_string(var)),
            (var == 0, nw.lit("0.0")),
            (var_exponent <= thr, _float_as_string(var.round(2))),
            # concat_str rather than `a + "E" + b`: pyarrow has no `add`
            # kernel for two strings, so the operator form raised
            # ArrowNotImplementedError there.
            otherwise=nw.concat_str(
                [
                    _float_as_string((var / (nw.lit(10.0) ** var_exponent)).round(2)),
                    nw.lit("E"),
                    _as_string(var_exponent),
                ]
            ),
        ).alias(varname)
        exprs_scient.append(exp_scient)

    frame = frame.with_columns(exprs_scient).drop(name_exponents)
    return frame if already_narwhals else frame.to_native()
