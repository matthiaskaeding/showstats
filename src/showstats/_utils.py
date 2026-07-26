from typing import Iterable

import polars as pl


def make_scientific(varname, thr):
    var = pl.col(varname)
    exponent = var.abs().log10().floor()
    predicate = exponent.abs().ge(thr)
    mantissa = pl.col("10").pow(exponent)
    start = var.truediv(mantissa).round(2)
    end = pl.format("{}E{}", start, exponent)
    otherwise = var.round_sig_figs(2).cast(pl.String)

    return (
        pl.when(var.eq(0))
        .then(pl.lit("0"))
        .when(var.is_infinite())
        .then(var.cast(pl.String))
        .when(predicate)
        .then(end)
        .otherwise(otherwise)
        .alias(varname)
    )


def convert_df_scientific(df: pl.LazyFrame, varnames: Iterable[str], thr: int = 4):
    """
    Converts a lazy dataframe to scientific notation.

    Args:
        varnames Iterable[str]: The names of the column to convert.
        thr (int): The threshold exponent for using scientific notation, entries
        white more decimals than 10 ^ thr are converted

    Returns:
        pl.DataFrame: new pl.DataFrame with entries converted
    """
    exprs_ex = []
    name_exponents = []
    for varname in varnames:
        nan = float("nan")
        var = pl.col(varname).fill_null(nan)  # Somewhat hacky way to deal with nulls:
        # Convert to nan, which have more methods defined. Otherwise the when - then
        # function will fail
        name_exponent = f"____EXPONENT____{varname}"
        name_exponents.append(name_exponent)
        exp_ex = (
            pl.when(var.is_finite(), var.ne(0))
            .then(var.abs().log10().floor())
            .alias(name_exponent)
        ).cast(pl.Int16)
        exprs_ex.append(exp_ex)
    df = df.with_columns(exprs_ex)

    # log10().floor() can land one off near exact powers of ten because of
    # floating-point error, which pushes the mantissa out of [1, 10) and
    # prints e.g. "10.0E5" instead of "1.0E6". Nudge the exponent back in
    # range based on the rounded mantissa it actually produces.
    exprs_correct = []
    for varname, name_exponent in zip(varnames, name_exponents):
        nan = float("nan")
        var = pl.col(varname).fill_null(nan)
        var_exponent = pl.col(name_exponent)
        mantissa = var.abs().truediv(pl.lit(10.0).pow(var_exponent)).round(2)
        corrected = (
            (
                pl.when(mantissa.ge(10))
                .then(var_exponent + 1)
                .when(mantissa.lt(1) & var.ne(0))
                .then(var_exponent - 1)
                .otherwise(var_exponent)
            )
            .cast(pl.Int16)
            .alias(name_exponent)
        )
        exprs_correct.append(corrected)
    df = df.with_columns(exprs_correct)

    exprs_scient = []
    for varname, name_exponent in zip(varnames, name_exponents):
        nan = float("nan")
        var = pl.col(varname).fill_null(nan)
        var_exponent = pl.col(name_exponent)
        exp_scient = (
            pl.when(var.is_nan())
            .then(pl.lit(""))
            .when(var.is_infinite())
            .then(var.cast(pl.String))
            .when(var.eq(0))
            .then(pl.lit("0.0"))
            .when(var_exponent.le(thr))
            .then(var.round(2).cast(pl.String))
            .otherwise(
                pl.format(
                    "{}E{}",
                    var.truediv(pl.lit(10.0).pow(var_exponent)).round(2),
                    var_exponent,
                )
            )
        ).alias(varname)
        exprs_scient.append(exp_scient)

    return df.with_columns(exprs_scient).drop(name_exponents)
