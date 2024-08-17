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


def convert_df_scientific(df, varnames, thr=4):
    exprs_ex = []
    exprs_scient = []
    name_exponents = []
    for varname in varnames:
        var = pl.col(varname)
        name_exponent = f"____EXPONENT____{varname}"
        name_exponents.append(name_exponent)
        exp_ex = (
            pl.when(var.is_finite().and_(var.ne(0)))
            .then(var.abs().log10().floor())
            .alias(name_exponent)
        ).cast(pl.Int16)

        var_exponent = pl.col(name_exponent)
        exp_scient = (
            pl.when(var.is_null().or_(var.is_infinite()).or_(var.is_nan()))
            .then(pl.lit(""))
            .when(var.eq(0))
            .then(pl.lit("0.0"))
            .when(var_exponent.le(pl.lit(thr)))
            .then(var.round(3).cast(pl.String))
            .otherwise(
                pl.format(
                    "{}E{}",
                    var.truediv(pl.lit(10.0).pow(var_exponent)).round(3),
                    pl.col(name_exponent),
                )
            )
        ).alias(varname)
        exprs_ex.append(exp_ex)
        exprs_scient.append(exp_scient)

    return df.with_columns(exprs_ex).with_columns(exprs_scient).drop(name_exponents)
