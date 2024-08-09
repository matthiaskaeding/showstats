# Functions that build the tables
from collections import defaultdict
from typing import TYPE_CHECKING, Dict, List, Union

import polars as pl


if TYPE_CHECKING:
    import pandas


def _set_expressions(
    df: pl.DataFrame,
    var_type: str,
    expressions: List[pl.Expr],
    varnames: Dict[str, List],
    sep: str,
) -> bool:
    """
    In place appends to a list of expressions
    Each expression is a function applied to one column in the data frame df

    varnames: maps varnames to var_types
    """
    from polars import selectors as cs

    if var_type == "num":
        vars = df.select(
            pl.col(
                pl.Decimal,
                pl.Float32,
                pl.Float64,
                pl.Int16,
                pl.Int32,
                pl.Int64,
                pl.Int8,
                pl.UInt16,
                pl.UInt32,
                pl.UInt64,
                pl.UInt8,
                pl.Boolean,
            )
        ).columns
        if len(vars) == 0:
            return
        functions = ["null_count", "min", "max", "mean", "median", "std"]

    elif var_type == "cat":
        vars = df.select(
            pl.col(pl.String), pl.col(pl.Enum), pl.col(pl.Categorical)
        ).columns
        if len(vars) == 0:
            return False
        functions = ["null_count", "min", "max"]

    elif var_type == "datetime":
        vars = df.select(cs.datetime()).columns
        if len(vars) == 0:
            return False
        functions = ["null_count", "min", "max", "mean", "median"]

    elif var_type == "date":
        vars = df.select(cs.date()).columns
        if len(vars) == 0:
            return False
        functions = ["null_count", "min", "max"]

    elif var_type == "null":
        vars = df.select(pl.col(pl.Null)).columns
        if len(vars) == 0:
            return False
        functions = ["null_count"]

    elif var_type == "cat_special":
        vars = df.select(
            pl.col(pl.String), pl.col(pl.Enum), pl.col(pl.Categorical)
        ).columns
        if len(vars) == 0:
            return False
        functions = ["null_count", "min", "max", "" "5th_most_frequent"]

    if var_type != "cat_special":
        for var in vars:
            for function in functions:
                varname = f"{function}{sep}{var}"
                varnames[var_type].append(varname)
                expr = getattr(pl.col(var), function)().alias(varname)
                expressions.append(expr)
    else:
        for var in vars:
            for function in functions:
                varname = f"{function}{sep}{var}"
                varnames[var_type].append(varname)
                if function != "5th_most_frequent":
                    expr = getattr(pl.col(var), function)().alias(varname)
                else:
                    expr = (
                        pl.col(var).drop_nulls().value_counts(sort=True).head(5).list()
                    )
                expressions.append(expr)

    return True


def _make_var_type_df(
    stats: dict, sep: str, varnames_vt: List[str], var_type: Union[str, None] = None
) -> pl.DataFrame:
    """Make a dataframe containing stats for a type of variable

    stats (pl.DataFrame): 1 row polars.DataFrame
    varnames_vt: list of varnames corresponding to one var_type
    var_type: Union[str, None] only set for special cases
    """

    fun, last_var = varnames_vt[0].split(sep, 1)
    row = {"Variable": last_var, fun: stats[varnames_vt[0]]}
    rows = []
    n = len(varnames_vt)

    if var_type == "cat_special":
        for i in range(1, n):
            varname = varnames_vt[i]
            function_name, var = varname.split(sep, 1)
            function_value = stats[varname]
            if var == last_var:  # Simple update
                row[function_name] = function_value
            else:  # Switch: store current row, make new one
                rows.append(row)
                last_var = var
                row = {"Variable": var, function_name: function_value}
        else:  # Edge case of 1-column df & last column in general
            rows.append(row)

    for i in range(1, n):
        varname = varnames_vt[i]
        function_name, var = varname.split(sep, 1)
        function_value = stats[varname]
        if var == last_var:  # Simple update
            row[function_name] = function_value
        else:  # Switch: store current row, make new one
            rows.append(row)
            last_var = var
            row = {"Variable": var, function_name: function_value}
    else:  # Edge case of 1-column df & last column in general
        rows.append(row)

    return pl.DataFrame(rows)


def _make_tables(
    df: Union[pl.DataFrame, "pandas.DataFrame"],
) -> Dict[str, pl.DataFrame]:
    """
    Makes a dictionary of dataframes, where each dataframes contains summary stats

    Args:
        df (pl.DataFrame): The input DataFrame. If not a polars.DataFrame, will try
        to cast

    Returns:
        Dict[str, pl.DataFrame]: A dictionary of summary statistics DataFrames for each data type.
    """
    # Fill expressions
    expressions = []
    varnames = defaultdict(list)
    sep = "____"
    var_types = ["num", "cat", "datetime", "date", "null"]
    for var_type in var_types:
        _set_expressions(df, var_type, expressions, varnames, sep)

    # Evaluate expressions
    stats = df.select(expressions).row(
        0, named=True
    )  # Dictionary of summary statistics
    dfs = dict()
    for var_type in varnames:
        dfs[var_type] = _make_var_type_df(stats, sep, varnames[var_type])

    return dfs
