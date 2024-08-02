# %%

import polars as pl


def _format_num_rows(num, thr):
    """
    Formats nicely a number
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


def _sample_df(n=100):
    # Generate sample data
    import random
    from datetime import date, datetime

    random.seed(a=1, version=2)
    int_data = range(n)
    float_data = [i / 100 for i in range(n)]
    bool_data = [i % 2 == 0 for i in range(n)]
    str_data = random.choices(["foo", "bar", "baz", "ABC"], k=n)
    date_data = pl.date_range(
        start=date(2022, 1, 1),
        end=date(2022, 1, 1) + pl.duration(days=n - 1),
        eager=True,
    )
    datetime_data = pl.datetime_range(
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1) + pl.duration(seconds=n - 1),
        interval="1s",
        eager=True,
    )
    cats = ["low", "medium", "high"]
    categorical_data = random.choices(cats, k=n)
    null_data = [None] * n

    return pl.DataFrame(
        {
            "int_col": int_data,
            "float_col": float_data,
            "bool_col": bool_data,
            "str_col": str_data,
            "date_col": pl.Series(date_data).cast(pl.Date),
            "datetime_col": pl.Series(datetime_data).cast(pl.Datetime),
            "categorical_col": pl.Series(categorical_data).cast(pl.Categorical),
            "enum_col": pl.Series(categorical_data).cast(pl.Enum(cats)),
            "null_col": pl.Series(null_data),
        }
    )


def _make_tables(df):
    """
    Calculate summary statistics for a DataFrame.
    Args:
        df (DataFrame): The input DataFrame.
        which (str, optional): The type of summary statistics to calculate.
            Possible values are "numeric" (default) and "character".
        nums_file (str, optional): File path to save the summary statistics for numeric variables.
        chars_file (str, optional): File path to save the summary statistics for character variables.
    Returns:
        DataFrame: The summary statistics DataFrame.
    """

    # All functions
    functions = {}
    functions_all = ["null_count", "min", "max"]

    # Map vars to functions
    vars = {}
    cols_num = df.select(
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
    if len(cols_num) > 0:
        vars["num"] = cols_num
        functions["num"] = functions_all + ["mean", "median", "std"]

    vars_cat = df.select(
        pl.col(pl.String), pl.col(pl.Enum), pl.col(pl.Categorical)
    ).columns
    if len(vars_cat) > 0:
        vars["cat"] = vars_cat
        functions["cat"] = functions_all + ["n_unique"]

    vars_datetime = df.select(pl.col(pl.Datetime)).columns
    if len(vars_datetime) > 0:
        vars["datetime"] = vars_datetime
        functions["datetime"] = functions_all + ["mean", "median"]

    vars_date = df.select(pl.col(pl.Date)).columns
    if len(vars_date) > 0:
        vars["date"] = vars_date
        functions["date"] = functions_all

    vars_null = df.select(pl.col(pl.Null)).columns
    if len(vars_null) > 0:
        vars["null"] = vars_null
        functions["null"] = ["null_count"]

    exprs = []
    for var_type in vars:
        functions_var_type = functions[var_type]
        vars_var_type = vars[var_type]
        for var in vars_var_type:
            for fun in functions_var_type:
                varname = f"{fun}_{var}"
                expr = getattr(pl.col(var), fun)().alias(varname)
                exprs.append(expr)

    # Compute summary statistics in one go, leveraging Polars' query planner
    stats = df.select(exprs)

    # Make split summary tables
    dfs = {}
    for var_type in vars:
        functions_var_type = functions[var_type]
        vars_var_type = vars[var_type]

        rows = []
        for var in vars_var_type:
            row = {"Variable": var}
            for fun in functions_var_type:
                vn = f"{fun}_{var}"
                row[fun] = stats.item(0, vn)
            rows.append(row)
        dfs[var_type] = pl.DataFrame(rows)

    return dfs


def _make_summary_table(df):
    dfs = _make_tables(df)
    varnames = [
        "Variable",
        "null_count",
        "mean",
        "median",
        "std",
        "min",
        "max",
    ]
    var_types = [x for x in ["num", "datetime", "date", "cat", "null"] if x in dfs]

    # Order
    for var_type in var_types:
        exprs = []
        for col_name in varnames:
            if col_name not in dfs[var_type].columns:
                exprs.append(pl.lit("").alias(col_name))
        dfs[var_type] = (
            dfs[var_type]
            .with_columns(pl.selectors.numeric().round(2))
            .with_columns(pl.col("*").cast(pl.String))
            .with_columns(*exprs)
            .select(varnames)
        )

    thr = 100_000
    if df.height < thr:
        name_var = f"Var; N = {_format_num_rows(df.height, thr)}"
    else:
        name_var = f"Var; N \u2248 {_format_num_rows(df.height, thr)}"
    return pl.concat([dfs[key] for key in var_types]).rename(
        {
            "Variable": name_var,
            "null_count": "Missings",
            "mean": "Mean",
            "median": "Median",
            "std": "Std.",
            "min": "Min",
            "max": "Max",
        }
    )


def print_summary(df):
    from polars import Config

    Config.set_tbl_hide_dataframe_shape(True).set_tbl_formatting(
        "ASCII_MARKDOWN"
    ).set_tbl_hide_column_data_types(True).set_float_precision(2)

    print(_make_summary_table(df))


if __name__ == "__main__":
    df = _sample_df(115_000)
    print_summary(df)
    # print(_format_num_rows(100100))
