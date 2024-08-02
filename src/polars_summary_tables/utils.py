# %%

import polars as pl


def make_tables(df):
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
    from collections import defaultdict

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

    rows = defaultdict(list)
    # return stats

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


def print_summary(df):
    from polars import Config

    Config.set_tbl_hide_dataframe_shape(True).set_tbl_formatting(
        "ASCII_MARKDOWN"
    ).set_tbl_hide_column_data_types(True).set_float_precision(2)

    dfs = make_tables(df)
    varnames = [
        "Variable",
        "null_count",
        "mean",
        "median",
        "std",
        "min",
        "max",
    ]
    var_types = [x for x in ["num", "cat", "datetime", "date", "null"] if x in dfs]

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

    df_super = pl.concat([dfs[key] for key in var_types]).rename(
        {
            "null_count": "Missings",
            "mean": "Mean",
            "median": "Median",
            "std": "Std.",
            "min": "Min",
            "max": "Max",
        }
    )
    print(df_super)


if __name__ == "__main__":
    from datetime import date, datetime

    import numpy as np
    import polars as pl

    # Generate data
    length = 100

    # Various data types
    int_data = np.random.randint(0, 100, size=length)
    float_data = np.random.rand(length)
    bool_data = np.random.choice([True, False], size=length)
    str_data = np.random.choice(["foo", "bar", "baz"], size=length)
    date_data = pl.date_range(
        start=date(2022, 1, 1), end=date(2022, 1, 1) + pl.duration(days=99), eager=True
    )
    datetime_data = pl.datetime_range(
        start=datetime(2022, 3, 1),
        end=datetime(2022, 3, 1) + pl.duration(days=99),
        interval="1d",
        eager=True,
    )
    categorical_data = np.random.choice(["low", "medium", "high"], size=length)

    # Create Polars DataFrame
    df = pl.DataFrame(
        {
            "int_col": int_data,
            "float_col": float_data,
            "bool_col": bool_data,
            "str_col": str_data,
            "date_col": pl.Series(date_data).cast(pl.Date),
            "datetime_col": pl.Series(datetime_data).cast(pl.Datetime),
            "categorical_col": pl.Series(categorical_data).cast(pl.Categorical),
        }
    )
    # out = make_tables(df)
    # print(out)
    # print_summary(df)

    null_df = pl.DataFrame({"null_col": [None] * 10})
    print(make_tables(df))

    # x = pl.DataFrame({"a": [1.0, -0.00005, 123.0]})
    # x = x.with_columns(pl.col("a").cast(pl.String))
    # print(x)
