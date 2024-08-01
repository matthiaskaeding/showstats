# %%

import polars as pl


def make_table(df):
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
    functions["num"] = ["null_count", "mean", "median", "std", "min", "max"]
    functions["cat"] = ["null_count", "n_unique", "min", "max"]
    functions["datetime"] = ["null_count", "mean", "median", "min", "max"]
    functions["date"] = ["null_count", "min", "max"]

    # Map vars to functions
    vars = {}
    vars["num"] = df.select(pl.col(pl.NUMERIC_DTYPES)).columns
    vars["cat"] = df.select(
        pl.col(pl.String), pl.col(pl.Enum), pl.col(pl.Categorical)
    ).columns
    vars["datetime"] = df.select(pl.col(pl.Datetime)).columns
    vars["date"] = df.select(pl.col(pl.Date)).columns

    # Variable names
    compound_varnames = defaultdict(list)

    exprs = []
    for var_type in vars:
        functions_var_type = functions[var_type]
        vars_var_type = vars[var_type]
        for var in vars_var_type:
            for fun in functions_var_type:
                print(fun, var)
                varname = f"{fun}_{var}"
                expr = getattr(pl.col(var), fun)().alias(varname)
                compound_varnames[var_type].append(varname)
                print(expr)
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


if __name__ == "__main__":
    import polars as pl
    import numpy as np
    from datetime import date, datetime

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
    out = make_table(df)
    print(out)
