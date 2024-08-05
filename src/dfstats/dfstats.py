# Central functions for table making
from typing import TYPE_CHECKING, Dict, Union

import polars as pl
from utils import _format_num_rows

if TYPE_CHECKING:
    import pandas


def _make_tables(
    df: Union[pl.DataFrame, "pandas.DataFrame"],
) -> Dict[str, pl.DataFrame]:
    """
    Calculate summary statistics for a DataFrame.

    Args:
        df (pl.DataFrame): The input DataFrame. If not a polars.DataFrame, will try
        to cast

    Returns:
        Dict[str, pl.DataFrame]: A dictionary of summary statistics DataFrames for each data type.
    """
    from polars import selectors as cs

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

    vars_datetime = df.select(cs.datetime()).columns
    if len(vars_datetime) > 0:
        vars["datetime"] = vars_datetime
        functions["datetime"] = functions_all + ["mean", "median"]

    vars_date = df.select(cs.date()).columns
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
    stats = df.select(exprs).row(0, named=True)

    # Make split summary tables
    dfs = {}
    for var_type in vars:
        functions_var_type = functions[var_type]
        vars_var_type = vars[var_type]
        rows = []
        for var in vars_var_type:
            row = {"Variable": var}
            for fun in functions_var_type:
                row[fun] = stats[f"{fun}_{var}"]
            rows.append(row)
        dfs[var_type] = pl.DataFrame(rows)

    return dfs


def make_stats_df(df: Union[pl.DataFrame, "pandas.DataFrame"]) -> pl.DataFrame:
    """
    Create a summary table for the given DataFrame.

    Args:
        df (pl.DataFrame): The input DataFrame.

    Returns:
        pl.DataFrame: A summary table with statistics for each variable.
    """

    if isinstance(df, pl.DataFrame) is False:
        print("Attempting to convert input to polars.DataFrame")
        try:
            df = pl.DataFrame(df)
        except Exception as e:
            print(f"Error occurred during attempted conversion: {e}")

    dfs = _make_tables(df)
    num_rows = df.height
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
        df_var_type = dfs[var_type].lazy()
        df_var_type = (
            df_var_type.with_columns(pl.selectors.float().round(2))
            .with_columns(
                pl.col("null_count")
                .truediv(num_rows)
                .alias("perc_missing")
                .mul(100)
                .round(1)
            )
            .with_columns(
                pl.format("{} ({}%)", pl.col("null_count"), pl.col("perc_missing"))
            )
        )
        # Special conversion for datetimes
        if var_type == "datetime":
            df_var_type = df_var_type.with_columns(
                pl.col("mean", "median", "min", "max").dt.to_string("%Y-%m-%d %H:%M:%S")
            )
        else:
            df_var_type = df_var_type.with_columns(pl.col("*").cast(pl.String))
        # Add missing values as ""
        for col_name in varnames:
            if col_name not in dfs[var_type].columns:
                df_var_type = df_var_type.with_columns(pl.lit("").alias(col_name))
        dfs[var_type] = df_var_type.select(varnames)

    thr = 100_000
    if num_rows < thr:
        name_var = f"Var; N = {_format_num_rows(num_rows, thr)}"
    else:
        name_var = f"Var; N \u2248 {_format_num_rows(num_rows, thr)}"
    return (
        pl.concat([dfs[key] for key in var_types])
        .rename(
            {
                "Variable": name_var,
                "null_count": "Missing",
                "mean": "Mean",
                "median": "Median",
                "std": "Std.",
                "min": "Min",
                "max": "Max",
            }
        )
        .collect()
    )


def show_stats(df: Union[pl.DataFrame, "pandas.DataFrame"]) -> None:
    """
    Print a summary table for the given DataFrame.

    Args:
        df (pl.DataFrame): The input DataFrame.
    """
    from polars import Config

    if df.height == 0 or df.width == 0:
        raise ValueError("Input data frame must have rows and columns")

    with Config(
        tbl_hide_dataframe_shape=True,
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        float_precision=2,
        fmt_str_lengths=100,
        set_tbl_rows=15,
    ):
        print(make_stats_df(df))


if __name__ == "__main__":
    from utils import _sample_df

    df = _sample_df(10000)
    # res = show_stats(df)
    print(df.columns)
    print(make_stats_df(df.to_pandas()))
    print(make_stats_df(df))
