from typing import Any, Iterable, Tuple

import narwhals as nw
import polars as pl

from showstats._utils import convert_df_scientific


# Basic idea of these helper functions:
#   table_type --> var_types --> functions
def _check_input_maybe_try_transform(input):
    df = nw.from_native(input, eager_only=True)
    if df.shape[0] == 0 or df.shape[1] == 0:
        raise ValueError("Input data frame must have rows and columns")
    return df


def _get_cols_for_var_type(df, var_type):
    schema = df.schema
    matching_cols = []

    for col_name, dtype in schema.items():
        dtype_str = str(dtype)
        if var_type == "num_float":
            if dtype in (nw.Decimal, nw.Float32, nw.Float64):
                matching_cols.append(col_name)
        elif var_type == "num_int":
            if dtype in (
                nw.Int8,
                nw.Int16,
                nw.Int32,
                nw.Int64,
                nw.UInt8,
                nw.UInt16,
                nw.UInt32,
                nw.UInt64,
            ):
                matching_cols.append(col_name)
        elif var_type == "num_bool":
            if dtype == nw.Boolean:
                matching_cols.append(col_name)
        elif var_type == "cat":
            if dtype in (nw.Enum, nw.String, nw.Categorical) or dtype_str.startswith(
                "Enum"
            ):
                matching_cols.append(col_name)
        elif var_type == "date":
            if dtype == nw.Date:
                matching_cols.append(col_name)
        elif var_type == "datetime":
            if dtype == nw.Datetime or dtype_str.startswith("Datetime"):
                matching_cols.append(col_name)
        elif var_type == "null":
            if dtype_str == "Null" or dtype == nw.Unknown:
                matching_cols.append(col_name)
        else:
            raise ValueError(f"var_type {var_type} not supported")

    return matching_cols


def _map_funs_to_var_type(var_type) -> Tuple[str]:
    if var_type in ("num_float", "num_int", "num_bool"):
        return ("null_count", "mean", "std", "median", "min", "max")
    elif var_type == "cat":
        return ("null_count", "n_unique")
    elif var_type == "date" or var_type == "datetime":
        return ("null_count", "min", "median", "max")
    elif var_type == "null":
        return ("null_count",)


def _map_cols_and_funs_for_var_type(df, var_type) -> Tuple[str]:
    cols = _get_cols_for_var_type(df, var_type)
    if len(cols) == 0:
        return None, None

    return cols, _map_funs_to_var_type(var_type)


def _map_table_type_to_var_types(table_type):
    """Maps table type to var types"""
    if table_type == "all":
        return ("num_float", "num_int", "num_bool", "date", "datetime", "null", "cat")
    elif table_type == "num":
        return ("num_float", "num_int", "num_bool", "null")
    elif table_type == "time":
        return ("date", "datetime")
    elif table_type == "cat":
        return ("cat",)
    else:
        raise ValueError("""Type must be either "all", "num" "time" or "cat" """)


class _Table:
    """Models the metadata of a table"""

    def __init__(
        self,
        df: Any,
        table_type: str,
        top_cols: Iterable = None,
    ):
        df = _check_input_maybe_try_transform(df)
        if isinstance(top_cols, str):
            top_cols = [top_cols]
        self.type = table_type
        self.stat_dfs = {}
        self.top_cols = top_cols
        self.num_rows = df.shape[0]
        vars_map = {}  # Maps var-type to columns in df
        funs_map = {}  # Maps var-type to functions
        stat_names_map = {}  # Maps var-type to names of computed statistics
        for var_type in _map_table_type_to_var_types(table_type):
            vars_vt, funs_vt = _map_cols_and_funs_for_var_type(df, var_type)
            if vars_vt:
                vars_map[var_type] = vars_vt
                funs_map[var_type] = funs_vt
                stat_names_map[var_type] = []
        self.funs_map = funs_map
        expressions = []
        sep = "____"
        for vt in vars_map:
            functions_vt = funs_map[vt]
            for var in vars_map[vt]:
                for function in functions_vt:
                    stat_name = f"{var}{sep}{function}"
                    expr = getattr(nw.col(var), function)().alias(stat_name)
                    expressions.append(expr)
                    stat_names_map[vt].append(stat_name)
        # Evaluate expressions
        # Cover special cases.
        # Those conditions must always hold:
        # (1) Stats is a dict.
        # (2) Each value in stats is one summary statistic.
        # (3) Each list in stat_names_mp is sorted by variable name.
        if len(expressions) == 0:
            stats = {}
        elif "cat" in vars_map:
            # For categorical columns, we need to use native backend for value_counts
            # First, get the basic stats
            stats_df = df.select(expressions)
            native_stats = nw.to_native(stats_df)
            if isinstance(native_stats, pl.DataFrame):
                stats = native_stats.row(0, named=True)
            else:
                # For pandas
                stats = dict(native_stats.iloc[0])

            # Now handle categorical value_counts
            native_df = nw.to_native(df)
            if isinstance(native_df, pl.DataFrame):
                from polars import selectors as cs

                expr = (
                    cs.by_name(vars_map["cat"])
                    .drop_nulls()
                    .value_counts(sort=True)
                    .head(3)
                    .implode()
                    .name.prefix(f"top_3{sep}")
                )
                cat_stats_df = native_df.select(expr)
                cat_stats = cat_stats_df.row(0, named=True)
                stats.update(cat_stats)
            else:
                # For pandas, we handle value_counts differently
                for var_name in vars_map["cat"]:
                    stat_name = f"top_3{sep}{var_name}"
                    value_counts = native_df[var_name].dropna().value_counts().head(3)
                    freq_list = []
                    for val, count in value_counts.items():
                        freq_list.append({var_name: val, "count": count})
                    stats[stat_name] = freq_list
        else:
            stats_df = df.select(expressions)
            native_stats = nw.to_native(stats_df)
            if isinstance(native_stats, pl.DataFrame):
                stats = native_stats.row(0, named=True)
            else:
                # For pandas
                stats = dict(native_stats.iloc[0])
        self.stat_names_map = stat_names_map
        self.stats = stats
        self.vars_map = vars_map
        self.sep = sep

    def make_dt(self, var_type: str) -> pl.DataFrame:
        data = {}
        data["Variable"] = self.vars_map[var_type]
        for fun_name in self.funs_map[var_type]:
            data[fun_name] = []
        stat_names = self.stat_names_map[var_type]
        for name in stat_names:
            _, fun_name = name.split(self.sep, 1)
            stat_value = self.stats[name]
            data[fun_name].append(stat_value)

        df = pl.LazyFrame(data)
        df = df.with_columns(
            pl.col("null_count").truediv(self.num_rows).mul(100).ceil().cast(pl.Int16)
        )

        # Some special cases
        if var_type == "num_float":
            df = convert_df_scientific(df, ["mean", "median", "min", "max", "std"])
        elif var_type in ("num_int", "num_bool"):
            df = convert_df_scientific(df, ["mean", "median", "std"]).with_columns(
                pl.col("min", "max").cast(pl.String),
            )
        elif var_type == "date" or var_type == "datetime":
            df = df.select(
                "Variable",
                "null_count",
                pl.col("median", "min", "max").cast(pl.String).str.slice(0, 19),
            )
        elif var_type == "null":
            df = df.with_columns(
                "null_count",
                pl.lit("").alias("mean"),
                pl.lit("").alias("std"),
                pl.lit("").alias("median"),
                pl.lit("").alias("min"),
                pl.lit("").alias("max"),
            )
        elif var_type == "cat":
            data = []
            for var_name in self.vars_map["cat"]:
                stat_name = f"top_3{self.sep}{var_name}"
                freq_list = self.stats[stat_name]
                row = {}
                for i, dd in enumerate(freq_list):
                    val, count = dd[var_name], dd["count"]
                    row[f"Top {i+1}"] = f"{val} ({count / self.num_rows:.0%})"
                data.append(row)
            right = pl.DataFrame(data).fill_null("")
            df = df.select(
                "Variable",
                pl.col("null_count").alias("NA%"),
                pl.col("n_unique").alias("Uniques"),
            )
            for col_name in right.columns:
                column = right.get_column(col_name)
                df = df.with_columns(column)
        return df

    def form_stat_df(self, table_type):
        """
        Makes the final data frame
        """
        from decimal import Decimal

        if table_type == "all":
            self.form_stat_df("time")
            self.form_stat_df("num")
            self.form_stat_df("cat")
            return

        if self.num_rows < 100_000:
            name_var = f"Var. N={self.num_rows}"
        else:
            name_var = f"Var. N={Decimal(self.num_rows):.2E}"
        subdfs = []

        for var_type in _map_table_type_to_var_types(table_type):
            if var_type in self.vars_map:
                subdfs.append(self.make_dt(var_type))

        if len(subdfs) == 0:
            return
        stat_df = pl.concat(subdfs)

        if table_type == "num":
            stat_df = stat_df.select(
                pl.col("Variable").alias(name_var),
                pl.col("null_count").alias("NA%"),
                pl.col("mean").alias("Avg"),
                pl.col("std").alias("SD"),
                pl.col("min").alias("Min"),
                pl.col("max").alias("Max"),
                pl.col("median").alias("Median"),
            )
        elif table_type == "cat":
            stat_df = stat_df.rename({"Variable": name_var})
        elif table_type == "time":
            stat_df = stat_df.select(
                pl.col("Variable").alias(name_var),
                pl.col("null_count").alias("NA%"),
                pl.col("min").alias("Min"),
                pl.col("max").alias("Max"),
                pl.col("median").alias("Median"),
            )

        if self.top_cols is not None:  # Put top_cols at front
            all_columns_in_order = []
            for vt in self.vars_map:
                all_columns_in_order.extend(self.vars_map[vt])
            new_order = self.top_cols + [
                var for var in all_columns_in_order if var not in self.top_cols
            ]
            stat_df = stat_df.with_columns(
                pl.col(name_var).cast(pl.Enum(new_order))
            ).sort(name_var)

        self.stat_dfs[table_type] = stat_df.collect()

    def show_one_table(self, table_type):
        if table_type in self.stat_dfs:
            with pl.Config(
                tbl_hide_dataframe_shape=True,
                tbl_formatting="NOTHING",
                tbl_hide_column_data_types=True,
                float_precision=2,
                fmt_str_lengths=100,
                tbl_rows=-1,
                tbl_cell_alignment="LEFT",
                set_fmt_float="full",
                set_tbl_width_chars=80,
            ):
                print(self.stat_dfs[table_type])
        else:
            if table_type == "num":
                print("No numerical columns found")
            elif table_type == "cat":
                print("No categorical columns found")

    def print_header(self, type_):
        if type_ == "time":
            lhs = "-Date and datetime columns"
        elif type_ == "cat":
            lhs = "-Categorical columns"
        elif type_ == "num":
            lhs = "-Numerical columns"
        rhs = "-" * (80 - len(lhs))
        print(f"{lhs}{rhs}")

    def show(self):
        if self.type in ("num", "cat", "time"):
            if self.type not in self.stat_dfs:
                if self.type == "num":
                    print("No numerical columns found")
                elif self.type == "cat":
                    print("No categorical columns found")
                else:
                    print("No date or datetime columns found")
            else:
                self.print_header(self.type)
                self.show_one_table(self.type)
        elif self.type == "all":
            for type_ in ["time", "num", "cat"]:
                if type_ in self.stat_dfs:
                    self.print_header(type_)
                    self.show_one_table(type_)
