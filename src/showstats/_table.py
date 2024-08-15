from typing import TYPE_CHECKING, Iterable, Tuple, Union

import polars as pl
from polars import selectors as cs

if TYPE_CHECKING:
    import pandas


def _check_input_maybe_try_transform(input):
    if isinstance(input, pl.DataFrame):
        if input.height == 0 or input.width == 0:
            raise ValueError("Input data frame must have rows and columns")
        else:
            return input
    else:
        print("Attempting to convert input to polars.DataFrame")
        try:
            out = pl.DataFrame(input)
        except Exception as e:
            print(f"Error occurred during attempted conversion: {e}")
    if out.height == 0 or out.width == 0:
        raise ValueError("Input not compatible")
    else:
        return out


def _get_cols_for_var_type(df, var_type):
    if var_type == "num_float":
        col_vt = pl.col(
            pl.Decimal,
            pl.Float32,
            pl.Float64,
        )
    elif var_type == "num_int":
        col_vt = pl.col(
            pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64,
        )
    elif var_type == "num_bool":
        col_vt = pl.col(pl.Boolean)
    elif var_type == "cat":
        col_vt = pl.col(pl.Enum, pl.String, pl.Categorical)
    elif var_type == "datetime":
        col_vt = pl.col(pl.Datetime)
    elif var_type == "date":
        col_vt = pl.col(pl.Date)
    elif var_type == "null":
        col_vt = pl.col(pl.Null)
    else:
        raise ValueError(f"var_type {var_type} not supported")

    return df.select(col_vt).columns


def _get_funs_for_var_type(var_type) -> Tuple[str]:
    if var_type in ("num_float", "num_int", "num_bool"):
        return ("null_count", "mean", "std", "median", "min", "max")
    elif var_type == "cat":
        return ("null_count", "n_unique")
    elif var_type == "datetime":
        return ("null_count", "mean", "median", "min", "max")
    elif var_type == "date":
        return ("null_count", "min", "max")
    elif var_type == "null":
        return ("null_count",)


def _get_cols_and_funs_for_var_type(df, var_type) -> Tuple[str]:
    cols = _get_cols_for_var_type(df, var_type)
    if len(cols) == 0:
        return None, None

    return cols, _get_funs_for_var_type(var_type)


def _map_table_type_to_var_types(table_type):
    if table_type == "all":
        return ("num_float", "num_int", "num_bool", "datetime", "date", "null", "cat")
    elif table_type == "num":
        return ("num_float", "num_int", "num_bool", "datetime", "date", "null")
    elif table_type == "cat":
        return ("cat",)
    else:
        raise ValueError("Type must be either 'all', 'num' or 'cat'")


class _Table:
    """Models the metadata of a table"""

    def __init__(
        self,
        df: Union[pl.DataFrame, "pandas.DataFrame"],
        table_type: str,
        top_cols: Iterable = None,
    ):
        df = _check_input_maybe_try_transform(df)
        if isinstance(top_cols, str):
            top_cols = [top_cols]
        self.type = table_type
        self.stat_dfs = {}
        self.top_cols = top_cols
        self.num_rows = df.height
        vars_map = {}  # Maps var-type to columns in df
        funs_map = {}  # Maps var-type to functions
        stat_names_map = {}  # Maps var-type to names of computed statistics

        for var_type in _map_table_type_to_var_types(table_type):
            vars_vt, funs_vt = _get_cols_and_funs_for_var_type(df, var_type)
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
                    expr = getattr(pl.col(var), function)().alias(stat_name)
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
            expr = (
                cs.by_name(vars_map["cat"])
                .drop_nulls()
                .value_counts(sort=True)
                .head(3)
                .implode()
                .name.prefix(f"top_3{sep}")
            )
            stats = df.select(*expressions, expr).row(0, named=True)
        else:
            stats = df.select(expressions).row(0, named=True)
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
            pl.col("null_count")
            .truediv(self.num_rows)
            .mul(100)
            .round(2)
            .alias("null_count")
        ).with_columns(
            # Group percentages. Because pl.cut is unstable, manually use pl.when
            pl.when(pl.col("null_count").eq(0))
            .then(pl.lit("0%"))
            .when(pl.col("null_count") < 1)
            .then(pl.lit("<1%"))
            .when(pl.col("null_count") < 2)
            .then(pl.lit("<2%"))
            .when(pl.col("null_count") < 3)
            .then(pl.lit("<3%"))
            .when(pl.col("null_count") < 5)
            .then(pl.lit("<5%"))
            .when(pl.col("null_count") < 10)
            .then(pl.lit("<10%"))
            .when(pl.col("null_count") < 20)
            .then(pl.lit("<20%"))
            .when(pl.col("null_count") < 30)
            .then(pl.lit("<30%"))
            .when(pl.col("null_count") < 40)
            .then(pl.lit("<40%"))
            .when(pl.col("null_count") < 50)
            .then(pl.lit("<50%"))
            .when(pl.col("null_count") < 60)
            .then(pl.lit("<60%"))
            .when(pl.col("null_count") < 70)
            .then(pl.lit("<70%"))
            .when(pl.col("null_count") < 80)
            .then(pl.lit("<80%"))
            .when(pl.col("null_count") < 90)
            .then(pl.lit("<90%"))
            .when(pl.col("null_count") < 100)
            .then(pl.lit("<100%"))
            .when(pl.col("null_count") == 100)
            .then(pl.lit("100%"))
            .alias("null_count")
        )

        # Some special cases
        if var_type == "num_float":
            df = df.with_columns(
                pl.col("mean", "median", "min", "max", "std")
                .round_sig_figs(2)
                .cast(pl.String)
            )
        elif var_type in ("num_int", "num_bool"):
            df = df.with_columns(
                pl.col("mean", "median", "std").round_sig_figs(2).cast(pl.String),
                pl.col("min", "max").cast(pl.String),
            )
        elif var_type == "datetime":
            df = df.select(
                "Variable",
                "null_count",
                pl.format(
                    "{}\n{}",
                    pl.col("mean").dt.date(),
                    pl.col("mean").dt.time(),
                ).alias("mean"),
                pl.lit("").alias("std"),
                pl.format(
                    "{}\n{}",
                    pl.col("median").dt.date(),
                    pl.col("median").dt.time(),
                ).alias("median"),
                pl.format(
                    "{}\n{}",
                    pl.col("min").dt.date(),
                    pl.col("min").dt.time(),
                ).alias("min"),
                pl.format(
                    "{}\n{}",
                    pl.col("max").dt.date(),
                    pl.col("max").dt.time(),
                ).alias("max"),
            )
        elif var_type == "date":
            df = df.select(
                "Variable",
                "null_count",
                pl.lit("").alias("mean"),
                pl.lit("").alias("std"),
                pl.lit("").alias("median"),
                pl.col("min", "max").cast(pl.String),
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
                freq_vals = []
                for i, dd in enumerate(freq_list):
                    val, count = dd[var_name], dd["count"]
                    freq_vals.append(f"{val} ({count / self.num_rows:.0%})")
                data.append("\n".join(freq_vals))
            df = df.select(
                "Variable",
                pl.col("null_count").alias("Null%"),
                pl.col("n_unique").alias("N distinct"),
                pl.Series("Top values", data),
            )
        return df

    def form_stat_df(self, table_type):
        """
        Makes the final data frame
        """
        from decimal import Decimal

        if table_type == "all":
            self.form_stat_df("num")
            self.form_stat_df("cat")
            # Pad variable column so that it aligns
            if "num" in self.stat_dfs and "cat" in self.stat_dfs:
                a, b = self.stat_dfs["num"], self.stat_dfs["cat"]
                col_0 = a.columns[0]
                m_a = a.get_column(col_0).str.len_chars().max()
                m_b = b.get_column(col_0).str.len_chars().max()
                if m_a < m_b:
                    a = a.with_columns(pl.col(col_0).str.pad_end(m_b, " "))
                else:
                    b = b.with_columns(pl.col(col_0).str.pad_end(m_a, " "))
                self.stat_dfs["num"] = a
                self.stat_dfs["cat"] = b
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
                pl.col("null_count").alias("Null%"),
                pl.col("mean").alias("Avg"),
                pl.col("std").alias("SD"),
                pl.col("median").alias("Median"),
                pl.col("min").alias("Min"),
                pl.col("max").alias("Max"),
            )
        elif table_type == "cat":
            stat_df = stat_df.rename({"Variable": name_var})

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
                tbl_formatting="ASCII_FULL_CONDENSED",
                tbl_hide_column_data_types=True,
                float_precision=2,
                fmt_str_lengths=100,
                tbl_rows=-1,
                tbl_cell_alignment="LEFT",
            ):
                print(self.stat_dfs[table_type])
        else:
            if table_type == "num":
                print("No numerical columns found")
            elif table_type == "cat":
                print("No categorical columns found")

    def show(self):
        if self.type == "num" or self.type == "cat":
            self.show_one_table(self.type)
        elif self.type == "all":
            self.show_one_table("num")
            self.show_one_table("cat")
