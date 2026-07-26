from typing import TYPE_CHECKING, Iterable, Tuple, Union

import narwhals as nw
import polars as pl
from polars import selectors as cs

from showstats._utils import convert_df_scientific

if TYPE_CHECKING:
    import pandas


# Basic idea of these helper functions:
#   table_type --> var_types --> functions
def _check_input_maybe_try_transform(input):
    if isinstance(input, pl.DataFrame):
        df = input
    else:
        # narwhals gives dataframe-agnostic conversion for any of its
        # supported backends (pandas, pyarrow, modin, cuDF, ...). Plain
        # Python literals (list, dict, ...) aren't narwhals-native
        # dataframes, so fall back to letting polars try to build one.
        try:
            df = nw.from_native(input, eager_only=True).to_polars()
        except TypeError:
            print("Attempting to convert input to polars.DataFrame")
            try:
                df = pl.DataFrame(input)
            except Exception as e:
                raise ValueError(f"Input not compatible: {e}") from e
    if df.height == 0 or df.width == 0:
        raise ValueError("Input data frame must have rows and columns")
    return df


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
    elif var_type == "date":
        col_vt = pl.col(pl.Date)
    elif var_type == "datetime":
        col_vt = pl.col(pl.Datetime)
    elif var_type == "null":
        col_vt = pl.col(pl.Null)
    else:
        raise ValueError(f"var_type {var_type} not supported")

    return df.select(col_vt).columns


QUANTILE_PREFIX = "quantile_"


def _quantile_stat_name(q: float) -> str:
    return f"{QUANTILE_PREFIX}{q}"


def _quantile_label(q: float) -> str:
    pct = q * 100
    pct_str = f"{pct:g}"
    return f"Q{pct_str}"


def _map_funs_to_var_type(var_type, quantiles: Iterable = None) -> Tuple[str]:
    if var_type in ("num_float", "num_int", "num_bool"):
        funs = ["null_count", "mean", "std", "median", "min", "max"]
        if quantiles:
            funs.extend(_quantile_stat_name(q) for q in quantiles)
        return tuple(funs)
    elif var_type == "cat":
        return ("null_count", "n_unique")
    elif var_type == "date" or var_type == "datetime":
        return ("null_count", "min", "median", "max")
    elif var_type == "null":
        return ("null_count",)


def _map_cols_and_funs_for_var_type(
    df, var_type, quantiles: Iterable = None
) -> Tuple[str]:
    cols = _get_cols_for_var_type(df, var_type)
    if len(cols) == 0:
        return None, None

    return cols, _map_funs_to_var_type(var_type, quantiles)


_MAX_VAR_NAME_LEN = 30


def _truncate_long_strings(expr: pl.Expr, max_len: int = _MAX_VAR_NAME_LEN) -> pl.Expr:
    """Truncates strings longer than max_len, marking the cut with an ellipsis.

    Long variable names otherwise wrap onto a new, misaligned line when the
    printed table exceeds its configured width.
    """
    return (
        pl.when(expr.str.len_chars().gt(max_len))
        .then(expr.str.slice(0, max_len - 1) + "…")
        .otherwise(expr)
    )


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
        df: Union[pl.DataFrame, "pandas.DataFrame"],
        table_type: str,
        top_cols: Iterable = None,
        quantiles: Iterable = None,
    ):
        df = _check_input_maybe_try_transform(df)
        if isinstance(top_cols, str):
            top_cols = [top_cols]
        if quantiles is not None:
            quantiles = sorted(set(quantiles))
            for q in quantiles:
                if not (0 <= q <= 1):
                    raise ValueError(f"quantiles must lie in [0, 1], got {q}")
        self.type = table_type
        self.stat_dfs = {}
        self.top_cols = top_cols
        self.quantiles = quantiles
        self.num_rows = df.height
        vars_map = {}  # Maps var-type to columns in df
        funs_map = {}  # Maps var-type to functions
        stat_names_map = {}  # Maps var-type to names of computed statistics
        for var_type in _map_table_type_to_var_types(table_type):
            vars_vt, funs_vt = _map_cols_and_funs_for_var_type(df, var_type, quantiles)
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
                    if function.startswith(QUANTILE_PREFIX):
                        q = float(function[len(QUANTILE_PREFIX) :])
                        col = pl.col(var)
                        if vt == "num_bool":
                            col = col.cast(pl.Int8)
                        expr = col.quantile(q).alias(stat_name)
                    else:
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
        self.quantile_stat_names = (
            [_quantile_stat_name(q) for q in quantiles] if quantiles else []
        )

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
            df = convert_df_scientific(
                df, ["mean", "median", "min", "max", "std"] + self.quantile_stat_names
            )
        elif var_type in ("num_int", "num_bool"):
            df = convert_df_scientific(
                df, ["mean", "median", "std"] + self.quantile_stat_names
            ).with_columns(
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
                *(pl.lit("").alias(name) for name in self.quantile_stat_names),
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
            quantile_cols = [
                pl.col(_quantile_stat_name(q)).alias(_quantile_label(q))
                for q in (self.quantiles or [])
            ]
            stat_df = stat_df.select(
                pl.col("Variable").alias(name_var),
                pl.col("null_count").alias("NA%"),
                pl.col("mean").alias("Avg"),
                pl.col("std").alias("SD"),
                pl.col("min").alias("Min"),
                pl.col("max").alias("Max"),
                pl.col("median").alias("Median"),
                *quantile_cols,
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

        stat_df = stat_df.with_columns(
            _truncate_long_strings(pl.col(name_var).cast(pl.String)).alias(name_var)
        )

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
