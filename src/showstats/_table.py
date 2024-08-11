from typing import Iterable, Union

import polars as pl
from polars import selectors as cs


class _Table:
    """Models the metadata of a table"""

    def __init__(
        self,
        df: pl.DataFrame,
        var_types: Union[Iterable, str] = (
            "num_float",
            "num_int",
            "num_bool",
            "cat",
            "datetime",
            "date",
            "null",
        ),
        top_cols: Iterable = None,
    ):
        if isinstance(var_types, str):
            var_types = (var_types,)
        if isinstance(top_cols, str):
            top_cols = [top_cols]
        if var_types == ("cat_special",):
            self.special_case = "cat_special"
        else:
            self.special_case = None
        self.top_cols = top_cols
        self.num_rows = df.height
        self.stat_df = None
        base_functions = ("null_count", "min", "max")
        vars_map = {}  # Maps var-type to columns in df
        funs_map = {}  # Maps var-type to functions
        stat_names_map = {}  # Maps var-type to names of computed statistics

        for var_type in var_types:
            if var_type == "num_float":
                col_vt = pl.col(
                    pl.Decimal,
                    pl.Float32,
                    pl.Float64,
                )
                funs_vp = base_functions + ("mean", "median", "std")
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
                funs_vp = base_functions + ("mean", "median", "std")
            elif var_type == "num_bool":
                col_vt = pl.col(pl.Boolean)
                funs_vp = base_functions + ("mean", "median", "std")
            elif var_type == "cat":
                col_vt = pl.col(pl.Enum, pl.String, pl.Categorical)
                funs_vp = base_functions
            elif var_type == "cat_special":
                col_vt = pl.col(pl.Enum, pl.String, pl.Categorical)
                funs_vp = (
                    "null_count",
                    "n_unique",
                )
            elif var_type == "datetime":
                col_vt = pl.col(pl.Datetime)
                funs_vp = base_functions + ("mean", "median")
            elif var_type == "date":
                col_vt = pl.col(pl.Date)
                funs_vp = base_functions
            elif var_type == "null":
                col_vt = pl.col(pl.Null)
                funs_vp = ("null_count",)
            else:
                raise ValueError(f"var_type {var_type} not supported")
            vars_vt = df.select(col_vt).columns
            if vars_vt:
                vars_map[var_type] = vars_vt
                funs_map[var_type] = funs_vp
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
        if self.special_case == "cat_special":
            expr = (
                cs.by_name(vars_map["cat_special"])
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

        # Columns of resultant table
        if self.special_case is None:  # Common case
            self.columns_stat_df = (
                "Variable",
                "null_count",
                "mean",
                "median",
                "std",
                "min",
                "max",
            )
        elif self.special_case == "cat_special":  # Special case
            self.columns_stat_df = (
                "Variable",
                "null_count",
                "n_unique",
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
        if "null_count" in self.columns_stat_df:
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
                pl.col("mean", "median", "min", "max", "std").round_sig_figs(2)
            )
        elif var_type in ("num_int", "num_bool"):
            df = df.with_columns(pl.col("mean", "median", "std").round_sig_figs(2))

        if var_type == "datetime":
            df = df.with_columns(
                pl.col("mean", "median", "min", "max").dt.to_string("%Y-%m-%d %H:%M:%S")
            )
        else:
            df = df.with_columns(pl.col("*").cast(pl.String))

        # Add missing columns as ""
        exprs = [pl.lit("").alias(x) for x in self.columns_stat_df if x not in data]
        df = df.with_columns(exprs)

        return df.select(self.columns_stat_df)

    def form_stat_df(self):
        """
        Makes the final data frame
        """
        from decimal import Decimal

        if self.num_rows < 100_000:
            name_var = f"Var. N={self.num_rows}"
        else:
            name_var = f"Var. N={Decimal(self.num_rows):.2E}"
        ll = [self.make_dt(var_type) for var_type in self.vars_map]
        stat_df = pl.concat(ll)
        if self.special_case is None:
            rename_dict = {
                "Variable": name_var,
                "null_count": "Null %",
                "mean": "Mean",
                "median": "Median",
                "std": "Std.",
                "min": "Min",
                "max": "Max",
            }
        elif self.special_case == "cat_special":
            rename_dict = {
                "Variable": name_var,
                "null_count": "Null %",
                "n_unique": "N uniq.",
            }

        stat_df = stat_df.rename(rename_dict)

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

        self.stat_df = stat_df.collect()

        if self.special_case == "cat_special":
            data = []
            for var_name in self.stat_df.get_column(name_var):
                stat_name = f"top_3{self.sep}{var_name}"
                freq_list = self.stats[stat_name]
                freq_vals = []
                for i, dd in enumerate(freq_list):
                    val, count = dd[var_name], dd["count"]
                    freq_vals.append(f"{val} ({count / self.num_rows:.0%})")
                data.append("\n".join(freq_vals))
            # sub_df = pl.DataFrame(
            #     [self.stat_df.get_column(name_var), pl.Series("Top values", data)]
            # )

            self.stat_df = self.stat_df.with_columns(pl.Series("Top values", data))

    def show(self):
        if self.stat_df is None:
            self.form_stat_df()
        cfg = pl.Config(
            tbl_hide_dataframe_shape=True,
            tbl_formatting="ASCII_MARKDOWN",
            tbl_hide_column_data_types=True,
            float_precision=2,
            fmt_str_lengths=100,
            tbl_rows=self.stat_df.height,
            tbl_cell_alignment="LEFT",
        )

        with cfg:
            print(self.stat_df)
