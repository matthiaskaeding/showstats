from typing import Iterable, Union
from collections import defaultdict
import polars as pl
from polars import selectors as cs


class Metatable:
    def __init__(
        self,
        df: pl.DataFrame,
        var_types: Union[Iterable, str] = ("num", "cat", "datetime", "date", "null"),
    ):
        if isinstance(var_types, str):
            var_types = (var_types,)

        self.num_rows = df.height
        self.stat_df = None
        base_functions = ("null_count", "min", "max")
        vars = {}
        stat_names = {key: [] for key in var_types}
        functions = {}
        expressions = []

        if "num" in var_types:
            vars["num"] = df.select(
                pl.col(
                    pl.Decimal,
                    pl.Float32,
                    pl.Float64,
                    pl.Int8,
                    pl.Int16,
                    pl.Int32,
                    pl.Int64,
                    pl.UInt8,
                    pl.UInt16,
                    pl.UInt32,
                    pl.UInt64,
                    pl.Boolean,
                )
            ).columns
            functions["num"] = base_functions + ("mean", "median", "std")

        if "cat" in var_types:
            vars["cat"] = df.select(pl.col(pl.Enum, pl.String, pl.Categorical)).columns
            functions["cat"] = base_functions
        if "datetime" in var_types:
            vars["datetime"] = df.select(pl.col(pl.Datetime)).columns
            functions["datetime"] = base_functions + ("mean", "median")

        if "date" in var_types:
            vars["date"] = df.select(pl.col(pl.Date)).columns
            functions["date"] = base_functions
        if "null" in var_types:
            vars["null"] = df.select(pl.col(pl.Null)).columns
            functions["null"] = ["null_count"]

        if "cat_special" in var_types:
            vars["cat_special"] = df.select(cs.string()).columns
            functions["cat_special"] = base_functions + ("n_unique",)

        sep = "____"
        for vt in var_types:
            functions_vt = functions[vt]
            vars_vp = vars[vt]
            for var in vars_vp:
                for function in functions_vt:
                    stat_name = f"{var}{sep}{function}"
                    expr = getattr(pl.col(var), function)().alias(stat_name)
                    expressions.append(expr)
                    stat_names[vt].append(stat_name)

        if "cat_special" in var_types:
            expr = (
                cs.by_name(vars["cat_special"])
                .value_counts(sort=True)
                .implode()
                .name.prefix(f"top_5{sep}")
            )
            stats = df.select(*expressions, expr).row(0, named=True)
            vars_cat_special = vars["cat_special"]
            for var in vars_cat_special:
                key = f"top_5{sep}{var}"
                top_5_list = stats[key]
                for i, dd in enumerate(top_5_list):
                    freq_value, count = dd[var], dd["count"]
                    nm = f"{var}{sep}top_{i+1}"  # varname for stats
                    share = count / self.num_rows  # % of rows which have <freq_value>
                    entry = f"{freq_value} ({share:.0%})"
                    stats[nm] = entry
                    stat_names["cat_special"].append(nm)
                del stats[key]

        else:
            stats = df.select(expressions).row(0, named=True)

        self.stat_names = stat_names
        self.stats = stats
        self.vars = vars
        self.sep = sep

        if var_types != ("cat_special",):
            all_functions = list(
                {item for sublist in functions.values() for item in sublist}
            )

            self.columns_stat_df = ["Variable"] + [
                x
                for x in [
                    "null_count",
                    "mean",
                    "median",
                    "std",
                    "min",
                    "max",
                ]
                if x in all_functions
            ]
        else:
            self.columns_stat_df = [
                "Variable",
                "null_count",
                "n_unique",
                "top_1",
                "top_2",
                "top_3",
                "top_4",
                "top_5",
            ]

    def make_dt(self, var_type: str) -> pl.DataFrame:
        data = defaultdict(list)
        data["Variable"] = self.vars[var_type]
        stat_names = self.stat_names[var_type]

        for name in stat_names:
            var_name, fun_name = name.split(self.sep, 1)
            stat_value = self.stats[name]
            data[fun_name].append(stat_value)

        df = (
            pl.LazyFrame(data)
            .with_columns(
                pl.col("null_count")
                .truediv(self.num_rows)
                .mul(100)
                .round(2)
                .alias("null_count")
            )
            .with_columns(
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
        )

        if var_type == "num":
            df = df.with_columns(
                pl.col("mean", "median", "min", "max", "std").round_sig_figs(2)
            )
        # Convert to string
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
        ll = [self.make_dt(var_type) for var_type in self.vars]
        stat_df = pl.concat(ll)
        self.stat_df = stat_df.rename(
            {
                "Variable": name_var,
                "null_count": "Null %",
                "mean": "Mean",
                "median": "Median",
                "std": "Std.",
                "min": "Min",
                "max": "Max",
            }
        )

    def print(self):
        if self.stat_df is None:
            self.form_stat_df()
        stat_df_in_memory = self.stat_df.collect()
        cfg = pl.Config(
            tbl_hide_dataframe_shape=True,
            tbl_formatting="ASCII_MARKDOWN",
            tbl_hide_column_data_types=True,
            float_precision=2,
            fmt_str_lengths=100,
            tbl_rows=stat_df_in_memory.height,
            tbl_cell_alignment="LEFT",
        )

        with cfg:
            print(stat_df_in_memory)
