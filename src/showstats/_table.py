from __future__ import annotations

import warnings
from typing import Iterable, Literal

import narwhals as nw
from narwhals.typing import IntoDataFrame

from showstats._utils import (
    TABLE_WIDTH,
    _branch,
    _ceil,
    convert_df_scientific,
    render_table,
)

# The table types show_stats/make_stats_tbl accept. Runtime validation reads
# the members off this alias via get_args, so the two cannot drift apart.
TableType = Literal["all", "num", "cat", "time"]

# Advisory warnings are emitted at most once per session. showstats is
# typically called repeatedly — in a loop, or over and over in a notebook
# cell — and repeating the same advice on every call is just noise.
_WARNED_ONCE = set()


def _warn_once(key: str, message: str) -> None:
    if key in _WARNED_ONCE:
        return
    _WARNED_ONCE.add(key)
    warnings.warn(message, stacklevel=3)


# Basic idea of these helper functions:
#   table_type --> var_types --> functions
def _check_input_maybe_try_transform(input: IntoDataFrame) -> nw.DataFrame:
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


QUANTILE_PREFIX = "quantile_"


def _quantile_stat_name(q: float) -> str:
    return f"{QUANTILE_PREFIX}{q}"


def _quantile_label(q: float) -> str:
    pct = q * 100
    pct_str = f"{pct:g}"
    return f"Q{pct_str}"


def _map_funs_to_var_type(var_type, quantiles: Iterable | None = None) -> tuple[str]:
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
    df, var_type, quantiles: Iterable | None = None
) -> tuple[str]:
    cols = _get_cols_for_var_type(df, var_type)
    if len(cols) == 0:
        return None, None

    return cols, _map_funs_to_var_type(var_type, quantiles)


_MAX_VAR_NAME_LEN = 30


def _truncate_long_strings(expr: nw.Expr, max_len: int = _MAX_VAR_NAME_LEN) -> nw.Expr:
    """Truncates strings longer than max_len, marking the cut with an ellipsis.

    Long variable names otherwise wrap onto a new, misaligned line when the
    printed table exceeds its configured width.

    "Longer than max_len" is asked as "does slicing to max_len change it",
    which sidesteps `str.len_chars` — that only arrived in a narwhals new
    enough to need Python 3.9 (#78) — and asks the question in exactly the
    units the slice below will use.
    """
    return _branch(
        (expr.str.slice(0, max_len) == expr, expr),
        otherwise=nw.concat_str([expr.str.slice(0, max_len - 1), nw.lit("…")]),
    )


def _blank_where_missing(name: str, rendered: nw.Expr) -> nw.Expr:
    """`rendered`, or blank wherever the underlying value is missing.

    Asked of the value rather than of its rendering, because backends
    disagree about what a missing value looks like once it is a string.
    pandas 1.5 casts `NaT` to the literal `"NaT"` while newer pandas gives
    null, so a `fill_null` after the cast blanked an all-null date column
    on one version and printed `NaT` on the other.
    """
    return _branch((nw.col(name).is_null(), nw.lit("")), otherwise=rendered)


def _median_expr(var: str, dtype) -> nw.Expr:
    """Median of a column, for dtypes some backends refuse to take it on.

    polars computes median() directly for booleans and datetimes, but the
    pandas backend raises `median operation not supported for non-numeric
    input type`. Casting to an integer, taking the median there and casting
    back produces the same answer on every backend — for datetimes the cast
    goes through the column's own dtype, so the time unit round-trips
    correctly rather than being assumed.

    Nulls are dropped before the cast, not left to median() to ignore.
    pandas represents a missing datetime as NaT, and casting that to Int64
    gives the int64 minimum rather than null — so the missing rows joined
    the median as enormous negative numbers, and a column of two nulls and
    the dates 2020-01-01, 2020-06-01, 2020-12-01 reported its median as
    2020-01-01. Not blank; simply wrong, and plausible enough to go
    unnoticed.
    """
    col = nw.col(var).drop_nulls()
    if dtype == nw.Boolean:
        return col.cast(nw.Int8).median()
    if dtype == nw.Date or dtype == nw.Datetime or str(dtype).startswith("Datetime"):
        return col.cast(nw.Int64).median().cast(dtype)
    return col.median()


def _std_expr(var: str, dtype) -> nw.Expr:
    """Standard deviation, for the one dtype pyarrow will not take it on.

    Arrow has no `stddev` kernel for booleans — polars and pandas both do —
    so it goes through Int8, the same detour `_median_expr` makes. Arrow's
    `mean` does accept booleans, so only this one needs it.
    """
    col = nw.col(var)
    if dtype == nw.Boolean:
        return col.cast(nw.Int8).std()
    return col.std()


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
        df: IntoDataFrame,
        table_type: TableType,
        top_cols: Iterable | None = None,
        quantiles: Iterable | None = None,
        fold_quantiles: bool = True,
    ):
        df = _check_input_maybe_try_transform(df)
        if isinstance(top_cols, str):
            top_cols = [top_cols]
        # Min, max and median *are* the 0th, 100th and 50th percentiles, so by
        # default they are relabelled and folded into the quantile sequence
        # rather than sitting alongside it under a second name. Setting
        # fold_quantiles=False keeps the column names stable regardless of
        # which quantiles are asked for, which matters for callers of
        # make_stats_tbl that index the result by column name.
        quantile_framing = bool(quantiles) and fold_quantiles
        if quantiles is not None:
            quantiles = sorted(set(quantiles))
            for q in quantiles:
                if not (0 <= q <= 1):
                    raise ValueError(f"quantiles must lie in [0, 1], got {q}")
            # 0 and 1 are only redundant when folding is on — that is what
            # makes them appear as Q0/Q100 already.
            redundant = (
                [q for q in quantiles if q in (0, 1)] if quantile_framing else []
            )
            if redundant:
                _warn_once(
                    "redundant_quantiles",
                    f"quantiles {redundant} ignored: 0 and 1 are always shown "
                    "as Q0 and Q100 (the min and max). Pass "
                    "fold_quantiles=False to keep Min/Max/Median as separate "
                    "columns instead.",
                )
                quantiles = [q for q in quantiles if q not in (0, 1)]
        self.type = table_type
        # Remembered so the formatted table can be rebuilt in the same
        # backend the caller handed in, rather than in polars.
        self.backend = nw.get_native_namespace(df)
        self.stat_dfs = {}
        self.top_cols = top_cols
        self.quantiles = quantiles
        self.quantile_framing = quantile_framing
        self.num_rows = df.shape[0]
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
        # Decimal columns are summarised as floats. Their statistics come
        # back as Decimals otherwise, and a frame holding a Decimal column
        # beside an ordinary float one then built a mixed list of Decimal
        # and float — which nw.from_dict rejects: "unexpected value while
        # building Series of type Decimal(38, 2); found value of type
        # Float64". A Decimal alone worked, which is why it went unnoticed.
        df = df.with_columns(
            nw.col(name).cast(nw.Float64)
            for name, dtype in df.schema.items()
            if dtype == nw.Decimal
        )
        schema = df.schema
        expressions = []
        sep = "____"
        for vt, vars_vt in vars_map.items():
            functions_vt = funs_map[vt]
            for var in vars_vt:
                for function in functions_vt:
                    stat_name = f"{var}{sep}{function}"
                    if function.startswith(QUANTILE_PREFIX):
                        q = float(function[len(QUANTILE_PREFIX) :])
                        col = nw.col(var)
                        if vt == "num_bool":
                            # narwhals has no quantile for booleans
                            col = col.cast(nw.Int8)
                        # "linear" (numpy's and pandas' default) keeps the
                        # sequence self-consistent: Q0 == min, Q50 == median,
                        # Q100 == max. polars' own default of "nearest" would
                        # make Q50 disagree with the Median column.
                        expr = col.quantile(q, interpolation="linear").alias(stat_name)
                    elif function == "median":
                        expr = _median_expr(var, schema[var]).alias(stat_name)
                    elif function == "std":
                        expr = _std_expr(var, schema[var]).alias(stat_name)
                    elif function == "n_unique":
                        # Nulls dropped first: they are already reported as
                        # NA%, and the Top N columns beside this one count
                        # only real values — so leaving them in made
                        # Uniques disagree with both of its neighbours. A
                        # column of "a", "b" and two nulls read as three
                        # uniques with only two ever listed.
                        expr = nw.col(var).drop_nulls().n_unique().alias(stat_name)
                    else:
                        expr = getattr(nw.col(var), function)().alias(stat_name)
                    expressions.append(expr)
                    stat_names_map[vt].append(stat_name)
        # Evaluate expressions.
        # Those conditions must always hold:
        # (1) Stats is a dict.
        # (2) Each value in stats is one summary statistic.
        # (3) Each list in stat_names_mp is sorted by variable name.
        #
        # One path for every backend: narwhals' rows() reads the single
        # aggregate row as a dict of Python values whatever the frame is
        # made of. This used to branch on isinstance(native, pl.DataFrame)
        # and fall back to pandas' .iloc[0] — a fallback that treated
        # "not polars" as "pandas", so pyarrow input reached .iloc and
        # raised AttributeError despite being accepted at the door.
        if len(expressions) == 0:
            stats = {}
        else:
            stats = df.select(expressions).rows(named=True)[0]

        if "cat" in vars_map:
            # Top-3 counts likewise: one Series.value_counts per column,
            # replacing a polars-selector expression and a hand-rolled
            # pandas loop that had to agree with each other by inspection.
            # The frames it yields are already named [<column>, "count"],
            # which is the shape make_dt reads.
            #
            # Counts of zero are dropped: a pandas Categorical remembers
            # every category it was declared with, and value_counts reports
            # the unobserved ones at 0. An enum column holding one value
            # therefore listed "alpha (67%)", "beta (0%)", "gamma (0%)" as
            # its top three, and an all-null one listed its whole
            # vocabulary. polars reports only what is present.
            for var in vars_map["cat"]:
                counts = df[var].drop_nulls().value_counts(sort=True)
                observed = [row for row in counts.rows(named=True) if row["count"] > 0]
                stats[f"top_3{sep}{var}"] = observed[:3]
        self.stat_names_map = stat_names_map
        self.stats = stats
        self.vars_map = vars_map
        self.sep = sep
        self.quantile_stat_names = (
            [_quantile_stat_name(q) for q in quantiles] if quantiles else []
        )

    def make_dt(self, var_type: str):
        """One row per variable of `var_type`, formatted for printing.

        Returns a frame of the input's own backend — the statistics have
        been Python values since `__init__` read them out, so the frame is
        rebuilt here, and rebuilding it in polars was the last thing
        forcing polars on a pandas or pyarrow user.
        """
        data = {}
        data["Variable"] = self.vars_map[var_type]
        for fun_name in self.funs_map[var_type]:
            data[fun_name] = []
        stat_names = self.stat_names_map[var_type]
        for name in stat_names:
            _, fun_name = name.split(self.sep, 1)
            stat_value = self.stats[name]
            data[fun_name].append(stat_value)

        top_names = ()
        if var_type == "cat":
            top_cols = self._top_value_columns()
            top_names = tuple(top_cols)
            data.update(top_cols)

        df = nw.from_dict(data, backend=self.backend)
        # Multiplied before dividing, which is not a stylistic choice: the
        # other order rounds twice, and polars evaluates `count / rows * 100`
        # for 6 of 10 as 60.00000000000001, so a column exactly 60% missing
        # was reported as 61%. Checked exhaustively over every count/rows
        # pair up to 60 rows on all three backends — 35 wrong answers this
        # way round, none the other.
        df = df.with_columns(
            _ceil(nw.col("null_count") * 100 / self.num_rows).cast(nw.Int16)
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
                # Lowercased because pandas renders a boolean as "True" where
                # polars and pyarrow give "true", and the printed table must
                # not depend on which backend held the value. A no-op on the
                # integers that share this branch.
                #
                # fill_null because a statistic that does not exist — the
                # minimum of an all-null column — is blank everywhere else in
                # the table. Casting a null to String leaves it null, so this
                # one branch was handing back None where the float branch,
                # which goes through convert_df_scientific, gives "".
                *(
                    _blank_where_missing(
                        name, nw.col(name).cast(nw.String).str.to_lowercase()
                    ).alias(name)
                    for name in ("min", "max")
                )
            )
        elif var_type == "date" or var_type == "datetime":
            df = df.select(
                "Variable",
                "null_count",
                *(
                    _blank_where_missing(
                        name, nw.col(name).cast(nw.String).str.slice(0, 19)
                    ).alias(name)
                    for name in ("median", "min", "max")
                ),
            )
        elif var_type == "null":
            df = df.with_columns(
                nw.lit("").alias("mean"),
                nw.lit("").alias("std"),
                nw.lit("").alias("median"),
                nw.lit("").alias("min"),
                nw.lit("").alias("max"),
                *(nw.lit("").alias(name) for name in self.quantile_stat_names),
            )
        elif var_type == "cat":
            df = df.select(
                "Variable",
                nw.col("null_count").alias("NA%"),
                nw.col("n_unique").alias("Uniques"),
                *top_names,
            )
        return df

    def _top_value_columns(self) -> dict:
        """ "Top 1".."Top 3" columns, as plain Python lists.

        One entry per categorical variable, each already rendered as
        "<value> (<share>%)". Variables with fewer than three distinct
        values leave the later columns blank — the polars version got there
        by building a ragged frame and filling its nulls, which needs a
        second frame; padding the lists reaches the same place and keeps
        every column a plain string column on every backend.
        """
        columns = {}
        variables = self.vars_map["cat"]
        for position, var_name in enumerate(variables):
            freq_list = self.stats[f"top_3{self.sep}{var_name}"]
            for i, dd in enumerate(freq_list):
                val, count = dd[var_name], dd["count"]
                column = columns.setdefault(f"Top {i + 1}", [""] * len(variables))
                column[position] = f"{val} ({count / self.num_rows:.0%})"
        return columns

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
            name_var = f"Col (N={self.num_rows})"
        else:
            name_var = f"Col (N={Decimal(self.num_rows):.2E})"
        subdfs = []

        for var_type in _map_table_type_to_var_types(table_type):
            if var_type in self.vars_map:
                subdfs.append(self.make_dt(var_type))

        if len(subdfs) == 0:
            return
        stat_df = nw.concat(subdfs, how="vertical")

        if table_type == "num":
            if self.quantile_framing:
                # min/max become the endpoints of the quantile sequence, so
                # the whole block reads in ascending order: Q0 … Q100. An
                # explicit 0.5 replaces the Median column outright, since Q50
                # is the same statistic under the same interpolation.
                median_col = (
                    [] if 0.5 in self.quantiles else [nw.col("median").alias("Median")]
                )
                tail_cols = [
                    *median_col,
                    nw.col("min").alias("Q0"),
                    *(
                        nw.col(_quantile_stat_name(q)).alias(_quantile_label(q))
                        for q in self.quantiles
                    ),
                    nw.col("max").alias("Q100"),
                ]
            else:
                # Named stats keep their names; any requested quantiles are
                # appended alongside them. Min and Max sit last: they are the
                # extremes, so the central statistics come first.
                tail_cols = [
                    nw.col("median").alias("Median"),
                    *(
                        nw.col(_quantile_stat_name(q)).alias(_quantile_label(q))
                        for q in (self.quantiles or [])
                    ),
                    nw.col("min").alias("Min"),
                    nw.col("max").alias("Max"),
                ]
            stat_df = stat_df.select(
                nw.col("Variable").alias(name_var),
                nw.col("null_count").alias("NA%"),
                nw.col("mean").alias("Avg"),
                nw.col("std").alias("SD"),
                *tail_cols,
            )
        elif table_type == "cat":
            stat_df = stat_df.rename({"Variable": name_var})
        elif table_type == "time":
            stat_df = stat_df.select(
                nw.col("Variable").alias(name_var),
                nw.col("null_count").alias("NA%"),
                nw.col("median").alias("Median"),
                nw.col("min").alias("Min"),
                nw.col("max").alias("Max"),
            )

        row_order = None
        if self.top_cols is not None:  # Put top_cols at front
            all_columns_in_order = []
            for vt in self.vars_map:
                all_columns_in_order.extend(self.vars_map[vt])
            new_order = self.top_cols + [
                var for var in all_columns_in_order if var not in self.top_cols
            ]
            # The polars version cast the name column to pl.Enum(new_order)
            # and sorted on it, letting the categorical ordering do the
            # work. narwhals has no equivalent, and its nearest thing,
            # replace_strict, needs polars >= 1 — which would put a floor on
            # a library that is no longer even required. So the permutation
            # is worked out in Python and applied to the rebuild below,
            # which materialises the table anyway. Computed before the
            # truncation on the next line, since a truncated name would no
            # longer match its entry in new_order.
            position = {name: i for i, name in enumerate(new_order)}
            names = stat_df[name_var].to_list()
            row_order = sorted(range(len(names)), key=lambda i: position[names[i]])

        stat_df = stat_df.with_columns(
            _truncate_long_strings(nw.col(name_var).cast(nw.String)).alias(name_var)
        )

        # Rebuilt so the row labels are fresh. pandas carries each
        # subframe's own index through a vertical concat, so the assembled
        # table came back indexed (0, 0, 0) — three rows all addressed as
        # row 0. Cheap: one row per column of the input.
        columns = {name: stat_df[name].to_list() for name in stat_df.columns}
        if row_order is not None:
            columns = {
                name: [values[i] for i in row_order] for name, values in columns.items()
            }
        self.stat_dfs[table_type] = nw.from_dict(
            columns, schema=stat_df.schema, backend=self.backend
        )

    def show_one_table(self, table_type):
        if table_type in self.stat_dfs:
            print(render_table(self.stat_dfs[table_type]), end="")
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
        rhs = "-" * (TABLE_WIDTH - len(lhs))
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
            if not self.stat_dfs:
                # Every other branch says why it has nothing to show; this
                # one printed absolute silence, which reads as a hang or a
                # swallowed exception. Reachable whenever no column has a
                # dtype showstats summarises — a frame of pandas `object`
                # columns, say.
                print("No summarisable columns found")
                return
            for type_ in ["time", "num", "cat"]:
                if type_ in self.stat_dfs:
                    self.print_header(type_)
                    self.show_one_table(type_)
