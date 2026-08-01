from __future__ import annotations

import warnings
from collections.abc import Iterable
from decimal import Decimal
from typing import Literal

import narwhals as nw
from narwhals.typing import IntoDataFrame

from showstats._utils import (
    TABLE_WIDTH,
    _branch,
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

    """
    return _branch(
        (expr.str.len_chars() <= max_len, expr),
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


def _is_temporal(dtype) -> bool:
    return dtype == nw.Date or dtype == nw.Datetime or str(dtype).startswith("Datetime")


# Integer widths that Int64 can hold, plus Boolean. Int64 and UInt64 are
# absent on purpose: there is nothing wider to widen them to, and casting
# a UInt64 past 2**63 into Int64 fails outright on polars and pyarrow.
_WIDENABLE = (
    nw.Boolean,
    nw.Int8,
    nw.Int16,
    nw.Int32,
    nw.UInt8,
    nw.UInt16,
    nw.UInt32,
)


def _widen_for_quantile(col: nw.Expr, dtype) -> nw.Expr:
    """A column in a type a quantile can safely interpolate over.

    A quantile lands between two values, and computing that in the
    column's own width overflows: pandas answers 64.0 for the median of an
    Int8 column holding 0 and -128, where it is -64.0. Booleans have no
    quantile on any backend and need widening regardless.

    Int64 rather than Float64, though the answer is a float either way.
    Arrow refuses a lossy integer-to-float cast outright — `Integer value
    9007199254740993 not in range: 0 to 9007199254740992` — so widening
    through the float would fail on exactly the values that most need a
    wider type.
    """
    return col.cast(nw.Int64) if dtype in _WIDENABLE else col


def _median_expr(var: str, dtype) -> nw.Expr:
    """Median of a column, as the 50th percentile.

    `median()` rather than `quantile(0.5)` would be the obvious call, but
    narwhals maps it to Arrow's `approximate_median`, which is a t-digest:
    for the two values 0.00 and 0.02 it answers 0.0 where the median is
    0.01. `quantile(0.5, "linear")` is exact on all three backends, and it
    is the same call the Q50 column makes — so Median and Q50 now agree by
    construction rather than by coincidence.

    Nulls are dropped rather than left to the aggregate to ignore, and the
    column is widened first — see `_widen_for_quantile`. Temporal columns
    do not come through here; see `_temporal_median`.
    """
    col = _widen_for_quantile(nw.col(var).drop_nulls(), dtype)
    return col.quantile(0.5, interpolation="linear")


def _temporal_median(series: nw.Series):
    """Median of a date or datetime column, computed on the values.

    Not an expression, because there is no expression that works. The
    detour every backend needs is different: pandas cannot take median()
    of a datetime at all, polars and pandas can cast temporal to Int64 and
    back, and pyarrow can do neither — `Unsupported cast from date32[day]
    to int64` — nor take a quantile of one. Casting through Int64 was the
    old approach and it made pyarrow raise on any date column (#86).

    Sorting and taking the middle works everywhere and needs no casts. For
    an even count the answer is the midpoint of the two middle instants,
    which is what the Int64 round-trip produced: `date + timedelta` keeps
    whole days only, so a midpoint half a day along truncates down, and a
    datetime keeps its full precision.

    The column is sorted rather than listed out, so only the two middle
    values are ever pulled into Python.
    """
    ordered = series.drop_nulls().sort()
    count = len(ordered)
    if count == 0:
        return None
    low, high = ordered[(count - 1) // 2], ordered[count // 2]
    return low + (high - low) / 2


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
                        col = _widen_for_quantile(nw.col(var), schema[var])
                        # "linear" (numpy's and pandas' default) keeps the
                        # sequence self-consistent: Q0 == min, Q50 == median,
                        # Q100 == max. polars' own default of "nearest" would
                        # make Q50 disagree with the Median column.
                        expr = col.quantile(q, interpolation="linear").alias(stat_name)
                    elif function == "median":
                        # A temporal median has no expression that works on
                        # every backend, so it is filled in below from the
                        # values. The name is still registered here, since
                        # make_dt reads the column order off stat_names_map.
                        expr = (
                            None
                            if _is_temporal(schema[var])
                            else _median_expr(var, schema[var]).alias(stat_name)
                        )
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
                    if expr is not None:
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

        for vt in ("date", "datetime"):
            for var in vars_map.get(vt, []):
                stats[f"{var}{sep}median"] = _temporal_median(df[var])

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
            (nw.col("null_count") * 100 / self.num_rows).ceil().cast(nw.Int16)
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
        if table_type == "all":
            self.form_stat_df("time")
            self.form_stat_df("num")
            self.form_stat_df("cat")
            return

        # Just "Col". The row count used to live here, and since it is
        # usually wider than the variable names it padded every row of the
        # first column out to its own length — 12 characters of "Col
        # (N=1461)" against a 7-character "weather". It moved to the
        # section rule, which is 80 characters of dashes with room to
        # spare (#75). A side benefit: this column's name no longer
        # changes with the row count, so callers of make_stats_tbl can
        # address it by name.
        name_var = "Col"
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
                median_col = [nw.col("median").alias("Median")]
                tail_cols = [
                    *(
                        nw.col(_quantile_stat_name(q)).alias(_quantile_label(q))
                        for q in (self.quantiles or [])
                    ),
                    nw.col("min").alias("Min"),
                    nw.col("max").alias("Max"),
                ]
            # Avg, Median, SD, then the extremes (#74): the two measures of
            # location sit together, with the spread beside them, rather
            # than SD splitting them apart.
            stat_df = stat_df.select(
                nw.col("Variable").alias(name_var),
                nw.col("null_count").alias("NA%"),
                nw.col("mean").alias("Avg"),
                *median_col,
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

    def row_count(self) -> str:
        """The row count as the header shows it.

        Scientific past 100,000, where the exact figure is noise and the
        digits would only widen the line.
        """
        if self.num_rows < 100_000:
            return str(self.num_rows)
        return f"{Decimal(self.num_rows):.2E}"

    def print_header(self, type_):
        if type_ == "time":
            name = "Date and datetime columns"
        elif type_ == "cat":
            name = "Categorical columns"
        elif type_ == "num":
            name = "Numerical columns"
        lhs = f"-{name} (N={self.row_count()})"
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
