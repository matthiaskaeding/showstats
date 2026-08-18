from __future__ import annotations

import warnings
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, get_args

import narwhals as nw
from narwhals.typing import Frame, IntoFrame

from showstats._utils import (
    TABLE_WIDTH,
    _branch,
    convert_df_scientific,
    render_table,
)

# The table types show_stats/make_stats_tbl accept. Runtime validation reads
# the members off this alias via get_args, so the two cannot drift apart.
TableType = Literal["all", "num", "cat", "time"]
VarType = Literal["num_float", "num_int", "num_bool", "cat", "date", "datetime", "null"]

# Advisory warnings are emitted at most once per session. showstats is
# typically called repeatedly — in a loop, or over and over in a notebook
# cell — and repeating the same advice on every call is just noise.
_WARNED_ONCE = set()


def _warn_once(key: str, message: str) -> None:
    if key in _WARNED_ONCE:
        return
    _WARNED_ONCE.add(key)
    warnings.warn(message, stacklevel=3)


@dataclass(frozen=True)
class PreparedFrame:
    """A Narwhals frame and the schema used to plan its summary."""

    frame: Frame
    schema: Mapping[str, object]


def prepare_input(input: IntoFrame) -> PreparedFrame:
    """Convert an input frame and read its schema without collecting its rows."""
    frame = nw.from_native(input)
    schema = frame.collect_schema()
    if len(schema) == 0:
        raise ValueError("Input data frame must have rows and columns")
    if isinstance(frame, nw.DataFrame) and frame.shape[0] == 0:
        raise ValueError("Input data frame must have rows and columns")
    return PreparedFrame(frame=frame, schema=schema)


def _get_cols_for_var_type(df_or_schema, var_type):
    schema = df_or_schema.schema if hasattr(df_or_schema, "schema") else df_or_schema
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

    The aggregate ignores nulls, and the column is widened first. See
    `_widen_for_quantile`. Temporal columns do not come through here. See
    `_temporal_median`.
    """
    col = _widen_for_quantile(nw.col(var), dtype)
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


def _mean_expr(var: str, dtype) -> nw.Expr:
    """Mean with Boolean values represented as zero and one."""
    col = nw.col(var)
    if dtype == nw.Boolean:
        return col.cast(nw.Int8).mean()
    return col.mean()


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


_STAT_SEPARATOR = "____"


@dataclass(frozen=True)
class SummaryConfig:
    """Normalized options shared by every summary stage."""

    table_type: TableType
    quantiles: tuple[float, ...]
    fold_quantiles: bool
    quantile_framing: bool


@dataclass(frozen=True)
class SummaryPlan:
    """The columns and statistics that computation must produce."""

    config: SummaryConfig
    schema: Mapping[str, object]
    vars_map: Mapping[VarType, tuple[str, ...]]
    funs_map: Mapping[VarType, tuple[str, ...]]
    stat_names_map: Mapping[VarType, tuple[str, ...]]
    quantile_stat_names: tuple[str, ...]


@dataclass(frozen=True)
class SummaryResult:
    """Computed values plus the metadata needed to format them."""

    plan: SummaryPlan
    backend: object
    num_rows: int
    stats: Mapping[str, object]


def normalize_config(
    table_type: TableType,
    quantiles: Iterable | None = None,
    fold_quantiles: bool = True,
) -> SummaryConfig:
    """Validate and normalize public options without reading the frame."""
    if table_type not in get_args(TableType):
        raise ValueError(
            f"table_type {table_type!r} not supported; "
            f"expected one of {get_args(TableType)}"
        )

    requested_quantiles = quantiles if quantiles is not None else ()
    normalized_quantiles = tuple(sorted(set(requested_quantiles)))
    for quantile in normalized_quantiles:
        if not 0 <= quantile <= 1:
            raise ValueError(f"quantiles must lie in [0, 1], got {quantile}")

    quantile_framing = bool(normalized_quantiles) and fold_quantiles
    redundant = (
        [q for q in normalized_quantiles if q in (0, 1)] if quantile_framing else []
    )
    if redundant:
        _warn_once(
            "redundant_quantiles",
            f"quantiles {redundant} ignored: 0 and 1 are always shown "
            "as Q0 and Q100 (the min and max). Pass "
            "fold_quantiles=False to keep Min/Max/Median as separate "
            "columns instead.",
        )
        normalized_quantiles = tuple(q for q in normalized_quantiles if q not in (0, 1))

    return SummaryConfig(
        table_type=table_type,
        quantiles=normalized_quantiles,
        fold_quantiles=fold_quantiles,
        quantile_framing=quantile_framing,
    )


def classify_columns(
    schema: Mapping[str, object], table_type: TableType
) -> dict[VarType, tuple[str, ...]]:
    """Group schema columns by the statistic rules they use."""
    classified = {}
    for var_type in _map_table_type_to_var_types(table_type):
        columns = _get_cols_for_var_type(schema, var_type)
        if columns:
            classified[var_type] = tuple(columns)
    return classified


def build_summary_plan(
    schema: Mapping[str, object], config: SummaryConfig
) -> SummaryPlan:
    """Build a calculation plan from a schema and normalized options."""
    vars_map = classify_columns(schema, config.table_type)
    funs_map = {
        var_type: _map_funs_to_var_type(var_type, config.quantiles)
        for var_type in vars_map
    }
    stat_names_map = {
        var_type: tuple(
            f"{var}{_STAT_SEPARATOR}{function}"
            for var in columns
            for function in funs_map[var_type]
        )
        for var_type, columns in vars_map.items()
    }
    return SummaryPlan(
        config=config,
        schema=dict(schema),
        vars_map=vars_map,
        funs_map=funs_map,
        stat_names_map=stat_names_map,
        quantile_stat_names=tuple(_quantile_stat_name(q) for q in config.quantiles),
    )


def build_stat_expressions(
    schema: Mapping[str, object], plan: SummaryPlan
) -> tuple[nw.Expr, ...]:
    """Return the backend-neutral aggregate expressions for a plan."""
    expressions = []
    for var_type, columns in plan.vars_map.items():
        for var in columns:
            for function in plan.funs_map[var_type]:
                stat_name = f"{var}{_STAT_SEPARATOR}{function}"
                if function.startswith(QUANTILE_PREFIX):
                    q = float(function[len(QUANTILE_PREFIX) :])
                    col = _widen_for_quantile(nw.col(var), schema[var])
                    expr = col.quantile(q, interpolation="linear").alias(stat_name)
                elif function == "median":
                    expr = (
                        None
                        if _is_temporal(schema[var])
                        else _median_expr(var, schema[var]).alias(stat_name)
                    )
                elif function == "std":
                    expr = _std_expr(var, schema[var]).alias(stat_name)
                elif function == "mean":
                    expr = _mean_expr(var, schema[var]).alias(stat_name)
                elif function == "n_unique":
                    expr = (
                        (
                            nw.col(var).n_unique()
                            - nw.col(var).is_null().any().cast(nw.Int64)
                        )
                        .cast(nw.Int64)
                        .alias(stat_name)
                    )
                elif function == "null_count":
                    expr = nw.col(var).null_count().cast(nw.Int64).alias(stat_name)
                else:
                    expr = getattr(nw.col(var), function)().alias(stat_name)
                if expr is not None:
                    expressions.append(expr)
    return tuple(expressions)


_ROW_COUNT_STAT = "__showstats_row_count"
_ROW_INDEX = "__showstats_row_index"


def _collect_if_lazy(frame: Frame) -> nw.DataFrame:
    return frame.collect() if isinstance(frame, nw.LazyFrame) else frame


def _temporal_median_for_frame(df: Frame, var: str, count: int, row_index: str):
    if count == 0:
        return None
    if isinstance(df, nw.DataFrame):
        return _temporal_median(df[var])

    low_index = (count - 1) // 2
    high_index = count // 2
    middle = (
        df.select(var)
        .drop_nulls(var)
        .with_row_index(row_index, order_by=var)
        .filter(nw.col(row_index).is_in([low_index, high_index]))
        .sort(row_index)
        .collect()
    )
    low, high = middle[var][0], middle[var][-1]
    return low + (high - low) / 2


def _top_counts_for_frame(
    df: Frame, var: str, row_index: str
) -> list[dict[str, object]]:
    if isinstance(df, nw.DataFrame):
        counts = df[var].drop_nulls().value_counts(sort=True)
        return [row for row in counts.rows(named=True) if row["count"] > 0][:3]

    native = df.to_native()
    namespace = nw.get_native_namespace(df).__name__
    if namespace == "polars":
        indexed = nw.from_native(native.with_row_index(row_index))
    elif namespace == "duckdb":
        indexed = nw.from_native(
            native.project(f"*, row_number() over () - 1 AS {row_index}")
        )
    else:
        indexed = None

    if indexed is None:
        counts = (
            df.group_by(var, drop_null_keys=True)
            .agg(nw.len().alias("count"))
            .sort(["count", var], descending=[True, False])
            .head(3)
            .collect()
        )
    else:
        counts = (
            indexed.group_by(var, drop_null_keys=True)
            .agg(
                nw.len().alias("count"),
                nw.col(row_index).min().alias(row_index),
            )
            .sort(["count", row_index], descending=[True, False])
            .head(3)
            .select(var, "count")
            .collect()
        )
    return counts.rows(named=True)


def compute_summary(df: Frame, plan: SummaryPlan) -> SummaryResult:
    """Collect computed summary values without collecting a lazy input."""
    decimal_columns = [
        name for name, dtype in plan.schema.items() if dtype == nw.Decimal
    ]
    if decimal_columns:
        df = df.with_columns(nw.col(name).cast(nw.Float64) for name in decimal_columns)
    expression_schema = {
        name: nw.Float64 if name in decimal_columns else dtype
        for name, dtype in plan.schema.items()
    }
    expressions = build_stat_expressions(expression_schema, plan)
    aggregate = _collect_if_lazy(
        df.select(nw.len().alias(_ROW_COUNT_STAT), *expressions)
    )
    stats = aggregate.rows(named=True)[0]
    num_rows = stats.pop(_ROW_COUNT_STAT)
    if num_rows == 0:
        raise ValueError("Input data frame must have rows and columns")

    row_index = _ROW_INDEX
    while row_index in plan.schema:
        row_index = f"{row_index}_"

    for var_type in ("date", "datetime"):
        for var in plan.vars_map.get(var_type, ()):
            non_null_count = num_rows - stats[f"{var}{_STAT_SEPARATOR}null_count"]
            stats[f"{var}{_STAT_SEPARATOR}median"] = _temporal_median_for_frame(
                df, var, non_null_count, row_index
            )

    for var in plan.vars_map.get("cat", ()):
        stats[f"top_3{_STAT_SEPARATOR}{var}"] = _top_counts_for_frame(
            df, var, row_index
        )

    return SummaryResult(
        plan=plan,
        backend=nw.get_native_namespace(aggregate),
        num_rows=num_rows,
        stats=stats,
    )


def _top_value_columns(summary: SummaryResult) -> dict[str, list[str]]:
    columns = {}
    variables = summary.plan.vars_map["cat"]
    for position, var_name in enumerate(variables):
        frequency = summary.stats[f"top_3{_STAT_SEPARATOR}{var_name}"]
        for index, value_count in enumerate(frequency):
            value = value_count[var_name]
            count = value_count["count"]
            column = columns.setdefault(f"Top {index + 1}", [""] * len(variables))
            column[position] = f"{value} ({count / summary.num_rows:.0%})"
    return columns


def format_var_type(summary: SummaryResult, var_type: VarType) -> nw.DataFrame:
    """Format one row for each variable in a classification group."""
    plan = summary.plan
    data = {"Variable": plan.vars_map[var_type]}
    data.update({function: [] for function in plan.funs_map[var_type]})
    for name in plan.stat_names_map[var_type]:
        _, function = name.split(_STAT_SEPARATOR, 1)
        data[function].append(summary.stats[name])

    top_names = ()
    if var_type == "cat":
        top_columns = _top_value_columns(summary)
        top_names = tuple(top_columns)
        data.update(top_columns)

    frame = nw.from_dict(data, backend=summary.backend)
    frame = frame.with_columns(
        (nw.col("null_count") * 100 / summary.num_rows).ceil().cast(nw.Int16)
    )

    quantile_names = list(plan.quantile_stat_names)
    if var_type == "num_float":
        frame = convert_df_scientific(
            frame, ["mean", "median", "min", "max", "std"] + quantile_names
        )
    elif var_type in ("num_int", "num_bool"):
        frame = convert_df_scientific(
            frame, ["mean", "median", "std"] + quantile_names
        ).with_columns(
            *(
                _blank_where_missing(
                    name, nw.col(name).cast(nw.String).str.to_lowercase()
                ).alias(name)
                for name in ("min", "max")
            )
        )
    elif var_type in ("date", "datetime"):
        frame = frame.select(
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
        frame = frame.with_columns(
            nw.lit("").alias("mean"),
            nw.lit("").alias("std"),
            nw.lit("").alias("median"),
            nw.lit("").alias("min"),
            nw.lit("").alias("max"),
            *(nw.lit("").alias(name) for name in quantile_names),
        )
    elif var_type == "cat":
        frame = frame.select(
            "Variable",
            nw.col("null_count").alias("NA%"),
            nw.col("n_unique").alias("Uniques"),
            *top_names,
        )
    return frame



def _rebuild_frame(
    frame: nw.DataFrame, backend: object, row_order: list[int] | None = None
) -> nw.DataFrame:
    columns = {name: frame[name].to_list() for name in frame.columns}
    if row_order is not None:
        columns = {
            name: [values[index] for index in row_order]
            for name, values in columns.items()
        }
    return nw.from_dict(columns, schema=frame.schema, backend=backend)


def format_section(
    summary: SummaryResult, table_type: Literal["num", "cat", "time"]
) -> nw.DataFrame | None:
    """Build one final table without mutating the computed summary."""
    subframes = [
        format_var_type(summary, var_type)
        for var_type in _map_table_type_to_var_types(table_type)
        if var_type in summary.plan.vars_map
    ]
    if not subframes:
        return None

    frame = nw.concat(subframes, how="vertical")
    config = summary.plan.config
    if table_type == "num":
        if config.quantile_framing:
            median_col = (
                [] if 0.5 in config.quantiles else [nw.col("median").alias("Median")]
            )
            tail_cols = [
                nw.col("min").alias("Q0"),
                *(
                    nw.col(_quantile_stat_name(q)).alias(_quantile_label(q))
                    for q in config.quantiles
                ),
                nw.col("max").alias("Q100"),
            ]
        else:
            median_col = [nw.col("median").alias("Median")]
            tail_cols = [
                *(
                    nw.col(_quantile_stat_name(q)).alias(_quantile_label(q))
                    for q in config.quantiles
                ),
                nw.col("min").alias("Min"),
                nw.col("max").alias("Max"),
            ]
        frame = frame.select(
            nw.col("Variable").alias("Col"),
            nw.col("null_count").alias("NA%"),
            nw.col("mean").alias("Avg"),
            *median_col,
            nw.col("std").alias("SD"),
            *tail_cols,
        )
    elif table_type == "cat":
        frame = frame.rename({"Variable": "Col"})
    else:
        frame = frame.select(
            nw.col("Variable").alias("Col"),
            nw.col("null_count").alias("NA%"),
            nw.col("median").alias("Median"),
            nw.col("min").alias("Min"),
            nw.col("max").alias("Max"),
        )

    columns_in_order = (
        name
        for var_type in summary.plan.vars_map
        for name in summary.plan.vars_map[var_type]
    )
    frame = frame.with_columns(
        _truncate_long_strings(nw.col("Col").cast(nw.String)).alias("Col")
    )
    return _rebuild_frame(frame, summary.backend, None)


def format_tables(summary: SummaryResult) -> dict[str, nw.DataFrame]:
    """Return every requested final table, keyed by section name."""
    requested = (
        ("time", "num", "cat")
        if summary.plan.config.table_type == "all"
        else (summary.plan.config.table_type,)
    )
    tables = {}
    for table_type in requested:
        table = format_section(summary, table_type)
        if table is not None:
            tables[table_type] = table
    return tables


def row_count(num_rows: int) -> str:
    """Format the row count shown in a section header."""
    if num_rows < 100_000:
        return str(num_rows)
    return f"{Decimal(num_rows):.2E}"


def section_header(table_type: str, num_rows: int) -> str:
    """Return the fixed-width header for one table section."""
    names = {
        "time": "Date and datetime columns",
        "cat": "Categorical columns",
        "num": "Numerical columns",
    }
    lhs = f"-{names[table_type]} (N={row_count(num_rows)})"
    return f"{lhs}{'-' * (TABLE_WIDTH - len(lhs))}"


def render_tables(
    tables: Mapping[str, nw.DataFrame], config: SummaryConfig, num_rows: int
) -> None:
    """Print final tables and empty-section messages."""
    if config.table_type != "all":
        table = tables.get(config.table_type)
        if table is None:
            messages = {
                "num": "No numerical columns found",
                "cat": "No categorical columns found",
                "time": "No date or datetime columns found",
            }
            print(messages[config.table_type])
            return
        print(section_header(config.table_type, num_rows))
        print(render_table(table), end="")
        return

    if not tables:
        print("No summarisable columns found")
        return
    for table_type in ("time", "num", "cat"):
        if table_type in tables:
            print(section_header(table_type, num_rows))
            print(render_table(tables[table_type]), end="")


class _Table:
    """Eager compatibility wrapper around the functional summary pipeline."""

    def __init__(
        self,
        df: IntoFrame,
        table_type: TableType,
        quantiles: Iterable | None = None,
        fold_quantiles: bool = True,
    ):
        prepared = prepare_input(df)
        config = normalize_config(table_type, quantiles, fold_quantiles)
        plan = build_summary_plan(prepared.schema, config)
        summary = compute_summary(prepared.frame, plan)

        self.config = config
        self.plan = plan
        self.summary = summary
        self.stat_dfs = format_tables(summary)

        # Keep the old read-only attributes while internal callers migrate.
        self.type = config.table_type
        self.backend = summary.backend
        self.quantiles = list(config.quantiles)
        self.quantile_framing = config.quantile_framing
        self.num_rows = summary.num_rows
        self.funs_map = plan.funs_map
        self.stat_names_map = plan.stat_names_map
        self.stats = summary.stats
        self.vars_map = plan.vars_map
        self.sep = _STAT_SEPARATOR
        self.quantile_stat_names = list(plan.quantile_stat_names)

    def make_dt(self, var_type: VarType) -> nw.DataFrame:
        return format_var_type(self.summary, var_type)

    def form_stat_df(self, table_type: TableType):
        """Return the already-built table without changing object state."""
        if table_type == "all":
            return self.stat_dfs
        return self.stat_dfs.get(table_type)

    def show_one_table(self, table_type: str) -> None:
        table = self.stat_dfs.get(table_type)
        if table is not None:
            print(render_table(table), end="")
        elif table_type == "num":
            print("No numerical columns found")
        elif table_type == "cat":
            print("No categorical columns found")

    def row_count(self) -> str:
        return row_count(self.num_rows)

    def print_header(self, table_type: str) -> None:
        print(section_header(table_type, self.num_rows))

    def show(self) -> None:
        render_tables(self.stat_dfs, self.config, self.num_rows)
