import narwhals as nw
import pandas as pd
import polars as pl

from showstats._utils import convert_df_scientific


def test_convert_df_scientific():
    df = pl.DataFrame(
        {
            "small": [0.002, 0.000023241, -1e7, 1000],
            "large": [1e4, 2343_342, 3e7, 4e8],
            "integer": [1, 1, 2, 1000_000],
            "mixed": [0.022, 100.551324234, 10000, 1e7],
            "special": [0.0, float("inf"), float("-inf"), float("nan")],
            "null": [None, None, None, None],
        }
    ).lazy()

    result = convert_df_scientific(
        df, ["small", "null", "large", "mixed", "special", "integer"]
    ).collect()
    assert result.get_column("small").to_list() == [
        "0.0",
        "0.0",
        "-1.0E7",
        "1000.0",
    ]
    assert result.get_column("large").to_list() == [
        "10000.0",
        "2.34E6",
        "3.0E7",
        "4.0E8",
    ]
    assert result.get_column("mixed").to_list() == [
        "0.02",
        "100.55",
        "10000.0",
        "1.0E7",
    ]
    assert result.get_column("special").to_list() == ["0.0", "inf", "-inf", ""]
    assert result.get_column("null").to_list() == [""] * 4


def test_convert_df_scientific_accepts_pandas():
    """Whatever kind of frame goes in comes back out.

    The conversion used to be a polars expression pipeline, so a pandas
    frame died on `with_columns`. Nothing but polars ever reached it, but
    that is exactly what made the LazyFrame in `make_dt` unavoidable.
    """
    df = pd.DataFrame({"values": [0.1, 10.0, 1000.0, 10000.0, 100000.0]})
    result = convert_df_scientific(df, ["values"])
    assert isinstance(result, pd.DataFrame)
    assert result["values"].tolist() == ["0.1", "10.0", "1000.0", "10000.0", "1.0E5"]


def test_convert_df_scientific_accepts_narwhals():
    df = nw.from_native(pl.DataFrame({"values": [0.1, 1e5]}), eager_only=True)
    result = convert_df_scientific(df, ["values"])
    assert isinstance(result, nw.DataFrame)
    assert result["values"].to_list() == ["0.1", "1.0E5"]


def test_convert_df_scientific_agrees_across_backends():
    """The strings must not depend on which frame carried the numbers."""
    values = [0.002, 0.000023241, -1e7, 1000.0, 2343342.0, 3e7, 0.0, 1e-7]
    polars_out = (
        convert_df_scientific(pl.DataFrame({"v": values}).lazy(), ["v"])
        .collect()
        .get_column("v")
        .to_list()
    )
    pandas_out = convert_df_scientific(pd.DataFrame({"v": values}), ["v"])["v"].tolist()
    assert polars_out == pandas_out


def test_convert_df_scientific_custom_threshold():
    df = pl.DataFrame({"values": [0.1, 10, 1000, 10000, 100000]}).lazy()

    # Use a threshold of 3
    result = convert_df_scientific(df, ["values"], thr=3).collect()

    assert result["values"].to_list() == ["0.1", "10.0", "1000.0", "1.0E4", "1.0E5"]


def test_convert_df_scientific_power_of_ten_rounding():
    # log10().floor() is imprecise near exact powers of ten (e.g. it can put
    # 1_000_000.0 at exponent 5 instead of 6), which used to surface as a
    # mantissa of 10 instead of 1, e.g. "10.0E5" instead of "1.0E6".
    df = pl.DataFrame(
        {"values": [10.0**k for k in range(-6, 13)] + [999999.999999, -999999.999999]}
    ).lazy()

    result = convert_df_scientific(df, ["values"]).collect()["values"].to_list()

    for value in result:
        if "E" in value:
            mantissa = value.split("E")[0].lstrip("-")
            assert 1.0 <= float(mantissa) < 10.0, f"mantissa out of range: {value}"
