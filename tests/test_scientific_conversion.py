import polars as pl
from showstats._utils import convert_df_scientific


def test_convert_df_scientific():
    df = pl.DataFrame(
        {
            "small": [0.001, 0.1, 10, 1000],
            "large": [1e4, 2343_342, 3e7, 4e8],
            "integer": [1, 1, 2, 1000_000],
            "mixed": [0.001, 100.551324234, 10000, 1e7],
            "special": [0.0, float("inf"), float("-inf"), float("nan")],
            "null": [None, None, None, None],
        }
    ).lazy()

    result = convert_df_scientific(
        df, ["small", "null", "large", "mixed", "special", "integer"]
    ).collect()
    assert result.get_column("small").to_list() == ["0.0", "0.1", "10.0", "1000.0"]
    assert result.get_column("large").to_list() == [
        "10000.0",
        "2.343E6",
        "3.0E7",
        "4.0E8",
    ]
    assert result.get_column("mixed").to_list() == ["0.0", "100.55", "10000.0", "1.0E7"]
    assert result.get_column("special").to_list() == ["0.0", "inf", "-inf", ""]
    assert result.get_column("null").to_list() == [""] * 4


def test_convert_df_scientific_custom_threshold():
    df = pl.DataFrame({"values": [0.1, 10, 1000, 10000, 100000]}).lazy()

    # Use a threshold of 3
    result = convert_df_scientific(df, ["values"], thr=3).collect()

    assert result["values"].to_list() == ["0.1", "10.0", "1000.0", "1.0E4", "1.0E5"]
