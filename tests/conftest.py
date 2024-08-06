import polars as pl
import pytest


def sample_series(
    seed: int = 1,
    n: int = 100,
    min: float = None,
    max: float = None,
    std: float = None,
    mean: float = None,
) -> pl.Series:
    """
    Samples a pl.Series with known moments.
    seed (int): Random seed
    min (float): Minimum
    max (float): Maximum
    std (float): Standard deviation
    mean (float): Mean

    """
    import random

    random.seed(seed)

    data = [random.gauss(mu=0.0, sigma=1.0) for _ in range(n)]
    sr = pl.Series(data)
    if mean is not None:
        sr_mean = sr.mean()
        sr = (sr - sr_mean) + mean
    if std is not None:
        st_std = sr.std()
        sr = sr * std / st_std
    if min is not None:
        sr_min = sr.min()
        sr = sr - sr_min + min
    if max is not None:
        sr_max = sr.max()
        sr = sr - sr_max + max

    return sr


def sample_df_(n: int = 100, seed: int = 1) -> pl.DataFrame:
    """
    Generate a sample DataFrame with various data types.

    Args:
        n (int): Number of rows to generate. Default is 100.

    Returns:
        pl.DataFrame: A DataFrame with sample data.
    """
    import random
    from datetime import date, datetime, timedelta

    assert n >= 100, "There must be >= 100 rows"

    random.seed(a=seed, version=2)
    int_data = range(n)
    float_data = [i / 100 for i in range(n)]
    bool_data = [i % 2 == 0 for i in range(n)]
    str_data = random.choices(["foo", "bar", "baz", "ABC"], k=n)
    date_col = pl.date_range(
        start=date(2022, 1, 1),
        end=date(2022, 1, 1) + timedelta(days=n - 1),
        eager=True,
    )
    date_col_2 = pl.date_range(
        start=date(1500, 1, 1),
        end=date(1500, 1, 1) + timedelta(days=n - 1),
        eager=True,
    )
    datetime_col = pl.datetime_range(
        start=datetime(2022, 1, 1),
        end=datetime(2022, 1, 1) + timedelta(seconds=n - 1),
        interval="1s",
        eager=True,
    )
    datetime_col_2 = pl.datetime_range(
        start=datetime(1995, 1, 1),
        end=datetime(1995, 1, 1) + timedelta(seconds=n - 1),
        interval="1s",
        eager=True,
    )

    cats = ["low", "medium", "high"]
    categorical_data = random.choices(cats, k=n)
    null_data = [None] * n

    int_with_missing_data = list(int_data)
    for i in range(10, 30):
        int_with_missing_data[i] = None

    return pl.DataFrame(
        {
            "int_col": int_data,
            "int_with_missing": int_with_missing_data,
            "float_col": float_data,
            "float_col_with_mean_2": sample_series(n=n, seed=seed, mean=2),
            "float_col_with_std_2": sample_series(n=n, seed=seed, std=2),
            "float_col_with_min_7": sample_series(n=n, seed=seed, min=7),
            "float_col_with_max_17": sample_series(n=n, seed=seed, max=17),
            "bool_col": bool_data,
            "str_col": str_data,
            "date_col": date_col,
            "date_col_2": date_col_2,
            "datetime_col": datetime_col,
            "datetime_col_2": datetime_col_2,
            "categorical_col": pl.Series(categorical_data, dtype=pl.Categorical),
            "enum_col": pl.Series(categorical_data, dtype=pl.Enum(cats)),
            "null_col": pl.Series(null_data),
        }
    )


@pytest.fixture(scope="session")
def sample_df():
    return sample_df_(n=500)
