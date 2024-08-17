from datetime import date, datetime

import polars as pl
import pytest
from numpy import random as np_random


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
    np_random.seed(seed)

    sr = pl.Series(np_random.normal(size=n))
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


def sample_datetimes(
    seed: int = 1,
    n: int = 100,
    start_date: date = date(1500, 1, 1),
    end_date: date = date(2000, 1, 1),
    name: str = "x",
):
    """
    Efficiently sample a large number of datetimes within a given range using Polars.

    Parameters:
    start_date (str): The start date in 'YYYY-MM-DD' format.
    end_date (str): The end date in 'YYYY-MM-DD' format.
    n_samples (int): The number of samples to generate.
    seed (int, optional): Random seed for reproducibility.

    Returns:
    pl.Series: A sorted Series of sampled datetimes.
    """
    import random

    random.seed(seed)

    # Calculate the total number of seconds in the date range
    total_seconds = int((end_date - start_date).total_seconds())

    # Generate random seconds offsets
    random_seconds = random.choices(range(total_seconds), k=n)

    return (
        pl.DataFrame(pl.Series("random_seconds", random_seconds))
        .select(
            pl.lit(datetime(start_date.year, start_date.month, start_date.day)).alias(
                name
            )
            + pl.duration(seconds=pl.col("random_seconds"))
        )
        .get_column(name)
    )


def sample_dates(
    seed: int = 1,
    n: int = 100,
    start_date: date = date(1500, 1, 1),
    end_date: date = date(2000, 1, 1),
    name: str = "x",
):
    """
    Efficiently sample a large number of datetimes within a given range using Polars.

    Parameters:
    start_date (str): The start date in 'YYYY-MM-DD' format.
    end_date (str): The end date in 'YYYY-MM-DD' format.
    n_samples (int): The number of samples to generate.
    seed (int, optional): Random seed for reproducibility.

    Returns:
    pl.Series: A sorted Series of sampled datetimes.
    """
    import random
    from datetime import date

    random.seed(seed)

    # Calculate the total number of seconds in the date range
    total_seconds = int((end_date - start_date).days)

    # Generate random seconds offsets
    random_days = random.choices(range(total_seconds), k=n)

    return (
        pl.DataFrame(pl.Series("random_days", random_days))
        .select(
            pl.lit(date(start_date.year, start_date.month, start_date.day)).alias(name)
            + pl.duration(days=pl.col("random_days"))
        )
        .get_column(name)
    )


def sample_df_(n: int = 100, seed: int = 1) -> pl.DataFrame:
    """
    Generate a sample DataFrame with various data types.

    Args:
        n (int): Number of rows to generate. Default is 100.

    Returns:
        pl.DataFrame: A DataFrame with sample data.
    """
    np_random.seed(seed)

    assert n >= 100, "There must be >= 100 rows"

    np_random.seed(seed)
    bool_data = [i % 2 == 0 for i in range(n)]
    str_data = np_random.choice(["foo", "bar", "baz", "ABC"], size=n)

    cats_enum = ["worst", "medium", "best"]
    cats_cat = ["Fara", "Car"]
    null_data = [None] * n

    df = (
        pl.LazyFrame(
            {
                "float_mean_2": sample_series(n=n, seed=seed, mean=2),
                "float_std_2": sample_series(n=n, seed=seed, std=2),
                "float_min_-7": sample_series(n=n, seed=seed, min=-7),
                "float_max_17": sample_series(n=n, seed=seed, max=17),
                "float_big": sample_series(n=n, seed=seed, max=1234928),
                "bool_col": bool_data,
                "str_col": str_data,
                "categorical_col": pl.Series(
                    np_random.choice(cats_cat, size=n), dtype=pl.Categorical
                ),
                "enum_col": pl.Series(
                    np_random.choice(cats_enum, size=n), dtype=pl.Enum(cats_enum)
                ),
                "null_col": pl.Series(null_data),
            }
        )
        .with_columns(
            pl.arange(n).alias("int_col"),
        )
        .with_columns(
            pl.col("int_col").truediv(100).alias("float_col"),
            sample_dates(n=n, name="date_col", seed=1),
            sample_dates(n=n, name="date_col_2", seed=2),
            sample_datetimes(n=n, name="datetime_col", seed=1),
            sample_datetimes(n=n, name="datetime_col_2", seed=2),
        )
    )

    uniforms = np_random.uniform(size=n)

    # Set some values to null
    df = df.with_columns(pl.Series(uniforms).alias("U")).with_columns(
        pl.when(pl.col("U").lt(0.1))
        .then(None)
        .otherwise(pl.col("int_col"))
        .alias("int_with_missings"),
        pl.when(pl.col("U").lt(0.34))
        .then(None)
        .otherwise(pl.col("bool_col"))
        .alias("bool_col"),
        pl.when(pl.col("U").lt(0.55))
        .then(None)
        .otherwise(pl.col("str_col"))
        .alias("str_col"),
    )

    return df.collect()


@pytest.fixture(scope="session")
def sample_df():
    return sample_df_(n=500)
