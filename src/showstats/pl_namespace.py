# Central functions for table making
from typing import TYPE_CHECKING, Iterable

import polars as pl

from showstats.showstats import show_cat_stats, show_stats

if TYPE_CHECKING:
    pass


@pl.api.register_dataframe_namespace("stats")
class StatsFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def show(self, top_cols: Iterable = None) -> None:
        show_stats(self._df, top_cols)

    def show_cat(self, top_cols: Iterable = None) -> None:
        show_cat_stats(self._df, top_cols)
