# Central functions for table making
from typing import TYPE_CHECKING, Iterable

import polars as pl

from showstats.showstats import make_stats_tbl, show_stats

if TYPE_CHECKING:
    pass


@pl.api.register_dataframe_namespace("stats_tbl")
class StatsFrame:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def show(self, table_type: str = "all", top_cols: Iterable = None) -> None:
        show_stats(self._df, table_type, top_cols)

    def make_tbl(self, table_type: str = "all", top_cols: Iterable = None) -> None:
        return make_stats_tbl(self._df, table_type, top_cols)
