from importlib.metadata import PackageNotFoundError, version

from showstats.showstats import make_stats_tbl, show_stats, table_one

try:
    __version__ = version("showstats")
except PackageNotFoundError:
    __version__ = "unknown"
finally:
    del version
    del PackageNotFoundError


__all__ = ["make_stats_tbl", "show_stats", "table_one"]
