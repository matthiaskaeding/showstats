from importlib.metadata import PackageNotFoundError, version

from .showstats import make_stats_tbl, show_stats

try:
    __version__ = version("showstats")
except PackageNotFoundError:
    __version__ = "unknown"
finally:
    del version
    del PackageNotFoundError


__all__ = ["show_stats", "make_stats_tbl"]
