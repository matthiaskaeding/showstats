from importlib.metadata import PackageNotFoundError, version

from .showstats import show_stats, show_cat_stats

try:
    __version__ = version("showstats")
except PackageNotFoundError:
    __version__ = "unknown"
finally:
    del version
    del PackageNotFoundError

__all__ = ["show_stats", "show_cat_stats"]
