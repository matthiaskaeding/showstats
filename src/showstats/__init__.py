from importlib.metadata import PackageNotFoundError, version

from .pl_namespace import StatsFrame
from .showstats import show_stats

try:
    __version__ = version("showstats")
except PackageNotFoundError:
    __version__ = "unknown"
finally:
    del version
    del PackageNotFoundError


__all__ = ["show_stats", "StatsFrame"]
