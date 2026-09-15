"""Command line summaries and row previews for local data files."""

from __future__ import annotations

import argparse
import shutil
import textwrap
from importlib import import_module
from pathlib import Path

import narwhals as nw

from showstats._utils import _cell_text
from showstats.showstats import show_stats


def _row_count(value: str) -> int:
    try:
        count = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(
            "row count must be a positive integer"
        ) from None
    if count < 1:
        raise argparse.ArgumentTypeError("row count must be a positive integer")
    return count


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="showstats",
        description="Print summary statistics or preview rows from a CSV or Parquet file.",
        add_help=False,
    )
    parser.add_argument("--help", action="help", help="show this help message and exit")
    parser.add_argument("file", type=Path, help="path to a .csv or .parquet file")
    parser.add_argument(
        "--type",
        dest="table_type",
        choices=("all", "cat", "num"),
        help="summary columns: all, categorical, or numerical (default: all)",
    )
    preview = parser.add_mutually_exclusive_group()
    preview.add_argument(
        "-h",
        "--head",
        nargs="?",
        const=3,
        type=_row_count,
        metavar="N",
        help="show the first N rows (default: 3)",
    )
    preview.add_argument(
        "-t",
        "--tail",
        nargs="?",
        const=3,
        type=_row_count,
        metavar="N",
        help="show the last N rows (default: 3)",
    )
    parser.add_argument(
        "--offset",
        type=int,
        metavar="N",
        help="start at zero-based row N; negative offsets count from the end (with --head)",
    )
    parser.add_argument(
        "--layout",
        choices=("auto", "table", "expanded"),
        help="preview layout (default: auto; expand when the table exceeds terminal width)",
    )
    return parser


def _read_file(path: Path) -> nw.DataFrame:
    suffix = path.suffix.lower()
    if suffix not in {".csv", ".parquet"}:
        raise ValueError("unsupported file type; expected .csv or .parquet")
    if not path.is_file():
        raise ValueError(f"file not found or not a regular file: {path}")

    # Reuse an installed backend. The CLI extra supplies PyArrow when none
    # is installed, without adding a backend to the library's dependencies.
    for name in ("polars", "pyarrow", "pandas"):
        try:
            backend = import_module(name)
        except ModuleNotFoundError as exc:
            if exc.name != name:
                raise
            continue
        reader = nw.read_csv if suffix == ".csv" else nw.read_parquet
        return reader(path, backend=backend)
    raise ImportError(
        "reading files needs a dataframe library; install it with "
        'uv tool install "showstats[cli]" or uv add "showstats[cli]"'
    )


def _preview_text(value: object) -> str:
    text = _cell_text(value)
    # Keep each value on one line, including text containing terminal controls.
    return "".join(
        char if char.isprintable() else char.encode("unicode_escape").decode("ascii")
        for char in text
    )


def _render_preview(
    frame: nw.DataFrame,
    count: int,
    *,
    tail: bool,
    layout: str = "auto",
    offset: int | None = None,
) -> str:
    if offset is not None:
        start = max(0, len(frame) + offset) if offset < 0 else offset
    else:
        start = max(0, len(frame) - count) if tail else 0
    selected = frame[start : start + count]
    title = "Rows" if offset is not None else "Tail" if tail else "Head"
    output = f"{title} ({len(selected)} of {len(frame)} rows)\n"
    if len(selected) == 0:
        return output

    width = max(20, shutil.get_terminal_size(fallback=(80, 24)).columns)
    labels = [_preview_text(name) for name in frame.columns]
    rows = [
        [str(index), *(_preview_text(value) for value in row)]
        for index, row in enumerate(selected.rows(), start=start)
    ]
    headers = ["Row", *labels]
    widths = [max(map(len, column)) for column in zip(headers, *rows)]
    table_width = sum(widths) + 2 * (len(widths) - 1)
    if layout == "table" or (layout == "auto" and table_width <= width):
        lines = [
            "  ".join(value.ljust(size) for value, size in zip(row, widths)).rstrip()
            for row in [headers, *rows]
        ]
        return output + "\n".join(lines) + "\n"

    label_width = max(map(len, labels), default=0)
    blocks = []
    for number, *values in rows:
        lines = [f"Row {number}"]
        for label, value in zip(labels, values):
            # Keep full names and values. Long values continue beneath the
            # first value; very long labels get a separate line.
            if label_width + 2 > width // 2:
                lines.extend(textwrap.wrap(label, width=width))
                prefix = "  "
            else:
                prefix = label.ljust(label_width) + "  "
            chunks = textwrap.wrap(
                value,
                width=width - len(prefix),
                replace_whitespace=False,
                drop_whitespace=False,
            ) or [""]
            lines.append(prefix + chunks[0])
            lines.extend(" " * len(prefix) + chunk for chunk in chunks[1:])
        blocks.append("\n".join(lines))
    return output + "\n" + "\n\n".join(blocks) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    if args.offset is not None and args.tail is not None:
        parser.error(
            "--offset cannot be combined with --tail; use --head for the count"
        )
    preview = args.head is not None or args.tail is not None or args.offset is not None
    if args.table_type is not None and preview:
        parser.error("--type is for summaries; it cannot be combined with row previews")
    if args.layout is not None and not preview:
        parser.error("--layout requires --head, --tail, or --offset")
    try:
        frame = _read_file(args.file)
        if preview:
            count = args.head if args.head is not None else args.tail or 3
            print(
                _render_preview(
                    frame,
                    count,
                    tail=args.tail is not None,
                    layout=args.layout or "auto",
                    offset=args.offset,
                ),
                end="",
            )
        else:
            show_stats(frame, table_type=args.table_type or "all")
    except BrokenPipeError:
        return 0
    except Exception as exc:  # noqa: BLE001
        # Reader exception classes differ between backends. Report their
        # message at the CLI boundary without a Python traceback.
        parser.exit(1, f"showstats: {exc}\n")
    return 0
