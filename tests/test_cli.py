"""Exercise file reading, argument handling, and compact row previews."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import narwhals as nw
import polars as pl
import pytest

import showstats
from showstats import _cli, show_stats


@pytest.fixture
def data():
    return pl.DataFrame({"value": list(range(1, 13)), "group": ["a", "b"] * 6})


@pytest.fixture(params=["csv", "parquet"])
def data_file(request, tmp_path, data):
    path = tmp_path / f"sample.{request.param}"
    getattr(data, f"write_{request.param}")(path)
    return path


@pytest.mark.parametrize("backend", ["polars", "pandas", "pyarrow"])
@pytest.mark.parametrize("table_type", [None, "all", "cat", "num"])
def test_summary_matches_public_api(
    data_file, capsys, monkeypatch, backend, table_type
):
    original_import = _cli.import_module

    def only_backend(name):
        if name in {"polars", "pandas", "pyarrow"} and name != backend:
            raise ModuleNotFoundError(name, name=name)
        return original_import(name)

    monkeypatch.setattr(_cli, "import_module", only_backend)
    reader = nw.read_csv if data_file.suffix == ".csv" else nw.read_parquet
    show_stats(reader(data_file, backend=backend), table_type=table_type or "all")
    expected = capsys.readouterr().out
    args = [str(data_file)]
    if table_type is not None:
        args += ["--type", table_type]
    assert _cli.main(args) == 0
    output = capsys.readouterr().out
    assert output == expected
    if table_type == "cat":
        assert "Categorical columns" in output
        assert "Numerical columns" not in output
    elif table_type == "num":
        assert "Numerical columns" in output
        assert "Categorical columns" not in output


@pytest.mark.parametrize("preview", [["-h"], ["-t"], ["--offset", "4"]])
def test_summary_type_rejects_row_preview(preview, capsys):
    with pytest.raises(SystemExit) as exc:
        _cli.main(["file.csv", "--type", "cat", *preview])
    assert exc.value.code == 2
    assert "--type is for summaries" in capsys.readouterr().err


def test_invalid_summary_type(capsys):
    with pytest.raises(SystemExit) as exc:
        _cli.main(["file.csv", "--type", "unknown"])
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


@pytest.mark.parametrize("flag", ["-h", "--head", "-t", "--tail"])
@pytest.mark.parametrize("count", [None, 1, 10, 20])
def test_preview_rows(data_file, capsys, monkeypatch, flag, count):
    monkeypatch.setenv("COLUMNS", "200")
    args = [str(data_file), flag] + ([] if count is None else [str(count)])
    assert _cli.main(args) == 0
    output = capsys.readouterr().out
    n = min(12, 3 if count is None else count)
    tail = flag in {"-t", "--tail"}
    numbers = list(range(12 - n, 12)) if tail else list(range(n))
    lines = output.splitlines()
    assert lines[0] == f"{'Tail' if tail else 'Head'} ({n} of 12 rows)"
    assert lines[1].split() == ["Row", "value", "group"]
    assert [line.split() for line in lines[2:]] == [
        [str(i), str(i + 1), "a" if i % 2 == 0 else "b"] for i in numbers
    ]
    assert "Numerical columns" not in output


def test_auto_expands_wide_preview_without_losing_values(monkeypatch):
    monkeypatch.setenv("COLUMNS", "40")
    frame = nw.from_dict({"label": ["x" * 100]}, backend="polars")
    output = _cli._render_preview(frame, 3, tail=False)
    assert max(map(len, output.splitlines())) <= 40
    assert "Row 0\n" in output
    assert output.count("x") == 100
    assert "…" not in output


def test_auto_layout_switches_at_table_width(monkeypatch):
    frame = nw.from_dict({"label": ["x" * 20]}, backend="polars")
    monkeypatch.setenv("COLUMNS", "25")
    assert "Row  label\n" in _cli._render_preview(frame, 3, tail=False)
    monkeypatch.setenv("COLUMNS", "24")
    assert "Row 0\n" in _cli._render_preview(frame, 3, tail=False)


@pytest.mark.parametrize("layout", ["table", "expanded"])
def test_cli_layout_override(data_file, capsys, monkeypatch, layout):
    monkeypatch.setenv("COLUMNS", "80" if layout == "expanded" else "20")
    assert _cli.main([str(data_file), "-h", "--layout", layout]) == 0
    output = capsys.readouterr().out
    if layout == "expanded":
        assert "Row 0\n" in output
        assert "Row 2\n" in output
    else:
        assert "Row  value  group\n" in output


def test_forced_table_can_exceed_terminal_width(monkeypatch):
    monkeypatch.setenv("COLUMNS", "20")
    frame = nw.from_dict({"label": ["x" * 40]}, backend="polars")
    output = _cli._render_preview(frame, 3, tail=False, layout="table")
    assert "x" * 40 in output
    assert "Row 0\n" not in output


def test_expanded_preserves_long_names_and_duplicate_row_label(monkeypatch):
    monkeypatch.setenv("COLUMNS", "20")
    frame = nw.from_dict({"a" * 30: ["b" * 30], "Row": [7]}, backend="polars")
    output = _cli._render_preview(frame, 3, tail=False, layout="expanded")
    assert output.count("a") == 31  # Includes the a in Head.
    assert output.count("b") == 30
    assert "Row 0\n" in output
    assert "Row\n  7" in output


@pytest.mark.parametrize(
    "offset, numbers",
    [
        (4, [4, 5, 6]),
        (0, [0, 1, 2]),
        (-3, [9, 10, 11]),
        (-1, [11]),
        (-50, [0, 1, 2]),
        (12, []),
        (50, []),
    ],
)
@pytest.mark.parametrize("explicit_count", [False, True])
def test_offset_rows(data_file, capsys, offset, numbers, explicit_count):
    args = [str(data_file), "--offset", str(offset), "--layout", "expanded"]
    if explicit_count:
        args += ["-h", "3"]
    assert _cli.main(args) == 0
    output = capsys.readouterr().out
    assert output.startswith(f"Rows ({len(numbers)} of 12 rows)\n")
    assert [line for line in output.splitlines() if line.startswith("Row ")] == [
        f"Row {i}" for i in numbers
    ]
    for i in numbers:
        assert f"value  {i + 1}\n" in output


def test_offset_honors_explicit_count(data_file, capsys):
    assert (
        _cli.main([str(data_file), "--offset", "4", "-h", "2", "--layout", "expanded"])
        == 0
    )
    output = capsys.readouterr().out
    assert "Row 4\n" in output and "Row 5\n" in output
    assert "Row 6\n" not in output


def test_expanded_tail_uses_original_zero_based_numbers(data_file, capsys):
    assert _cli.main([str(data_file), "-t", "--layout", "expanded"]) == 0
    output = capsys.readouterr().out
    assert [line for line in output.splitlines() if line.startswith("Row ")] == [
        "Row 9",
        "Row 10",
        "Row 11",
    ]


@pytest.mark.parametrize("flag", ["-h", "-t"])
def test_empty_preview(tmp_path, capsys, flag):
    path = tmp_path / "empty.parquet"
    pl.DataFrame(schema={"value": pl.Int64}).write_parquet(path)
    assert _cli.main([str(path), flag]) == 0
    assert (
        capsys.readouterr().out
        == f"{'Head' if flag == '-h' else 'Tail'} (0 of 0 rows)\n"
    )


@pytest.mark.parametrize(
    "args",
    [
        [],
        ["file.csv", "-h", "0"],
        ["file.csv", "-t", "-1"],
        ["file.csv", "-h", "abc"],
        ["file.csv", "-h", "-t"],
        ["file.csv", "--layout", "expanded"],
        ["file.csv", "-h", "--layout", "transposed"],
        ["file.csv", "--offset", "abc"],
        ["file.csv", "--offset", "4", "-t"],
    ],
)
def test_invalid_arguments(args, capsys):
    with pytest.raises(SystemExit) as exc:
        _cli.main(args)
    assert exc.value.code == 2
    assert "error:" in capsys.readouterr().err


def test_help_needs_no_file(capsys):
    with pytest.raises(SystemExit) as exc:
        _cli.main(["--help"])
    assert exc.value.code == 0
    assert "--head" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("name", "content", "message"),
    [
        ("missing.csv", None, "file not found"),
        ("data.txt", "x", "unsupported file type"),
        ("broken.parquet", "not parquet", "showstats:"),
    ],
)
def test_file_errors(tmp_path, capsys, name, content, message):
    path = tmp_path / name
    if content is not None:
        path.write_text(content)
    with pytest.raises(SystemExit) as exc:
        _cli.main([str(path)])
    assert exc.value.code == 1
    output = capsys.readouterr()
    assert message in output.err
    assert "Traceback" not in output.err
    assert output.out == ""


def test_missing_backend(data_file, monkeypatch, capsys):
    def missing(name):
        raise ModuleNotFoundError(name, name=name)

    monkeypatch.setattr(_cli, "import_module", missing)
    with pytest.raises(SystemExit) as exc:
        _cli.main([str(data_file)])
    assert exc.value.code == 1
    assert 'uv tool install "showstats[cli]"' in capsys.readouterr().err


def test_module_entrypoint(data_file):
    env = dict(os.environ)
    env["PYTHONPATH"] = str(Path(showstats.__file__).resolve().parent.parent)
    result = subprocess.run(
        [sys.executable, "-m", "showstats", str(data_file), "-h"],
        capture_output=True,
        check=False,
        text=True,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert "Head (3 of 12 rows)" in result.stdout
