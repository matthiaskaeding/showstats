import nox

nox.options.default_venv_backend = "uv"


@nox.session(venv_backend="none")
def lint(session):
    """Lint with the project's own pinned ruff.

    `session.install("ruff")` fetched whatever was newest, which
    `required-version = "==0.16.0"` then refused to run — as it did the
    moment ruff shipped 0.16.1. Delegating to `uv run` keeps the pin in
    one place, the dev group, rather than adding another to forget.
    """
    session.run("uv", "run", "ruff", "check", ".", external=True)
    session.run("uv", "run", "ruff", "format", "--check", ".", external=True)


@nox.session(name="python_versions", python=["3.10", "3.11", "3.12"])
def test(session):
    session.install(
        "duckdb>=1.0.0",
        "pytest>=8.3.2",
        "hypothesis>=6.113.0",
        "polars>=0.20.21",
        "pandas>=1.5.3",
        "pyarrow>=10.0.0",
        "narwhals>=2.20.0",
    )

    session.run("pytest", "tests/")


@nox.parametrize("polars_version", ["0.20.21", "1.4.1"])
@nox.parametrize("pandas_version", ["1.5.3"])
@nox.session(name="polars_pandas", python="3.10")
def test_polars_versions(session, polars_version, pandas_version):
    session.install(
        "duckdb>=1.0.0",
        "pytest>=8.3.2",
        "hypothesis>=6.113.0",
        f"polars=={polars_version}",
        f"pandas>={pandas_version}",
        "pyarrow>=10.0.0",
        "narwhals>=2.20.0",
    )
    session.run("pytest", "tests/")
