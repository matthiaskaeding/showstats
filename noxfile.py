import nox

nox.options.default_venv_backend = "uv"




@nox.session
def lint(session):
    session.install("ruff")
    session.run("ruff", "check")

@nox.session(name="python_versions", python=["3.8", "3.9", "3.10", "3.11", "3.12"])
def test(session):
    session.install("pytest>=8.3.2")
    session.install("polars>=0.20.21")
    session.install("pandas>=1.5.3")
    session.install("pyarrow>=17.0.0")

    session.run("pytest", "tests/")


@nox.parametrize("polars_version", ["0.20.21", "1.0.0", "1.4.1"])
@nox.parametrize("pandas_version", ["1.5.3"])
@nox.session(name="polars_version")
def test_polars_versions(session, polars_version, pandas_version):
    session.install("pytest>=8.3.2")
    session.install(f"polars=={polars_version}")
    session.install(f"pandas=={pandas_version}")

    session.run("pytest", "tests/")



