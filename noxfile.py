import nox


@nox.session
def lint(session):
    session.install("ruff")
    session.run("ruff", "check")


@nox.session(python=["3.8", "3.9", "3.10", "3.11", "3.12"], venv_backend="uv")
def test(session):
    session.install("-r", "dev-requirements.txt")
    session.run("pytest", "tests/")
