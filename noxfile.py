import nox


@nox.session
def docs(session):
    session.install(".")
    session.install("sphinx", "sphinx-autobuild")
    session.run("rm", "-rf", "build/html", external=True)
    sphinx_args = ["-W", "docs/source", "build/html"]

    if "serve" in session.posargs:
        session.run("sphinx-autobuild", *sphinx_args)
    else:
        session.run("sphinx-build", *sphinx_args)
