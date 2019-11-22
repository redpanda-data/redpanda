import click
import pathlib
import lddwrap


@click.command()
@click.option("--binary", required=True, help="path to binary")
def print_deps(binary):
    """prints ldd output"""
    binpath = pathlib.Path(binary)
    deps = lddwrap.list_dependencies(path=binpath)
    for dep in deps:
        click.echo("Found dep: %s" % dep)
