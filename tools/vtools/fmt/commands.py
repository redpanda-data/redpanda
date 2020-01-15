import click

from absl import logging
from ..vlib import config
from ..vlib import shell


@click.group(short_help='format cpp, go and python code')
def fmt():
    pass


@fmt.command()
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--unstaged', help='Only work on unstaged files.', is_flag=True)
def go(conf, unstaged):
    vconfig = config.VConfig(conf)
    crlfmt(vconfig, unstaged)


@fmt.command()
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--unstaged', help='Only work on unstaged files.', is_flag=True)
def cpp(conf, unstaged):
    vconfig = config.VConfig(conf, clang=True)
    clangfmt(vconfig, unstaged)


@fmt.command()
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--unstaged', help='Only work on unstaged files.', is_flag=True)
def py(conf, unstaged):
    vconfig = config.VConfig(conf)
    yapf(vconfig, unstaged)


def clangfmt(vconfig, unstaged):
    logging.debug("Running clang-format")
    fmt = f'{vconfig.clang_path}/bin/clang-format'
    exts = [".cc", ".cpp", ".h", ".hpp", ".proto", ".java", ".js"]
    for f in _git_files(vconfig, exts, unstaged):
        shell.run_subprocess(
            f'{fmt} -style=file -fallback-style=none -verbose -i {f}')


def crlfmt(vconfig, unstaged):
    logging.debug("Running crlfmt")

    fmt = f'{vconfig.go_path}/bin/crlfmt'

    for f in _git_files(vconfig, ['.go'], unstaged):
        shell.run_oneline(
            f'cd {vconfig.src_dir}/ && {fmt} -w -diff=false -wrap=80 {f}')


def yapf(vconfig, unstaged):
    logging.debug("Running yapf")
    fmt = f'{vconfig.build_root}/venv/v/bin/yapf'
    for f in _git_files(vconfig, ['.py'], unstaged):
        shell.run_oneline(f'{fmt} -i {f}')


def _git_files(vconfig, exts, unstaged):
    if unstaged:
        cmd = f'cd {vconfig.src_dir} && git diff --name-only --diff-filter=d'
    else:
        cmd = f'cd {vconfig.src_dir} && git ls-files --full-name'
    ret = shell.raw_check_output(cmd)
    for f in ret.split("\n"):
        for e in exts:
            if f.endswith(e):
                yield f
