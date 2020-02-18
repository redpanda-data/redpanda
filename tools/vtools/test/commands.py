import click
import lddwrap
import os
import pathlib

from absl import logging
from ..vlib import config
from ..vlib import shell


@click.group(short_help='run redpanda and rpk tests')
def test():
    pass


@test.command(short_help='execute rpk unit tests')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def go(conf):
    vconfig = config.VConfig(conf)
    with os.scandir(vconfig.go_src_dir) as it:
        for fd in it:
            if not fd.name.startswith('.') and fd.is_dir():
                shell.run_subprocess(
                    f'cd {vconfig.go_src_dir}/{fd.name}/pkg && '
                    f'{vconfig.gobin} test ./...',
                    env=vconfig.environ)


@test.command(short_help='execute redpanda unit tests')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', None],
                                case_sensitive=False),
              default=None)
@click.option('--clang',
              help=('Test binaries compiled by clang.'),
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--args', help=('passes raw args to testing'), default=None)
def cpp(build_type, conf, clang, args):
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    if vconfig.compiler == 'clang':
        # define LD_LIBRARY_PATH for clang builds
        ld_path = (f'/lib:/lib64:/usr/local/lib:/usr/local/lib64:'
                   f'{vconfig.external_path}/lib:'
                   f'{vconfig.external_path}/lib64')
        logging.info(f'Setting LD_LIBRARY_PATH={ld_path}')
        vconfig.environ['LD_LIBRARY_PATH'] = ld_path

    args = f' {args}' if args else '-R \".*_rp(unit|bench|int)$\"'
    shell.run_subprocess(f'cd {vconfig.build_dir} && '
                         f'ctest '
                         f' {"-V" if os.environ.get("CI") else ""} ' + args,
                         env=vconfig.environ)


@test.command(short_help='print runtime dependencies of a binary')
@click.option("--binary", required=True, help="path to binary")
def print_deps(binary):
    """prints ldd output"""
    binpath = pathlib.Path(binary)
    deps = lddwrap.list_dependencies(path=binpath)
    for dep in deps:
        click.echo("Found dep: %s" % dep)
