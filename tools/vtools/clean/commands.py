import click

from ..vlib import cmake
from ..vlib import config

@click.group()
def clean():
    pass


@clean.command(short_help='Clean build configuration (cmake) cache.')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', None],
                                case_sensitive=False),
              default=None)
@click.option('--clang',
              help='Build clang and install in <build-root>/llvm/llvm-bin.',
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def build_config(build_type, clang, conf):
    vconfig = config.VConfig(config_file=conf, build_type=build_type,
                             clang=clang)
    cmake.rm_cache(vconfig)

@clean.command(short_help='Clean build.')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', None],
                                case_sensitive=False),
              default=None)
@click.option('--clang',
              help='Build clang and install in <build-root>/llvm/llvm-bin.',
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def build(build_type, clang, conf):
    vconfig = config.VConfig(config_file=conf, build_type=build_type,
                             clang=clang)
    shell.run_subprocess(f'cd {vconfig.build_dir} && ninja -t clean')
