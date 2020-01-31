import click
import math
import os

from absl import logging
from ..vlib import cmake
from ..vlib import config
from ..vlib import packaging
from ..vlib import shell


@click.group(short_help='build redpanda and rpk binaries as well as packages')
def build():
    pass


@build.command(short_help='build the redpanda binary.')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release'], case_sensitive=False),
              default=None)
@click.option('--skip-external',
              help='Do not build external projects.',
              is_flag=True)
@click.option('--clang',
              help='Build clang and install in <build-root>/llvm/llvm-bin.',
              is_flag=True)
@click.option('--reconfigure',
              help='Run cmake regardless of whether cmake cache exists.',
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def cpp(build_type, conf, skip_external, clang, reconfigure):
    """
    Build the `redpanda` binary using the system's default compiler. To use
    clang, the `build.clang` YAML configuration option needs to be specified,
    which should be pointing to the install prefix for clang (e.g. /usr/ or
    /usr/local). Alternatively, the `--clang` flag can be given, in which case
    it is assumed to be available in `llvm/llvm-bin` inside the build root
    folder. If it is not found there, it is installed from source. The
    `--clang` flag overrides the value in the `build.clang` YAML configuration
    option.

    In addition, external dependencies are installed from source unless the
    `--skip-external` flag is given. If `--skip-external` is given, the build
    expects to find it in the folder pointed by the `build.external` option of
    the YAML config file. If that value is not given, external dependencies are
    expected to be in the default folder inside the build root (v_deps_install/
    folder).
    """
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    if not cmake.cache_exists(vconfig) or reconfigure:
        cmake.configure_build(vconfig,
                              build_external=(not skip_external),
                              build_external_only=False)
    else:
        logging.info(f'Found cmake cache, skipping cmake configuration.')

    # assign jobs so that we have 2.0GB/core
    total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    num_jobs = math.floor(total_memory / (2 * 1024.**3))
    num_jobs = min(num_jobs, os.sysconf('SC_NPROCESSORS_ONLN'))
    shell.run_subprocess(f'cd {vconfig.build_dir} && ninja -j{num_jobs}')


@build.command(short_help='build the rpk binary')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--targets',
              help="target to build ('rpk', 'metrics').",
              multiple=True)
def go(conf, targets):
    allowed_argets = ['rpk', 'metrics']
    build_flags = '-buildmode=pie -v -a -tags netgo'
    vconfig = config.VConfig(conf)
    os.makedirs(vconfig.go_out_dir, exist_ok=True)

    if len(targets) == 0:
        targets = allowed_argets

    for t in targets:
        if t not in allowed_argets:
            logging.fatal(f'Unknown target {t}')

        shell.run_subprocess(
            f'cd {vconfig.go_src_dir}/{t} && '
            f'{vconfig.gobin} build {build_flags} -o {vconfig.go_out_dir} ./...')


@build.command(short_help='build tar, deb or rpm packages.')
@click.option('--format',
              help="format to build ('rpm', 'deb' and 'tar').",
              multiple=True,
              required=True)
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release'], case_sensitive=False),
              default=None)
@click.option('--clang',
              help='Use binary files that were compiled with clang.',
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def pkg(build_type, clang, conf, format):
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    for f in format:
        if f not in ['tar', 'deb', 'rpm']:
            logging.fatal(f'Unknown format {format}')

    packaging.create_packages(vconfig, format, vconfig.build_type)
