import click
import math
import os

from absl import logging
from ..vlib import cmake
from ..vlib import config
from ..vlib import packaging
from ..vlib import shell
from ..vlib import git


@click.group(short_help='build redpanda and rpk binaries as well as packages')
def build():
    pass


@build.command(short_help='build the redpanda binary.')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
                                case_sensitive=False),
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
@click.option(
    '--targets',
    help=('ninja targets to build, for example, --targets=redpanda '
          'will effectively invoke ninja -C build/<type>/clang redpanda'),
    default=None)
def cpp(build_type, conf, skip_external, clang, reconfigure, targets):
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

    # assign jobs so that we have 2.0GB/core (4.0GB/core on CI)
    if vconfig.environ["CI"] == "1":
        gb_per_core = 3
    else:
        gb_per_core = 2

    total_memory = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    num_jobs = math.floor(total_memory / (gb_per_core * 1024.**3))
    num_jobs = min(num_jobs, os.sysconf('SC_NPROCESSORS_ONLN'))
    cmd = f'cd {vconfig.build_dir} && ninja -j{num_jobs}'
    if targets != None:
        cmd = f"{cmd} {targets}"
    shell.run_subprocess(cmd, env=vconfig.environ)


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
    vconfig = config.VConfig(config_file=conf)
    allowed_argets = ['rpk', 'metrics']
    build_flags = '-buildmode=pie -v -a -tags netgo'
    vconfig = config.VConfig(conf)
    os.makedirs(vconfig.go_out_dir, exist_ok=True)

    if len(targets) == 0:
        targets = allowed_argets

    for t in targets:
        if t not in allowed_argets:
            logging.fatal(f'Unknown target {t}')

        if t == "rpk":
            tag = git.get_latest_tag(vconfig.src_dir)
            sha = git.get_head_sha(vconfig.src_dir)
            pkg = 'vectorized/pkg/cli/cmd/version'
            build_flags += f' -ldflags "-X {pkg}.version={tag} -X {pkg}.rev={sha}"'

        shell.run_subprocess(
            f'cd {vconfig.go_src_dir}/{t} && '
            f'go build {build_flags} -o {vconfig.go_out_dir} ./...',
            env=vconfig.environ)


@build.command(short_help='build tar, deb or rpm packages.')
@click.option('--format',
              help="format to build ('rpm', 'deb', 'tar', and 'dir').",
              multiple=True,
              required=True)
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
                                case_sensitive=False),
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
        if f not in ['tar', 'deb', 'rpm', 'dir']:
            logging.fatal(f'Unknown format {format}')

    vconfig.product = "redpanda"
    packaging.create_packages(vconfig, format)
    if f == "tar" or f == 'dir':
        vconfig.product = "pandaproxy"
        packaging.create_packages(vconfig, format)


@build.command(short_help='build vectorized java applications')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option(
    '--targets',
    help="target to build ('kafka-verifier', 'compacted-log-verifier').",
    multiple=True)
def java(conf, targets):
    allowed_targets = ['kafka-verifier', 'compacted-log-verifier']
    vconfig = config.VConfig(conf)
    os.makedirs(vconfig.java_build_dir, exist_ok=True)
    os.makedirs(vconfig.java_bin_dir, exist_ok=True)

    if len(targets) == 0:
        targets = allowed_targets

    for t in targets:
        if t not in allowed_targets:
            logging.fatal(f'Unknown target {t}')

        shell.run_subprocess(
            f'cd {vconfig.java_src_dir}/{t} && '
            f'mvn clean package --batch-mode -DbuildDir={vconfig.java_build_dir}/{t}',
            env=vconfig.environ)


@build.command(short_help='build v/js applications')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def js(conf):
    vconfig = config.VConfig(conf)

    shell.run_subprocess(f'cd {vconfig.node_build_dir} && '
                         f'npm run build',
                         env=vconfig.environ)
