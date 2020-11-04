from os.path import join

import click
import docker
import glob
import json
import lddwrap
import os
import pathlib
import shutil
import shlex
import time
import yaml
import unittest

from absl import logging

from ..build import commands as vbuild
from ..vlib import config
from ..vlib import terraform
from ..vlib import shell


@click.group(short_help='run unit and integration tests')
def test():
    pass


@test.command(short_help='rpk unit tests')
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
                vbuild.generate_wasm_dependency(vconfig)
                shell.run_subprocess(
                    f'cd {vconfig.go_src_dir}/{fd.name}/pkg && go test ./...',
                    env=vconfig.environ)


@test.command(short_help='python tests')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def py(conf):
    vconfig = config.VConfig(conf)
    test_loader = unittest.defaultTestLoader
    test_runner = unittest.TextTestRunner(verbosity=3, failfast=True)
    test_suite = test_loader.discover(
        start_dir=f"{vconfig.src_dir}/tools/vtools", pattern="*_test.py")
    test_runner.run(test_suite)


@test.command(short_help='redpanda unit tests')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
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

    if vconfig.environ['CI'] != 0:
        REQUIRED_AIO_MAX = 1048576
        current_aio_max = int(shell.run_oneline("sysctl -nb fs.aio-max-nr"))

        if current_aio_max < REQUIRED_AIO_MAX:
            shell.run_oneline(f"sysctl -w fs.aio-max-nr={REQUIRED_AIO_MAX}")

    args = f' {args}' if args else '-R \".*_rp(unit|bench|int)$\"'
    shell.run_subprocess(f'cd {vconfig.build_dir} && '
                         f'ctest '
                         f' {"-V" if os.environ.get("CI") else ""} ' + args,
                         env=vconfig.environ)


@test.command(short_help='redpanda integration (ducktape) tests')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
                                case_sensitive=False),
              default=None)
@click.option('--clang', help='Test binaries compiled by clang.', is_flag=True)
@click.option('--debug', help='Enable ducktape debug output.', is_flag=True)
@click.option('--conf',
              help=('Path to vtools configuration file. If not given, a '
                    '.vtools.yml file is searched recursively starting from '
                    'the current working directory'),
              default=None)
@click.option('--test',
              help='A test case to run. Multiple allowed',
              multiple=True)
@click.option('--skip-build',
              help='Skip building test binaries before running tests',
              is_flag=True,
              default=False)
@click.pass_context
def ducky(ctx, build_type, clang, debug, conf, test, skip_build):
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    if not skip_build:
        # prepare build artifacts via `vtools build pkg`
        ctx.invoke(vbuild.pkg,
                   build_type=build_type,
                   clang=clang,
                   conf=conf,
                   format=["dir"])

    # prepare ducktape `--globals` options. these are passed to ducktape as a
    # set of key-value pairs encoded as a shell escaped json dictionary string.
    ducktape_global_params = dict(
        redpanda_build_type=vconfig.build_type,
        redpanda_compiler=vconfig.compiler,
    )

    ducktape_params = [
        "--globals",
        shlex.quote(json.dumps(ducktape_global_params))
    ]

    if debug:
        ducktape_params.append("--debug")

    env = dict(
        TC_PATHS=" ".join(test),
        _DUCKTAPE_OPTIONS=" ".join(ducktape_params),
        **vconfig.environ,
    )

    runner = os.path.join(vconfig.src_dir, "tests/docker", "run_tests.sh")
    shell.run_subprocess(runner, env)


@test.command(short_help='print runtime dependencies of a binary')
@click.option("--binary", required=True, help="path to binary")
def print_deps(binary):
    """prints ldd output"""
    binpath = pathlib.Path(binary)
    deps = lddwrap.list_dependencies(path=binpath)
    for dep in deps:
        click.echo("Found dep: %s" % dep)


@test.command(short_help='runs npm test on build/node')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def js(conf):
    vconfig = config.VConfig(conf)

    shell.run_subprocess(f'cd {vconfig.node_build_dir} && '
                         f'npm test',
                         env=vconfig.environ)
