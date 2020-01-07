import click
import io
import os
import shutil
import tarfile
import urllib

from absl import logging
from ..vlib import cmake
from ..vlib import clang as llvm
from ..vlib import config
from ..vlib import shell


@click.group()
def install():
    pass


@install.command(short_help='build and install external cmake projects.')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', None],
                                case_sensitive=False),
              default=None)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--clang',
              help=('Build external projects using clang, including libc++ '
                    'and libc++abi.'),
              is_flag=True)
def cpp_deps(build_type, conf, clang):
    vconfig = config.VConfig(config_file=conf, build_type=build_type,
                             clang=clang)

    cmake.configure_build(vconfig,
                          build_external=True,
                          build_external_only=True)

    # FIXME https://app.asana.com/0/1149841353291489/1153763539998305
    src = (f'{vconfig.build_dir}/v_deps_build/seastar-prefix/'
           f'src/seastar-build/apps/iotune/iotune')
    dst = f'{vconfig.external_path}/bin/iotune'
    if os.path.isfile(dst):
        os.remove(dst)
    shutil.move(src, dst)


@install.command(short_help='install clang from source.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def clang(conf):
    llvm.install_clang(config.VConfig(conf, build_type='ignored', clang=True))


@install.command(short_help='install go build dependencies.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def go_deps(conf):
    vconfig = config.VConfig(conf, build_type='ignored')
    shell.run_subprocess(f'cd {vconfig.go_src_dir} && go mod download')


@install.command(short_help='install the go compiler.')
@click.option('--version',
              help="Version of Go compiler to install.",
              default='1.13.5')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def go_compiler(version, conf):
    vconfig = config.VConfig(conf, build_type='ignored')

    url = f'https://dl.google.com/go/go{version}.linux-amd64.tar.gz'

    logging.info("Downloading " + url)
    handle = urllib.request.urlopen(url)
    io_bytes = io.BytesIO(handle.read())

    logging.info(f'Extracting go tarball to {vconfig.go_path}')
    tar = tarfile.open(fileobj=io_bytes, mode='r')
    tar.extractall(path=os.path.dirname(vconfig.go_path))
