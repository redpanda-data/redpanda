import click
import io
import os
import tarfile
import urllib
import shutil

from absl import logging
from ..vlib import cmake
from ..vlib import clang as llvm
from ..vlib import config
from ..vlib import shell


@click.group(short_help='install build dependencies')
def install():
    pass


@install.command(short_help='build and install external cmake projects.')
@click.option('--build-type',
              help=('Build configuration to select. If none given, the '
                    '`build.default_type` option from the vtools YAML config '
                    'is used (an error is thrown if not defined).'),
              type=click.Choice(['debug', 'release', 'dev'],
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
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    cmake.configure_build(vconfig,
                          build_external=True,
                          build_external_only=True)


@install.command(short_help='install clang from source.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def clang(conf):
    llvm.install_clang(config.VConfig(conf, clang=True))


@install.command(short_help='install go build dependencies.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def go_deps(conf):
    vconfig = config.VConfig(conf)
    with os.scandir(vconfig.go_src_dir) as it:
        for fd in it:
            if not fd.name.startswith('.') and fd.is_dir():
                shell.run_subprocess(
                    f'cd {vconfig.go_src_dir}/{fd.name} && go mod download',
                    env=vconfig.environ)
    shell.run_subprocess(
        f'cd {vconfig.go_src_dir}/rpk && '
        f'go install '
        f'  github.com/cockroachdb/crlfmt '
        f'  mvdan.cc/sh/v3/cmd/shfmt',
        env=vconfig.environ)


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
    vconfig = config.VConfig(conf)

    if os.path.isfile(f'{vconfig.go_path}/bin/go'):
        logging.info(f'Found {vconfig.go_path}/bin/go. Skipping installation.')
        return

    url = f'https://dl.google.com/go/go{version}.linux-amd64.tar.gz'

    logging.info("Downloading " + url)
    handle = urllib.request.urlopen(url)
    io_bytes = io.BytesIO(handle.read())

    logging.info(f'Extracting go tarball to {vconfig.go_path}')
    tar = tarfile.open(fileobj=io_bytes, mode='r')
    tar.extractall(path=os.path.dirname(vconfig.go_path))


@install.command(short_help='install java')
@click.option('--version',
              help="Version of Zulu java to install.",
              default='11.37.17')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def java(version, conf):
    vconfig = config.VConfig(conf)

    if os.path.isfile(f'{vconfig.java_home_dir}/bin/java'):
        logging.info(
            f'Found {vconfig.java_home_dir}/bin/java Skipping installation.')
        return

    url = f'https://cdn.azul.com/zulu/bin/zulu{version}-ca-jdk11.0.6-linux_x64.tar.gz'
    logging.info("Downloading " + url)
    handle = urllib.request.urlopen(url)
    io_bytes = io.BytesIO(handle.read())
    logging.info(f'Extracting java tarball to {vconfig.java_home_dir}')

    tar = tarfile.open(fileobj=io_bytes, mode='r')

    topdir = tar.getmembers()[0].name
    for member in tar.getmembers()[1:]:
        member.name = member.name.replace(f'{topdir}/', "")
        tar.extract(member, path=vconfig.java_home_dir)


# http://us.mirrors.quenda.co/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
@install.command(short_help='install maven')
@click.option('--version',
              help="Version of Zulu java to install.",
              default='3.6.3')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def maven(version, conf):
    vconfig = config.VConfig(conf)

    if os.path.isfile(f'{vconfig.maven_home_dir}/bin/mvn'):
        logging.info(
            f'Found {vconfig.maven_home_dir}/bin/mvn Skipping installation.')
        return

    url = f'http://us.mirrors.quenda.co/apache/maven/maven-3/3.6.3/binaries/apache-maven-{version}-bin.tar.gz'
    logging.info("Downloading " + url)
    handle = urllib.request.urlopen(url)
    io_bytes = io.BytesIO(handle.read())
    logging.info(f'Extracting maven tarball to {vconfig.maven_home_dir}')

    tar = tarfile.open(fileobj=io_bytes, mode='r')
    topdir = f'apache-maven-{version}/'
    for member in tar.getmembers():
        member.name = member.name.replace(topdir, '')
        tar.extract(member, path=vconfig.maven_home_dir)
