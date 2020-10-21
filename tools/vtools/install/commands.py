import click
import io
import os
import tarfile
import urllib

from absl import logging
from ..vlib import cmake
from ..vlib import clang as llvm
from ..vlib import config
from ..vlib import install_deps
from ..vlib import shell
from ..vlib import http


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
@click.option('--enable-dpdk',
              help=('Enable DPDK support'),
              default=None,
              type=bool)
def cpp_deps(build_type, conf, clang, enable_dpdk):
    # enable dpdk by default on release builds
    vconfig = config.VConfig(config_file=conf,
                             build_type=build_type,
                             clang=clang)

    if enable_dpdk is None:
        enable_dpdk = (vconfig.build_type == "release")

    cmake.configure_build(vconfig,
                          build_external=True,
                          build_external_only=True,
                          enable_dpdk=enable_dpdk)


@install.command(short_help='install clang from source.')
@click.option('--fetch',
              help=('Download and uncompress source; skip build.'),
              is_flag=True)
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def clang(conf, fetch):
    llvm.install_clang(config.VConfig(conf, clang=True), download_only=fetch)


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
    http.download_and_extract(url, 'java', vconfig.java_home_dir, None, 1)


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

    topdir = f'apache-maven-{version}/'
    url = f'https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-{version}-bin.tar.gz'
    http.download_and_extract(url, 'maven', vconfig.maven_home_dir, topdir)


@install.command(short_help='install infrastructure dependencies.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def infra_deps(conf):
    """Installs dependencies required to work with infrastructure. These
    dependencies cannot be installed with the OS package manager or pip.
    Specifically, this command installs terraform, the AWS CLI, and
    ansible-galaxy roles.
    """
    vconfig = config.VConfig(conf)

    # install terraform
    install_deps.install_deps(vconfig, deps=['terraform'])

    # awscli and ansible roles (skip in CI)
    if vconfig.environ['CI'] == "0":
        install_deps.install_deps(vconfig, deps=['awscli'])
        reqs = f'{vconfig.ansible_dir}/requirements.yml'
        shell.run_subprocess(f'ansible-galaxy install -r {reqs}',
                             env=vconfig.environ)


@install.command(short_help='install nodejs')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
def js(conf):
    vconfig = config.VConfig(conf)
    if os.path.exists(f'{vconfig.node_build_dir}/bin/node'):
        logging.info(
            f'Found {vconfig.node_build_dir}/bin/node. Skipping installation.')
    else:
        url = f'https://nodejs.org/dist/v12.16.1/node-v12.16.1-linux-x64.tar.xz'
        pkg = 'node-v12.16.1-linux-x64/'
        http.download_and_extract(url, 'node', vconfig.node_build_dir, pkg)
        os.rmdir(f'{vconfig.node_build_dir}/{pkg}')

    #install nodejs dependencies
    logging.info("Installing nodejs dependencies")
    shell.run_subprocess(
        f'cp {vconfig.node_src_dir}/package.json {vconfig.node_build_dir} && '
        f'cp {vconfig.node_src_dir}/package-lock.json {vconfig.node_build_dir} && '
        f'cp {vconfig.node_src_dir}/generate-entries.sh {vconfig.node_build_dir} && '
        f'cd {vconfig.node_build_dir} && npm install',
        env=vconfig.environ)
