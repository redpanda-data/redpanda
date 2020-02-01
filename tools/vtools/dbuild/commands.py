import click
import os

from absl import logging
from ..vlib import shell


@click.group(short_help='build redpanda and rpk using a docker image.')
def dbuild():
    """This set of commands provide the ability to build binaries (or create
    deb/rpm packages) by using a frozen toolchain that has been previously used
    to produce binaries for tagged commits. Currently, only toolchains
    associated to clang-release builds are persisted (frozen) by the CI
    pipeline. The folder used for these builds is $VROOT/dbuild and not build/
    of non-containerized builds.

    Each command in this group operates in the following way: a Docker image is
    fetched, a container is instantiated from it, and the internal vtools
    installation is invoked. For example, to build the redpanda binary (for
    the current working directory) using the toolchain that was used to produce
    release 0.1:

      vtools dbuild cpp --ref release-0.1

    Note that an older frozen toolchain might not be able to build the current
    working directory as it might be missing newer build dependencies;
    conversely, a newer toolchain might not be able to build older commits.

    To make use of this group of commands, Docker needs to be installed and
    credentials for GCP's container registry need to be properly configured.
    The gcloud SDK can be used to configure docker:

      gcloud auth configure-docker --project=redpanda-ci
    """
    pass


@dbuild.command(short_help='build the redpanda binary')
@click.option('--ref',
              help="Version of toolchain to use (a git ref).",
              default=None,
              required=True)
def cpp(ref):
    _run_in_docker(ref, 'cpp')


@dbuild.command(short_help='build the rpk binary')
@click.option('--ref',
              help="Version of toolchain to use (a git ref).",
              default=None,
              required=True)
def go(ref):
    _run_in_docker(ref, 'go')


@dbuild.command(short_help='create rpm and deb packages')
@click.option('--ref',
              help="Version of toolchain to use (a git ref).",
              default=None,
              required=True)
def pkg(tag):
    _run_in_docker(tag, 'pkg', '--format deb --format rpm')


def _run_in_docker(tag, command, extra=''):
    if not os.path.exists('.git/'):
        logging.fatal(
            "Unable to find .git/ folder. This command needs to be executed "
            "from the project's root folder.")
    shell.run_oneline(
        f'docker run --rm -ti'
        f'  -w /workspace -v $PWD:/workspace -v $PWD/dbuild:/workspace/build'
        f'  gcr.io/redpandaci/builder-clang-release:{tag}'
        f'    vtools build {command} --conf tools/ci/vtools-clang-release.yml'
        f'      --skip-external {extra}')
