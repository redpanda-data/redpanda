import click

from ..vlib import shell


@click.group(short_help='build redpanda and rpk using a docker image.')
def dbuild():
    """This set of commands provide the ability to build binaries (or create
    deb/rpm packages) by using a frozen toolchain that has been previously used
    to produce binaries for tagged commits. Currently, only toolchains
    associated to clang-release builds are persisted (frozen) by the CI
    pipeline.

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
    shell.run_oneline(
        f'docker run --rm -ti -v $PWD:/workspace -w /workspace '
        f'gcr.io/redpandaci/builder-clang-release:{tag} vtools build {command}'
        f' --conf tools/ci/vtools-clang-release.yml --skip-external {extra}')
