import subprocess

import click

from ..vlib import config
from ..vlib import docker


@click.group(short_help='build redpanda and rpk using a docker image.')
def dbuild():
    """This set of commands provide the ability to build binaries (or create
    deb/rpm packages) by using a frozen toolchain. To make use of this group of
    commands, Docker needs to be installed and credentials for GCP's container registry
    need to be properly configured. The gcloud SDK can be used to configure docker:

      gcloud auth configure-docker --project=redpanda-ci
    """
    pass


@dbuild.command(short_help='build the redpanda binary')
def cpp():
    docker._run_in_docker('cpp')


@dbuild.command(short_help='build the rpk binary')
def go():
    docker._run_in_docker('golang')


@dbuild.command(short_help='create rpm and deb packages')
def pkg():
    docker._run_in_docker('pkg', '--format deb --format rpm')


@dbuild.command(
    short_help='Obtain the registry path (including tag) for given image.')
@click.argument('IMAGE')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--clang',
              help='Build cpp dependencies with clang.',
              is_flag=True)
@click.option('--build-type',
              help='Build type for cpp dependencies.',
              is_flag=True)
def show_sha(clang, conf, build_type, image):
    """Show the registry path, including its sha, for one of the images that make up the
    toolchain stack. Possible values for IMAGE are: ``base``, ``clang``, ``golang`` and
    ``builder``. The ``--clang`` and ``--build-type`` values are only relevant for
    ``builder``.

    The SHA of an image is obtained by hashing the contents of its file dependencies, as
    well as the SHA of their image dependencies. For example, the SHA1 of the ``clang``
    image is obtained by hashing the ``tools/vtools/vlib/clang.py`` and
    ``cmake/caches/llvm.cmake`` files, plus the SHA of the ``base`` image.
    """
    vconfig = config.VConfig(conf)
    toolchain_images = docker._get_toolchain_image_metadata(vconfig)
    print(f'{toolchain_images[image]["name_tag"]}')


@dbuild.command(short_help='Build a docker image containing the toolchain.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option('--clang',
              help='Build cpp dependencies with clang.',
              is_flag=True)
@click.option('--build-type',
              help='Build type for cpp dependencies.',
              is_flag=True)
def toolchain(clang, conf, build_type):
    """Efficiently build a docker image containing the toolchain to build redpanda and
    rpk, with the option of building C++ dependencies with GCC/Clang and multiple build
    types (release and debug). The build of this image is done in an efficient way by
    taking into account previously built images pushed to gcr.io. The builder image is
    tagged with the ``latest`` after successfully building it.
    """
    vconfig = config.VConfig(conf)
    toolchain_images = docker._get_toolchain_image_metadata(vconfig)
    docker._get_image('builder', toolchain_images, vconfig)

    # tag the image with ``latest`` so its available for being used without
    # having to reference the specific SHA
    subprocess.run(
        f'docker tag'
        f' {toolchain_images["builder"]["name_tag"]}'
        f' {toolchain_images["builder"]["name"]}:latest',
        shell=True,
        check=True)
