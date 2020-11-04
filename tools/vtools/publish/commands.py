import click
import os
import pathlib

from absl import logging
from ..vlib import config
from ..vlib import packagecloud as pc
from ..vlib import shell


@click.command(short_help='Publish packages to packagecloud.io.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory'),
              default=None)
@click.option(
    '--access-token',
    help=("Access token for packagecloud.io. If None is given, the "
          "'PACKAGECLOUD_TOKEN' environment variable is probed and an error "
          "is thrown if not defined. For how to obtain an access token, see "
          "https://packagecloud.io/api_token. If None"),
    default=None)
def publish(conf, access_token):
    vconfig = config.VConfig(config_file=conf)

    if vconfig.compiler != 'clang' or vconfig.build_type != 'release':
        logging.info('We only publish clang-release packages.')
        return

    tag_name = os.environ.get('TAG_NAME', None)

    if not tag_name:
        logging.info("No 'TAG_NAME' environment variable defined, skipping.")
        return

    if 'release-' not in tag_name:
        logging.info("We only upload 'release-*' builds to packagecloud.")
        return

    if not access_token:
        logging.info('No --access-token given, reading PACKAGECLOUD_TOKEN '
                     'environment variable')
        access_token = os.environ.get('PACKAGECLOUD_TOKEN', None)
        if not access_token:
            logging.fatal(f"Could not read 'PACKAGECLOUD_TOKEN' variable.")

    pc.publish_packages(vconfig, access_token)

    publish_docker_image(tag_name, f'{vconfig.build_dir}/dist/debian/')


def publish_docker_image(tag_name, pkg_dir, image_name="vectorized/redpanda"):

    # Prepare to push image to dockerhub
    tag = tag_name.lstrip('release-')
    pattern = f'redpanda_{tag}*_amd64.deb'
    # find release deb
    files = pathlib.Path(pkg_dir).glob(pattern)
    if not files:
        logging.fatal(
            f"Could not find any debian packages to install in the docker image"
        )

    pkg_path = next(files).resolve()
    build_pkg_path = 'infra/redpanda.deb'

    # Files need to be in the same directory as the Dockerfile
    os.link(pkg_path, build_pkg_path)

    docker_user = os.environ.get('DOCKER_USERNAME', None)
    docker_pass = os.environ.get('DOCKER_PASSWORD', None)

    if not (docker_user and docker_pass):
        logging.fatal('No Dockerhub credentials were found, aborting!')

    shell.run_subprocess(f'docker login -u {docker_user} -p {docker_pass}')

    logging.info(f'Building image {image_name}:v{tag_name} & latest')

    shell.run_subprocess(
        f'docker build'
        f'  -t {image_name}:{tag_name}'
        f'  -t {image_name}:latest'
        f'  -f infra/Dockerfile'
        f'  --build-arg redpanda_pkg=redpanda.deb'
        f'  infra/', )

    os.unlink(build_pkg_path)

    logging.info(f"Pushing {image_name} tags to dockerhub")
    shell.run_subprocess(f'docker push {image_name}')

    shell.run_subprocess(f'docker logout')
