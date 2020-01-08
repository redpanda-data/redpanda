import click
import os

from absl import logging
from ..vlib import config
from ..vlib import packagecloud as pc


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
        logging.fatal("Expecting 'TAG_NAME' environment variable.")

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
