import json
import click
from absl import logging
from ..vlib import rotate_ssh_keys as keys

@click.group()
def ssh():
    pass

@ssh.command()
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
def rotate_ssh_keys(log):
    logging.set_verbosity(log)
    if not keys.needs_rotation():
        logging.info("All good!")
        return 0

    keys.rotate_ssh_keys()
    logging.info("Remember:")
    logging.info("1. Use your external key for github & external accounts")
    logging.info("2. Use your internal key for VPN systems")
    logging.info("3. Use your deploy key for our production systems")
