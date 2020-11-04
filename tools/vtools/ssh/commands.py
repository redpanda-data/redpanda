import os
import click
from absl import logging
from ..vlib import rotate_ssh_keys as keys
from ..vlib import ssh as vssh

known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")


@click.group(short_help='utilities for dealing with ssh keys')
def ssh():
    pass


@ssh.command(short_help='execute an ssh key rotation.')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
def rotate_keys(log):
    logging.set_verbosity(log)
    if not keys.needs_rotation():
        logging.info("All good!")
        return 0

    keys.rotate_ssh_keys()
    logging.info("Remember:")
    logging.info("1. Use your external key for github & external accounts")
    logging.info("2. Use your internal key for VPN systems")
    logging.info("3. Use your deploy key for our production systems")


@ssh.command(short_help='add a host to the known_hosts list.')
@click.option(
    '--host',
    help="The hostname or IP to add. If ommitted, the IP will be added instead."
)
@click.option('--port', default=22, help="The host's SSH port.")
@click.option('--timeout', default=10, help="The host's SSH port.")
@click.option('--retries', default=0, help="The number of retries.")
def add_known_host(host, port, timeout, retries):
    vssh.add_known_host(host, port, timeout, retries)
