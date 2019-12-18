import json
import os
import click
import paramiko
from absl import logging
from ..vlib import rotate_ssh_keys as keys

known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")


@click.group()
def ssh():
    pass


@ssh.command()
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


@ssh.command()
@click.option(
    '--host',
    help="The hostname or IP to add. If ommitted, the IP will be added instead."
)
@click.option('--port', default=22, help="The host's SSH port.")
@click.option('--timeout', default=10, help="The host's SSH port.")
@click.option('--retries', default=0, help="The number of retries.")
def add_known_host(host, port, timeout, retries):
    """
    Adds the host and its public key to ~/.ssh/known_hosts.

    Attempts a connection to the host's SSH server, retrying `retries` times
    if an attempt fails, and downloads the server's public key to store it in
    ~/.ssh/known_hosts, which prevents `scp` and `ssh` from asking for
    confirmation when a connection is made to a server.

    Parameters:
    host (string): The IP or domain of the remote host.
    port (int): The host's SSH server port.
    timeout (float): The timeout for each connection attempt.
    retries (int): The number of attempts that should be made before failing.

    Returns:
    paramiko.PKey: The server's public key.
    """
    key = _scan_host(host, port, timeout, retries)
    if key is None:
        logging.error("The host's public key couldn't be retrieved")
        return
    hostkeys = paramiko.hostkeys.HostKeys()
    hostkeys.load(known_hosts_file)
    hostkeys.add(host, key.get_name(), key)
    hostkeys.save(known_hosts_file)


def _scan_host(host, port, timeout=10, retries=0):
    exception = None
    while retries > 0:
        retries -= 1
        try:
            t = paramiko.transport.Transport(f'{host}:{port}')
            t.start_client(timeout=timeout)
            return t.get_remote_server_key()
        except Exception as e:
            logging.error(str(e))
            logging.info(f'Retries left: {retries}')
    raise exception
