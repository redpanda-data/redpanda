import os
import paramiko

from absl import logging

known_hosts_file = os.path.expanduser("~/.ssh/known_hosts")


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


def run_subprocess(ip, ssh_user, ssh_key_path, cmd):
    client = paramiko.SSHClient()
    client.load_host_keys(known_hosts_file)
    k = paramiko.RSAKey.from_private_key_file(ssh_key_path)
    client.connect(ip, pkey=k, username=ssh_user)
    return client.exec_command(cmd)


def _scan_host(host, port, timeout=10, retries=0):
    exception = Exception('Maximum retries reached')
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
