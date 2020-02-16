import click

from ..vlib import config
from . import cluster as cl


@click.group(short_help='execute infrastructure-related tasks.')
def deploy():
    pass


@deploy.command(short_help='deploy redpanda.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--install-deps',
              default=False,
              help='Download and install the dependencies')
@click.option('--destroy',
              default=False,
              help='Tear down the deployed resources')
@click.option('--ssh-key',
              help='The path where of the SSH to use (the key will be' +
              'generated if it doesn\'t exist)',
              default='~/.ssh/infra-key')
@click.option('--ssh-port', default=22, help='The SSH port on the remote host')
@click.option('--ssh-timeout',
              default=60,
              help='The amount of time (in secs) to wait for an SSH connection'
              )
@click.option('--ssh-retries',
              default=3,
              help='How many times to retry the SSH connection')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def cluster(conf, install_deps, destroy, ssh_key, ssh_port, ssh_timeout,
            ssh_retries, log, tfvars):
    vconfig = config.VConfig(conf)
    if destroy:
        cl.destroy(vconfig, install_deps, ssh_key, log, tfvars)
        return
    cl.deploy(vconfig, install_deps, ssh_key, ssh_port, ssh_timeout,
              ssh_retries, log, tfvars)
