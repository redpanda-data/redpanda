import click

from . import cluster


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
def cluster(conf, install_deps, ssh_key, ssh_port, ssh_timeout, ssh_retries,
            log, tfvars):
    vconfig = config.VConfig(conf)
    cluster.deploy(vconfig, install_deps, ssh_key, ssh_port, ssh_timeout,
                   ssh_retries, log, tfvars)


@deploy.command(short_help='destroy redpanda deployment.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--install-deps',
              default=False,
              help='Download and install the dependencies')
@click.option('--ssh-key',
              help='The path to the SSH key',
              default='~/.ssh/infra-key')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def destroy(conf, install_deps, ssh_key, log, tfvars):
    vconfig = config.VConfig(conf)
    cluster.destroy(vconfig, install_deps, ssh_key, log, tfvars)
