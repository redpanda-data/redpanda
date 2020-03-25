import click
import os

from ..vlib import config
from ..vlib import shell
from ..vlib import terraform as tf


@click.group(short_help='execute infrastructure-related tasks.')
def deploy():
    pass


@deploy.command(short_help='Deploy a set of nodes.')
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
        tf.destroy(vconfig, install_deps, ssh_key, log)
        return
    tf.apply(vconfig, install_deps, ssh_key, ssh_port, ssh_timeout,
             ssh_retries, log, tfvars)


@deploy.command(short_help='Run ansible against a cluster.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--ssh-key',
              help='The path where of the SSH to use (the key will be' +
              'generated if it doesn\'t exist)',
              default='~/.ssh/infra-key')
@click.option('--playbook', help='Ansible playbook to run', required=True)
@click.option('--var',
              help='Ansible variable in FOO=BAR format',
              multiple=True,
              required=False)
def ansible(conf, playbook, ssh_key, var):
    """Runs a playbook against a cluster deployed with 'vtools deploy cluster'
    """
    vconfig = config.VConfig(conf)

    tf_out = tf._get_tf_outputs(vconfig, 'cluster')

    # write hosts.ini
    os.makedirs(f'{vconfig.build_root}/ansible/', exist_ok=True)
    invfile = f'{vconfig.build_root}/ansible/hosts.ini'
    with open(invfile, 'w') as f:
        zipped = zip(tf_out['ip']['value'], tf_out['private_ips']['value'])
        for i, (ip, pip) in enumerate(zipped):
            f.write(f'{ip} ansible_user=root ansible_become=True '
                    f'private_ip={pip} id={i+1}\n')

    # create extra vars flags
    evar = f'-e {" -e ".join(var)}' if var else ''

    # run given playbook
    ansbin = f'{vconfig.build_root}/venv/v/bin/ansible-playbook'
    cmd = f'{ansbin} --private-key {ssh_key} {evar} -i {invfile} -v {playbook}'

    print(f'command: {cmd}')

    vconfig.environ.update({'ANSIBLE_HOST_KEY_CHECKING': 'False'})
    shell.run_subprocess(cmd, env=vconfig.environ)
