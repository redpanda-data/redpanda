import click
import json
import os

from ..vlib import config
from ..vlib import shell
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
        cl.destroy(vconfig, install_deps, ssh_key, log)
        return
    cl.deploy(vconfig, install_deps, ssh_key, ssh_port, ssh_timeout,
              ssh_retries, log, tfvars)


@deploy.command(short_help='deploy redpanda.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--ssh-key',
              help='The path where of the SSH to use (the key will be' +
              'generated if it doesn\'t exist)',
              default='~/.ssh/infra-key')
@click.option('--package-file',
              help='The path to the package to be installed',
              default=None)
@click.option('--playbook', help='Ansible playbook to run', required=True)
def ansible(conf, playbook, ssh_key, package_file):
    vconfig = config.VConfig(conf)

    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', 'cluster')
    tf_bin = os.path.join(vconfig.infra_bin_dir, 'terraform')
    cmd = f'cd {module_dir} && {tf_bin} output -json ip'
    out = shell.run_oneline(cmd, env=vconfig.environ)
    ips = json.loads(out)

    os.makedirs(f'{vconfig.build_root}/ansible/', exist_ok=True)
    invfile = f'{vconfig.build_root}/ansible/hosts.ini'
    with open(invfile, 'w') as f:
        for ip in ips:
            f.write(f'{ip} ansible_user=root ansible_become=True\n')

    ansbin = f'{vconfig.build_root}/venv/v/bin/ansible-playbook'
    cmd = f'{ansbin} --private-key {ssh_key} -i {invfile} -v {playbook}'

    if package_file:
        cmd += f' -e redpanda_pkg_file={package_file}'
    vconfig.environ.update({'ANSIBLE_HOST_KEY_CHECKING': 'False'})
    shell.run_subprocess(cmd, env=vconfig.environ)
