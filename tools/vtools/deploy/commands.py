import click
import os

from absl import logging

from ..vlib import config
from ..vlib import git
from ..vlib import rotate_ssh_keys as keys
from ..vlib import shell
from ..vlib import ssh as vssh
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
@click.option('--provider',
              required=False,
              type=click.Choice(['aws', 'gcp'], case_sensitive=False))
@click.option('--destroy',
              default=False,
              help='Tear down the deployed resources')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def cluster(conf, provider, destroy, log, tfvars):
    logging.set_verbosity(log)
    vconfig = config.VConfig(conf)
    module = 'cluster'

    if destroy:
        destroy_deployment(vconfig, provider, module)
        return

    git.verify(vconfig.src_dir)
    user_email = git.get_email(vconfig.src_dir)
    user = user_email.replace('@vectorized.io', '')

    ssh_key = f'~/.ssh/vectorized/deployments/{provider}-cluster'
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    key_path, pub_key_path = keys.generate_key(abs_path)

    tfvars += (f'owner={user}', f'public_key_path={pub_key_path}')

    tf.apply(vconfig, provider, module, tfvars)


@deploy.command(short_help='Run ansible against a cluster.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--playbook',
              help='Ansible playbook to run',
              required=True,
              multiple=True)
@click.option('--ssh-key',
              help='The path where of the SSH to use (the key will be' +
              'generated if it doesn\'t exist)')
@click.option('--provider',
              required=False,
              type=click.Choice(['aws', 'gcp'], case_sensitive=False))
@click.option('--module',
              default='cluster',
              type=click.Choice(['cluster'], case_sensitive=False))
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.option('--var',
              help='Ansible variable in FOO=BAR format',
              multiple=True,
              required=False)
def ansible(conf, playbook, ssh_key, provider, module, log, var):
    """Runs a playbook against a cluster deployed with 'vtools deploy cluster'
    """
    logging.set_verbosity(log)
    vconfig = config.VConfig(conf)

    tf_out = tf.get_tf_outputs(vconfig, provider, module)
    ssh_user = tf_out['ssh_user']['value']
    os.makedirs(vconfig.ansible_dir, exist_ok=True)

    # write hosts.ini: n-1 redpanda machines; 1 monitor
    invfile = f'{vconfig.ansible_tmp_dir}/hosts.ini'
    ips = tf_out['ip']['value']
    pips = tf_out['private_ips']['value']

    if not ssh_key:
        public_key = tf_out['public_key_path']['value']
        ssh_key = public_key.replace('.pub', '')

    with open(invfile, 'w') as f:
        f.write('[redpanda]\n')
        for i, (ip, pip) in enumerate(zip(ips, pips)):
            if i + 1 == len(ips):
                f.write('[monitor]\n')
            f.write(f'{ip} ansible_user={ssh_user} ansible_become=True '
                    f'private_ip={pip} id={i}\n')

    # create extra vars flags
    evar = f'-e {" -e ".join(var)}' if var else ''

    # run given playbooks
    cmd = (
        f'cd {vconfig.ansible_dir} && ansible-playbook --private-key {ssh_key}'
        f' {evar} -i {invfile} -v {" ".join(playbook)}')

    shell.run_subprocess(cmd, env=vconfig.environ)


@deploy.command(short_help='SSH into the deployed VMs')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--provider',
              required=False,
              type=click.Choice(['aws', 'gcp'], case_sensitive=False))
@click.option('--module',
              default='cluster',
              type=click.Choice(['cluster'], case_sensitive=False))
def ssh(conf, provider, module):
    vconfig = config.VConfig(conf)
    tf_out = tf.get_tf_outputs(vconfig, provider, module)

    if not tf_out:
        logging.fatal(
            f'No deployment found for module {module} in ptovider {provider}')

    public_key = tf_out['public_key_path']['value']
    key = public_key.replace('.pub', '')
    chosen_ip = ''

    ssh_user = tf_out['ssh_user']['value']
    ip = tf_out['ip']['value']
    if isinstance(ip, list):
        ip = sorted(ip)
        length = len(ip)
        if length == 1:
            chosen_ip = ip[0]
        else:
            for i in range(0, length):
                click.echo(f'{i}: {ip[i]}')
            val = click.prompt(
                f'Please select a host from the list above (0 - {length - 1})',
                type=int)
            chosen_ip = ip[val]
    else:
        chosen_ip = ip
    vssh.remove_known_host(chosen_ip)
    vssh.add_known_host(chosen_ip)
    logging.info(f'/usr/bin/ssh -i "{key}" {ssh_user}@{chosen_ip}')
    os.execve('/usr/bin/ssh',
              ['/usr/bin/ssh', '-i', key, f'{ssh_user}@{chosen_ip}'],
              os.environ)


def destroy_deployment(vconfig, provider, module):
    if not tf.deployment_exists(vconfig, provider, module):
        logging.fatal(
            f'No deployment found for module {module} in provider {provider}.')
    tf_out = tf.get_tf_outputs(vconfig, provider, module)
    if tf_out and 'ip' in tf_out:
        # Remove the ansible cache in case previous deployed VMs shared the same
        # public IPs
        _rm_ansible_cache(vconfig, tf_out['ip']['value'])
        pub_key_path = tf_out['public_key_path']['value']
        key_path = pub_key_path.replace('.pub', '')
        os.remove(pub_key_path)
        os.remove(key_path)
    tf.destroy(vconfig, provider, module)


def _rm_ansible_cache(vconfig, ips):
    logging.debug(f'Removing ansible caches for {ips}')
    for ip in ips:
        path = f'/tmp/vectorizedio/ansible/cache/{ip}'
        if os.path.exists(path):
            os.remove(path)
