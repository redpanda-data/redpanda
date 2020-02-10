import os
import click
import git
from datetime import date
from absl import logging
from ..vlib import shell
from ..vlib import rotate_ssh_keys as keys
from ..vlib import config
from . import install_deps as deps


@click.group(short_help='execute infrastructure-related tasks.')
def infra():
    pass


@infra.command(short_help='deploy redpanda.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--module',
              help='The name of the module to deploy',
              required=True)
@click.option('--install-deps',
              default=False,
              help='Download and install the dependencies')
@click.option('--ssh-key',
              help='The path where of the SSH to use (the key will be' +
              'generated if it doesn\'t exist)',
              default='~/.ssh/infra-key')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def deploy(conf, module, install_deps, ssh_key, log, tfvars):
    vconfig = config.VConfig(conf)
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(vconfig)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    tfvars = tfvars + (f'private_key_path={key_path}',
                       f'public_key_path={pub_key_path}')
    _run_terraform_cmd(vconfig, 'apply', module, install_deps, log, tfvars)


@infra.command(short_help='destroy redpanda deployment.')
@click.option('--conf',
              help=('Path to configuration file. If not given, a .vtools.yml '
                    'file is searched recursively starting from the current '
                    'working directory.'),
              default=None)
@click.option('--module',
              help='The name of the module to deploy',
              required=True)
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
def destroy(conf, module, install_deps, ssh_key, log, tfvars):
    vconfig = config.VConfig(conf)
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(vconfig)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    tfvars = tfvars + (f'private_key_path={key_path}',
                       f'public_key_path={pub_key_path}')
    _run_terraform_cmd(vconfig, 'destroy', module, install_deps, log, tfvars)


def _run_terraform_cmd(vconfig, action, module, install_deps, log, tfvars):
    logging.set_verbosity(log)
    _check_deps(vconfig, install_deps)

    terraform_vars = _get_tf_vars(tfvars)
    _run_terraform(vconfig, action, module, terraform_vars)


def _run_terraform(vconfig, action, module, tf_vars):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', module)
    tf_bin = os.path.join(vconfig.infra_bin_dir, 'terraform')
    base_cmd = f'cd {module_dir} && {tf_bin}'
    init_cmd = f'{base_cmd} init'
    shell.run_subprocess(init_cmd)
    cmd = f'{base_cmd} {action} -auto-approve {tf_vars}'
    logging.info(f'Running {cmd}')
    shell.run_subprocess(cmd)


def _get_tf_vars(tfvars):
    if tfvars == None:
        return ''
    return ' '.join([f'-var {v}' for v in tfvars])


def _check_deps(vconfig, force_install):
    deps_installed = deps.check_deps_installed(vconfig)
    if not deps_installed or force_install:
        deps.install_deps(vconfig)


def _get_ssh_metadata(vconfig):
    r = git.Repo(vconfig.src_dir, search_parent_directories=True)
    reader = r.config_reader()
    email = reader.get_value("user", "email")
    today = date.today()
    return f'user={email},date={today}'
