import os
import click
import git
from datetime import date
from absl import logging
from ..vlib import shell
from ..vlib import rotate_ssh_keys as keys
from . import install_deps as deps


@click.group(short_help='execute infrastructure-related tasks.')
def infra():
    pass


@infra.command(short_help='deploy redpanda.')
@click.option('--module',
              help='The name of the module to deploy',
              required=True)
@click.option('--install-deps',
              default=False,
              help='Download and install the dependencies')
@click.option('--v-root', help='The path to the v repo', required=True)
@click.option('--ssh-key',
              help='The path where of the SSH to use (the key will be' +
              'generated if it doesn\'t exist)',
              default='~/.ssh/infra-key')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def deploy(module, install_deps, v_root, ssh_key, log, tfvars):
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(v_root)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    tfvars = tfvars + (f'private_key_path={key_path}',
                       f'public_key_path={pub_key_path}')
    _run_terraform_cmd('apply', module, install_deps, v_root, log, tfvars)


@infra.command(short_help='destroy redpanda deployment.')
@click.option('--module',
              help='The name of the module to deploy',
              required=True)
@click.option('--install-deps',
              default=False,
              help='Download and install the dependencies')
@click.option('--v-root', help='The path to the v repo', required=True)
@click.option('--ssh-key',
              help='The path to the SSH key',
              default='~/.ssh/infra-key')
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def destroy(module, install_deps, v_root, ssh_key, log, tfvars):
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(v_root)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    tfvars = tfvars + (f'private_key_path={key_path}',
                       f'public_key_path={pub_key_path}')
    _run_terraform_cmd('destroy', module, install_deps, v_root, log, tfvars)


def _run_terraform_cmd(action, module, install_deps, v_root, log, tfvars):
    logging.set_verbosity(log)
    _check_deps(v_root, install_deps)

    terraform_vars = _get_tf_vars(tfvars)
    _run_terraform(action, module, terraform_vars, v_root)


def _run_terraform(action, module, tf_vars, v_root):
    module_dir = os.path.join(v_root, 'infra', 'modules', module)
    tf_bin = deps.get_terraform_path(v_root)
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


def _check_deps(v_root, force_install):
    deps_installed = deps.check_deps_installed(v_root)
    if not deps_installed or force_install:
        deps.install_deps(v_root)


def _get_ssh_metadata(v_root):
    r = git.Repo(v_root, search_parent_directories=True)
    reader = r.config_reader()
    email = reader.get_value("user", "email")
    today = date.today()
    return f'user={email},date={today}'
