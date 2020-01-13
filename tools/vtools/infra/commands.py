import os
import click
from absl import logging
from ..vlib import rotate_ssh_keys as keys
from ..vlib import shell
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
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def deploy(module, install_deps, v_root, log, tfvars):
    _run_terraform_cmd('apply', module, install_deps, v_root, log, tfvars)


@infra.command(short_help='destroy redpanda deployment.')
@click.option('--module',
              help='The name of the module to deploy',
              required=True)
@click.option('--install-deps',
              default=False,
              help='Download and install the dependencies')
@click.option('--v-root', help='The path to the v repo', required=True)
@click.option('--log',
              default='info',
              type=click.Choice(['debug', 'info', 'warning', 'error', 'fatal'],
                                case_sensitive=False))
@click.argument('tfvars', nargs=-1)
def destroy(module, install_deps, v_root, log, tfvars):
    _run_terraform_cmd('destroy', module, install_deps, v_root, log, tfvars)


def _run_terraform_cmd(action, module, install_deps, v_root, log, tfvars):
    logging.set_verbosity(log)
    _check_keys()
    _check_deps(v_root, install_deps)

    terraform_vars = _get_tf_vars(tfvars)
    _run_terraform(action, module, terraform_vars, v_root)


def _run_terraform(action, module, tf_vars, v_root):
    key_name = 'internal_key'
    home = os.environ['HOME']
    keys_dir = os.path.join(home, '.ssh', 'vectorized', 'current')
    priv_key_relpath = os.readlink(os.path.join(keys_dir, key_name))
    pub_key_relpath = os.readlink(os.path.join(keys_dir, f'{key_name}.pub'))
    priv_key_path = os.path.join(keys_dir, priv_key_relpath)
    pub_key_path = os.path.join(keys_dir, pub_key_relpath)
    module_dir = os.path.join(v_root, 'infra', 'modules', module)
    tf_bin = deps.get_terraform_path(v_root)
    cmd = f'''cd {module_dir} && \
{tf_bin} {action} -auto-approve \
-var 'private_key_path={priv_key_path}' \
-var 'public_key_path={pub_key_path}' \
{tf_vars}'''
    logging.info(f'Running {cmd}')
    shell.run_subprocess(cmd)


def _get_tf_vars(tfvars):
    if tfvars == None:
        return ''
    return ' '.join([f'-var {v}' for v in tfvars])


def _check_keys():
    if keys.needs_rotation():
        if click.confirm(
                'Your keys need rotation. Would you like to do it now?'):
            keys.rotate_ssh_keys()
        else:
            logging.info('Please run `vtools rotate-ssh-keys` and try again.')


def _check_deps(v_root, force_install):
    deps_installed = deps.check_deps_installed(v_root)
    if not deps_installed or force_install:
        deps.install_deps()
