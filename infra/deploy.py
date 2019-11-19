#!/usr/bin/env python3

import argparse
import logging
import os
import shutil
import sys

root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

sys.path.append(os.path.join(root_dir, 'tools'))
sys.path.append(os.path.dirname(__file__))

import cli
import log
import shell
import install_deps
import rotate_ssh_keys as keys

logger = logging.getLogger('rp')


def _get_tf_vars(vs):
    if vs == None:
        return ''
    return ' '.join([f'-var {v}' for v in vs])


def _run_terraform(action, module, tf_vars):
    key_name = 'internal_key'
    home = os.environ['HOME']
    keys_dir = os.path.join(home, '.ssh', 'vectorized', 'current')
    priv_key_relpath = os.readlink(os.path.join(keys_dir, key_name))
    pub_key_relpath = os.readlink(os.path.join(keys_dir, f'{key_name}.pub'))
    priv_key_path = os.path.join(keys_dir, priv_key_relpath)
    pub_key_path = os.path.join(keys_dir, pub_key_relpath)
    module_dir = os.path.join(root_dir, 'infra', 'modules', module)
    tf_bin = install_deps.get_terraform_path()
    cmd = f'''cd {module_dir} && \
{tf_bin} {action} -auto-approve \
-var 'private_key_path={priv_key_path}' \
-var 'public_key_path={pub_key_path}' \
{tf_vars}'''
    logger.info(f'Running {cmd}')
    shell.run_subprocess(cmd)


def main():
    parser = _build_parser()
    opts = parser.parse_args()
    log.set_logger_for_main(getattr(logging, opts.log.upper()))

    if opts.action == 'apply' and keys.needs_rotation():
        logger.error('Your keys need rotation.')
        logger.error('Please run tools/rotate_ssh_keys.py before deploying.')
        sys.exit(1)

    deps_installed = install_deps.check_deps_installed()
    if not deps_installed or opts.install_deps:
        install_deps.install_deps()

    tf_vars = _get_tf_vars(opts.vars)
    _run_terraform(opts.action, opts.module, tf_vars)


def _build_parser():
    parser = argparse.ArgumentParser(description='Deploy test environments')
    parser.add_argument('--install-deps',
                        type=cli.str2bool,
                        default='false',
                        help='Download and install the dependencies')
    parser.add_argument(
        '--action',
        type=str,
        choices=['apply', 'destroy'],
        default='apply',
        help='The terraform action to perform. [apply, destroy]')
    parser.add_argument(
        '--vars',
        nargs='+',
        type=str,
        help="A list of variables of the form 'key=value' to pass to terraform"
    )
    parser.add_argument('--module',
                        type=str,
                        default='cluster',
                        help="The name of the plan to build")
    parser.add_argument(
        '--log',
        type=str,
        choices=['critical', 'error', 'warning', 'info', 'debug'],
        default='info',
        help="The log level")
    return parser


if __name__ == '__main__':
    main()
