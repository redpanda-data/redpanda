import os
import re

import git
import json

from absl import logging
from datetime import date

from ..vlib import rotate_ssh_keys as keys
from ..vlib import shell

tfvars_key = 'deploy.cluster.tf.vars'

known_tfvars = [
    'nodes',
    'distro',
    'instance_type',
    'local_package_abs_path',
    'ssh_timeout',
    'ssh_retries',
    'packagecloud_token',
    'private_key_path',
    'public_key_path',
]


def apply(vconfig, ssh_key, ssh_port, ssh_timeout, ssh_retries, log, tfvars):
    if tfvars_key in vconfig.kv:
        logging.error(
            f'''Found another deployment with vars {vconfig.kv[tfvars_key]}.
Please run `vtools deploy cluster --destroy true` before deploying again.''')
        return
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(vconfig)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    tfvars = tfvars + (f'private_key_path={key_path}',
                       f'public_key_path={pub_key_path}')
    terraform_vars = _parse_tf_vars(tfvars)
    vconfig.kv[tfvars_key] = terraform_vars
    module = 'cluster'
    _run_terraform_cmd(vconfig, 'apply', module, log, terraform_vars)


def destroy(vconfig, ssh_key, log):
    if tfvars_key not in vconfig.kv:
        logging.info('No cluster deployments found. Nothing to destroy.')
        return
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(vconfig)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    _run_terraform_cmd(vconfig, 'destroy', 'cluster', log,
                       vconfig.kv[tfvars_key])
    del vconfig.kv[tfvars_key]


def _run_terraform_cmd(vconfig, action, module, log, tfvars):
    logging.set_verbosity(log)
    _run_terraform(vconfig, action, module, tfvars)


def _run_terraform(vconfig, action, module, tf_vars):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', module)
    base_cmd = f'cd {module_dir} && terraform'
    init_cmd = f'{base_cmd} init'
    shell.run_subprocess(init_cmd, env=vconfig.environ)
    cmd = f'{base_cmd} {action} -auto-approve {tf_vars}'
    logging.info(f'Running {cmd}')
    shell.run_subprocess(cmd, env=vconfig.environ)


def _parse_tf_vars(tfvars):
    if tfvars is None:
        return ''
    for v in tfvars:
        res = re.match(r'(\w+)=[.\d\S]+', v)
        if res is None:
            logging.fatal(
                f'"{v}" does not match the required "key=value" format')
        key = res.group(1)
        if key not in known_tfvars:
            logging.fatal(
                f'Unrecognized variable "{key}". Allowed vars: {known_tfvars}')
    return ' '.join([f'-var {v}' for v in tfvars])


def _get_tf_outputs(vconfig, module):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', module)
    cmd = f'cd {module_dir} && terraform output -json'
    logging.info(f'Running {cmd}')
    out = shell.raw_check_output(cmd, env=vconfig.environ)
    return json.loads(out)


def _get_ssh_metadata(vconfig):
    r = git.Repo(vconfig.src_dir, search_parent_directories=True)
    reader = r.config_reader()
    email = reader.get_value("user", "email")
    today = date.today()
    return f'user={email},date={today}'
