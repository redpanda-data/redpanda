import os
import re

import json

from absl import logging
from datetime import date

from ..vlib import shell

tfvars_key = 'deploy.cluster.tf.vars'

known_tfvars = {
    'aws': {
        'cluster': [
            'nodes',
            'distro',
            'owner',
            'instance_type',
            'public_key_path',
            'distro_ami',
            'distro_ssh_user',
        ],
    },
    'gcp': {
        'cluster': [
            'region',
            'zone',
            'owner',
            'nodes',
            'disks',
            'image',
            'machine_type',
            'ssh_user',
            'public_key_path',
            'ssh_user',
        ],
    },
}


def apply(vconfig, provider, module, tfvars):
    # Check the legacy key first, in case there's a remaining deployment using it.
    # TODO remove this when everyone has updated to this version.
    if tfvars_key in vconfig.kv:
        logging.error(
            f'''Found another deployment with vars {vconfig.kv[tfvars_key]}.
Please run `vtools deploy cluster --provider [gcp, aws] --destroy true`
before deploying again.''')
        return
    deploy_key = _get_deployment_key(provider, module)
    if deploy_key in vconfig.kv:
        logging.error(
            f'''Found another deployment for module {module} in provider
{provider}, with vars {vconfig.kv[deploy_key]}. Please run
`vtools deploy cluster --provider {provider} --destroy true` before deploying
again.''')
        return
    terraform_vars = _parse_tf_vars(tfvars, provider, module)
    vconfig.kv[deploy_key] = terraform_vars

    _run_terraform_cmd(vconfig, 'apply', provider, module, terraform_vars)


def destroy(vconfig, provider, module):
    deploy_key = _get_deployment_key(provider, module)

    if deploy_key in vconfig.kv:
        _run_terraform_cmd(vconfig, 'destroy', provider, module,
                           vconfig.kv[deploy_key])
        del vconfig.kv[deploy_key]
        return

    logging.info(f'''No deployment found for module {module} in provider
{provider}. Nothing to destroy.''')


def deployment_exists(vconfig, provider, module):
    deploy_key = _get_deployment_key(provider, module)
    return deploy_key in vconfig.kv


def _get_deployment_key(provider, module):
    return f'deploy.{provider}.{module}'


def _run_terraform_cmd(vconfig, action, provider, module, tfvars):
    _run_terraform(vconfig, action, provider, module, tfvars)


def _run_terraform(vconfig, action, provider, module, tf_vars):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', provider,
                              module)
    base_cmd = f'cd {module_dir} && terraform'
    init_cmd = f'{base_cmd} init'
    shell.run_subprocess(init_cmd, env=vconfig.environ)
    cmd = f'{base_cmd} {action} -auto-approve {tf_vars}'
    logging.info(f'Running {cmd}')
    shell.run_subprocess(cmd, env=vconfig.environ)


def _parse_tf_vars(tfvars, provider, module):
    if tfvars is None:
        return ''
    for v in tfvars:
        res = re.match(r'(\w+)=[.\d\S]+', v)
        if res is None:
            logging.fatal(
                f'"{v}" does not match the required "key=value" format')
        key = res.group(1)
        if key not in known_tfvars[provider][module]:
            logging.fatal(
                f'Unrecognized variable "{key}". Allowed vars: {known_tfvars}')
    return ' '.join([f'-var {v}' for v in tfvars])


def get_tf_outputs(vconfig, provider, module):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', provider,
                              module)
    cmd = f'cd {module_dir} && terraform output -json'
    logging.info(f'Running {cmd}')
    out = shell.raw_check_output(cmd, env=vconfig.environ)
    return json.loads(out)
