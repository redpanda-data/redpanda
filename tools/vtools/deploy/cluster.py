import os
import git
from datetime import date
import json
from absl import logging
import paramiko
from ..vlib import rotate_ssh_keys as keys
from ..vlib import shell
from ..vlib import rotate_ssh_keys as keys
from ..vlib import config
from ..vlib import ssh
from . import install_deps as deps

tfvars_key = 'deploy.cluster.tf.vars'


def deploy(vconfig, install_deps, ssh_key, ssh_port, ssh_timeout, ssh_retries,
           log, tfvars):
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
    vconfig.kv[tfvars_key] = tfvars
    module = 'cluster'
    _run_terraform_cmd(vconfig, 'apply', module, install_deps, log, tfvars)
    outputs = _get_tf_outputs(vconfig, module)
    ssh_user = outputs['ssh_user']['value']
    private_ips = outputs['private_ips']['value']
    public_ips = outputs['ip']['value']
    joined_private_ips = ','.join(private_ips)
    for i in range(len(private_ips)):
        ip = private_ips[i]
        pub_ip = public_ips[i]
        cmd = f'''sudo rpk config set seed-nodes --hosts {joined_private_ips} && \
sudo rpk config set kafka-api --ip {ip} --port 9092  && \
sudo rpk config set rpc-server --ip {ip} --port 33145  && \
sudo rpk config set stats-id --organization io.vectorized --cluster-id test  && \
sudo systemctl start redpanda'''
        ssh.add_known_host(pub_ip, ssh_port, ssh_timeout, ssh_retries)
        ssh.run_subprocess(pub_ip, ssh_user, key_path, cmd)


def destroy(vconfig, install_deps, ssh_key, log):
    if tfvars_key not in vconfig.kv:
        logging.info('No cluster deployments found. Nothing to destroy.')
        return
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    comment = _get_ssh_metadata(vconfig)
    key_path, pub_key_path = keys.generate_key(abs_path, comment, '""')
    _run_terraform_cmd(vconfig, 'destroy', 'cluster', install_deps, log,
                       vconfig.kv[tfvars_key])
    del vconfig.kv[tfvars_key]


def _run_terraform_cmd(vconfig, action, module, install_deps, log, tfvars):
    logging.set_verbosity(log)
    _check_deps(vconfig, install_deps)

    terraform_vars = _parse_tf_vars(tfvars)
    _run_terraform(vconfig, action, module, terraform_vars)


def _run_terraform(vconfig, action, module, tf_vars):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', module)
    tf_bin = os.path.join(vconfig.infra_bin_dir, 'terraform')
    base_cmd = f'cd {module_dir} && {tf_bin}'
    init_cmd = f'{base_cmd} init'
    shell.run_subprocess(init_cmd, env=vconfig.environ)
    cmd = f'{base_cmd} {action} -auto-approve {tf_vars}'
    logging.info(f'Running {cmd}')
    shell.run_subprocess(cmd, env=vconfig.environ)


def _parse_tf_vars(tfvars):
    if tfvars == None:
        return ''
    return ' '.join([f'-var {v}' for v in tfvars])


def _get_tf_outputs(vconfig, module):
    module_dir = os.path.join(vconfig.src_dir, 'infra', 'modules', module)
    tf_bin = os.path.join(vconfig.infra_bin_dir, 'terraform')
    cmd = f'cd {module_dir} && {tf_bin} output -json'
    logging.info(f'Running {cmd}')
    out = shell.raw_check_output(cmd, env=vconfig.environ)
    return json.loads(out)


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
