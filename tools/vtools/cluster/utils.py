import psutil
import re
import os
import subprocess
import shutil
import vtools.vlib.shell
import vtools.vlib.tls
import json


def _parse_node_id(cmd_line):
    for arg in cmd_line:
        match = re.search('.*n(\d)\.yaml', arg)
        if match:
            return match.group(1)


def _get_running_nodes():
    running = {}
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'create_time']):
        if proc.status() == psutil.STATUS_ZOMBIE:
            continue
        if proc.info['name'] == 'redpanda':
            running[_parse_node_id(proc.info['cmdline'])] = proc
    return running


def _get_data_dir(id, vconfig):
    return os.path.join(vconfig.build_root, 'cluster', 'data', f'n{id}')


def _get_secrets_dir(id, vconfig):
    return os.path.join(vconfig.build_root, 'cluster', 'secrets', f'n{id}')


def _get_common_secrets_dir(vconfig):
    return os.path.join(vconfig.build_root, 'cluster', 'secrets')


def _create_common_secrets(secrets_provider, vconfig):
    common_secrets_dir = _get_common_secrets_dir(vconfig)
    os.makedirs(common_secrets_dir, exist_ok=True)
    # store truststore (CA cert)
    with open(os.path.join(common_secrets_dir, 'ca_cert.pem'), 'wb') as f:
        f.write(secrets_provider.truststore_pem())
    # create client certificate
    [private_key, public_key,
     cert] = secrets_provider.generate_secrets(cn='localhost', ip='127.0.0.1')

    #store client cert/key in PEM format
    [key_bytes, cert_bytes] = secrets_provider.encode_as_pem(private_key, cert)

    with open(os.path.join(common_secrets_dir, 'client_cert.pem'), 'wb') as f:
        f.write(cert_bytes)
    with open(os.path.join(common_secrets_dir, 'client_key.pem'), 'wb') as f:
        f.write(key_bytes)

    # write java keystores for use with java clients
    with open(os.path.join(common_secrets_dir, 'truststore.jks'),
              mode='wb') as f:
        f.write(secrets_provider.truststore_jks('hungry_panda_cub'))

    with open(os.path.join(common_secrets_dir, 'keystore.jks'),
              mode='wb') as f:
        f.write(
            secrets_provider.keystore_jks(cert, private_key,
                                          'hungry_panda_cub'))


class Rpk:
    def __init__(self, exe, cfg_file):
        self._exe = exe
        self._cfg_file = cfg_file

    def set_config_json(self, root, cfg):
        return subprocess.check_output([
            self._exe, 'config', 'set', root, '--format', 'json', '--config',
            self._cfg_file,
            json.dumps(cfg)
        ])

    def set_config(self, root, value):
        return subprocess.check_output([
            self._exe, 'config', 'set', root, '--config', self._cfg_file, value
        ])

    def start_redpanda(self, install_dir, log_file):
        # start redpanda
        cmd = [
            self._exe, 'start', '--config', self._cfg_file, '--install-dir',
            install_dir
        ]

        with open(log_file, "wb") as out:
            subprocess.Popen(cmd, stdout=out, stderr=out)


def _start_single_node(id, cores_per_node, mem_per_node, log_level, vconfig,
                       secrets_provider):
    src_cfg_dir = os.path.join(vconfig.src_dir, 'conf', 'local_multi_node')
    exe = os.path.join(vconfig.build_root, 'go', 'bin', 'rpk')
    install_dir = os.path.join(vconfig.build_dir)
    cluster_dir = os.path.join(vconfig.build_root, 'cluster')
    cfg_dir = os.path.join(cluster_dir, 'cfg')
    secrets_dir = _get_secrets_dir(id, vconfig)
    data_dir = _get_data_dir(id, vconfig)
    os.makedirs(cfg_dir, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    cfg_file = f'{cfg_dir}/n{id}.yaml'
    # copy config file if doesn't exists
    if not os.path.exists(cfg_file):
        shutil.copyfile(f'{src_cfg_dir}/n{id}.yaml', cfg_file)
    rpk = Rpk(exe, cfg_file)

    if secrets_provider is not None:
        os.makedirs(secrets_dir, exist_ok=True)
        # create node cert/key
        [private_key, public_key,
         cert] = secrets_provider.generate_secrets(cn='localhost',
                                                   ip='127.0.0.1')
        [key_bytes,
         cert_bytes] = secrets_provider.encode_as_pem(private_key, cert)
        # store PEM format key and file
        cert_file = os.path.join(secrets_dir, 'cert.pem')
        key_file = os.path.join(secrets_dir, 'key.pem')
        ca_file = os.path.join(_get_common_secrets_dir(vconfig), 'ca_cert.pem')
        with open(cert_file, 'wb') as f:
            f.write(cert_bytes)
        with open(key_file, 'wb') as f:
            f.write(key_bytes)

        # set kafka tls configuration
        tls_cfg = {
            'cert_file': cert_file,
            'enabled': True,
            'key_file': key_file,
            'require_client_auth': True,
            'truststore_file': ca_file
        }

        cfg = {'kafka_api_tls': tls_cfg}
        rpk.set_config_json('redpanda', cfg)

        cfg = {'rpc_server_tls': tls_cfg}
        rpk.set_config_json('redpanda', cfg)

        admin_api_tls = {
            'cert_file': cert_file,
            'enabled': True,
            'key_file': key_file,
            'require_client_auth': False
        }

        cfg = {'admin_api_tls': admin_api_tls}
        rpk.set_config_json('redpanda', cfg)
    else:
        tls_cfg = {'enabled': False}

        cfg = {'kafka_api_tls': tls_cfg}
        rpk.set_config_json('redpanda', cfg)
        cfg = {'rpc_server_tls': tls_cfg}
        rpk.set_config_json('redpanda', cfg)
        cfg = {'admin_api_tls': tls_cfg}
        rpk.set_config_json('redpanda', cfg)

    log_dir = os.path.join(cluster_dir, 'logs')
    os.makedirs(log_dir, exist_ok=True)

    # configure node
    start_core = (int(id) - 1) * cores_per_node
    end_core = int(id) * cores_per_node - 1
    flags = [
        f'default-log-level={log_level}', f'smp={cores_per_node}',
        f'cpuset={start_core}-{end_core}', f'memory={mem_per_node}'
    ]
    flags_str = ', '.join(f'"{f}"' for f in flags)

    # set node config
    rpk.set_config_json('rpk.additional_start_flags', flags)
    rpk.set_config('redpanda.data_directory', data_dir)
    # start redpanda
    rpk.start_redpanda(install_dir, f'{log_dir}/n{id}.log')
