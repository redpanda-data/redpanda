import psutil
import re
import os
import subprocess
import shutil
import vtools.vlib.shell


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


def _start_single_node(id, cores_per_node, mem_per_node, log_level, vconfig):
    src_cfg_dir = os.path.join(vconfig.src_dir, 'conf', 'local_multi_node')
    exe = os.path.join(vconfig.build_root, 'go', 'bin', 'rpk')
    install_dir = os.path.join(vconfig.build_dir)
    cluster_dir = os.path.join(vconfig.build_root, 'cluster')
    cfg_dir = os.path.join(cluster_dir, 'cfg')
    os.makedirs(cfg_dir, exist_ok=True)
    cfg_file = f'{cfg_dir}/n{id}.yaml'

    # copy config file if doesn't exists
    if not os.path.exists(cfg_file):
        shutil.copyfile(f'{src_cfg_dir}/n{id}.yaml', cfg_file)

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
    subprocess.Popen([
        exe, 'config', 'set', 'rpk.additional_start_flags', '--format', 'json',
        '--config', cfg_file, f'[{flags_str}]'
    ]).wait()

    # start redpanda
    cmd = [exe, 'start', '--config', cfg_file, '--install-dir', install_dir]

    with open(f'{log_dir}/n{id}.log', "wb") as out:
        subprocess.Popen(cmd, stdout=out, stderr=out)