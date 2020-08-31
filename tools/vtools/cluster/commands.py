import click
import os
import json
import requests
import psutil
import time
import shutil
import pathlib
import random

from ..vlib import tls
from ..vlib import config

from absl import logging

from . import utils


@click.group(short_help='local cluster developer tool')
def cluster():
    pass


@cluster.command(short_help='Start local cluster nodes')
@click.option('--log-level', help=('Cluster nodes log level'), default='info')
@click.option('--cores-per-node',
              help=('Cores used by single cluster node'),
              default=1)
@click.option('--mem-per-node',
              help=('Memory used by single cluster node'),
              default='6G')
@click.option('--build-type', help=('Redpanda binary source'), default='')
@click.option('--clang',
              help='Build clang and install in <build-root>/llvm/llvm-bin.',
              is_flag=True)
@click.option('--secured', help='Enable TLS ', is_flag=True)
@click.option('--nodes',
              multiple=True,
              help=('Nodes to start'),
              default=['1', '2', '3'])
def start(log_level, cores_per_node, mem_per_node, build_type, clang, secured,
          nodes):
    vconfig = config.VConfig(build_type=build_type, clang=clang)
    running = utils._get_running_nodes()
    secrets_provider = None
    if secured:
        secrets_provider = tls.SecretsProvider(logging)
        utils._create_common_secrets(secrets_provider, vconfig)
    for id in nodes:
        if id in running:
            logging.info(f"node {id} is already running")
            return

        logging.info(f"starting node local redpanda - {id}")
        utils._start_single_node(id=id,
                                 log_level=log_level,
                                 cores_per_node=cores_per_node,
                                 mem_per_node=mem_per_node,
                                 vconfig=vconfig,
                                 secrets_provider=secrets_provider)


@cluster.command(short_help='Stop local cluster')
@click.option('--nodes',
              multiple=True,
              help=('Nodes to stop'),
              default=['1', '2', '3'])
@click.option('--kill',
              help='If set will send SIGKILL to redpanda processes',
              is_flag=True)
def stop(nodes, kill):
    running = utils._get_running_nodes()
    for id in nodes:
        if id in running:
            proc = running[id]
            pid = proc.info['pid']
            logging.info(f"stopping node {id} with pid {pid}")
            if kill:
                proc.kill()
            else:
                proc.terminate()
        else:
            logging.info(f'node {id} is aleready stopped')


@cluster.command(short_help='Display cluster status')
def status():
    running = utils._get_running_nodes()
    for [id, proc] in running.items():
        uptime = int(time.time() - proc.info['create_time'])
        pid = proc.info['pid']
        logging.info(f'node: {id}, pid: {pid}, uptime: {uptime} seconds')


@cluster.command(short_help='Clear cluster nodes data')
@click.option('--nodes',
              multiple=True,
              help=('Nodes to stop'),
              default=['1', '2', '3'])
def clear(nodes):
    vconfig = config.VConfig()
    for id in nodes:
        logging.info(f'removing node {id} data')
        data_dir = utils._get_data_dir(id, vconfig)
        shutil.rmtree(data_dir, ignore_errors=True)


@cluster.command(short_help='Simulate local cluster crashes')
@click.option('--log-level', help=('Cluster nodes log level'), default='info')
@click.option('--cores-per-node',
              help=('Cores used by single cluster node'),
              default=1)
@click.option('--mem-per-node',
              help=('Memory used by single cluster node'),
              default='6G')
@click.option('--build-type', help=('Redpanda binary source'), default='')
@click.option('--clang',
              help='Build clang and install in <build-root>/llvm/llvm-bin.',
              is_flag=True)
@click.option('--step-interval',
              help=('Time between punisher actions in seconds'),
              default=5)
@click.option('--allow-minority',
              help=('If set punisher will kill up to n-1 nodes otherwise'
                    ' it will always leave majority up and running'),
              is_flag=True)
def punisher(log_level, cores_per_node, mem_per_node, build_type, clang,
             step_interval, allow_minority):
    # for now we support 3 nodes local cluster
    number_of_nodes = 3
    cluster_nodes = [str(i) for i in range(1, number_of_nodes + 1)]
    majority = number_of_nodes / 2 + 1
    running = utils._get_running_nodes()
    vconfig = config.VConfig(build_type=build_type, clang=clang)

    while True:
        time.sleep(step_interval)
        running = utils._get_running_nodes()
        down = list(set(cluster_nodes) - set(running.keys()))
        up = list(running.keys())
        logging.info(f"up: {up}, stopped: {down}")

        actions = ['nop']
        if len(running) > majority or (allow_minority and len(running) > 1):
            actions.append('stop_one')
            actions.append('kill_one')
        if len(down) > 0:
            actions.append('start_one')
        next_action = random.choice(actions)

        # execute actions

        if next_action == 'stop_one':
            id = random.choice(up)
            logging.info(f'stopping node {id}')
            running[id].terminate()
            continue
        if next_action == 'kill_one':
            id = random.choice(up)
            logging.info(f'killing node {id}')
            running[id].kill()
            continue
        if next_action == 'start_one':
            id = random.choice(down)
            logging.info(f'starting node {id}')
            utils._start_single_node(id=id,
                                     log_level=log_level,
                                     cores_per_node=cores_per_node,
                                     mem_per_node=mem_per_node,
                                     vconfig=vconfig)
            continue
        if next_action == 'nop':
            continue
