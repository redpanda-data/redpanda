import click
import io
import os
import tarfile
import urllib
import configparser
from vtools.vlib import config
import re
import paramiko
from absl import logging
import random
import time
import datetime


@click.group(short_help='simple remote punisher tool')
def punisher():
    pass


def _parse_nodes_repository(path):
    rp_section = False
    nodes = {}
    with open(path, 'r') as f:
        for l in f:
            if "[redpanda]" in l:
                rp_section = True
                continue
            if rp_section:
                if re.match("\[.+\]", l):
                    rp_section = False
                    break
                n = _parse_redpanda_host_line(l)
                nodes[int(n['id'])] = n
    return nodes


def _parse_redpanda_host_line(l):
    node = {}
    parts = l.split(' ')

    node['ip'] = parts[0]
    for p in parts[1:]:
        [k, v] = p.split('=')
        node[k] = v.strip()
    return node


class RemoteExecutor:
    def __init__(self, nodes, host_key):
        self.nodes = nodes
        self.kafka_ips = ",".join(
            map(lambda x: f"{x[1]['private_ip']}:9092", self.nodes.items()))
        self.clients = {}
        self.host_key = host_key

    def __enter__(self):
        for [id, n] in self.nodes.items():
            client = paramiko.SSHClient()
            client.load_system_host_keys()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            k = paramiko.RSAKey.from_private_key_file(self.host_key)
            client.connect(n['ip'], pkey=k, username=n['ansible_user'])
            self.clients[id] = client
        return self

    def __exit__(self, type, value, traceback):
        for c in self.clients.values():
            c.close()

    def execute_on_node(self, id, func):
        return func(self.nodes[id], self.clients[id], self.kafka_ips)

    def execute_on_all(self, func):
        results = {}
        for [id, n] in self.nodes.items():
            results[id] = func(n, self.clients[id], self.kafka_ips)
        return results


def _is_running(node, client, kips):
    private_ip = node['private_ip']
    stdin, stdout, strerr = client.exec_command(
        f'curl http://{private_ip}:9644/metrics')

    return stdout.channel.recv_exit_status() == 0


def _remote_execute(client, cmd):
    logging.info(f"Executing {cmd}")
    return client.exec_command(cmd, timeout=3)


def _kafka_create_topic(node, client, kips):
    _remote_execute(client, ("/opt/kafka-dev/bin/kafka-topics.sh "
                             f"--bootstrap-server {kips} "
                             "--create --topic sfo --partitions 36 "
                             "--replication-factor 3 "
                             "| systemd-cat -t punisher -p info "))


def _kafka_produce_topic(node, client, kips):
    _remote_execute(client, ("/opt/kafka-dev/bin/kafka-run-class.sh "
                             "org.apache.kafka.tools.ProducerPerformance "
                             "--record-size 1024 "
                             "--topic sfo "
                             "--num-records 1024 "
                             "--throughput 65535 "
                             "--producer-props acks=1 "
                             "client.id=vectorized.punisher.producer "
                             f"bootstrap.servers={kips} "
                             "batch.size=81960 "
                             "buffer.memory=1048576 "
                             "| systemd-cat -t punisher -p info "))


def _kafka_consume_topic(node, client, kips):
    # NOTE: The --timeout option is artificially set to 10ms because
    # we do not want to block the consumer, we just want to trigger
    # the lookup api in redpanda. Usually this reads around 2-300 records
    _remote_execute(client, ("/opt/kafka-dev/bin/kafka-consumer-perf-test.sh "
                             f"--broker-list={kips} "
                             "--fetch-size=1048576 "
                             "--timeout=10 "
                             "--messages=1024 "
                             "--group=vectorized.pusher.consumer "
                             "--topic=sfo "
                             "--threads 1 "
                             "| systemd-cat -t punisher -p info "))


def _transient_kill(node, client, ips):
    _remote_execute(
        client, 'cat /var/lib/redpanda/data/pid.lock | xargs sudo kill -9')


def _perm_kill(node, client, ips):
    _transient_kill(node, client, ips)
    _remote_execute(client, 'sudo systemctl stop redpanda')


def _stop(node, client, ips):
    _remote_execute(client, 'sudo systemctl stop redpanda')


def _start(node, client, ips):
    _remote_execute(client, 'sudo systemctl start redpanda')


def _get_logs(lines):
    def get(node, client, ips):
        stdin, stdout, strerr = client.exec_command(
            f'journalctl -u redpanda -n {lines}')
        return map(lambda l: l.strip(), stdout)

    return get


class RemoteCluster:
    def __init__(self, executor):
        self.executor = executor
        self.up = set({})
        self.down = set({})

    def update_state(self):
        logging.info("Updating cluster state")
        res = self.executor.execute_on_all(_is_running)
        for [id, r] in res.items():
            if r == True:
                self.up.add(id)
                self.down.discard(id)
            else:
                self.down.add(id)
                self.up.discard(id)

    def get_last_logs(self):
        return self.executor.execute_on_all(_get_logs(50))

    def get_detailed_log(self, id):
        return self.executor.execute_on_node(id, _get_logs(1000))

    def execute(self, command, id):
        if command == 'transient_kill':
            return self.executor.execute_on_node(id, _transient_kill)
        if command == 'stop':
            return self.executor.execute_on_node(id, _stop)
        if command == 'kill':
            return self.executor.execute_on_node(id, _perm_kill)
        if command == 'start':
            return self.executor.execute_on_node(id, _start)
        if command == 'create':
            return self.executor.execute_on_node(id, _kafka_create_topic)
        if command == 'produce':
            return self.executor.execute_on_node(id, _kafka_produce_topic)
        if command == 'consume':
            return self.executor.execute_on_node(id, _kafka_consume_topic)

    def print_status(self):
        for [id, node] in self.executor.nodes.items():
            state = 'started' if id in self.up else 'stopped'
            logging.info(
                "Node {:2d} - ip: {:16s} private_ip: {:16s} - {}".format(
                    id, node['ip'], node['private_ip'], state))


class ClusterStateVerifier():
    def __init__(self, up, down):
        self.up = set(up)
        self.down = set(down)

    def execute(self, command, id):
        if command == 'transient_kill':
            return
        if command == 'stop' or command == 'kill':
            self.down.add(id)
            self.up.discard(id)
        if command == 'start':
            self.down.discard(id)
            self.up.add(id)


def _write_op(f, cl, id, msg):
    ip = cl.executor.nodes[id]['ip']
    now = datetime.datetime.utcnow().strftime('%b %d %H:%M:%S')
    f.write(f'{now} [node: {id} {ip}] {msg}\n')
    f.flush()


def _store_detailed_logs(cl, id, failed_log_dir, name_prefix):
    ip = cl.executor.nodes[id]['ip']
    log = cl.get_detailed_log(id)
    ts = datetime.datetime.utcnow().strftime('%b-%d-%H:%M:%S')
    path = os.path.join(failed_log_dir, f'{name_prefix}-{ts}-node-{ip}.log')
    with open(path, 'w') as f:
        for l in log:
            f.write(f'{l}\n')
    logging.info(f"Node {ip} log stored in {path}")


def _punisher_loop(cl, allow_minority, sleep_time, failed_log_dir):
    cl.update_state()
    verifier = ClusterStateVerifier(cl.up, cl.down)
    with open(os.path.join(failed_log_dir, 'operations.log'), 'w') as op_log:
        # first create the kafka topic
        if len(cl.up) > 0:
            id = random.choice(list(cl.up))
            cl.execute('create', id)
        while True:
            logging.info(
                f'Cluster state - running: {cl.up}, stopped: {cl.down}')
            cl.print_status()
            stop_commands = ['nop', 'transient_kill', 'stop', 'kill']
            start_commands = ['nop', 'start']
            kafka_commands = ['produce', 'consume']
            count = len(cl.executor.nodes)
            majority = (count / 2) + 1
            cmd = 'nop'
            can_stop = len(cl.up) > majority or (allow_minority
                                                 and len(cl.up) > 0)
            # always execute kafka command first
            if len(cl.up) > 0:
                id = random.choice(list(cl.up))
                cl.execute(random.choice(kafka_commands), id)

            if can_stop:
                id = random.choice(list(cl.up))
                cmd = random.choice(stop_commands)
            else:
                id = random.choice(list(cl.down))
                cmd = random.choice(start_commands)

            if cmd != 'nop':
                logging.info(f"Executing {cmd} on {id}")
                _write_op(op_log, cl, id, cmd)
                cl.execute(cmd, id)
                verifier.execute(cmd, id)

            # wait after action
            for i in range(0, sleep_time):
                print(f'Waiting {sleep_time - i} seconds...', end='\r')
                time.sleep(1)

            cl.update_state()
            # check with verifier
            if verifier.up != cl.up or verifier.down != cl.down:
                logging.error(
                    f'Cluster state error - [expected started: {verifier.up},'
                    f' expected stopped: {verifier.down} , have started: {cl.up},'
                    f' have stopped: {cl.down}')

                crashed = verifier.up.difference(cl.up)
                if len(crashed) > 0:
                    logging.error(
                        f"Nodes {crashed} are expected to be started ")
                    for n in crashed:
                        _write_op(op_log, cl, id, "expected to be started")
                        _store_detailed_logs(cl, n, failed_log_dir,
                                             "start-failed")

                not_stopped = verifier.down.difference(cl.down)
                if len(not_stopped) > 0:
                    logging.error(
                        f"Nodes {not_stopped} are expected to be stopped")
                    for n in crashed:
                        _write_op(op_log, cl, id, "expected to be stopped")
                        _store_detailed_logs(cl, n, failed_log_dir,
                                             "stop-failed")


@punisher.command(short_help='Simulate local cluster crashes')
@click.option('--step-interval',
              help=('Time between punisher actions in seconds'),
              default=5)
@click.option('--allow-minority',
              help=('If set punisher will kill up to n-1 nodes otherwise'
                    ' it will always leave majority up and running'),
              is_flag=True)
@click.option('--provider',
              required=True,
              type=click.Choice(['aws', 'gcp'], case_sensitive=False))
def start(step_interval, allow_minority, provider):
    vconfig = config.VConfig()
    repo_path = os.path.join(vconfig.ansible_tmp_dir, 'hosts.ini')
    log_dir = os.path.join(vconfig.build_root, 'punisher')
    nodes = _parse_nodes_repository(repo_path)
    os.makedirs(log_dir, exist_ok=True)
    ssh_key = f'~/.ssh/vectorized/deployments/{provider}-cluster'
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    with RemoteExecutor(nodes, abs_path) as exe:
        _punisher_loop(RemoteCluster(exe), allow_minority, step_interval,
                       log_dir)
