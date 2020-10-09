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


class KafkaArgs:
    def __init__(self, ips, topic=None):
        self.ips = ips
        self.topic = topic

    def __str__(self):
        return f'kafka_args: [ips: {self.ips}, topic: {self.topic}]'


class RemoteExecutor:
    def __init__(self, nodes, host_key):
        self.nodes = nodes
        self.clients = {}
        self.host_key = host_key

    def __enter__(self):
        for [id, n] in self.nodes.items():
            self.clients[id] = self._create_node_ssh_client(n)
        return self

    def __exit__(self, type, value, traceback):
        for c in self.clients.values():
            c.close()

    def _create_node_ssh_client(self, node):
        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        k = paramiko.RSAKey.from_private_key_file(self.host_key)
        client.connect(node['ip'], pkey=k, username=node['ansible_user'])
        return client

    def execute_on_node(self, id, func, kafka_args=None):
        client = self.clients[id]
        if not client.get_transport().is_active():
            self.clients[id] = self._create_node_ssh_client(self.nodes[id])
        if kafka_args:
            return func(self.nodes[id], self.clients[id], kafka_args)
        else:
            return func(self.nodes[id], self.clients[id])

    def execute_on_all(self, func):
        results = {}
        for [id, n] in self.nodes.items():
            results[id] = self.execute_on_node(id, func)
        return results


def _is_running(node, client):
    private_ip = node['private_ip']
    stdin, stdout, strerr = client.exec_command(
        f'curl http://{private_ip}:9644/metrics > /dev/null')

    return stdout.channel.recv_exit_status() == 0


def _remote_execute(client, cmd):
    logging.info(f"Executing {cmd}")
    return client.exec_command(cmd, timeout=90)


def _kafka_delete_topic(node, client, kafka_args):
    return _remote_execute(client, ("/opt/kafka-dev/bin/kafka-topics.sh "
                                    f"--bootstrap-server {kafka_args.ips} "
                                    f"--delete --topic {kafka_args.topic}"))


def _kafka_create_topic(node, client, kafka_args):
    cleanup_type = random.choice(["delete", "compact", "delete,compact"])
    return _remote_execute(
        client, ("/opt/kafka-dev/bin/kafka-topics.sh "
                 f"--bootstrap-server {kafka_args.ips} "
                 f"--create --topic {kafka_args.topic} --partitions 36 "
                 "--replication-factor 3 "
                 f"--config cleanup.policy={cleanup_type} "
                 "| systemd-cat -t punisher -p info "))


def _kafka_list_topic(node, client, kafka_args):
    return _remote_execute(client, ("/opt/kafka-dev/bin/kafka-topics.sh "
                                    f"--bootstrap-server {kafka_args.ips} "
                                    "--list"))


def _kafka_produce_topic(node, client, kafka_args):
    compression_type = random.choice(["zstd", "snappy", "none", "gzip", "lz4"])
    return _remote_execute(client,
                           ("/opt/kafka-dev/bin/kafka-run-class.sh "
                            "org.apache.kafka.tools.ProducerPerformance "
                            "--record-size 1024 "
                            f"--topic {kafka_args.topic} "
                            "--num-records 1024 "
                            "--throughput 65535 "
                            "--producer-props acks=1 "
                            "client.id=vectorized.punisher.producer "
                            f"bootstrap.servers={kafka_args.ips} "
                            "batch.size=81960 "
                            f"compression.type={compression_type} "
                            "buffer.memory=1048576"))


def _kafka_consume_topic(node, client, kafka_args):
    # NOTE: The --timeout option is artificially set to 10ms because
    # we do not want to block the consumer, we just want to trigger
    # the lookup api in redpanda. Usually this reads around 2-300 records
    return _remote_execute(client,
                           ("/opt/kafka-dev/bin/kafka-consumer-perf-test.sh "
                            f"--broker-list={kafka_args.ips} "
                            "--fetch-size=1048576 "
                            "--timeout=10 "
                            "--messages=1024 "
                            "--group=vectorized.pusher.consumer "
                            f"--topic={kafka_args.topic} "
                            "--threads 1 "))


def _transient_kill(node, client):
    _remote_execute(
        client, 'cat /var/lib/redpanda/data/pid.lock | xargs sudo kill -9')


def _perm_kill(node, client):
    _transient_kill(node, client)
    _remote_execute(client, 'sudo systemctl stop redpanda')


def _stop(node, client):
    _remote_execute(client, 'sudo systemctl stop redpanda')


def _start(node, client):
    _remote_execute(client, 'sudo systemctl start redpanda')


def _reboot(node, client):
    _remote_execute(client, 'sudo systemctl reboot')


def _get_logs(lines):
    def get(node, client):
        stdin, stdout, strerr = client.exec_command(
            f'journalctl -u redpanda -n {lines}')
        return map(lambda l: l.strip(), stdout)

    return get


class RemoteCluster:
    def __init__(self, executor):
        self.executor = executor
        self.kafka_ips = ",".join(
            map(lambda x: f"{x[1]['private_ip']}:9092",
                self.executor.nodes.items()))
        self.up = set({})
        self.down = set({})
        self.topics = []

    def update_state(self):
        logging.info("Updating cluster state")
        nodes = list(self.executor.nodes.keys())
        _, out, err = self.executor.execute_on_node(
            random.choice(nodes), _kafka_list_topic,
            KafkaArgs(ips=self.kafka_ips))
        lines = out.readlines()
        self.topics.clear()

        for l in lines:
            stripped = l.strip()
            if stripped != "":
                self.topics.append(stripped)

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

    def execute_kafka(self, command, id, kafka_args):
        if command == 'create-topic':
            return self.executor.execute_on_node(id, _kafka_create_topic,
                                                 kafka_args)
        if command == 'delete-topic':
            return self.executor.execute_on_node(id, _kafka_delete_topic,
                                                 kafka_args)
        if command == 'produce':
            return self.executor.execute_on_node(id, _kafka_produce_topic,
                                                 kafka_args)
        if command == 'consume':
            return self.executor.execute_on_node(id, _kafka_consume_topic,
                                                 kafka_args)

    def execute(self, command, id):
        if command == 'transient_kill':
            return self.executor.execute_on_node(id, _transient_kill)
        if command == 'stop':
            return self.executor.execute_on_node(id, _stop)
        if command == 'kill':
            return self.executor.execute_on_node(id, _perm_kill)
        if command == 'start':
            return self.executor.execute_on_node(id, _start)
        if command == 'reboot':
            return self.executor.execute_on_node(id, _reboot)

    def print_status(self):
        logging.info("Topics: {}".format(self.topics))
        for [id, node] in self.executor.nodes.items():
            state = 'started' if id in self.up else 'stopped'
            logging.info(
                "Node {:2d} - ip: {:16s} private_ip: {:16s} - {}".format(
                    id, node['ip'], node['private_ip'], state))


class ClusterStateVerifier():
    def __init__(self, up, down, topics):
        self.up = set(up)
        self.down = set(down)
        self.topics = set(topics)

    def execute(self, command, id):
        if command == 'transient_kill' or command == 'reboot':
            return
        if command == 'stop' or command == 'kill':
            self.down.add(id)
            self.up.discard(id)
        if command == 'start':
            self.down.discard(id)
            self.up.add(id)

    def execute_kafka(self, command, id, kafka_args):
        if command == 'create-topic':
            self.topics.add(kafka_args.topic)
        if command == 'delete-topic':
            self.topics.remove(kafka_args.topic)


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


def _weighted_choice(command_list):
    ranges = []
    prev = 0
    for cmd in command_list:
        # cmd,range tuple
        ranges.append((cmd.name, range(prev, cmd.weight + prev)))
        prev += cmd.weight
    v = random.randint(0, prev - 1)
    # choice from ranges
    for cmd_name, r in ranges:
        if v in r:
            return cmd_name


class PunisherCommand():
    def __init__(self, name, weight):
        self.name = name
        self.weight = weight


def _topic_name():
    return f'punisher-tp-{random.randint(0,10000)}'


def _punisher_loop(cl, allow_minority, sleep_time, failed_log_dir, kill_only):
    cl.update_state()
    verifier = ClusterStateVerifier(cl.up, cl.down, cl.topics)
    with open(os.path.join(failed_log_dir, 'operations.log'), 'w') as op_log:
        while True:
            logging.info(
                f'Cluster state - running: {cl.up}, stopped: {cl.down}')
            cl.print_status()
            stop_commands = [
                PunisherCommand('nop', 3),
                PunisherCommand('transient_kill', 75),
                PunisherCommand('reboot', 2)
            ]

            if not kill_only:
                stop_commands.append(PunisherCommand('stop', 10))
                stop_commands.append(PunisherCommand('kill', 10))

            start_commands = [
                PunisherCommand('nop', 5),
                PunisherCommand('start', 95)
            ]
            kafka_commands = [
                PunisherCommand('produce', 25),
                PunisherCommand('consume', 25),
                PunisherCommand('create-topic', 25),
                PunisherCommand('delete-topic', 25),
            ]
            count = len(cl.executor.nodes)
            majority = (count / 2) + 1
            cmd = 'nop'
            can_stop = len(cl.up) > majority or (allow_minority
                                                 and len(cl.up) > 0)
            # always execute kafka command first
            if len(cl.up) > 0:
                id = random.choice(list(cl.up))
                available_topics = list(
                    filter(lambda tp: tp.startswith("punisher"), cl.topics))

                if len(available_topics) == 0:
                    cmd = 'create-topic'
                else:
                    cmd = _weighted_choice(kafka_commands)

                if cmd == 'create-topic':
                    kafka_args = KafkaArgs(ips=cl.kafka_ips,
                                           topic=_topic_name())
                else:
                    kafka_args = KafkaArgs(
                        ips=cl.kafka_ips,
                        topic=random.choice(available_topics))
                logging.info(f"Executing {cmd} on {id} with args {kafka_args}")
                _, out, err = cl.execute_kafka(cmd, id, kafka_args)
                status = out.channel.recv_exit_status()
                logging.info(
                    f'{cmd} - {kafka_args.topic} - success: {status == 0}')
                _write_op(
                    op_log, cl, id,
                    f'{cmd} - {kafka_args.topic} - success: {status == 0}')
                if status == 0:
                    verifier.execute_kafka(cmd, id, kafka_args)

            if can_stop:
                id = random.choice(list(cl.up))
                cmd = _weighted_choice(stop_commands)
            else:
                id = random.choice(list(cl.down))
                cmd = _weighted_choice(start_commands)

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
            expected = verifier.topics
            have = set(cl.topics)
            should_be_created = expected.difference(have)
            should_be_removed = have.difference(expected)
            if len(should_be_created) > 0:
                logging.error(f'Topics {should_be_created} should exists')
            if len(should_be_created) > 0:
                logging.error(f'Topics {should_be_removed} should not exists')


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
@click.option(
    '--kill-only',
    help=('If set punisher will only execute transient kill operations'),
    is_flag=True,
    default=True)
def start(step_interval, allow_minority, provider, kill_only):
    vconfig = config.VConfig()
    repo_path = os.path.join(vconfig.ansible_tmp_dir, 'hosts.ini')
    log_dir = os.path.join(vconfig.build_root, 'punisher')
    nodes = _parse_nodes_repository(repo_path)
    os.makedirs(log_dir, exist_ok=True)
    ssh_key = f'~/.ssh/vectorized/deployments/{provider}-cluster'
    abs_path = os.path.abspath(os.path.expanduser(ssh_key))
    with RemoteExecutor(nodes, abs_path) as exe:
        _punisher_loop(RemoteCluster(exe), allow_minority, step_interval,
                       log_dir, kill_only)
