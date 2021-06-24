# Copyright 2020 Vectorized, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import time
import os
import signal
import tempfile
import shutil
import requests
import threading
import collections

import yaml
from ducktape.services.service import Service
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.utils.util import wait_until
from ducktape.cluster.cluster import ClusterNode
from prometheus_client.parser import text_string_to_metric_families

from rptest.clients.kafka_cat import KafkaCat
from rptest.services.storage import ClusterStorage, NodeStorage
from rptest.services.admin import Admin
from rptest.clients.python_librdkafka import PythonLibrdkafka
from kafka import KafkaAdminClient

Partition = collections.namedtuple('Partition',
                                   ['index', 'leader', 'replicas'])


class RedpandaService(Service):
    PERSISTENT_ROOT = "/var/lib/redpanda"
    DATA_DIR = os.path.join(PERSISTENT_ROOT, "data")
    CONFIG_FILE = "/etc/redpanda/redpanda.yaml"
    STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT, "redpanda.log")
    WASM_STDOUT_STDERR_CAPTURE = os.path.join(PERSISTENT_ROOT,
                                              "wasm_engine.log")
    CLUSTER_NAME = "my_cluster"
    READY_TIMEOUT_SEC = 5

    LOG_LEVEL_KEY = "redpanda_log_level"
    DEFAULT_LOG_LEVEL = "info"

    SUPERUSER_CREDENTIALS = ("admin", "admin", "SCRAM-SHA-256")

    logs = {
        "redpanda_start_stdout_stderr": {
            "path": STDOUT_STDERR_CAPTURE,
            "collect_default": True
        },
        "wasm_engine_start_stdout_stderr": {
            "path": WASM_STDOUT_STDERR_CAPTURE,
            "collect_default": True
        }
    }

    def __init__(self,
                 context,
                 num_brokers,
                 client_type,
                 enable_rp=True,
                 extra_rp_conf=None,
                 enable_pp=False,
                 enable_sr=False,
                 topics=None,
                 num_cores=3):
        super(RedpandaService, self).__init__(context, num_nodes=num_brokers)
        self._context = context
        self._client_type = client_type
        self._enable_rp = enable_rp
        self._extra_rp_conf = extra_rp_conf or dict()
        self._enable_pp = enable_pp
        self._enable_sr = enable_sr
        self._log_level = self._context.globals.get(self.LOG_LEVEL_KEY,
                                                    self.DEFAULT_LOG_LEVEL)
        self._topics = topics or ()
        self._num_cores = num_cores
        self._admin = Admin(self)

        # client is intiialized after service starts
        self._client = None

        self.config_file_lock = threading.Lock()

    def sasl_enabled(self):
        return self._extra_rp_conf and self._extra_rp_conf.get(
            "enable_sasl", False)

    def start(self):
        super(RedpandaService, self).start()

        self._admin.create_user(*self.SUPERUSER_CREDENTIALS)

        self.logger.info("Waiting for all brokers to join cluster")
        expected = set(self.nodes)
        wait_until(lambda: {n
                            for n in self.nodes
                            if self.registered(n)} == expected,
                   timeout_sec=30,
                   backoff_sec=1,
                   err_msg="Cluster membership did not stabilize")

        self.logger.info("Verifying storage is in expected state")
        storage = self.storage()
        for node in storage.nodes:
            assert set(node.ns) == {"redpanda"}
            assert set(node.ns["redpanda"].topics) == {"controller", "kvstore"}

        self._create_initial_topics()

        security_settings = dict()
        if self.sasl_enabled():
            username, password, algorithm = self.SUPERUSER_CREDENTIALS
            security_settings = dict(security_protocol='SASL_PLAINTEXT',
                                     sasl_mechanism=algorithm,
                                     sasl_plain_username=username,
                                     sasl_plain_password=password,
                                     request_timeout_ms=30000,
                                     api_version_auto_timeout_ms=3000)
        self._client = KafkaAdminClient(bootstrap_servers=self.brokers_list(),
                                        **security_settings)

    def _create_initial_topics(self):
        client = self._client_type(self)
        for spec in self._topics:
            self.logger.debug(f"Creating initial topic {spec}")
            client.create_topic(spec)

    def start_node(self, node, override_cfg_params=None):
        """
        Start a single instance of redpanda. This function will not return until
        redpanda appears to have started successfully. If redpanda does not
        start within a timeout period the service will fail to start. Thus this
        function also acts as an implicit test that redpanda starts quickly.
        """
        node.account.mkdirs(RedpandaService.DATA_DIR)
        node.account.mkdirs(os.path.dirname(RedpandaService.CONFIG_FILE))

        self.write_conf_file(node, override_cfg_params)

        if self.coproc_enabled():
            self.start_wasm_engine(node)

        cmd = (f"nohup {self.find_binary('redpanda')}"
               f" --redpanda-cfg {RedpandaService.CONFIG_FILE}"
               f" --default-log-level {self._log_level}"
               f" --logger-log-level=exception=debug:archival=debug "
               f" --kernel-page-cache=true "
               f" --overprovisioned "
               f" --smp {self._num_cores} "
               f" --memory 6G "
               f" --reserve-memory 0M "
               f" >> {RedpandaService.STDOUT_STDERR_CAPTURE} 2>&1 &")

        node.account.ssh(cmd)

        wait_until(
            lambda: Admin.ready(node).get("status") == "ready",
            timeout_sec=RedpandaService.READY_TIMEOUT_SEC,
            err_msg=f"Redpanda service {node.account.hostname} failed to start",
            retry_on_exc=True)

    def coproc_enabled(self):
        coproc = self._extra_rp_conf.get('enable_coproc')
        dev_mode = self._extra_rp_conf.get('developer_mode')
        return coproc is True and dev_mode is True

    def start_wasm_engine(self, node):
        wcmd = (f"nohup {self.find_binary('node')}"
                f" {self.find_wasm_root()}/main.js"
                f" {RedpandaService.CONFIG_FILE} "
                f" >> {RedpandaService.WASM_STDOUT_STDERR_CAPTURE} 2>&1 &")

        self.logger.info(
            f"Starting wasm engine on {node.account} with command: {wcmd}")

        # wait until the wasm engine has finished booting up
        wasm_port = 43189
        conf_value = self._extra_rp_conf.get('coproc_supervisor_server')
        if conf_value is not None:
            wasm_port = conf_value['port']

        with node.account.monitor_log(
                RedpandaService.WASM_STDOUT_STDERR_CAPTURE) as mon:
            node.account.ssh(wcmd)
            mon.wait_until(
                f"Starting redpanda wasm service on port: {wasm_port}",
                timeout_sec=RedpandaService.READY_TIMEOUT_SEC,
                backoff_sec=0.5,
                err_msg=
                f"Wasm engine didn't finish startup in {RedpandaService.READY_TIMEOUT_SEC} seconds",
            )

    def monitor_log(self, node):
        assert node in self.nodes
        return node.account.monitor_log(RedpandaService.STDOUT_STDERR_CAPTURE)

    def find_wasm_root(self):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/opt/wasm"

    def find_binary(self, name):
        rp_install_path_root = self._context.globals.get(
            "rp_install_path_root", None)
        return f"{rp_install_path_root}/bin/{name}"

    def stop_node(self, node):
        pids = self.pids(node)

        for pid in pids:
            node.account.signal(pid, signal.SIGTERM, allow_fail=False)

        timeout_sec = 30
        wait_until(lambda: len(self.pids(node)) == 0,
                   timeout_sec=timeout_sec,
                   err_msg="Redpanda node failed to stop in %d seconds" %
                   timeout_sec)

    def clean_node(self, node):
        node.account.kill_process("redpanda", clean_shutdown=False)
        node.account.remove(f"{RedpandaService.PERSISTENT_ROOT}/*")
        node.account.remove(f"{RedpandaService.CONFIG_FILE}")

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "ps ax | grep -i 'redpanda\|node' | grep -v grep | awk '{print $1}'"
            pid_arr = [
                pid for pid in node.account.ssh_capture(
                    cmd, allow_fail=True, callback=int)
            ]
            return pid_arr
        except (RemoteCommandError, ValueError):
            return []

    def write_conf_file(self, node, override_cfg_params):
        node_info = {self.idx(n): n for n in self.nodes}

        conf = self.render("redpanda.yaml",
                           node=node,
                           data_dir=RedpandaService.DATA_DIR,
                           cluster=RedpandaService.CLUSTER_NAME,
                           nodes=node_info,
                           node_id=self.idx(node),
                           enable_rp=self._enable_rp,
                           enable_pp=self._enable_pp,
                           enable_sr=self._enable_sr,
                           superuser=self.SUPERUSER_CREDENTIALS,
                           sasl_enabled=self.sasl_enabled())

        if self._extra_rp_conf:
            doc = yaml.full_load(conf)
            self.logger.debug(
                "Setting custom Redpanda configuration options: {}".format(
                    self._extra_rp_conf))
            doc["redpanda"].update(self._extra_rp_conf)
            conf = yaml.dump(doc)

        if override_cfg_params:
            doc = yaml.full_load(conf)
            self.logger.debug(
                "Setting custom Redpanda node configuration options: {}".
                format(override_cfg_params))
            doc["redpanda"].update(override_cfg_params)
            conf = yaml.dump(doc)

        self.logger.info("Writing Redpanda config file: {}".format(
            RedpandaService.CONFIG_FILE))
        self.logger.debug(conf)
        node.account.create_file(RedpandaService.CONFIG_FILE, conf)

    def restart_nodes(self, nodes):
        nodes = [nodes] if isinstance(nodes, ClusterNode) else nodes
        for node in nodes:
            self.stop_node(node)
        for node in nodes:
            self.start_node(node)

    def registered(self, node):
        idx = self.idx(node)
        self.logger.debug(
            f"Checking if broker {idx} ({node.name} is registered")
        client = PythonLibrdkafka(self)
        brokers = client.brokers()
        broker = brokers.get(idx, None)
        self.logger.debug(f"Found broker info: {broker}")
        return broker is not None

    def controller(self):
        kc = KafkaCat(self)
        cid = kc.metadata()["controllerid"]
        self.logger.debug("Controller reported with id: {}".format(cid))
        if cid != -1:
            node = self.get_node(cid)
            self.logger.debug("Controller node found: {}".format(node))
            return node

    def node_storage(self, node):
        """
        Retrieve a summary of storage on a node.
        """
        def listdir(path, only_dirs=False):
            ents = node.account.sftp_client.listdir(path)
            if not only_dirs:
                return ents
            paths = map(lambda fn: (fn, os.path.join(path, fn)), ents)
            return [p[0] for p in paths if node.account.isdir(p[1])]

        store = NodeStorage(RedpandaService.DATA_DIR)
        for ns in listdir(store.data_dir, True):
            if ns == '.coprocessor_offset_checkpoints':
                continue
            ns = store.add_namespace(ns, os.path.join(store.data_dir, ns))
            for topic in listdir(ns.path):
                topic = ns.add_topic(topic, os.path.join(ns.path, topic))
                for num in listdir(topic.path):
                    partition = topic.add_partition(
                        num, node, os.path.join(topic.path, num))
                    partition.add_files(listdir(partition.path))
        return store

    def storage(self):
        store = ClusterStorage()
        for node in self.nodes:
            s = self.node_storage(node)
            store.add_node(s)
        return store

    def copy_data(self, dest, node):
        # after copying, move all files up a directory level so the caller does
        # not need to know what the name of the storage directory is.
        with tempfile.TemporaryDirectory() as d:
            node.account.copy_from(RedpandaService.DATA_DIR, d)
            data_dir = os.path.basename(RedpandaService.DATA_DIR)
            data_dir = os.path.join(d, data_dir)
            for fn in os.listdir(data_dir):
                shutil.move(os.path.join(data_dir, fn), dest)

    def data_checksum(self, node):
        """Run command that computes MD5 hash of every file in redpanda data 
        directory. The results of the command are turned into a map from path
        to hash-size tuples."""
        cmd = f"find {RedpandaService.DATA_DIR} -type f -exec md5sum '{{}}' \; -exec stat -c %s '{{}}' \;"
        lines = node.account.ssh_output(cmd)
        tokens = lines.split()
        return {
            tokens[ix + 1].decode(): (tokens[ix].decode(), int(tokens[ix + 2]))
            for ix in range(0, len(tokens), 3)
        }

    def broker_address(self, node):
        assert node in self.nodes
        cfg = self.read_configuration(node)
        return f"{node.account.hostname}:{cfg['redpanda']['kafka_api']['port']}"

    def node_id(self, node):
        assert node in self.nodes
        cfg = self.read_configuration(node)
        return int(cfg['redpanda']['node_id'])

    def brokers(self, limit=None):
        brokers = ",".join(
            map(lambda n: self.broker_address(n), self.nodes[:limit]))
        return brokers

    def brokers_list(self, limit=None):
        return [self.broker_address(n) for n in self.nodes[:limit]]

    def node_id_list(self, limit=None):
        return [self.node_id(n) for n in self.nodes[:limit]]

    def metrics(self, node):
        assert node in self.nodes
        url = f"http://{node.account.hostname}:9644/metrics"
        resp = requests.get(url)
        assert resp.status_code == 200
        return text_string_to_metric_families(resp.text)

    def read_configuration(self, node):
        assert node in self.nodes
        with self.config_file_lock:
            with node.account.open(RedpandaService.CONFIG_FILE) as f:
                return yaml.full_load(f.read())

    def shards(self):
        """
        Fetch the max shard id for each node.
        """
        shards_per_node = {}
        for node in self.nodes:
            num_shards = 0
            metrics = self.metrics(node)
            for family in metrics:
                for sample in family.samples:
                    if sample.name == "vectorized_reactor_utilization":
                        num_shards = max(num_shards,
                                         int(sample.labels["shard"]))
            assert num_shards > 0
            shards_per_node[self.idx(node)] = num_shards
        return shards_per_node

    def describe_topics(self, topics=None):
        """
        Describe topics. Pass topics=None to describe all topics, or a pass a
        list of topic names to restrict the call to a set of specific topics.

        Sample return value:
            [
              {'error_code': 0,
               'topic': 'topic-kabn',
               'is_internal': False,
               'partitions': [
                 {'error_code': 0,
                  'partition': 0,
                  'leader': 1,
                  'replicas': [1],
                  'isr': [1],
                  'offline_replicas': []}
               }
            ]
        """
        return self._client.describe_topics(topics)

    def partitions(self, topic):
        """
        Return partition metadata for the topic.
        """
        kc = KafkaCat(self)
        md = kc.metadata()
        topic = next(filter(lambda t: t["topic"] == topic, md["topics"]))

        def make_partition(p):
            index = p["partition"]
            leader_id = p["leader"]
            leader = None if leader_id == -1 else self.get_node(leader_id)
            replicas = [self.get_node(r["id"]) for r in p["replicas"]]
            return Partition(index, leader, replicas)

        return [make_partition(p) for p in topic["partitions"]]

    def create_topic(self, spec):
        client = self._client_type(self)
        self.logger.debug(f"Creating topic {spec}")
        client.create_topic(spec)
